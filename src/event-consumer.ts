import { createHash } from "node:crypto";
import type { EntityManager } from "typeorm";
import { InvalidInput, requiredText, uuid } from "./domain";
import { db } from "./migrate";
import { count } from "./telemetry";

export interface EventEnvelope {
  eventId: string; eventType: string; aggregateId: string; correlationId: string;
  version: number; occurredAt: string; data: Record<string, unknown>;
}
export class EventPayloadConflict extends Error {}

function canonical(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
  const object = value as Record<string, unknown>;
  return "{" + Object.keys(object).sort().map(key => JSON.stringify(key) + ":" + canonical(object[key])).join(",") + "}";
}

// Reference downstream consumer port: the inbox marker and the actual downstream
// effect share a SQL transaction. It never reapplies a published wallet debit.
export async function consumeIntegrationEvent(value: unknown, consumerName: string,
  apply: (tx: EntityManager, event: Readonly<EventEnvelope>) => Promise<void>) {
  const event = JSON.parse(JSON.stringify(value)) as EventEnvelope;
  if (!event || typeof event !== "object" || !Number.isInteger(event.version) || event.version < 1 ||
    typeof event.occurredAt !== "string" || !Number.isFinite(Date.parse(event.occurredAt)) ||
    !event.data || typeof event.data !== "object" || Array.isArray(event.data)) throw new InvalidInput("INVALID_EVENT");
  event.eventId = uuid(event.eventId); event.aggregateId = uuid(event.aggregateId);
  requiredText(event.eventType); requiredText(event.correlationId);
  const consumer = requiredText(consumerName);
  const hash = createHash("sha256").update(canonical(event)).digest("hex");
  const applied = await db.transaction(async tx => {
    const inserted = await tx.query(`INSERT INTO inbox(consumer,message_id,hash) VALUES($1,$2,$3)
      ON CONFLICT DO NOTHING RETURNING message_id`, [consumer, event.eventId, hash]);
    if (!inserted.length) {
      const [stored] = await tx.query("SELECT hash FROM inbox WHERE consumer=$1 AND message_id=$2", [consumer, event.eventId]);
      if (stored.hash !== hash) throw new EventPayloadConflict("EVENT_PAYLOAD_CONFLICT");
      return false;
    }
    await apply(tx, event);
    return true;
  });
  count(applied ? "integration_events_applied" : "integration_event_duplicates");
  return { eventId: event.eventId, applied };
}
