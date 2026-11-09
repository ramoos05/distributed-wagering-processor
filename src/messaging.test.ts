import { beforeAll, afterAll, expect, test } from "bun:test";
import { DataSource } from "typeorm";
import { SQSClient, CreateQueueCommand, DeleteQueueCommand, GetQueueAttributesCommand,
  SendMessageCommand, ReceiveMessageCommand, DeleteMessageCommand } from "@aws-sdk/client-sqs";
import { db } from "./migrate";
import { Money, parseWager } from "./domain";
import { openWallet, reconcile, submit } from "./processor";
import { WagerTransactionProcessed } from "./events";

const enabled = process.env.TEST_SQS === "1";
const scenario = test.skipIf(!enabled);
const sqs = new SQSClient({ region: "us-east-1", endpoint: process.env.SQS_ENDPOINT ?? "http://localhost:4566",
  credentials: { accessKeyId: "test", secretAccessKey: "test" }, maxAttempts: 1 });
const prefix = "test-" + crypto.randomUUID();
const schema = "sqs_" + crypto.randomUUID().replaceAll("-", "");
const originalOptions = { ...db.options };
const originalSchema = process.env.DATABASE_SCHEMA;
let schemaCreated = false;
const names = { input: prefix + "-input.fifo", output: prefix + "-output.fifo", dlq: prefix + "-dlq.fifo" };
const urls: string[] = [];
let input: string, output: string, dead: string;
let queueModule: typeof import("./queue");
if (enabled) {
  beforeAll(async () => {
    await db.initialize();
    await db.query(`CREATE SCHEMA ${schema}`);
    schemaCreated = true;
    await db.destroy();
    db.setOptions({ schema, extra: { ...originalOptions.extra, options: `-c search_path=${schema}` } } as typeof db.options);
    process.env.DATABASE_SCHEMA = schema;
    await db.initialize(); await db.runMigrations();
    async function create(name: string, extra: Record<string, string> = {}) {
      const result = await sqs.send(new CreateQueueCommand({ QueueName: name, Attributes: { FifoQueue: "true", ...extra } }));
      urls.push(result.QueueUrl!); return result.QueueUrl!;
    }
    dead = await create(names.dlq);
    const attributes = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dead, AttributeNames: ["QueueArn"] }));
    input = await create(names.input, { RedrivePolicy: JSON.stringify({ deadLetterTargetArn: attributes.Attributes!.QueueArn, maxReceiveCount: "5" }) });
    output = await create(names.output);
    process.env.SQS_INPUT_QUEUE = names.input;
    process.env.SQS_OUTPUT_QUEUE = names.output;
    process.env.SQS_DLQ_QUEUE = names.dlq;
    queueModule = await import("./queue");
  }, 30000);
  afterAll(async () => {
    try {
      for (const QueueUrl of urls) await sqs.send(new DeleteQueueCommand({ QueueUrl }));
    } finally {
      if (db.isInitialized) await db.destroy();
      db.setOptions(originalOptions);
      if (originalSchema === undefined) delete process.env.DATABASE_SCHEMA;
      else process.env.DATABASE_SCHEMA = originalSchema;
      if (schemaCreated) {
        const maintenance = new DataSource(originalOptions);
        await maintenance.initialize();
        try { await maintenance.query(`DROP SCHEMA ${schema} CASCADE`); }
        finally { await maintenance.destroy(); }
      }
      sqs.destroy();
    }
  });
}
async function fixture() {
  const playerId = crypto.randomUUID();
  const wallet = await openWallet(playerId, Money.from({ amount: "100", currency: "BRL" }));
  const id = crypto.randomUUID();
  const body = { playerId, walletId: wallet.id, providerId: prefix, roundId: id, gameId: "game", kind: "BET",
    money: { amount: "80.00", currency: "BRL" }, externalTransactionId: id, idempotencyKey: id };
  const envelope = { messageId: id, type: "WagerTransactionRequested", occurredAt: new Date().toISOString(), data: body };
  return { wallet, body, envelope, command: parseWager(body, id) };
}
async function send(envelope: object) {
  await sqs.send(new SendMessageCommand({ QueueUrl: input, MessageBody: JSON.stringify(envelope),
    MessageGroupId: crypto.randomUUID(), MessageDeduplicationId: crypto.randomUUID() }));
}
function child(mode: string, env: Record<string, string> = {}) {
  return Bun.spawn([process.execPath, "src/support/worker.ts", mode], { stdout: "pipe", stderr: "pipe",
    env: { ...process.env, SQS_INPUT_QUEUE: names.input, SQS_OUTPUT_QUEUE: names.output, SQS_DLQ_QUEUE: names.dlq, ...env } });
}
async function checkpoint(proc: ReturnType<typeof child>) {
  const reader = proc.stdout.getReader();
  const timeout = setTimeout(() => proc.kill(9), 10000);
  try {
    let text = "";
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) throw new Error("Worker exited before checkpoint: " + await new Response(proc.stderr).text());
      text += new TextDecoder().decode(chunk.value);
      if (text.includes("TEST_CHECKPOINT")) return;
    }
  } finally { clearTimeout(timeout); reader.releaseLock(); }
}
async function consistent(walletId: string, amount: string) {
  const result = await reconcile(walletId);
  expect(result.consistent).toBe(true);
  expect(result.storedBalance.amount).toBe(amount);
}
async function receiveEvents(ids: string[]) {
  const remaining = new Set(ids);
  const deadline = Date.now() + 12000;
  while (remaining.size && Date.now() < deadline) {
    const batch = await sqs.send(new ReceiveMessageCommand({ QueueUrl: output, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
    for (const message of batch.Messages ?? []) {
      remaining.delete(JSON.parse(message.Body!).eventId);
      await sqs.send(new DeleteMessageCommand({ QueueUrl: output, ReceiptHandle: message.ReceiptHandle }));
    }
  }
  expect([...remaining]).toEqual([]);
}

scenario("worker morto depois do commit e antes do ack: redelivery tem um único débito", async () => {
  const f = await fixture();
  await send(f.envelope);
  const proc = child("consume-crash");
  try { await checkpoint(proc); } finally { proc.kill(9); await proc.exited; }
  await Bun.sleep(3500);
  await queueModule.consume(new AbortController().signal, { visibilitySeconds: 3 });
  const [row] = await db.query("SELECT count(*)::text AS total FROM ledger WHERE wallet_id=$1 AND delta<0", [f.wallet.id]);
  expect(row.total).toBe("1");
  const [inbox] = await db.query("SELECT count(*)::text AS total FROM inbox WHERE consumer='wager' AND message_id=$1", [f.envelope.messageId]);
  expect(inbox.total).toBe("1");
  const replay = await submit(f.command, f.envelope.messageId);
  expect(replay.idempotentReplay).toBe(true);
  expect(replay.balance!.amount).toBe("20.00");
  await consistent(f.wallet.id, "20.00");
}, 25000);

scenario("commit antes da publicação: outra instância recupera a outbox", async () => {
  const f = await fixture();
  const proc = child("submit-crash", { TEST_COMMAND: JSON.stringify(f.body) });
  try { await checkpoint(proc); } finally { proc.kill(9); await proc.exited; }
  const [pending] = await db.query("SELECT count(*)::text AS total FROM outbox WHERE wallet_id=$1 AND published_at IS NULL", [f.wallet.id]);
  expect(Number(pending.total)).toBeGreaterThanOrEqual(4);
  for (let i = 0; i < Number(pending.total); i++) {
    expect(await child("publish", { TEST_WALLET_ID: f.wallet.id }).exited).toBe(0);
  }
  const [remaining] = await db.query("SELECT count(*)::text AS total FROM outbox WHERE wallet_id=$1 AND published_at IS NULL", [f.wallet.id]);
  expect(remaining.total).toBe("0");
  const events = await db.query("SELECT id FROM outbox WHERE wallet_id=$1", [f.wallet.id]);
  await receiveEvents(events.map((event: { id: string }) => event.id));
  await consistent(f.wallet.id, "20.00");
}, 30000);

scenario("dois publishers disputam a mesma linha; crash após envio permite republicação", async () => {
  const f = await fixture();
  // Keep a single candidate without deleting audit records.
  const [row] = await db.query("SELECT id FROM outbox WHERE wallet_id=$1 ORDER BY id LIMIT 1", [f.wallet.id]);
  await db.query("UPDATE outbox SET next_attempt=now()+interval '1 hour' WHERE wallet_id=$1 AND id<>$2", [f.wallet.id, row.id]);
  const proc = child("publish-crash", { TEST_WALLET_ID: f.wallet.id });
  try {
    await checkpoint(proc);
    expect(await child("publish", { TEST_WALLET_ID: f.wallet.id }).exited).toBe(0);
    const [held] = await db.query("SELECT published_at FROM outbox WHERE id=$1", [row.id]);
    expect(held.published_at).toBeNull();
  } finally { proc.kill(9); await proc.exited; }
  expect(await child("publish", { TEST_WALLET_ID: f.wallet.id }).exited).toBe(0);
  const [published] = await db.query("SELECT published_at FROM outbox WHERE id=$1", [row.id]);
  expect(published.published_at).not.toBeNull();
  await receiveEvents([row.id]);
  await consistent(f.wallet.id, "100.00");
}, 25000);

scenario("indisponibilidade SQS persiste retry/backoff da outbox", async () => {
  const f = await fixture();
  await queueModule.publish({ walletId: f.wallet.id, queueName: prefix + "-absent.fifo" });
  const [row] = await db.query("SELECT attempts,EXTRACT(epoch FROM next_attempt-created_at)::float AS delay FROM outbox WHERE wallet_id=$1 AND attempts>0", [f.wallet.id]);
  expect(row.attempts).toBe(1);
  expect(row.delay).toBeGreaterThanOrEqual(1);
  await db.query("UPDATE outbox SET next_attempt=now() WHERE wallet_id=$1", [f.wallet.id]);
  await queueModule.publish({ walletId: f.wallet.id });
  await consistent(f.wallet.id, "100.00");
}, 15000);

scenario("retry com backoff, esgotamento em FAILED e DLQ real", async () => {
  const f = await fixture();
  await send(f.envelope);
  const before = Date.now();
  const execute: typeof submit = async () => { throw new Error("SIMULATED_INFRA_FAILURE"); };
  await queueModule.consume(new AbortController().signal, { execute });
  const immediate = await sqs.send(new ReceiveMessageCommand({ QueueUrl: input, WaitTimeSeconds: 0 }));
  expect(immediate.Messages ?? []).toHaveLength(0);
  // Each consume receives a new receipt and the broker's actual delivery count;
  // the full backoff (2+4+8+16s) remains exercised rather than mocked.
  for (const delay of [2100, 4100, 8100, 16100]) {
    await Bun.sleep(delay);
    await queueModule.consume(new AbortController().signal, { execute });
  }
  expect(Date.now() - before).toBeGreaterThanOrEqual(30000);
  const [row] = await db.query("SELECT status,failure_code FROM wager WHERE idem=$1", [f.command.key]);
  expect(row.status).toBe("FAILED");
  expect(row.failure_code).toBe("DELIVERY_EXHAUSTED");
  const messages = await sqs.send(new ReceiveMessageCommand({ QueueUrl: dead, WaitTimeSeconds: 1 }));
  expect(messages.Messages?.some(m => JSON.parse(m.Body!).messageId === f.envelope.messageId)).toBe(true);
  await consistent(f.wallet.id, "100.00");
}, 55000);

scenario("mensagem inválida vai para DLQ; shutdown drena mensagem e encerra long polling", async () => {
  await send({ messageId: "invalid", type: "Unknown" });
  await queueModule.consume(new AbortController().signal);
  const messages = await sqs.send(new ReceiveMessageCommand({ QueueUrl: dead, WaitTimeSeconds: 1 }));
  expect(messages.Messages?.some(m => JSON.parse(m.Body!).messageId === "invalid")).toBe(true);
  const f = await fixture();
  await send(f.envelope);
  const draining = child("consume-drain");
  try {
    await checkpoint(draining);
    draining.kill("SIGTERM");
    expect(await draining.exited).toBe(0);
  } finally { if (draining.exitCode === null) { draining.kill(9); await draining.exited; } }
  const inputAfterDrain = await sqs.send(new ReceiveMessageCommand({ QueueUrl: input, WaitTimeSeconds: 0 }));
  expect(inputAfterDrain.Messages ?? []).toHaveLength(0);
  await consistent(f.wallet.id, "20.00");
  const proc = child("workers");
  try {
    await checkpoint(proc);
    proc.kill("SIGTERM");
    expect(await proc.exited).toBe(0);
  } finally { if (proc.exitCode === null) { proc.kill(9); await proc.exited; } }
}, 15000);

scenario("redrive automático aparece na métrica da DLQ sem incrementar envio explícito", async () => {
  const queue = await sqs.send(new CreateQueueCommand({ QueueName: prefix + "-redrive.fifo", Attributes: {
    FifoQueue: "true", RedrivePolicy: JSON.stringify({
      deadLetterTargetArn: (await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dead, AttributeNames: ["QueueArn"] }))).Attributes!.QueueArn,
      maxReceiveCount: "2",
    }),
  } }));
  urls.push(queue.QueueUrl!);
  const id = crypto.randomUUID();
  const before = queueModule.metrics().dlq ?? 0;
  await sqs.send(new SendMessageCommand({ QueueUrl: queue.QueueUrl, MessageBody: JSON.stringify({ messageId: id }),
    MessageGroupId: id, MessageDeduplicationId: id }));
  for (let i = 0; i < 3; i++) await sqs.send(new ReceiveMessageCommand({ QueueUrl: queue.QueueUrl, VisibilityTimeout: 0, WaitTimeSeconds: 1 }));
  let found = false;
  const deadline = Date.now() + 10000;
  while (!found && Date.now() < deadline) {
    const result = await sqs.send(new ReceiveMessageCommand({ QueueUrl: dead, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
    found = (result.Messages ?? []).some(message => JSON.parse(message.Body!).messageId === id);
    for (const message of result.Messages ?? []) {
      if (JSON.parse(message.Body!).messageId !== id) await sqs.send(new DeleteMessageCommand({ QueueUrl: dead, ReceiptHandle: message.ReceiptHandle }));
    }
  }
  expect(found).toBe(true);
  const gauges = await queueModule.deadLetterMetrics();
  expect(gauges.dlqMetricsAvailable).toBe(true);
  expect(gauges.dlqMessages!).toBeGreaterThanOrEqual(1);
  expect(queueModule.metrics().dlq ?? 0).toBe(before);
}, 20000);

scenario("duas entregas SQS com dedup IDs diferentes têm um efeito downstream após reinício", async () => {
  const f = await fixture();
  const event = WagerTransactionProcessed.from(f.wallet.id, crypto.randomUUID(), { status: "PROCESSED" }).toJSON();
  const table = "test_effect_" + crypto.randomUUID().replaceAll("-", "");
  await db.query(`CREATE TABLE ${table}(event_id uuid PRIMARY KEY,applications integer NOT NULL)`);
  try {
    let deliveries = 0;
    for (let i = 0; i < 2; i++) {
      await sqs.send(new SendMessageCommand({ QueueUrl: output, MessageBody: JSON.stringify(event),
        MessageGroupId: event.eventId, MessageDeduplicationId: crypto.randomUUID() }));
      const deadline = Date.now() + 10000;
      let received = false;
      while (!received && Date.now() < deadline) {
        const result = await sqs.send(new ReceiveMessageCommand({ QueueUrl: output, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
        for (const message of result.Messages ?? []) {
          if (JSON.parse(message.Body!).eventId === event.eventId) {
            const proc = child("consume-event", { TEST_EFFECT_TABLE: table, TEST_EVENT: message.Body! });
            expect(await proc.exited).toBe(0);
            received = true; deliveries++;
          }
          await sqs.send(new DeleteMessageCommand({ QueueUrl: output, ReceiptHandle: message.ReceiptHandle }));
        }
      }
      expect(received).toBe(true);
    }
    expect(deliveries).toBe(2);
    const [effect] = await db.query(`SELECT applications FROM ${table} WHERE event_id=$1`, [event.eventId]);
    expect(effect.applications).toBe(1);
    await consistent(f.wallet.id, "100.00");
  } finally { await db.query(`DROP TABLE ${table}`); }
}, 30000);
