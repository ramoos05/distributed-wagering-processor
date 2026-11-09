import {
  SQSClient, GetQueueUrlCommand, ReceiveMessageCommand, DeleteMessageCommand,
  SendMessageCommand, ChangeMessageVisibilityCommand,
  GetQueueAttributesCommand,
} from "@aws-sdk/client-sqs";
import { db } from "./migrate";
import { InvalidInput, parseWager, requiredText, type Command } from "./domain";
import { auditDeliveryFailure, Conflict, MissingWallet, retryReferences, submit } from "./processor";
import { count, observe, log } from "./telemetry";
import { OutboxMessage } from "./models";
export { count, metrics } from "./telemetry";

const sqs = new SQSClient({
  region: "us-east-1", endpoint: process.env.SQS_ENDPOINT ?? "http://localhost:4566",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});
const queue = async (QueueName: string) => {
  const { QueueUrl } = await sqs.send(new GetQueueUrlCommand({ QueueName }), { abortSignal: AbortSignal.timeout(5000) });
  if (!QueueUrl) throw new Error("QUEUE_NOT_FOUND");
  return QueueUrl;
};
const input = process.env.SQS_INPUT_QUEUE ?? "wager-transactions.fifo";
const output = process.env.SQS_OUTPUT_QUEUE ?? "wager-events.fifo";
const dlq = process.env.SQS_DLQ_QUEUE ?? "wager-transactions-dlq.fifo";
export async function ready() {
  await Promise.all([db.query("SELECT 1"), queue(input), queue(output), queue(dlq)]);
}

// Broker gauges include automatic redrive, which no local send counter observes.
export async function deadLetterMetrics() {
  try {
    const response = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: await queue(dlq),
      AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"] }),
      { abortSignal: AbortSignal.timeout(5000) });
    const a = response.Attributes ?? {};
    const visible = Number(a.ApproximateNumberOfMessages ?? 0);
    const inFlight = Number(a.ApproximateNumberOfMessagesNotVisible ?? 0);
    const delayed = Number(a.ApproximateNumberOfMessagesDelayed ?? 0);
    return { dlqMetricsAvailable: true, dlqVisibleMessages: visible, dlqInFlightMessages: inFlight,
      dlqDelayedMessages: delayed, dlqMessages: visible + inFlight + delayed };
  } catch {
    count("dlq_metrics_errors");
    return { dlqMetricsAvailable: false, dlqVisibleMessages: null, dlqInFlightMessages: null,
      dlqDelayedMessages: null, dlqMessages: null };
  }
}

export async function publish(options: { walletId?: string; queueName?: string; afterSend?: () => Promise<void> } = {}) {
  await db.transaction(async (tx) => {
    const [row] = await tx.query(`SELECT * FROM outbox WHERE published_at IS NULL AND next_attempt<=now()
      AND ($1::uuid IS NULL OR wallet_id=$1)
      ORDER BY next_attempt,id LIMIT 1 FOR UPDATE SKIP LOCKED`, [options.walletId ?? null]);
    if (!row) return;
    const message = OutboxMessage.rehydrate({ id: row.id, aggregateId: row.wallet_id,
      eventType: row.payload.eventType, payload: row.payload, occurredAt: row.created_at,
      attempts: row.attempts, nextAttemptAt: row.next_attempt, publishedAt: row.published_at ?? undefined });
    observe("outbox_publish_age_ms", Date.now() - new Date(row.created_at).getTime());
    try {
      const QueueUrl = await queue(options.queueName ?? output);
      await sqs.send(new SendMessageCommand({
        QueueUrl, MessageBody: JSON.stringify(row.payload),
        MessageGroupId: row.wallet_id, MessageDeduplicationId: row.id,
      }), { abortSignal: AbortSignal.timeout(5000) });
    } catch (error) {
      count("outbox_retries");
      message.scheduleRetry(new Date());
      await tx.query("UPDATE outbox SET attempts=$2,next_attempt=$3 WHERE id=$1", [message.id, message.attempts, message.nextAttemptAt]);
      log("outbox_retry", { eventId: message.id, walletId: message.aggregateId,
        attempts: message.attempts, nextAttemptAt: message.nextAttemptAt, code: (error as { name?: string })?.name });
      return;
    }
    await options.afterSend?.();
    message.markPublished(new Date());
    await tx.query("UPDATE outbox SET published_at=$2 WHERE id=$1", [message.id, message.publishedAt]);
    count("outbox_published");
  });
}

export async function consume(signal: AbortSignal, hooks: {
  afterCommit?: () => Promise<void>; visibilitySeconds?: number;
  execute?: typeof submit;
} = {}) {
  const QueueUrl = await queue(input);
  const response = await sqs.send(new ReceiveMessageCommand({
    QueueUrl, WaitTimeSeconds: 5, VisibilityTimeout: hooks.visibilitySeconds ?? 30, MessageSystemAttributeNames: ["ApproximateReceiveCount"],
  }), { abortSignal: signal });
  for (const message of response.Messages ?? []) {
    const ReceiptHandle = message.ReceiptHandle;
    const started = performance.now();
    let context: Record<string, unknown> = { messageId: message.MessageId };
    let identified: Command | undefined;
    // A heartbeat keeps ownership while SQL or SQS is slow. Failure is safe:
    // another delivery still passes through PostgreSQL idempotency and locks.
    const heartbeat = setInterval(() => {
      void sqs.send(new ChangeMessageVisibilityCommand({ QueueUrl, ReceiptHandle, VisibilityTimeout: hooks.visibilitySeconds ?? 30 }),
        { abortSignal: AbortSignal.timeout(5000) }).catch(() => count("visibility_extension_errors"));
    }, Math.max(500, (hooks.visibilitySeconds ?? 30) * 1000 / 3));
    try {
    try {
      const envelope = JSON.parse(message.Body ?? "");
      if (!envelope || envelope.type !== "WagerTransactionRequested" || typeof envelope.occurredAt !== "string" ||
        !Number.isFinite(Date.parse(envelope.occurredAt))) {
        throw new InvalidInput("INVALID_ENVELOPE");
      }
      const command = parseWager(envelope.data, envelope.data?.idempotencyKey);
      identified = command;
      const messageId = requiredText(envelope.messageId);
      context = { correlationId: messageId, messageId, walletId: command.wallet, providerId: command.provider };
      const result = await (hooks.execute ?? submit)(command, messageId);
      await hooks.afterCommit?.();
      if (result.status === "FAILED") {
        await sqs.send(new SendMessageCommand({ QueueUrl: await queue(dlq), MessageBody: message.Body ?? "{}",
          MessageGroupId: command.wallet, MessageDeduplicationId: message.MessageId }), { abortSignal: AbortSignal.timeout(5000) });
        count("dlq");
      }
      log("wager_handled", { ...context, transactionId: result.transactionId,
        status: result.status, durationMs: performance.now() - started });
    } catch (error) {
      if (error instanceof MissingWallet) {
        count("transactions_rejected");
        log("missing_wallet", context);
      } else if (error instanceof InvalidInput || error instanceof SyntaxError || error instanceof Conflict) {
        await sqs.send(new SendMessageCommand({
          QueueUrl: await queue(dlq), MessageBody: message.Body ?? "{}",
          MessageGroupId: "invalid", MessageDeduplicationId: message.MessageId,
        }), { abortSignal: AbortSignal.timeout(5000) });
        count("dlq");
      } else {
        const attempts = Number(message.Attributes?.ApproximateReceiveCount ?? 1);
        if (attempts >= 5 && identified) {
          // If audit storage is unavailable, leave the message unacknowledged.
          await auditDeliveryFailure(identified);
          await sqs.send(new SendMessageCommand({ QueueUrl: await queue(dlq),
            MessageBody: message.Body ?? "{}", MessageGroupId: identified.wallet,
            MessageDeduplicationId: message.MessageId }), { abortSignal: AbortSignal.timeout(5000) });
          count("dlq");
          await sqs.send(new DeleteMessageCommand({ QueueUrl, ReceiptHandle }), { abortSignal: AbortSignal.timeout(5000) });
          continue;
        }
        count("queue_retries");
        log("queue_retry", { ...context, attempts,
          visibilitySeconds: Math.min(2 ** attempts, 120), code: (error as { code?: string })?.code });
        await sqs.send(new ChangeMessageVisibilityCommand({
          QueueUrl, ReceiptHandle, VisibilityTimeout: Math.min(2 ** attempts, 120),
        }), { abortSignal: AbortSignal.timeout(5000) });
        continue;
      }
    }
    await sqs.send(new DeleteMessageCommand({ QueueUrl, ReceiptHandle }), { abortSignal: AbortSignal.timeout(5000) });
    } finally { clearInterval(heartbeat); observe("queue_handling_ms", performance.now() - started); }
  }
}

export function workers() {
  const stop = new AbortController();
  const loop = async (name: string, run: () => Promise<void>, delay: number) => {
    while (!stop.signal.aborted) {
      try { await run(); }
      catch (error) {
        if (!stop.signal.aborted) log("worker_error", { worker: name,
          code: (error as { code?: string })?.code });
      }
      if (!stop.signal.aborted) await Bun.sleep(delay);
    }
  };
  const tasks = [
    loop("inbox", () => consume(stop.signal), 250),
    loop("outbox", () => publish(), 250),
    loop("references", retryReferences, 1000),
  ];
  return async () => {
    stop.abort();
    await Promise.all(tasks);
  };
}
