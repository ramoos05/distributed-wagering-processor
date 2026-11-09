// Integration-test subprocess: no HTTP fault endpoint or production env switch.
import { db } from "../migrate";
import { consume, publish, workers } from "../queue";
import { parseWager } from "../domain";
import { submit } from "../processor";
import { consumeIntegrationEvent } from "../event-consumer";

if (import.meta.main) {
  await db.initialize();
  const checkpoint = async () => {
    console.log("TEST_CHECKPOINT");
    await new Promise<void>(() => {});
  };
  try {
    const mode = process.argv[2];
    if (mode === "consume-event") {
      const table = process.env.TEST_EFFECT_TABLE ?? "";
      if (!/^test_effect_[a-f0-9]+$/.test(table)) throw new Error("INVALID_TEST_TABLE");
      const result = await consumeIntegrationEvent(JSON.parse(process.env.TEST_EVENT!), "test-event-consumer", async (tx, event) => {
        await tx.query(`INSERT INTO ${table}(event_id,applications) VALUES($1,1)
          ON CONFLICT(event_id) DO UPDATE SET applications=${table}.applications+1`, [event.eventId]);
      });
      console.log(JSON.stringify(result));
    } else if (mode === "consume-crash") await consume(new AbortController().signal, { afterCommit: checkpoint, visibilitySeconds: 3 });
    else if (mode === "consume-drain") {
      const stop = new AbortController();
      process.once("SIGTERM", () => stop.abort());
      await consume(stop.signal, { afterCommit: async () => {
        console.log("TEST_CHECKPOINT");
        await Bun.sleep(1000);
      } });
    }
    else if (mode === "submit-crash") {
      const payload = JSON.parse(process.env.TEST_COMMAND!);
      await submit(parseWager(payload, payload.idempotencyKey));
      await checkpoint();
    } else if (mode === "publish" || mode === "publish-crash") {
      await publish({ walletId: process.env.TEST_WALLET_ID, afterSend: mode === "publish-crash" ? checkpoint : undefined });
    } else if (mode === "workers") {
      const stop = workers();
      console.log("TEST_CHECKPOINT");
      await new Promise<void>(resolve => {
        process.once("SIGTERM", () => { void stop().then(resolve); });
        process.once("SIGINT", () => { void stop().then(resolve); });
      });
    } else throw new Error("UNKNOWN_TEST_WORKER");
  } finally { await db.destroy(); }
}
