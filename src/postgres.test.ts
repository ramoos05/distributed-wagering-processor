import { beforeAll, afterAll, expect, test } from "bun:test";
import { DataSource } from "typeorm";
import { db } from "./migrate";
import { Money, parseWager } from "./domain";
import { auditDeliveryFailure, openWallet, reconcile, retryReferences, submit } from "./processor";
import { consumeIntegrationEvent } from "./event-consumer";
import { WagerTransactionProcessed } from "./events";

const enabled = process.env.TEST_POSTGRES === "1";
if (enabled) {
  beforeAll(async () => { await db.initialize(); await db.runMigrations(); });
  afterAll(async () => { if (db.isInitialized) await db.destroy(); });
}
const scenario = test.skipIf(!enabled);

scenario("evento republicado tem um efeito persistente entre processos e após reinício", async () => {
  const table = "test_effect_" + crypto.randomUUID().replaceAll("-", "");
  const event = WagerTransactionProcessed.from(crypto.randomUUID(), crypto.randomUUID(), { status: "PROCESSED" }).toJSON();
  await db.query(`CREATE TABLE ${table}(event_id uuid PRIMARY KEY,applications integer NOT NULL)`);
  const execute = () => Bun.spawn([process.execPath, "src/support/worker.ts", "consume-event"], {
    env: { ...process.env, TEST_EFFECT_TABLE: table, TEST_EVENT: JSON.stringify(event) }, stdout: "ignore", stderr: "inherit" });
  try {
    expect(await execute().exited).toBe(0);
    await db.query("UPDATE inbox SET processed_at=now()-interval '1 day' WHERE consumer='test-event-consumer' AND message_id=$1", [event.eventId]);
    const results = await Promise.all(Array.from({ length: 6 }, () => execute().exited));
    expect(results.every(code => code === 0)).toBe(true);
    expect(await execute().exited).toBe(0);
    const [effect] = await db.query(`SELECT applications FROM ${table} WHERE event_id=$1`, [event.eventId]);
    expect(effect.applications).toBe(1);
    await expect(consumeIntegrationEvent({ ...event, data: { status: "DIFFERENT" } }, "test-event-consumer", async () => {}))
      .rejects.toThrow("EVENT_PAYLOAD_CONFLICT");
  } finally { await db.query(`DROP TABLE ${table}`); }
}, 20000);

scenario("falha no consumidor de eventos desfaz inbox e efeito downstream juntos", async () => {
  const table = "test_effect_" + crypto.randomUUID().replaceAll("-", "");
  const event = WagerTransactionProcessed.from(crypto.randomUUID(), crypto.randomUUID(), { status: "PROCESSED" }).toJSON();
  await db.query(`CREATE TABLE ${table}(event_id uuid PRIMARY KEY,applications integer NOT NULL)`);
  const apply = async (tx: import("typeorm").EntityManager) => { await tx.query(`INSERT INTO ${table} VALUES($1,1)`, [event.eventId]); };
  try {
    await expect(consumeIntegrationEvent(event, "test-event-consumer", async tx => { await apply(tx); throw new Error("effect failed"); }))
      .rejects.toThrow("effect failed");
    expect((await db.query(`SELECT * FROM ${table}`))).toHaveLength(0);
    expect((await db.query("SELECT * FROM inbox WHERE consumer='test-event-consumer' AND message_id=$1", [event.eventId]))).toHaveLength(0);
    expect((await consumeIntegrationEvent(event, "test-event-consumer", apply)).applied).toBe(true);
    expect((await consumeIntegrationEvent(event, "test-event-consumer", apply)).applied).toBe(false);
  } finally { await db.query(`DROP TABLE ${table}`); }
});
async function fixture() {
  const playerId = crypto.randomUUID();
  const wallet = await openWallet(playerId, Money.from({ amount: "100", currency: "BRL" }));
  const body = { playerId, walletId: wallet.id, providerId: crypto.randomUUID(),
    roundId: "round", gameId: "game", kind: "BET", money: { amount: "10", currency: "BRL" } };
  function command(patch: object = {}) {
    const id = crypto.randomUUID();
    return parseWager({ ...body, externalTransactionId: id, ...patch }, id);
  }
  return { wallet, command };
}

scenario("constraints PostgreSQL e ledger imutável", async () => {
  const { wallet, command } = await fixture();
  const result = await submit(command());
  await expect(db.query("UPDATE wallet SET balance=-1 WHERE id=$1", [wallet.id])).rejects.toThrow();
  await expect(db.query("UPDATE wager SET status='PENDING' WHERE id=$1", [result.transactionId])).rejects.toThrow();
  await expect(db.query("DELETE FROM wager WHERE id=$1", [result.transactionId])).rejects.toThrow();
  await expect(db.query("UPDATE ledger SET delta=delta WHERE wallet_id=$1", [wallet.id])).rejects.toThrow();
  await expect(db.query("INSERT INTO wallet(id,player_id,currency,balance) SELECT $2,player_id,currency,0 FROM wallet WHERE id=$1", [wallet.id, crypto.randomUUID()])).rejects.toThrow();
  await expect(db.query("INSERT INTO ledger(wallet_id,wager_id,delta,before_balance,after_balance) SELECT wallet_id,wager_id,delta,before_balance,after_balance FROM ledger WHERE wager_id=$1", [result.transactionId])).rejects.toThrow();
  await expect(db.query("INSERT INTO ledger(wallet_id,wager_id,delta,before_balance,after_balance) VALUES($1,$2,1,0,2)",
    [wallet.id, result.transactionId])).rejects.toThrow();
  expect((await reconcile(wallet.id)).consistent).toBe(true);
});

scenario("erro permanente de infraestrutura é auditado FAILED depois do rollback", async () => {
  const { wallet, command } = await fixture();
  const cmd = command();
  const trigger = "test_permanent_" + crypto.randomUUID().replaceAll("-", "");
  await db.query(`CREATE FUNCTION ${trigger}() RETURNS trigger LANGUAGE plpgsql AS
    $$ BEGIN IF NEW.wallet_id='${wallet.id}'::uuid AND NEW.payload->>'eventType'='WagerTransactionProcessed'
      THEN RAISE EXCEPTION 'injected permission error' USING ERRCODE='42501'; END IF; RETURN NEW; END; $$;
    CREATE TRIGGER ${trigger} BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION ${trigger}();`);
  try {
    const result = await submit(cmd);
    expect(result.status).toBe("FAILED");
    expect(result.failureCode).toBe("INFRASTRUCTURE_PERMANENT");
    const [entries] = await db.query("SELECT count(*)::text AS total FROM ledger WHERE wager_id=$1", [result.transactionId]);
    expect(entries.total).toBe("0");
  } finally { await db.query(`DROP TRIGGER ${trigger} ON outbox; DROP FUNCTION ${trigger}()`); }
  expect((await reconcile(wallet.id)).consistent).toBe(true);
  expect((await reconcile(wallet.id)).storedBalance.amount).toBe("100.00");
});

scenario("FAILED auditável não sobrescreve commit financeiro", async () => {
  const { wallet, command } = await fixture();
  const processed = command();
  await submit(processed);
  expect((await auditDeliveryFailure(processed)).status).toBe("PROCESSED");
  const failed = command();
  const result = await auditDeliveryFailure(failed);
  expect(result.status).toBe("FAILED");
  expect((await submit(failed)).status).toBe("FAILED");
  const [event] = await db.query("SELECT count(*)::text AS total FROM outbox WHERE payload->>'eventType'='WagerTransactionFailed' AND payload->>'correlationId'=$1", [result.transactionId]);
  expect(event.total).toBe("1");
  expect((await reconcile(wallet.id)).consistent).toBe(true);
});

scenario("inbox persistente deduplica e replay preserva saldo após nova movimentação", async () => {
  const { wallet, command } = await fixture();
  const cmd = command();
  const messageId = crypto.randomUUID();
  const original = await submit(cmd, messageId);
  await submit(command({ kind: "WIN", money: { amount: "5", currency: "BRL" } }));
  const replay = await submit(cmd, messageId);
  expect(replay.idempotentReplay).toBe(true);
  expect(replay.balance).toEqual(original.balance);
  await expect(submit(command(), messageId)).rejects.toThrow("INBOX_CONFLICT");
  const [inbox] = await db.query("SELECT count(*)::text AS total FROM inbox WHERE message_id=$1 AND consumer='wager'", [messageId]);
  expect(inbox.total).toBe("1");
  expect((await reconcile(wallet.id)).storedBalance.amount).toBe("95.00");
  expect((await reconcile(wallet.id)).consistent).toBe(true);
});

scenario("referência expirada é rejeitada e não altera saldo", async () => {
  const { wallet, command } = await fixture();
  for (const kind of ["REFUND", "ROLLBACK"]) {
  const result = await submit(command({ kind, referenceExternalTransactionId: "absent" }));
  await db.query("UPDATE wager SET attempts=8,next_attempt=now() WHERE id=$1", [result.transactionId]);
  await retryReferences();
  const [row] = await db.query("SELECT status,failure_code FROM wager WHERE id=$1", [result.transactionId]);
  expect(row).toEqual({ status: "REJECTED", failure_code: "REFERENCE_EXPIRED" });
  }
  expect((await reconcile(wallet.id)).storedBalance.amount).toBe("100.00");
  expect((await reconcile(wallet.id)).consistent).toBe(true);
});

scenario("referência exige valor, rodada e carteira corretos; LOSS não gera ledger nem versão", async () => {
  const { wallet, command } = await fixture();
  const bet = command();
  await submit(bet);
  for (const [patch, failure] of [
    [{ money: { amount: "9.00", currency: "BRL" } }, "REFERENCE_AMOUNT_MISMATCH"],
    [{ roundId: "other" }, "REFERENCE_SCOPE_MISMATCH"],
  ] as const) {
    const result = await submit(command({ kind: "REFUND", referenceExternalTransactionId: bet.external, ...patch }));
    expect(result.failureCode).toBe(failure);
  }
  const before = (await db.query("SELECT balance,version FROM wallet WHERE id=$1", [wallet.id]))[0];
  const loss = await submit(command({ kind: "LOSS", money: { amount: "0", currency: "BRL" } }));
  expect(loss.status).toBe("PROCESSED");
  const [entries] = await db.query("SELECT count(*)::text AS total FROM ledger WHERE wager_id=$1", [loss.transactionId]);
  expect(entries.total).toBe("0");
  expect((await db.query("SELECT balance,version FROM wallet WHERE id=$1", [wallet.id]))[0]).toEqual(before);
  const reversal = command({ kind: "REFUND", referenceExternalTransactionId: bet.external });
  const original = await submit(reversal);
  expect(original.status).toBe("PROCESSED");
  const replay = await submit(reversal);
  expect(replay.idempotentReplay).toBe(true);
  expect(replay.balance).toEqual(original.balance);
  expect((await reconcile(wallet.id)).consistent).toBe(true);
});

scenario("falha SQL reverte carteira, wager, ledger e outbox juntos", async () => {
  const { wallet } = await fixture();
  const [before] = await db.query("SELECT (SELECT count(*) FROM outbox WHERE wallet_id=$1)::text AS events, balance::text FROM wallet WHERE id=$1", [wallet.id]);
  await expect(db.transaction(async tx => {
    await tx.query("UPDATE wallet SET balance=0 WHERE id=$1", [wallet.id]);
    await tx.query("INSERT INTO outbox(id,wallet_id,payload) VALUES($1,$2,'{}')", [crypto.randomUUID(), wallet.id]);
    await tx.query("SELECT 1/0");
  })).rejects.toThrow();
  const [after] = await db.query("SELECT (SELECT count(*) FROM outbox WHERE wallet_id=$1)::text AS events, balance::text FROM wallet WHERE id=$1", [wallet.id]);
  expect(after).toEqual(before);
  expect((await reconcile(wallet.id)).consistent).toBe(true);
});

scenario("falha na outbox do processador reverte efeito financeiro, inbox e ledger", async () => {
  const { wallet, command } = await fixture();
  const cmd = command();
  const messageId = crypto.randomUUID();
  const trigger = "test_fail_" + crypto.randomUUID().replaceAll("-", "");
  const snapshot = async () => (await db.query(`SELECT balance::text,version::text,
    (SELECT count(*) FROM wager WHERE wallet_id=$1)::text AS wagers,
    (SELECT count(*) FROM ledger WHERE wallet_id=$1)::text AS entries,
    (SELECT count(*) FROM outbox WHERE wallet_id=$1)::text AS events,
    (SELECT count(*) FROM inbox WHERE message_id=$2)::text AS inbox
    FROM wallet WHERE id=$1`, [wallet.id, messageId]))[0];
  const before = await snapshot();
  // Identifiers and UUID below are generated locally, never supplied by a user.
  await db.query(`CREATE FUNCTION ${trigger}() RETURNS trigger LANGUAGE plpgsql AS
    $$ BEGIN IF NEW.wallet_id='${wallet.id}'::uuid THEN RAISE EXCEPTION 'injected outbox failure'; END IF; RETURN NEW; END; $$;
    CREATE TRIGGER ${trigger} BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION ${trigger}();`);
  try { await expect(submit(cmd, messageId)).rejects.toThrow("injected outbox failure"); }
  finally { await db.query(`DROP TRIGGER ${trigger} ON outbox; DROP FUNCTION ${trigger}()`); }
  expect(await snapshot()).toEqual(before);
  expect((await submit(cmd, messageId)).status).toBe("PROCESSED");
  expect((await reconcile(wallet.id)).consistent).toBe(true);
});

scenario("migrations sobem, revertem e reaplicam em schema isolado", async () => {
  const schema = "test_" + crypto.randomUUID().replaceAll("-", "");
  await db.query(`CREATE SCHEMA ${schema}`);
  const isolated = new DataSource({ ...db.options, schema,
    extra: { options: `-c search_path=${schema}` } } as typeof db.options);
  try {
    await isolated.initialize();
    expect(await isolated.runMigrations()).toHaveLength(3);
    for (let i = 0; i < 3; i++) await isolated.undoLastMigration();
    const [remaining] = await isolated.query("SELECT count(*)::text AS total FROM information_schema.tables WHERE table_schema=$1 AND table_name IN ('wallet','ledger','wager','inbox','outbox')", [schema]);
    expect(remaining.total).toBe("0");
    expect(await isolated.runMigrations()).toHaveLength(3);
  } finally {
    if (isolated.isInitialized) await isolated.destroy();
    await db.query(`DROP SCHEMA ${schema} CASCADE`);
  }
});

scenario("lock de uma carteira permite processar outra", async () => {
  const first = await fixture();
  const second = await fixture();
  const runner = db.createQueryRunner();
  await runner.connect();
  await runner.startTransaction();
  try {
    await runner.query("SELECT id FROM wallet WHERE id=$1 FOR UPDATE", [first.wallet.id]);
    expect((await submit(second.command())).status).toBe("PROCESSED");
  } finally {
    await runner.rollbackTransaction();
    await runner.release();
  }
  expect((await reconcile(first.wallet.id)).consistent).toBe(true);
  expect((await reconcile(second.wallet.id)).consistent).toBe(true);
}, 5000);
