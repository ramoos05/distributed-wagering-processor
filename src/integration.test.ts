import { beforeAll, expect, test } from "bun:test";

const origins = (process.env.TEST_API_URLS ?? "").split(",").filter(Boolean);
async function request(path: string, options?: RequestInit, index = 0) {
  const response = await fetch(origins[index % origins.length] + path, options);
  return { status: response.status, body: await response.json() };
}
function post(body: object, key?: string) {
  return {
    method: "POST",
    headers: { "Content-Type": "application/json", ...(key ? { "Idempotency-Key": key } : {}) },
    body: JSON.stringify(body),
  };
}
async function fixture() {
  const playerId = crypto.randomUUID();
  const { body } = await request("/wallets", post({ playerId, initialBalance: { amount: "100.00", currency: "BRL" } }));
  return {
    playerId, walletId: body.id, providerId: "integration", roundId: crypto.randomUUID(), gameId: "test",
    kind: "BET", money: { amount: "80.00", currency: "BRL" },
  };
}
async function ledger(wallet: string, balance: string) {
  const { body } = await request("/wallets/" + wallet + "/reconciliation", { method: "POST" });
  expect(body.consistent).toBe(true);
  expect(body.storedBalance.amount).toBe(balance);
  return body.checkedEntries;
}
if (origins.length) {
  beforeAll(async () => {
    expect(origins.length).toBeGreaterThanOrEqual(3);
    for (const url of origins) expect((await fetch(url + "/health/live")).status).toBe(200);
    if (process.env.TEST_SQS === "1") {
      for (const url of origins) expect((await fetch(url + "/health/ready")).status).toBe(200);
    }
  });
}

test.skipIf(!origins.length)("métricas expõem atraso da outbox e disponibilidade das gauges da DLQ", async () => {
  const { status, body } = await request("/metrics");
  expect(status).toBe(200);
  expect(body.outboxLagSeconds).toBeGreaterThanOrEqual(0);
  expect(typeof body.dlqMetricsAvailable).toBe("boolean");
  if (process.env.TEST_SQS === "1") expect(body.dlqMetricsAvailable).toBe(true);
  if (body.dlqMetricsAvailable) expect(body.dlqMessages).toBeGreaterThanOrEqual(0);
  else expect(body.dlqMessages).toBeNull();
});
test.skipIf(!origins.length)("50 duplicatas entre três instâncias causam um débito", async () => {
  const body = await fixture();
  const id = crypto.randomUUID();
  const replies = await Promise.all(Array.from({ length: 50 }, (_, i) =>
    request("/wagering/transactions", post({ ...body, externalTransactionId: id }, id), i)));
  expect(replies.every((r) => r.status === 200 && r.body.balance.amount === "20.00")).toBe(true);
  expect(replies.filter((r) => !r.body.idempotentReplay)).toHaveLength(1);
  const conflict = await request("/wagering/transactions", post({ ...body, externalTransactionId: id, gameId: "other" }, id));
  expect(conflict.status).toBe(409);
  expect(await ledger(body.walletId, "20.00")).toBe(2);
}, 15000);

test.skipIf(!origins.length)("apostas distintas disputam 100 BRL sem saldo negativo", async () => {
  const body = await fixture();
  const replies = await Promise.all(Array.from({ length: 2 }, (_, i) => {
    const id = crypto.randomUUID();
    return request("/wagering/transactions", post({ ...body, externalTransactionId: id }, id), i);
  }));
  expect(replies.map((r) => r.status).sort()).toEqual([200, 422]);
  expect(replies.find((r) => r.status === 422)?.body.failureCode).toBe("INSUFFICIENT_FUNDS");
  expect(await ledger(body.walletId, "20.00")).toBe(2);
});

test.skipIf(!origins.length)("12 carteiras recebem operações em paralelo entre três processos", async () => {
  const wallets = await Promise.all(Array.from({ length: 12 }, () => fixture()));
  const replies = await Promise.all(wallets.flatMap((body, walletIndex) =>
    Array.from({ length: 5 }, (_, operation) => {
      const id = crypto.randomUUID();
      return request("/wagering/transactions", post({ ...body, externalTransactionId: id,
        money: { amount: "10.00", currency: "BRL" } }, id), walletIndex + operation);
    })));
  expect(replies).toHaveLength(60);
  expect(replies.every(reply => reply.status === 200)).toBe(true);
  for (const wallet of wallets) expect(await ledger(wallet.walletId, "50.00")).toBe(6);
}, 20000);

test.skipIf(!origins.length)("replay conserva saldo original após WIN e LOSS", async () => {
  const body = await fixture();
  const id = crypto.randomUUID();
  const bet = { ...body, externalTransactionId: id };
  expect((await request("/wagering/transactions", post(bet, id))).body.balance.amount).toBe("20.00");
  for (const [kind, amount] of [["WIN", "30.00"], ["LOSS", "0.00"]]) {
    const key = crypto.randomUUID();
    expect((await request("/wagering/transactions", post({ ...body, kind,
      money: { amount, currency: "BRL" }, externalTransactionId: key }, key))).status).toBe(200);
  }
  const replay = await request("/wagering/transactions", post(bet, id), 2);
  expect(replay.body.idempotentReplay).toBe(true);
  expect(replay.body.balance.amount).toBe("20.00");
  await ledger(body.walletId, "50.00");
});

for (const kind of ["REFUND", "ROLLBACK"]) {
  test.skipIf(!origins.length)(kind + " antes da referência e reversão duplicada", async () => {
    const body = await fixture();
    const reference = crypto.randomUUID();
    const id = crypto.randomUUID();
    const reversal = { ...body, kind, externalTransactionId: id, referenceExternalTransactionId: reference };
    expect((await request("/wagering/transactions", post(reversal, id))).status).toBe(202);
    expect((await request("/wagering/transactions", post({ ...body, externalTransactionId: reference }, reference))).status).toBe(200);
    const deadline = Date.now() + 15000;
    let status = "PENDING_REFERENCE";
    while (Date.now() < deadline && status === "PENDING_REFERENCE") {
      await Bun.sleep(200);
      status = (await request("/providers/integration/wagering/transactions/" + id)).body.status;
    }
    expect(status).toBe("PROCESSED");
    const duplicate = crypto.randomUUID();
    const rejected = await request("/wagering/transactions", post({ ...reversal, externalTransactionId: duplicate }, duplicate));
    expect(rejected.body.failureCode).toBe("ALREADY_REVERSED");
    await ledger(body.walletId, "100.00");
  }, 20000);
}

test.skipIf(!origins.length)("ROLLBACK de WIN não pode criar saldo negativo; moeda conflitante não debita", async () => {
  const body = await fixture();
  const win = crypto.randomUUID();
  expect((await request("/wagering/transactions", post({ ...body, kind: "WIN", externalTransactionId: win }, win))).status).toBe(200);
  const bet = crypto.randomUUID();
  expect((await request("/wagering/transactions", post({ ...body, externalTransactionId: bet,
    money: { amount: "150.00", currency: "BRL" } }, bet))).status).toBe(200);
  const rollback = crypto.randomUUID();
  expect((await request("/wagering/transactions", post({ ...body, kind: "ROLLBACK",
    externalTransactionId: rollback, referenceExternalTransactionId: win }, rollback))).body.failureCode)
    .toBe("REVERSAL_INSUFFICIENT_FUNDS");
  const foreign = crypto.randomUUID();
  expect((await request("/wagering/transactions", post({ ...body, externalTransactionId: foreign,
    money: { amount: "1.00", currency: "USD" } }, foreign))).status).toBe(409);
  await ledger(body.walletId, "30.00");
});
