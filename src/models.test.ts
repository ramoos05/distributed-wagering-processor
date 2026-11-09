import { expect, test } from "bun:test";
import { Money } from "./domain";
import { InboxMessage, OutboxMessage, Wallet, WalletLedgerEntry, WagerTransaction, type WagerTransactionState,
  InvalidTransactionStateError, type WagerKind, type WagerStatus } from "./models";
import { WagerTransactionProcessed } from "./events";

const money = (amount: string, currency = "BRL") => Money.from({ amount, currency });
const id = () => crypto.randomUUID();
const scope = { walletId: id(), playerId: id(), providerId: "provider", roundId: "round" };
function referenceState(patch: Partial<WagerTransactionState> = {}): WagerTransactionState {
  return { ...scope, id: id(), externalTransactionId: "bet", idempotencyKey: id(), payloadHash: "hash",
    gameId: "game", kind: "BET", money: money("10"), createdAt: new Date(), status: "PROCESSED", ...patch };
}
function wager(kind: WagerKind = "BET", referenceExternalTransactionId?: string) {
  return WagerTransaction.create({ id: id(), providerId: "test", externalTransactionId: id(),
    idempotencyKey: id(), payloadHash: "hash", walletId: id(), playerId: id(), roundId: "round",
    gameId: "game", kind, money: money(kind === "LOSS" ? "0" : "10"),
    referenceExternalTransactionId, createdAt: new Date() });
}

test("Money: subtração, sinais, comparação e imutabilidade", () => {
  const a = money("10.10");
  expect(a.subtract(money("0.20")).toString()).toBe("9.90 BRL");
  expect(a.negate().isNegative()).toBe(true);
  expect(a.isPositive()).toBe(true);
  expect(Money.zero("BRL").isZero()).toBe(true);
  expect(money("1").isLessThan(a)).toBe(true);
  expect(a.equals(money("10.1"))).toBe(true);
  expect(a.equals(money("10.1", "USD"))).toBe(false);
  expect(() => a.isLessThan(money("10", "USD"))).toThrow("CURRENCY_MISMATCH");
  expect(Object.isFrozen(a)).toBe(true);
  expect(a.toJSON().amount).toBe("10.10");
});

test("Wallet retorna ledger balanceado e incrementa versão só com movimento válido", () => {
  const wallet = Wallet.open({ id: id(), playerId: id(), initialBalance: money("100") });
  expect(wallet.version).toBe(1n);
  const debit = wallet.debit(money("80"), id());
  expect(debit.direction).toBe("DEBIT");
  expect(debit.isBalanced()).toBe(true);
  expect(wallet.balance.toJSON().amount).toBe("20.00");
  expect(wallet.version).toBe(2n);
  expect(() => wallet.debit(money("80"), id())).toThrow("INSUFFICIENT_FUNDS");
  expect(() => wallet.credit(money("1", "USD"), id())).toThrow("CURRENCY_MISMATCH");
  expect(() => wallet.credit(money("0"), id())).toThrow("INVALID_OPERATION_AMOUNT");
  expect(wallet.version).toBe(2n);
  expect(wallet.credit(money("10"), id()).isBalanced()).toBe(true);
  expect(wallet.version).toBe(3n);
});

test("Wallet rejeita overflow e saldo inicial negativo sem mudar estado", () => {
  expect(() => Wallet.open({ id: id(), playerId: id(), initialBalance: money("1").negate() })).toThrow();
  const wallet = Wallet.open({ id: id(), playerId: id(), initialBalance: money("92233720368547758.07") });
  expect(() => wallet.credit(money("0.01"), id())).toThrow("BALANCE_LIMIT_EXCEEDED");
  expect(wallet.version).toBe(1n);
});

test("ledger create verifica aritmética, moeda e protege datas", () => {
  const at = new Date();
  const state = { id: id(), walletId: id(), transactionId: id(), direction: "CREDIT" as const,
    money: money("1"), balanceBefore: money("10"), balanceAfter: money("11"), createdAt: at };
  const entry = WalletLedgerEntry.create(state);
  at.setTime(0);
  entry.createdAt.setTime(0);
  expect(entry.createdAt.getTime()).not.toBe(0);
  expect(Object.isFrozen(entry)).toBe(true);
  expect(() => WalletLedgerEntry.create({ ...state, balanceAfter: money("12") })).toThrow("INVALID_LEDGER_ENTRY");
  expect(() => WalletLedgerEntry.create({ ...state, balanceAfter: money("11", "USD") })).toThrow();
});

test("tipos de operação definem direção, referência obrigatória e LOSS sem ledger", () => {
  const bet = wager();
  expect(bet.ledgerDirectionFor()).toBe("DEBIT");
  expect(wager("WIN").ledgerDirectionFor()).toBe("CREDIT");
  expect(wager("REFUND", "ref").ledgerDirectionFor()).toBe("CREDIT");
  expect(wager("LOSS").affectsBalance()).toBe(false);
  expect(() => wager("LOSS").ledgerDirectionFor()).toThrow("NO_LEDGER_EFFECT");
  expect(() => wager("REFUND")).toThrow("REFERENCE_REQUIRED");
  expect(() => wager("ROLLBACK")).toThrow("REFERENCE_REQUIRED");
  bet.markProcessed(undefined, new Date());
  expect(wager("ROLLBACK", "ref").ledgerDirectionFor(bet)).toBe("CREDIT");
  for (const kind of ["WIN", "REFUND"] as const) {
    const ref = wager(kind, "ref");
    ref.markProcessed(kind === "REFUND" ? id() : undefined, new Date());
    expect(wager("ROLLBACK", "ref").ledgerDirectionFor(ref)).toBe("DEBIT");
  }
  expect(() => wager("ROLLBACK", "ref").ledgerDirectionFor(wager())).toThrow("INVALID_REFERENCE");
});

test("domínio decide referência pendente, expirada, inválida, divergente e já revertida", () => {
  const reversal = WagerTransaction.create({ ...referenceState(), kind: "REFUND", referenceExternalTransactionId: "bet" });
  const reference = WagerTransaction.rehydrate(referenceState());
  expect(reversal.referenceDecision(undefined, false, 0)).toEqual({ status: "WAIT" });
  expect(reversal.referenceDecision(undefined, false, 8)).toEqual({ status: "REJECT", code: "REFERENCE_EXPIRED" });
  expect(reversal.referenceDecision(reference, false, 0)).toEqual({ status: "READY" });
  expect(reversal.referenceDecision(reference, true, 0)).toEqual({ status: "REJECT", code: "ALREADY_REVERSED" });
  for (const patch of [{ walletId: id() }, { playerId: id() }, { providerId: "other" },
    { roundId: "other" }, { money: money("10", "USD") }]) {
    expect(reversal.referenceDecision(WagerTransaction.rehydrate(referenceState(patch)), false, 0))
      .toEqual({ status: "REJECT", code: "REFERENCE_SCOPE_MISMATCH" });
  }
  expect(reversal.referenceDecision(WagerTransaction.rehydrate(referenceState({ money: money("9") })), false, 0))
    .toEqual({ status: "REJECT", code: "REFERENCE_AMOUNT_MISMATCH" });
  for (const patch of [{ status: "REJECTED" as const }, { status: "FAILED" as const }, { kind: "WIN" as const }]) {
    expect(reversal.referenceDecision(WagerTransaction.rehydrate(referenceState(patch)), false, 0))
      .toEqual({ status: "REJECT", code: "INVALID_REFERENCE" });
  }
});

test("domínio calcula efeitos e distingue insuficiência de aposta e reversão", () => {
  expect(wager().financialEffect(money("5")).failureCode).toBe("INSUFFICIENT_FUNDS");
  expect(wager("WIN").financialEffect(money("5")).after.toJSON().amount).toBe("15.00");
  expect(wager("LOSS").financialEffect(money("5")).delta.isZero()).toBe(true);
  const win = wager("WIN"); win.markProcessed(undefined, new Date());
  expect(wager("ROLLBACK", "win").financialEffect(money("5"), win).failureCode).toBe("REVERSAL_INSUFFICIENT_FUNDS");
  expect(wager("WIN").financialEffect(money("92233720368547758.07")).failureCode).toBe("BALANCE_LIMIT_EXCEEDED");
});

for (const status of ["PROCESSED", "REJECTED", "FAILED"] as const) {
  test(status + " protege todas as transições terminais", () => {
    const transaction = wager();
    transaction.markPendingReference();
    if (status === "PROCESSED") transaction.markProcessed(undefined, new Date());
    if (status === "REJECTED") transaction.reject("INSUFFICIENT_FUNDS");
    if (status === "FAILED") transaction.fail("DELIVERY_EXHAUSTED");
    expect(transaction.status).toBe(status);
    for (const transition of [() => transaction.markProcessed(undefined, new Date()),
      () => transaction.markPendingReference(), () => transaction.reject("code"), () => transaction.fail("code")]) {
      expect(transition).toThrow(InvalidTransactionStateError);
    }
    expect(transaction.matchesPayload("hash")).toBe(true);
    expect(transaction.matchesPayload("other")).toBe(false);
  });
}

test("rehydrate reconstrói estado persistido sem executar validações de criação", () => {
  const transaction = WagerTransaction.rehydrate({ id: id(), providerId: "test", externalTransactionId: id(),
    idempotencyKey: id(), payloadHash: "hash", walletId: id(), playerId: id(), roundId: "r", gameId: "g",
    kind: "REFUND", money: money("0"), status: "FAILED" as WagerStatus, createdAt: new Date() });
  expect(transaction.status).toBe("FAILED");
  expect(transaction.isTerminal()).toBe(true);
});

test("inbox é processada uma única vez", () => {
  const inbox = InboxMessage.receive({ messageId: id(), consumerName: "wager", payloadHash: "hash" });
  expect(inbox.isProcessed()).toBe(false);
  inbox.markProcessed(new Date());
  expect(inbox.isProcessed()).toBe(true);
  expect(() => inbox.markProcessed(new Date())).toThrow(InvalidTransactionStateError);
  expect(InboxMessage.receive({ messageId: id(), consumerName: "wager", payloadHash: "a".repeat(265) }).isProcessed()).toBe(false);
});

test("outbox calcula backoff limitado e publicação é terminal", () => {
  const message = OutboxMessage.enqueue(WagerTransactionProcessed.from(id(), id(), { status: "PROCESSED" }));
  const now = new Date();
  for (let attempt = 0; attempt < 12; attempt++) {
    message.scheduleRetry(now);
    expect(message.nextAttemptAt!.getTime() - now.getTime()).toBe(1000 * 2 ** Math.min(attempt, 8));
    expect(message.attempts).toBe(attempt + 1);
  }
  expect(message.isDue(now)).toBe(false);
  expect(message.isDue(new Date(now.getTime() + 256000))).toBe(true);
  const payload = message.payload;
  (payload as Record<string, unknown>).eventId = "mutated";
  expect(message.payload.eventId).toBe(message.id);
  message.markPublished(now);
  expect(message.isPending()).toBe(false);
  expect(() => message.scheduleRetry(now)).toThrow(InvalidTransactionStateError);
  expect(() => message.markPublished(now)).toThrow(InvalidTransactionStateError);
});
