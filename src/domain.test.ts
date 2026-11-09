import { expect, test } from "bun:test";
import { Money, parseWager } from "./domain";

test("Money calcula centavos sem perda de precisão e usa duas casas", () => {
  const base = Money.from({ amount: "90071992547409.91", currency: "BRL" });
  expect(base.add(Money.from({ amount: "0.01", currency: "BRL" })).toJSON())
    .toEqual({ amount: "90071992547409.92", currency: "BRL" });
  expect(base.toJSON().amount).toBe("90071992547409.91");
  expect(Money.from({ amount: "5.1", currency: "BRL" }).toJSON().amount).toBe("5.10");
});

test("Money rejeita entradas inexatas e conflito de moeda", () => {
  for (const amount of ["", "NaN", "Infinity", "1e2", "-1.00", "1.001", 1, "92233720368547758.08"]) {
    expect(() => Money.from({ amount, currency: "BRL" })).toThrow();
  }
  expect(() => Money.from({ amount: "1", currency: "BRL" })
    .add(Money.from({ amount: "1", currency: "USD" }))).toThrow("CURRENCY_MISMATCH");
});

test("hash é estável e exclui a chave de idempotência", () => {
  const id = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1";
  const body = { playerId: id, walletId: id, providerId: "test", externalTransactionId: "one",
    roundId: "round", gameId: "game", kind: "BET", money: { amount: "1", currency: "BRL" } };
  const a = parseWager(body, "a");
  expect(a.hash).toBe(parseWager({ ...body, money: { amount: "1.00", currency: "BRL" } }, "b").hash);
  expect(a.hash).not.toBe(parseWager({ ...body, roundId: "other" }, "a").hash);
});
