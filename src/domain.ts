import { createHash } from "node:crypto";

export class InvalidInput extends Error {}

export class Money {
  private constructor(readonly cents: bigint, readonly currency: string) { Object.freeze(this); }

  static from(input: unknown): Money {
    const { amount, currency } = (input ?? {}) as Record<string, unknown>;
    if (typeof currency !== "string" || !/^[A-Z]{3}$/.test(currency) ||
        typeof amount !== "string" || !/^[0-9]{1,17}(\.[0-9]{1,2})?$/.test(amount)) {
      throw new InvalidInput("INVALID_MONEY");
    }
    const [whole, fraction = ""] = amount.split(".");
    if (BigInt(whole) * 100n + BigInt(fraction.padEnd(2, "0")) > 9223372036854775807n) throw new InvalidInput("MONEY_LIMIT_EXCEEDED");
    return Money.rehydrate(BigInt(whole) * 100n + BigInt(fraction.padEnd(2, "0")), currency);
  }

  static rehydrate(cents: bigint, currency: string) {
    return new Money(cents, currency);
  }

  add(other: Money) {
    if (other.currency !== this.currency) throw new InvalidInput("CURRENCY_MISMATCH");
    return Money.rehydrate(this.cents + other.cents, this.currency);
  }

  static zero(currency: string) { return Money.from({ amount: "0.00", currency }); }
  subtract(other: Money) { return this.add(other.negate()); }
  negate() { return Money.rehydrate(-this.cents, this.currency); }
  isZero() { return this.cents === 0n; }
  isPositive() { return this.cents > 0n; }
  isNegative() { return this.cents < 0n; }
  isLessThan(other: Money) {
    if (this.currency !== other.currency) throw new InvalidInput("CURRENCY_MISMATCH");
    return this.cents < other.cents;
  }
  equals(other: Money) { return this.currency === other.currency && this.cents === other.cents; }
  toString() { return `${this.toJSON().amount} ${this.currency}`; }

  toJSON() {
    const absolute = this.cents < 0n ? -this.cents : this.cents;
    return {
      amount: (this.cents < 0n ? "-" : "") + absolute / 100n + "." + String(absolute % 100n).padStart(2, "0"),
      currency: this.currency,
    };
  }
}

export function requiredText(value: unknown) {
  if (typeof value !== "string" || !value.trim() || value.length > 200) {
    throw new InvalidInput("INVALID_TEXT_FIELD");
  }
  return value;
}

export function uuid(value: unknown) {
  const id = requiredText(value);
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) {
    throw new InvalidInput("INVALID_UUID");
  }
  return id.toLowerCase();
}

export function parseWager(value: unknown, key: unknown) {
  const data = (value ?? {}) as Record<string, unknown>;
  const kind = requiredText(data.kind);
  if (!["BET", "WIN", "LOSS", "REFUND", "ROLLBACK"].includes(kind)) {
    throw new InvalidInput("INVALID_KIND");
  }
  const money = Money.from(data.money);
  if ((kind === "LOSS" && money.cents !== 0n) || (kind !== "LOSS" && money.cents === 0n)) {
    throw new InvalidInput("INVALID_OPERATION_AMOUNT");
  }
  const reference = data.referenceExternalTransactionId === undefined
    ? null : requiredText(data.referenceExternalTransactionId);
  if ((kind === "REFUND" || kind === "ROLLBACK") && !reference) {
    throw new InvalidInput("REFERENCE_REQUIRED");
  }
  const payload = {
    external: requiredText(data.externalTransactionId),
    game: requiredText(data.gameId),
    kind,
    money: money.toJSON(),
    player: uuid(data.playerId),
    provider: requiredText(data.providerId),
    reference,
    round: requiredText(data.roundId),
    wallet: uuid(data.walletId),
  };
  return {
    ...payload, money, key: requiredText(key),
    hash: createHash("sha256").update(JSON.stringify(payload)).digest("hex"),
  };
}

export type Command = ReturnType<typeof parseWager>;
