import { InvalidInput, Money, requiredText, uuid } from "./domain";
import type { IntegrationEvent } from "./events";

export type WagerKind = "OPENING" | "BET" | "WIN" | "LOSS" | "REFUND" | "ROLLBACK";
export type WagerStatus = "PENDING" | "PENDING_REFERENCE" | "PROCESSED" | "REJECTED" | "FAILED";
export type LedgerDirection = "DEBIT" | "CREDIT";
export class InvalidTransactionStateError extends Error {}

export interface LedgerEntryState {
  id: string; walletId: string; transactionId: string; direction: LedgerDirection;
  money: Money; balanceBefore: Money; balanceAfter: Money; createdAt: Date;
}
export class WalletLedgerEntry {
  private constructor(private readonly state: Readonly<LedgerEntryState>) {
    Object.freeze(this.state); Object.freeze(this);
  }
  static create(state: LedgerEntryState) {
    const entry = new this({ ...state, createdAt: new Date(state.createdAt) });
    if (!state.money.isPositive() || state.balanceBefore.isNegative() || state.balanceAfter.isNegative() || !entry.isBalanced()) {
      throw new InvalidInput("INVALID_LEDGER_ENTRY");
    }
    return entry;
  }
  static rehydrate(state: LedgerEntryState) { return new this({ ...state, createdAt: new Date(state.createdAt) }); }
  get id() { return this.state.id; }
  get walletId() { return this.state.walletId; }
  get transactionId() { return this.state.transactionId; }
  get direction() { return this.state.direction; }
  get money() { return this.state.money; }
  get balanceBefore() { return this.state.balanceBefore; }
  get balanceAfter() { return this.state.balanceAfter; }
  get createdAt() { return new Date(this.state.createdAt); }
  isBalanced() {
    return this.balanceBefore.add(this.direction === "DEBIT" ? this.money.negate() : this.money).equals(this.balanceAfter);
  }
}

export interface WalletState {
  id: string; playerId: string; balance: Money; version: bigint; createdAt: Date; updatedAt: Date;
}
export class Wallet {
  private constructor(private readonly state: WalletState) {}
  static open(props: { id: string; playerId: string; initialBalance: Money }) {
    if (props.initialBalance.isNegative()) throw new InvalidInput("NEGATIVE_BALANCE");
    const now = new Date();
    return new this({ id: uuid(props.id), playerId: uuid(props.playerId), balance: props.initialBalance,
      version: 1n, createdAt: now, updatedAt: now });
  }
  static rehydrate(state: WalletState) {
    return new this({ ...state, createdAt: new Date(state.createdAt), updatedAt: new Date(state.updatedAt) });
  }
  get id() { return this.state.id; }
  get playerId() { return this.state.playerId; }
  get currency() { return this.balance.currency; }
  get balance() { return this.state.balance; }
  get version() { return this.state.version; }
  get createdAt() { return new Date(this.state.createdAt); }
  get updatedAt() { return new Date(this.state.updatedAt); }
  debit(money: Money, transactionId: string, at = new Date()) { return this.move(money, transactionId, "DEBIT", at); }
  credit(money: Money, transactionId: string, at = new Date()) { return this.move(money, transactionId, "CREDIT", at); }
  private move(money: Money, transactionId: string, direction: LedgerDirection, at: Date) {
    if (!money.isPositive()) throw new InvalidInput("INVALID_OPERATION_AMOUNT");
    const after = this.balance.add(direction === "DEBIT" ? money.negate() : money);
    if (after.isNegative()) throw new InvalidInput("INSUFFICIENT_FUNDS");
    if (after.cents > 9223372036854775807n) throw new InvalidInput("BALANCE_LIMIT_EXCEEDED");
    const entry = WalletLedgerEntry.create({ id: crypto.randomUUID(), walletId: this.id,
      transactionId, direction, money, balanceBefore: this.balance, balanceAfter: after, createdAt: at });
    this.state.balance = after; this.state.version++; this.state.updatedAt = new Date(at);
    return entry;
  }
}

export interface WagerTransactionState {
  id: string; providerId: string; externalTransactionId: string; idempotencyKey: string; payloadHash: string;
  walletId: string; playerId: string; roundId: string; gameId: string; kind: WagerKind; money: Money;
  referenceExternalTransactionId?: string; createdAt: Date; status: WagerStatus;
  referenceTransactionId?: string; failureCode?: string; processedAt?: Date;
}
export class WagerTransaction {
  private constructor(private readonly state: WagerTransactionState) {}
  static create(props: Omit<WagerTransactionState, "status" | "failureCode" | "processedAt" | "referenceTransactionId">) {
    if (!["OPENING", "BET", "WIN", "LOSS", "REFUND", "ROLLBACK"].includes(props.kind)) throw new InvalidInput("INVALID_KIND");
    if ((props.kind === "LOSS" && !props.money.isZero()) || (props.kind !== "LOSS" && !props.money.isPositive())) {
      throw new InvalidInput("INVALID_OPERATION_AMOUNT");
    }
    if (["REFUND", "ROLLBACK"].includes(props.kind) && !props.referenceExternalTransactionId) throw new InvalidInput("REFERENCE_REQUIRED");
    return new this({ ...props, createdAt: new Date(props.createdAt), status: "PENDING" });
  }
  static rehydrate(state: WagerTransactionState) {
    return new this({ ...state, createdAt: new Date(state.createdAt), processedAt: state.processedAt && new Date(state.processedAt) });
  }
  get id() { return this.state.id; }
  get providerId() { return this.state.providerId; }
  get externalTransactionId() { return this.state.externalTransactionId; }
  get idempotencyKey() { return this.state.idempotencyKey; }
  get payloadHash() { return this.state.payloadHash; }
  get walletId() { return this.state.walletId; }
  get playerId() { return this.state.playerId; }
  get roundId() { return this.state.roundId; }
  get gameId() { return this.state.gameId; }
  get referenceExternalTransactionId() { return this.state.referenceExternalTransactionId; }
  get createdAt() { return new Date(this.state.createdAt); }
  get kind() { return this.state.kind; }
  get money() { return this.state.money; }
  get status() { return this.state.status; }
  get referenceTransactionId() { return this.state.referenceTransactionId; }
  get failureCode() { return this.state.failureCode; }
  get processedAt() { return this.state.processedAt && new Date(this.state.processedAt); }
  isTerminal() { return ["PROCESSED", "REJECTED", "FAILED"].includes(this.status); }
  requiresReference() { return this.kind === "REFUND" || this.kind === "ROLLBACK"; }
  affectsBalance() { return this.kind !== "LOSS"; }
  matchesPayload(hash: string) { return this.state.payloadHash === hash; }
  referenceDecision(reference: WagerTransaction | undefined, alreadyReversed: boolean, attempts: number):
    { status: "READY" | "WAIT" } | { status: "REJECT"; code: string } {
    if (!this.referenceExternalTransactionId) return { status: "READY" };
    if (reference && (reference.externalTransactionId !== this.referenceExternalTransactionId ||
      reference.providerId !== this.providerId || reference.walletId !== this.walletId ||
      reference.playerId !== this.playerId || reference.money.currency !== this.money.currency || reference.roundId !== this.roundId)) {
      return { status: "REJECT", code: "REFERENCE_SCOPE_MISMATCH" };
    }
    if (!reference || !reference.isTerminal()) {
      return attempts >= 8 ? { status: "REJECT", code: "REFERENCE_EXPIRED" } : { status: "WAIT" };
    }
    const allowed = this.kind === "ROLLBACK" ? ["BET", "WIN", "REFUND"] : ["BET"];
    if (reference.status !== "PROCESSED" || !allowed.includes(reference.kind)) return { status: "REJECT", code: "INVALID_REFERENCE" };
    if (this.requiresReference() && !this.money.equals(reference.money)) return { status: "REJECT", code: "REFERENCE_AMOUNT_MISMATCH" };
    if (this.requiresReference() && alreadyReversed) return { status: "REJECT", code: "ALREADY_REVERSED" };
    return { status: "READY" };
  }
  financialEffect(balance: Money, reference?: WagerTransaction) {
    const delta = !this.affectsBalance() ? Money.zero(this.money.currency)
      : this.ledgerDirectionFor(reference) === "DEBIT" ? this.money.negate() : this.money;
    const after = balance.add(delta);
    const failureCode = after.isNegative() ? (this.requiresReference() ? "REVERSAL_INSUFFICIENT_FUNDS" : "INSUFFICIENT_FUNDS")
      : after.cents > 9223372036854775807n ? "BALANCE_LIMIT_EXCEEDED" : null;
    return { delta, after, failureCode };
  }
  private assertMutable() {
    if (this.isTerminal()) throw new InvalidTransactionStateError("TERMINAL_TRANSACTION");
  }
  markProcessed(referenceTransactionId: string | undefined, at: Date) {
    this.assertMutable();
    if (this.requiresReference() && !referenceTransactionId) throw new InvalidInput("REFERENCE_REQUIRED");
    this.state.status = "PROCESSED"; this.state.referenceTransactionId = referenceTransactionId; this.state.processedAt = new Date(at);
  }
  markPendingReference() { this.assertMutable(); this.state.status = "PENDING_REFERENCE"; }
  reject(code: string) { this.assertMutable(); this.state.failureCode = requiredText(code); this.state.status = "REJECTED"; }
  fail(code: string) { this.assertMutable(); this.state.failureCode = requiredText(code); this.state.status = "FAILED"; }
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    if (!this.affectsBalance()) throw new InvalidInput("NO_LEDGER_EFFECT");
    if (this.kind === "BET") return "DEBIT";
    if (this.kind === "ROLLBACK") {
      if (!reference || reference.status !== "PROCESSED" || !["BET", "WIN", "REFUND"].includes(reference.kind)) {
        throw new InvalidInput("INVALID_REFERENCE");
      }
      return reference.kind === "BET" ? "CREDIT" : "DEBIT";
    }
    return "CREDIT";
  }
}

interface InboxState { messageId: string; consumerName: string; payloadHash: string; receivedAt: Date; processedAt?: Date }
export class InboxMessage {
  private constructor(private readonly state: InboxState) {}
  static receive(props: Omit<InboxState, "processedAt" | "receivedAt">) {
    if (typeof props.payloadHash !== "string" || !props.payloadHash || props.payloadHash.length > 512) {
      throw new InvalidInput("INVALID_PAYLOAD_HASH");
    }
    return new this({ messageId: requiredText(props.messageId), consumerName: requiredText(props.consumerName),
      payloadHash: props.payloadHash, receivedAt: new Date() });
  }
  static rehydrate(state: InboxState) {
    return new this({ ...state, receivedAt: new Date(state.receivedAt), processedAt: state.processedAt && new Date(state.processedAt) });
  }
  get messageId() { return this.state.messageId; }
  get consumerName() { return this.state.consumerName; }
  get payloadHash() { return this.state.payloadHash; }
  get receivedAt() { return new Date(this.state.receivedAt); }
  get processedAt() { return this.state.processedAt && new Date(this.state.processedAt); }
  isProcessed() { return this.state.processedAt !== undefined; }
  markProcessed(at: Date) {
    if (this.isProcessed()) throw new InvalidTransactionStateError("INBOX_ALREADY_PROCESSED");
    this.state.processedAt = new Date(at);
  }
}

interface OutboxState {
  id: string; aggregateId: string; eventType: string; payload: Readonly<Record<string, unknown>>;
  occurredAt: Date; attempts: number; nextAttemptAt?: Date; publishedAt?: Date;
}
export class OutboxMessage {
  private constructor(private readonly state: OutboxState) {}
  static enqueue(event: IntegrationEvent) {
    return new this({ id: event.eventId, aggregateId: event.aggregateId, eventType: event.eventType,
      payload: structuredClone(event.toJSON()), occurredAt: new Date(event.occurredAt), attempts: 0 });
  }
  static rehydrate(state: OutboxState) {
    return new this({ ...state, payload: structuredClone(state.payload), occurredAt: new Date(state.occurredAt),
      nextAttemptAt: state.nextAttemptAt && new Date(state.nextAttemptAt), publishedAt: state.publishedAt && new Date(state.publishedAt) });
  }
  get id() { return this.state.id; }
  get aggregateId() { return this.state.aggregateId; }
  get eventType() { return this.state.eventType; }
  get payload() { return structuredClone(this.state.payload); }
  get occurredAt() { return new Date(this.state.occurredAt); }
  get attempts() { return this.state.attempts; }
  get nextAttemptAt() { return this.state.nextAttemptAt && new Date(this.state.nextAttemptAt); }
  get publishedAt() { return this.state.publishedAt && new Date(this.state.publishedAt); }
  isPending() { return this.state.publishedAt === undefined; }
  isDue(now: Date) { return this.isPending() && (!this.state.nextAttemptAt || this.state.nextAttemptAt <= now); }
  markPublished(at: Date) {
    if (!this.isPending()) throw new InvalidTransactionStateError("OUTBOX_ALREADY_PUBLISHED");
    this.state.publishedAt = new Date(at);
  }
  scheduleRetry(now: Date) {
    if (!this.isPending()) throw new InvalidTransactionStateError("OUTBOX_ALREADY_PUBLISHED");
    this.state.nextAttemptAt = new Date(now.getTime() + 1000 * 2 ** Math.min(this.state.attempts, 8));
    this.state.attempts++;
  }
}
