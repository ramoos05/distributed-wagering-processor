export abstract class IntegrationEvent {
  abstract readonly eventType: string;
  readonly version = 1;
  readonly eventId = crypto.randomUUID();
  readonly occurredAt = new Date().toISOString();

  protected constructor(
    readonly aggregateId: string,
    readonly correlationId: string,
    readonly data: Readonly<object>,
  ) {}

  toJSON() {
    return {
      eventId: this.eventId, eventType: this.eventType, version: this.version,
      aggregateId: this.aggregateId, correlationId: this.correlationId,
      occurredAt: this.occurredAt, data: this.data,
    };
  }
}

export class WagerTransactionProcessed extends IntegrationEvent {
  readonly eventType = "WagerTransactionProcessed";
  static from(walletId: string, transactionId: string, data: object) {
    return new this(walletId, transactionId, data);
  }
}

export class WagerTransactionRejected extends IntegrationEvent {
  readonly eventType = "WagerTransactionRejected";
  static from(walletId: string, transactionId: string, data: object) {
    return new this(walletId, transactionId, data);
  }
}

export class WagerTransactionPendingReference extends IntegrationEvent {
  readonly eventType = "WagerTransactionPendingReference";
  static from(walletId: string, transactionId: string) {
    return new this(walletId, transactionId, { transactionId });
  }
}

export class WalletBalanceChanged extends IntegrationEvent {
  readonly eventType = "WalletBalanceChanged";
  static from(walletId: string, transactionId: string, data: object) {
    return new this(walletId, transactionId, data);
  }
}

export class WagerTransactionFailed extends IntegrationEvent {
  readonly eventType = "WagerTransactionFailed";
  static from(walletId: string, transactionId: string, data: object) {
    return new this(walletId, transactionId, data);
  }
}
