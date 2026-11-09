import type { EntityManager } from "typeorm";
import { db } from "./migrate";
import { Money, type Command } from "./domain";
import { count, observe, log } from "./telemetry";
import { permanentInfrastructureFailure } from "./failures";
import { InboxMessage, OutboxMessage, Wallet as WalletAggregate, WalletLedgerEntry,
  WagerTransaction, type WagerKind, type WagerStatus } from "./models";
import {
  type IntegrationEvent, WagerTransactionPendingReference, WagerTransactionProcessed,
  WagerTransactionRejected, WalletBalanceChanged,
  WagerTransactionFailed,
} from "./events";

export class Conflict extends Error {}
export class MissingWallet extends Error {}
type Wallet = { id: string; player_id: string; currency: string; balance: string; version: string; created_at: Date; updated_at: Date };
type Wager = {
  id: string; wallet_id: string; provider: string; external_id: string; idem: string; hash: string;
  kind: string; amount: string; round_id: string; ref_external: string | null;
  status: WagerStatus; failure_code: string | null; balance: string | null; attempts: number;
  game_id: string; created_at: Date; processed_at: Date | null; ref_id: string | null;
  player_id?: string; currency?: string;
};

const amount = (cents: bigint | string, currency: string) => Money.rehydrate(BigInt(cents), currency).toJSON();
const terminal = (row: Wager) => ["PROCESSED", "REJECTED", "FAILED"].includes(row.status);
function transaction(row: Wager, wallet: Wallet) {
  return WagerTransaction.rehydrate({ id: row.id, providerId: row.provider, externalTransactionId: row.external_id,
    idempotencyKey: row.idem, payloadHash: row.hash, walletId: row.wallet_id, playerId: row.player_id ?? wallet.player_id,
    roundId: row.round_id, gameId: row.game_id, kind: row.kind as WagerKind,
    money: Money.rehydrate(BigInt(row.amount), row.currency ?? wallet.currency), referenceExternalTransactionId: row.ref_external ?? undefined,
    createdAt: row.created_at, status: row.status, failureCode: row.failure_code ?? undefined,
    referenceTransactionId: row.ref_id ?? undefined, processedAt: row.processed_at ?? undefined });
}

async function lockWallet(tx: EntityManager, id: string): Promise<Wallet> {
  const started = performance.now();
  await tx.query("SAVEPOINT wallet_lock");
  let rows: Wallet[];
  try {
    rows = await tx.query("SELECT * FROM wallet WHERE id=$1 FOR UPDATE NOWAIT", [id]);
  } catch (error) {
    if ((error as { code?: string })?.code !== "55P03") throw error;
    await tx.query("ROLLBACK TO SAVEPOINT wallet_lock");
    count("wallet_lock_contentions");
    rows = await tx.query("SELECT * FROM wallet WHERE id=$1 FOR UPDATE", [id]);
  }
  await tx.query("RELEASE SAVEPOINT wallet_lock");
  const [wallet] = rows;
  observe("wallet_lock_wait_ms", performance.now() - started);
  if (!wallet) throw new MissingWallet("WALLET_NOT_FOUND");
  return wallet;
}

function result(row: Wager, currency: string, idempotentReplay: boolean) {
  return {
    transactionId: row.id, status: row.status, failureCode: row.failure_code,
    balance: row.balance === null ? null : amount(row.balance, currency), idempotentReplay,
  };
}

async function event(tx: EntityManager, event: IntegrationEvent) {
  const message = OutboxMessage.enqueue(event);
  await tx.query("INSERT INTO outbox(id,wallet_id,payload) VALUES($1,$2,$3)",
    [message.id, message.aggregateId, message.payload]);
}

async function apply(tx: EntityManager, wallet: Wallet, row: Wager) {
  const aggregate = transaction(row, wallet);
  let failure: string | null = null;
  let reference: Wager | undefined;
  let alreadyReversed = false;
  if (row.ref_external) {
    [reference] = await tx.query(`SELECT w.*,wallet.player_id,wallet.currency FROM wager w
      JOIN wallet ON wallet.id=w.wallet_id WHERE w.provider=$1 AND w.external_id=$2`, [row.provider, row.ref_external]);
    if (reference && aggregate.requiresReference()) {
      const [existing] = await tx.query("SELECT id FROM wager WHERE ref_id=$1 AND kind=$2 AND status='PROCESSED'", [reference.id, row.kind]);
      alreadyReversed = !!existing;
    }
  }
  const referenceAggregate = reference && transaction(reference, wallet);
  const decision = aggregate.referenceDecision(referenceAggregate, alreadyReversed, row.attempts);
  if (decision.status === "WAIT") {
    aggregate.markPendingReference();
    await tx.query(`UPDATE wager SET status='PENDING_REFERENCE', attempts=attempts+1,
      next_attempt=now()+make_interval(secs => power(2,attempts)) WHERE id=$1`, [row.id]);
    if (row.status === "PENDING") await event(tx, WagerTransactionPendingReference.from(row.wallet_id, row.id));
    return result({ ...row, status: "PENDING_REFERENCE" }, wallet.currency, false);
  }
  if (decision.status === "REJECT") failure = decision.code;
  const before = BigInt(wallet.balance);
  const effect = failure ? null : aggregate.financialEffect(Money.rehydrate(before, wallet.currency), referenceAggregate);
  const delta = effect?.delta.cents ?? 0n;
  const after = effect?.after.cents ?? before;
  failure ??= effect?.failureCode ?? null;

  const status = failure ? "REJECTED" : "PROCESSED";
  if (failure) aggregate.reject(failure);
  else aggregate.markProcessed(reference?.id, new Date());
  const balance = String(failure ? before : after);
  await tx.query("UPDATE wager SET status=$2,failure_code=$3,balance=$4,ref_id=$5,next_attempt=NULL,processed_at=$6 WHERE id=$1",
    [row.id, aggregate.status, aggregate.failureCode ?? null, balance, reference?.id ?? null, aggregate.processedAt ?? null]);
  const response = result({ ...row, status, failure_code: failure, balance }, wallet.currency, false);
  const Outcome = failure ? WagerTransactionRejected : WagerTransactionProcessed;
  await event(tx, Outcome.from(row.wallet_id, row.id, response));
  if (!failure && delta !== 0n) {
    const opening = row.kind === "OPENING";
    const value = Money.rehydrate(delta < 0n ? -delta : delta, wallet.currency);
    const state = WalletAggregate.rehydrate({ id: wallet.id, playerId: wallet.player_id,
      balance: Money.rehydrate(before, wallet.currency), version: BigInt(wallet.version),
      createdAt: wallet.created_at, updatedAt: wallet.updated_at });
    const entry = opening ? WalletLedgerEntry.create({ id: crypto.randomUUID(), walletId: wallet.id,
      transactionId: row.id, direction: "CREDIT", money: value, balanceBefore: state.balance,
      balanceAfter: Money.rehydrate(after, wallet.currency), createdAt: new Date() })
      : delta < 0n ? state.debit(value, row.id) : state.credit(value, row.id);
    await tx.query("INSERT INTO ledger(wallet_id,wager_id,delta,before_balance,after_balance) VALUES($1,$2,$3,$4,$5)",
      [entry.walletId, entry.transactionId, String(delta), String(entry.balanceBefore.cents), String(entry.balanceAfter.cents)]);
    await tx.query("UPDATE wallet SET balance=$2,version=version+$3,updated_at=now() WHERE id=$1", [wallet.id, balance, opening ? 0 : 1]);
    await event(tx, WalletBalanceChanged.from(row.wallet_id, row.id, {
      walletId: wallet.id, transactionId: row.id, direction: delta < 0n ? "DEBIT" : "CREDIT",
      money: amount(delta < 0n ? -delta : delta, wallet.currency), balanceBefore: amount(before, wallet.currency),
      balanceAfter: amount(balance, wallet.currency), walletVersion: String(BigInt(wallet.version) + (opening ? 0n : 1n)),
    }));
  }
  return response;
}

export async function submit(command: Command, messageId?: string) {
  const started = performance.now();
  try {
  const response = await db.transaction(async (tx) => {
    if (messageId) {
      const inbox = InboxMessage.receive({ messageId, consumerName: "wager", payloadHash: command.hash + ":" + command.key });
      inbox.markProcessed(new Date());
      await tx.query("INSERT INTO inbox(consumer,message_id,hash) VALUES('wager',$1,$2) ON CONFLICT DO NOTHING",
        [inbox.messageId, inbox.payloadHash]);
      const [stored] = await tx.query("SELECT hash FROM inbox WHERE consumer='wager' AND message_id=$1", [messageId]);
      if (stored.hash !== inbox.payloadHash) throw new Conflict("INBOX_CONFLICT");
    }
    const wallet = await lockWallet(tx, command.wallet);
    if (wallet.player_id !== command.player || wallet.currency !== command.money.currency) {
      throw new Conflict("WALLET_IDENTITY_MISMATCH");
    }
    const id = crypto.randomUUID();
    WagerTransaction.create({ id, providerId: command.provider, externalTransactionId: command.external,
      idempotencyKey: command.key, payloadHash: command.hash, walletId: command.wallet, playerId: command.player,
      roundId: command.round, gameId: command.game, kind: command.kind as WagerKind, money: command.money,
      referenceExternalTransactionId: command.reference ?? undefined, createdAt: new Date() });
    const [inserted]: Wager[] = await tx.query(`INSERT INTO wager
      (id,provider,external_id,idem,hash,wallet_id,kind,round_id,game_id,amount,ref_external,status)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'PENDING') ON CONFLICT DO NOTHING RETURNING *`,
      [id, command.provider, command.external, command.key, command.hash, command.wallet, command.kind,
        command.round, command.game, String(command.money.cents), command.reference]);
    if (inserted) return apply(tx, wallet, inserted);
    const [existing]: Wager[] = await tx.query("SELECT * FROM wager WHERE idem=$1", [command.key]);
    if (!existing || existing.hash !== command.hash) throw new Conflict("IDEMPOTENCY_CONFLICT");
    return result(existing, wallet.currency, true);
  });
  count("transactions_" + response.status.toLowerCase());
  if (response.idempotentReplay) count("duplicates");
  log("transaction_completed", { transactionId: response.transactionId, walletId: command.wallet,
    providerId: command.provider, correlationId: messageId ?? command.external, messageId: messageId ?? null,
    status: response.status, replay: response.idempotentReplay });
  return response;
  } catch (error) {
    if (["40P01", "55P03", "40001"].includes((error as { code?: string })?.code ?? "")) count("lock_conflicts");
    if (permanentInfrastructureFailure(error)) return auditDeliveryFailure(command, "INFRASTRUCTURE_PERMANENT");
    throw error;
  } finally { observe("transaction_duration_ms", performance.now() - started); }
}

export async function openWallet(player: string, money: Money) {
  return db.transaction(async (tx) => {
    const id = crypto.randomUUID();
    WalletAggregate.open({ id, playerId: player, initialBalance: money });
    const [wallet]: Wallet[] = await tx.query(
      "INSERT INTO wallet(id,player_id,currency,balance) VALUES($1,$2,$3,0) ON CONFLICT DO NOTHING RETURNING *",
      [id, player, money.currency]);
    if (!wallet) throw new Conflict("WALLET_EXISTS");
    if (money.cents > 0n) {
      const openingId = crypto.randomUUID();
      const [opening]: Wager[] = await tx.query(`INSERT INTO wager
        (id,provider,external_id,idem,hash,wallet_id,kind,round_id,game_id,amount,status)
        VALUES($1::uuid,'internal',$1::uuid::text,'opening:'||$1::uuid::text,'internal',$2,'OPENING','internal','internal',$3,'PENDING') RETURNING *`,
        [openingId, id, String(money.cents)]);
      await apply(tx, wallet, opening);
    }
    return { id, playerId: player, balance: money.toJSON(), version: 1 };
  });
}

// Called only after infrastructure delivery attempts are exhausted. A committed
// financial result wins over failure auditing (including an ambiguous commit).
export async function auditDeliveryFailure(command: Command, failureCode = "DELIVERY_EXHAUSTED") {
  const response = await db.transaction(async tx => {
    const wallet = await lockWallet(tx, command.wallet);
    if (wallet.player_id !== command.player || wallet.currency !== command.money.currency) {
      throw new Conflict("WALLET_IDENTITY_MISMATCH");
    }
    const [inserted]: Wager[] = await tx.query(`INSERT INTO wager
      (id,provider,external_id,idem,hash,wallet_id,kind,round_id,game_id,amount,ref_external,status)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'PENDING') ON CONFLICT DO NOTHING RETURNING *`,
      [crypto.randomUUID(), command.provider, command.external, command.key, command.hash, command.wallet,
        command.kind, command.round, command.game, String(command.money.cents), command.reference]);
    const row = inserted ?? (await tx.query("SELECT * FROM wager WHERE idem=$1", [command.key]))[0] as Wager | undefined;
    if (!row || row.hash !== command.hash) throw new Conflict("IDEMPOTENCY_CONFLICT");
    if (terminal(row)) return result(row, wallet.currency, true);
    transaction(row, wallet).fail(failureCode);
    await tx.query("UPDATE wager SET status='FAILED',failure_code=$3,balance=$2,next_attempt=NULL WHERE id=$1",
      [row.id, wallet.balance, failureCode]);
    const response = result({ ...row, status: "FAILED", failure_code: failureCode, balance: wallet.balance }, wallet.currency, false);
    await event(tx, WagerTransactionFailed.from(wallet.id, row.id, response));
    return response;
  });
  if (!response.idempotentReplay) {
    count("transactions_failed");
    log("infrastructure_failure", { transactionId: response.transactionId, walletId: command.wallet,
      providerId: command.provider, correlationId: command.external, messageId: null, failureCode });
  }
  return response;
}

export async function retryReferences() {
  const due: Wager[] = await db.query(
    "SELECT * FROM wager WHERE status='PENDING_REFERENCE' AND next_attempt<=now() ORDER BY next_attempt LIMIT 50",
  );
  for (const candidate of due) {
    const started = performance.now();
    const response = await db.transaction(async (tx) => {
      const wallet = await lockWallet(tx, candidate.wallet_id);
      const [row]: Wager[] = await tx.query(
        "SELECT * FROM wager WHERE id=$1 AND status='PENDING_REFERENCE' AND next_attempt<=now()", [candidate.id],
      );
      if (row) return apply(tx, wallet, row);
    });
    if (response) {
      count("reference_retries");
      count("transactions_" + response.status.toLowerCase());
      observe("reference_processing_ms", performance.now() - started);
      log("reference_retried", { transactionId: response.transactionId, walletId: candidate.wallet_id,
        providerId: candidate.provider, correlationId: candidate.external_id, messageId: null, status: response.status });
    }
  }
}

export async function walletView(id: string) {
  const [row]: Wallet[] = await db.query("SELECT * FROM wallet WHERE id=$1", [id]);
  if (!row) throw new MissingWallet("WALLET_NOT_FOUND");
  return { id, playerId: row.player_id, balance: amount(row.balance, row.currency), version: Number(row.version) };
}

export async function reconcile(id: string) {
  return db.transaction(async (tx) => {
    const wallet = await lockWallet(tx, id);
    const [ledger] = await tx.query("SELECT COALESCE(SUM(delta),0)::text AS total,COUNT(*)::text AS entries FROM ledger WHERE wallet_id=$1", [id]);
    const difference = BigInt(wallet.balance) - BigInt(ledger.total);
    if (difference !== 0n) {
      count("reconciliation_mismatches");
      log("reconciliation_mismatch", { walletId: id, differenceCents: String(difference) });
    }
    return {
      walletId: id, storedBalance: amount(wallet.balance, wallet.currency),
      calculatedBalance: amount(ledger.total, wallet.currency), difference: amount(difference, wallet.currency),
      consistent: difference === 0n, checkedEntries: Number(ledger.entries),
    };
  });
}
