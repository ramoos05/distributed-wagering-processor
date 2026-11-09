import { DataSource, type MigrationInterface, type QueryRunner } from "typeorm";

class Initial1760000000000 implements MigrationInterface {
  async up(q: QueryRunner) {
    await q.query(`
      CREATE TABLE wallet (
        id uuid PRIMARY KEY, player_id uuid NOT NULL, currency text NOT NULL CHECK(currency ~ '^[A-Z]{3}$'),
        balance bigint NOT NULL CHECK(balance >= 0), version bigint NOT NULL DEFAULT 1 CHECK(version >= 1),
        UNIQUE(player_id,currency)
      );
      CREATE TABLE wager (
        id uuid PRIMARY KEY, provider text NOT NULL, external_id text NOT NULL, idem text NOT NULL UNIQUE,
        hash text NOT NULL, wallet_id uuid NOT NULL REFERENCES wallet,
        kind text NOT NULL CHECK(kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        round_id text NOT NULL, game_id text NOT NULL, amount bigint NOT NULL CHECK(amount >= 0), ref_external text,
        ref_id uuid REFERENCES wager, status text NOT NULL CHECK(status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        failure_code text, balance bigint CHECK(balance >= 0), attempts integer NOT NULL DEFAULT 0,
        next_attempt timestamptz, created_at timestamptz NOT NULL DEFAULT now(),
        UNIQUE(provider,external_id), UNIQUE(wallet_id,id)
      );
      CREATE UNIQUE INDEX wager_reversal ON wager(kind,ref_id) WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK');
      CREATE INDEX wager_due ON wager(next_attempt,id) WHERE status='PENDING_REFERENCE';
      CREATE TABLE ledger (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, wallet_id uuid NOT NULL REFERENCES wallet,
        wager_id uuid NOT NULL, delta bigint NOT NULL CHECK(delta <> 0),
        before_balance bigint NOT NULL CHECK(before_balance >= 0), after_balance bigint NOT NULL CHECK(after_balance >= 0),
        created_at timestamptz NOT NULL DEFAULT now(),
        CHECK(before_balance + delta = after_balance), UNIQUE(wallet_id,wager_id),
        FOREIGN KEY(wallet_id,wager_id) REFERENCES wager(wallet_id,id)
      );
      CREATE INDEX ledger_wallet_cursor ON ledger(wallet_id,id);
      CREATE FUNCTION deny_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS
      $$ BEGIN RAISE EXCEPTION 'ledger is append only'; END; $$;
      CREATE TRIGGER ledger_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON ledger
        FOR EACH STATEMENT EXECUTE FUNCTION deny_ledger_mutation();
      CREATE TABLE inbox (
        consumer text NOT NULL, message_id text NOT NULL, hash text NOT NULL,
        processed_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(consumer,message_id)
      );
      CREATE TABLE outbox (
        id uuid PRIMARY KEY, wallet_id uuid NOT NULL REFERENCES wallet, payload jsonb NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz,
        attempts integer NOT NULL DEFAULT 0, next_attempt timestamptz NOT NULL DEFAULT now()
      );
      CREATE INDEX outbox_due ON outbox(next_attempt,id) WHERE published_at IS NULL;
    `);
  }
  async down(q: QueryRunner) {
    await q.query("DROP TABLE outbox,inbox,ledger,wager,wallet; DROP FUNCTION deny_ledger_mutation()");
  }
}

class TerminalGuard1790000000000 implements MigrationInterface {
  async up(q: QueryRunner) {
    await q.query(`CREATE FUNCTION guard_wager_terminal() RETURNS trigger LANGUAGE plpgsql AS
      $$ BEGIN
        IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
          RAISE EXCEPTION 'terminal wager is immutable' USING ERRCODE='23514';
        END IF;
        IF NEW.status NOT IN ('PENDING_REFERENCE','PROCESSED','REJECTED','FAILED') THEN
          RAISE EXCEPTION 'invalid wager transition' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END; $$;
      CREATE TRIGGER wager_terminal BEFORE UPDATE OR DELETE ON wager
      FOR EACH ROW EXECUTE FUNCTION guard_wager_terminal();`);
  }
  async down(q: QueryRunner) {
    await q.query("DROP TRIGGER wager_terminal ON wager; DROP FUNCTION guard_wager_terminal()");
  }
}

class DomainState1790000000001 implements MigrationInterface {
  async up(q: QueryRunner) {
    await q.query(`ALTER TABLE wallet ADD COLUMN created_at timestamptz NOT NULL DEFAULT now(),
      ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();
      ALTER TABLE wager ADD COLUMN processed_at timestamptz;
      ALTER TABLE wager ADD CONSTRAINT wager_operation_amount CHECK
        ((kind='LOSS' AND amount=0) OR (kind<>'LOSS' AND amount>0)),
        ADD CONSTRAINT wager_reference_required CHECK(kind NOT IN ('REFUND','ROLLBACK') OR ref_external IS NOT NULL),
        ADD CONSTRAINT wager_terminal_balance CHECK(status NOT IN ('PROCESSED','REJECTED','FAILED') OR balance IS NOT NULL),
        ADD CONSTRAINT wager_failure_code CHECK(status NOT IN ('REJECTED','FAILED') OR failure_code IS NOT NULL),
        ADD CONSTRAINT wager_processed_reference CHECK(status<>'PROCESSED' OR kind NOT IN ('REFUND','ROLLBACK') OR ref_id IS NOT NULL);
      CREATE OR REPLACE FUNCTION guard_wager_terminal() RETURNS trigger LANGUAGE plpgsql AS
      $$ BEGIN
        IF TG_OP='DELETE' OR OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
          RAISE EXCEPTION 'wager is audit immutable' USING ERRCODE='23514';
        END IF;
        IF NEW.status NOT IN ('PENDING_REFERENCE','PROCESSED','REJECTED','FAILED') THEN
          RAISE EXCEPTION 'invalid wager transition' USING ERRCODE='23514';
        END IF;
        IF ROW(NEW.id,NEW.provider,NEW.external_id,NEW.idem,NEW.hash,NEW.wallet_id,NEW.kind,NEW.round_id,NEW.game_id,NEW.amount,NEW.ref_external)
          IS DISTINCT FROM ROW(OLD.id,OLD.provider,OLD.external_id,OLD.idem,OLD.hash,OLD.wallet_id,OLD.kind,OLD.round_id,OLD.game_id,OLD.amount,OLD.ref_external) THEN
          RAISE EXCEPTION 'wager identity is immutable' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END; $$;`);
  }
  async down(q: QueryRunner) {
    await q.query(`ALTER TABLE wager DROP CONSTRAINT wager_operation_amount,
      DROP CONSTRAINT wager_reference_required, DROP CONSTRAINT wager_terminal_balance,
      DROP CONSTRAINT wager_failure_code, DROP CONSTRAINT wager_processed_reference,
      DROP COLUMN processed_at;
      ALTER TABLE wallet DROP COLUMN created_at, DROP COLUMN updated_at;
      CREATE OR REPLACE FUNCTION guard_wager_terminal() RETURNS trigger LANGUAGE plpgsql AS
      $$ BEGIN
        IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
          RAISE EXCEPTION 'terminal wager is immutable' USING ERRCODE='23514';
        END IF;
        IF NEW.status NOT IN ('PENDING_REFERENCE','PROCESSED','REJECTED','FAILED') THEN
          RAISE EXCEPTION 'invalid wager transition' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END; $$;`);
  }
}

const schema = process.env.DATABASE_SCHEMA ?? "public";
if (!/^[a-z_][a-z0-9_]*$/.test(schema)) throw new Error("INVALID_DATABASE_SCHEMA");
export const db = new DataSource({
  type: "postgres",
  url: process.env.DATABASE_URL ?? "postgres://wager:wager@localhost:5432/wager",
  schema,
  migrations: [Initial1760000000000, TerminalGuard1790000000000, DomainState1790000000001],
  extra: { statement_timeout: 20000, idle_in_transaction_session_timeout: 30000,
    options: `-c search_path=${schema}` },
});

if (import.meta.main) {
  await db.initialize();
  try {
    if (process.argv.includes("--down")) await db.undoLastMigration();
    else await db.runMigrations();
  } finally {
    await db.destroy();
  }
}
