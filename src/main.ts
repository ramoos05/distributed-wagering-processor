import "reflect-metadata";
import { All, Controller, Module, Req, Res } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { InvalidInput, parseWager, Money, uuid, requiredText } from "./domain";
import { db } from "./migrate";
import { Conflict, MissingWallet, openWallet, reconcile, submit, walletView } from "./processor";
import { metrics, ready, workers, deadLetterMetrics } from "./queue";
import { log } from "./telemetry";

interface Request {
  method: string; path: string; body?: Record<string, unknown>;
  headers: Record<string, unknown>; query: Record<string, unknown>;
}
interface Response {
  status(code: number): Response;
  json(payload: unknown): void;
}

class ProviderIdentityPort {
  verify(_req: Request) {
    // Ponto de extensão para validação OIDC e associação de identidade ao providerId.
  }
}

const identity = new ProviderIdentityPort();
const money = (value: string, currency: string) => Money.rehydrate(BigInt(value), currency).toJSON();

@Controller()
class Api {
  @All("*")
  async handle(@Req() req: Request, @Res() res: Response) {
    try {
      const { path, method, body } = req;
      if (method === "GET" && path === "/health/live") return res.json({ status: "ok" });
      if (method === "GET" && path === "/health/ready") {
        await ready();
        return res.json({ status: "ok" });
      }
      identity.verify(req);
      if (method === "GET" && path === "/metrics") {
        const [lag] = await db.query(`SELECT COALESCE(EXTRACT(epoch FROM now()-min(created_at)),0) AS seconds
          FROM outbox WHERE published_at IS NULL`);
        return res.json({ ...metrics(), outboxLagSeconds: Number(lag.seconds), ...await deadLetterMetrics() });
      }
      if (method === "POST" && path === "/wallets") {
        return res.status(201).json(await openWallet(uuid(body?.playerId), Money.from(body?.initialBalance)));
      }
      if (method === "POST" && path === "/wagering/transactions") {
        const command = parseWager(body, req.headers["idempotency-key"]);
        const result = await submit(command);
        const status = result.status === "FAILED" ? 503 : result.status === "PENDING_REFERENCE" ? 202 : result.status === "REJECTED" ? 422 : 200;
        return res.status(status).json(result);
      }
      const walletPath = /^\/wallets\/([^/]+)(?:\/(ledger|reconciliation))?$/.exec(path);
      if (walletPath) {
        const id = uuid(walletPath[1]);
        if (method === "GET" && !walletPath[2]) return res.json(await walletView(id));
        if (method === "POST" && walletPath[2] === "reconciliation") {
          const result = await reconcile(id);
          return res.json(result);
        }
        if (method === "GET" && walletPath[2] === "ledger") {
          const wallet = await walletView(id);
          const limit = Number(req.query.limit ?? 50);
          if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw new InvalidInput("INVALID_LIMIT");
          const cursor = req.query.cursor === undefined ? "0" :
            Buffer.from(requiredText(req.query.cursor), "base64url").toString();
          if (!/^\d{1,19}$/.test(cursor) || BigInt(cursor) > 9223372036854775807n) {
            throw new InvalidInput("INVALID_CURSOR");
          }
          const rows = await db.query("SELECT * FROM ledger WHERE wallet_id=$1 AND id>$2 ORDER BY id LIMIT $3",
            [id, cursor, limit + 1]);
          const page = rows.slice(0, limit);
          const currency = wallet.balance.currency;
          return res.json({
            items: page.map((row: Record<string, string>) => ({
              id: row.id, transactionId: row.wager_id, createdAt: row.created_at,
              delta: money(row.delta, currency), balanceBefore: money(row.before_balance, currency),
              balanceAfter: money(row.after_balance, currency),
            })),
            nextCursor: rows.length > limit ? Buffer.from(page[limit - 1].id).toString("base64url") : null,
          });
        }
      }
      const internal = /^\/wagering\/transactions\/([^/]+)$/.exec(path);
      const external = /^\/providers\/([^/]+)\/wagering\/transactions\/([^/]+)$/.exec(path);
      if (method === "GET" && (internal || external)) {
        const [row] = await db.query(
          "SELECT w.*, wallet.currency FROM wager w JOIN wallet ON wallet.id=w.wallet_id WHERE " +
            (internal ? "w.id=$1" : "w.provider=$1 AND w.external_id=$2"),
          internal ? [uuid(internal[1])] : external!.slice(1).map(decodeURIComponent),
        );
        if (row) return res.json({
          transactionId: row.id, providerId: row.provider, externalTransactionId: row.external_id,
          walletId: row.wallet_id, status: row.status, kind: row.kind, failureCode: row.failure_code,
          money: money(row.amount, row.currency), balance: row.balance === null ? null : money(row.balance, row.currency),
        });
      }
      return res.status(404).json({ code: "NOT_FOUND" });
    } catch (error) {
      if (error instanceof InvalidInput || error instanceof URIError) {
        return res.status(400).json({ code: error instanceof InvalidInput ? error.message : "INVALID_PATH" });
      }
      if (error instanceof Conflict) return res.status(409).json({ code: error.message });
      if (error instanceof MissingWallet) return res.status(404).json({ code: error.message });
      log("http_error", { code: (error as { code?: string })?.code });
      return res.status(503).json({ code: "SERVICE_UNAVAILABLE" });
    }
  }
}

@Module({ controllers: [Api] })
class App {}

await db.initialize();
const app = await NestFactory.create(App, { logger: false });
await app.listen(Number(process.env.PORT ?? 3000), "0.0.0.0");
const stopWorkers = workers();
let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  await Promise.all([app.close(), stopWorkers()]);
  await db.destroy();
}
process.on("SIGTERM", stop);
process.on("SIGINT", stop);
