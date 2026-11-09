# Distributed Wagering Processor

Implementação do desafio Jungle Gaming com Bun, TypeScript, NestJS, TypeORM, PostgreSQL e SQS no LocalStack.

## Executar

Com Docker Compose v2, na pasta do projeto:

```sh
docker compose up --build --scale api=3
```

Quando as três réplicas estiverem prontas, consulte `/health/ready` nas portas 3000 a 3002. As migrations sobem pelo serviço `migrate`. A operação `bun src/migrate.ts --down` reverte somente a última migration aplicada; reverter a migration inicial remove os dados.

Para executar os testes no host com Bun instalado:

```sh
bun install
bun run typecheck
bun test
```

Com as três instâncias prontas, aponte `TEST_API_URLS` para `http://localhost:3000,http://localhost:3001,http://localhost:3002` e execute `bun test` novamente. No PowerShell, use `$env:TEST_API_URLS = "http://localhost:3000,http://localhost:3001,http://localhost:3002"`. A suíte grava carteiras descartáveis com IDs únicos.

## Fluxo HTTP

1. `POST /wallets` com `playerId` em UUID e `initialBalance` como `{"amount":"100.00","currency":"BRL"}`. Guarde o ID retornado.
2. `POST /wagering/transactions` com `Idempotency-Key` e o objeto abaixo. Use os mesmos IDs e o mesmo payload para repetir a chamada.
3. Consulte `GET /wallets/:walletId/ledger` e `POST /wallets/:walletId/reconciliation` para conferir o saldo e os lançamentos.

```json
{
  "providerId": "provider-a",
  "externalTransactionId": "bet-001",
  "walletId": "SUBSTITUA_PELO_ID_DA_WALLET",
  "playerId": "SUBSTITUA_PELO_UUID_DO_JOGADOR",
  "roundId": "round-1",
  "gameId": "game-1",
  "kind": "BET",
  "money": {"amount": "80.00", "currency": "BRL"}
}
```

O SQS recebe o mesmo objeto em `data`, acrescido de `idempotencyKey`, dentro de um envelope com `messageId`, `occurredAt` e `type: WagerTransactionRequested`. O destino da outbox é `wager-events.fifo`.

## Windows / PowerShell

Com Docker Desktop usando containers Linux e Bun instalado:

```powershell
docker compose config --quiet
docker compose up --build --scale api=3 -d
3000..3002 | ForEach-Object { Invoke-RestMethod "http://localhost:$_/health/ready" }
bun install --frozen-lockfile
bun run typecheck
bun test src/domain.test.ts src/models.test.ts
$env:TEST_API_URLS = 'http://localhost:3000,http://localhost:3001,http://localhost:3002'
bun test src/integration.test.ts
$env:DATABASE_URL = 'postgres://wager:wager@localhost:5432/wager'
$env:TEST_POSTGRES = '1'
bun test src/postgres.test.ts
$env:TEST_SQS = '1'
bun test src/messaging.test.ts
.\scripts\verify-restart.ps1
docker compose logs --tail 100 api
docker compose stop -t 30
```

A suíte PostgreSQL aplica migrations e grava fixtures permanentes com UUIDs novos. Use um banco de testes. Se já houver PostgreSQL na porta 5432, defina `$env:POSTGRES_PORT = '55432'` antes de subir o Compose e ajuste `DATABASE_URL`. As suítes HTTP/PostgreSQL/SQS são ignoradas quando suas variáveis não estão definidas; testes ignorados não comprovam integração. Os testes HTTP verificam liveness e operações financeiras; com `TEST_SQS=1` também exigem readiness de todas as instâncias.

Para os testes de sinais e crash, execute a suíte SQS no Linux do container (ela cria filas FIFO isoladas, sem consumir a entrada das APIs):

```powershell
docker compose run --rm -e TEST_SQS=1 api bun test src/messaging.test.ts
```

Para repetir toda a validação em containers, mantendo três APIs em execução:

```powershell
# Use uma porta livre; 55433 foi utilizada nesta revisão.
$env:POSTGRES_PORT = '55433'
docker compose up --build --scale api=3 -d
docker compose run --rm --no-deps api bun run typecheck
docker compose run --rm -e TEST_POSTGRES=1 -e TEST_SQS=1 api bun test src/domain.test.ts src/models.test.ts src/postgres.test.ts src/messaging.test.ts
$env:TEST_API_URLS = 'http://localhost:3000,http://localhost:3001,http://localhost:3002'
$env:TEST_SQS = '1'
bun test src/integration.test.ts
.\scripts\verify-restart.ps1
```

`verify-restart.ps1` cria uma carteira, aposta R$ 80, credita R$ 30, reinicia as três APIs e exige que o replay devolva R$ 20 enquanto a reconciliação mantém saldo atual de R$ 50. Ele deixa as APIs em execução. A suíte HTTP também distribui 60 operações entre 12 carteiras simultaneamente e concilia todas elas.

O teste cria também um schema PostgreSQL isolado, herdado pelos subprocessos por `DATABASE_SCHEMA`, para que os publishers das APIs não disputem as fixtures. Remove filas e schema ao terminar. Ele mata subprocessos depois do commit financeiro e depois do envio à fila e verifica o ledger no fim. A falha operacional do teste de retry é injetada na porta do use case; a contagem de entregas, visibilidade, backoff e DLQ são reais. Nenhum endpoint de falha é exposto pela API. No Windows, a entrega de sinais para subprocessos não tem a mesma semântica de SIGTERM do Linux; use o comando acima para comprovar o shutdown.

Com um PostgreSQL de testes já acessível em `DATABASE_URL`, também é possível executar o financeiro sem SQS:

```powershell
bun src/support/verify-http.ts
```

Esse comando aplica migrations, inicia três processos nas portas 3100–3102, executa os testes HTTP e encerra os processos. Os logs ficam em `.test-artifacts/`. Readiness continua retornando 503 enquanto SQS estiver indisponível. Essa execução não comprova a integração com o broker.

Sem Bun global, é possível instalar as dependências com `npm install`, baixar Bun local com `npm install --no-save --package-lock=false bun` e substituir `bun` por `.\node_modules\.bin\bun.cmd`. O typecheck também pode ser executado com `npm exec -- tsc --noEmit`. O lock oficial do projeto continua sendo `bun.lock`.

## Resultado da validação desta revisão

O enunciado foi lido em `C:\Users\ramoo\Downloads\Markdown colado(2).md`. A modelagem agora inclui Money, Wallet, WagerTransaction, WalletLedgerEntry, InboxMessage e OutboxMessage, com factories, reidratação e transições explícitas usadas pelo processamento.

Passaram typecheck no container, 16 testes unitários, 12 testes PostgreSQL, oito testes SQS e oito testes HTTP com três APIs reais: **44 testes, 318 assertions**, sem falhas, além do teste PowerShell de reinício e da configuração do Compose. O ambiente final usou PostgreSQL 16, LocalStack 4 e três APIs via Compose. A comparação SQL final encontrou **29 carteiras e zero divergências** entre saldo e ledger no schema principal; as fixtures financeiras da suíte SQS também foram conciliadas antes da remoção de seu schema isolado.

Foram comprovados: um débito para 50 duplicatas, saldo final de R$ 20 na disputa de saldo, 12 carteiras processadas em paralelo, replay com saldo original após WIN/LOSS e após reinício, reversões antecipadas/duplicadas, rollback sem fundos, expiração, constraints, FAILED, atomicidade sob falha da outbox, migrations reversíveis, redelivery após crash antes do ack, recuperação após commit antes da publicação, publishers concorrentes, republicação após envio, retry com backoff, DLQ, redrive automático e shutdown com drain. O consumidor de referência em `src/event-consumer.ts` comprovou um único efeito SQL downstream entre processos, após reinício e com mensagens de deduplicação FIFO diferentes.

`/metrics` agora consulta as quantidades aproximadas visível, em voo e atrasada da DLQ, incluindo redrive automático. Se o broker estiver indisponível, retorna `dlqMetricsAvailable: false` e valores null, preservando as outras métricas. As regras de referência, reversão, expiração e efeito financeiro estão encapsuladas em WagerTransaction.

O banco temporário foi encerrado e seus arquivos ficaram em `.test-postgres/` (ignorado por Git e Docker). Com os binários PostgreSQL instalados, ele pode ser reaberto assim:

```powershell
pg_ctl -D .test-postgres -l .test-postgres/server.log -o '-p 55432 -h 127.0.0.1' start
$env:DATABASE_URL = 'postgres://wager@127.0.0.1:55432/wager'
$env:TEST_POSTGRES = '1'
bun test src/postgres.test.ts
bun src/support/verify-http.ts
pg_ctl -D .test-postgres -m fast stop
```

O bloqueio inicial do Docker foi resolvido ao encerrar sua interface travada e iniciar novamente o backend. Não restaram testes bloqueados nesta revisão. Os containers foram mantidos em execução: APIs nas portas 3000–3002, PostgreSQL na 55433 e LocalStack na 4566. Veja as limitações operacionais documentadas em `ARCHITECTURE.md`.
# distributed-wagering-processor
