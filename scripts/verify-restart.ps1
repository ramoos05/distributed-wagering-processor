param([string[]]$Origins = @('http://localhost:3000', 'http://localhost:3001', 'http://localhost:3002'))
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Wait-Ready {
    foreach ($origin in $Origins) {
        $deadline = [DateTime]::UtcNow.AddSeconds(40)
        $ready = $false
        while ([DateTime]::UtcNow -lt $deadline) {
            try {
                $ready = (Invoke-RestMethod "$origin/health/ready" -TimeoutSec 3).status -eq 'ok'
            } catch { $ready = $false }
            if ($ready) { break }
            Start-Sleep -Milliseconds 200
        }
        if (-not $ready) { throw "API indisponivel: $origin" }
    }
}
function Send-Json($origin, $path, $payload, $key) {
    $headers = @{}
    if ($key) { $headers['Idempotency-Key'] = $key }
    Invoke-RestMethod "$origin$path" -Method Post -ContentType 'application/json' -Headers $headers `
        -Body ($payload | ConvertTo-Json -Depth 10) -TimeoutSec 10
}

Wait-Ready
$playerId = [guid]::NewGuid().ToString()
$wallet = Send-Json $Origins[0] '/wallets' @{ playerId = $playerId; initialBalance = @{ amount = '100.00'; currency = 'BRL' } } $null
$key = [guid]::NewGuid().ToString()
$bet = @{ providerId = 'restart-test'; playerId = $playerId; walletId = $wallet.id; roundId = $key;
    gameId = 'test'; kind = 'BET'; money = @{ amount = '80.00'; currency = 'BRL' }; externalTransactionId = $key }
$original = Send-Json $Origins[0] '/wagering/transactions' $bet $key
if ($original.status -ne 'PROCESSED' -or $original.balance.amount -ne '20.00') { throw 'Aposta inicial incorreta' }
$win = $bet.Clone()
$win.kind = 'WIN'
$win.money = @{ amount = '30.00'; currency = 'BRL' }
$win.externalTransactionId = [guid]::NewGuid().ToString()
$credited = Send-Json $Origins[1] '/wagering/transactions' $win $win.externalTransactionId
if ($credited.balance.amount -ne '50.00') { throw 'Credito incorreto' }

docker compose restart api
if ($LASTEXITCODE -ne 0) { throw 'Falha ao reiniciar as APIs' }
Wait-Ready
$replay = Send-Json $Origins[2] '/wagering/transactions' $bet $key
$reconciliation = Invoke-RestMethod "$($Origins[2])/wallets/$($wallet.id)/reconciliation" -Method Post
if (-not $replay.idempotentReplay -or $replay.transactionId -ne $original.transactionId -or $replay.balance.amount -ne '20.00') {
    throw 'Replay divergiu depois do reinicio'
}
if (-not $reconciliation.consistent -or $reconciliation.storedBalance.amount -ne '50.00' -or $reconciliation.checkedEntries -ne 3) {
    throw 'Saldo e ledger divergiram depois do reinicio'
}
@{ scenario = 'restart-three-apis'; passed = $true; walletId = $wallet.id;
    originalReplayBalance = $replay.balance.amount; currentBalance = $reconciliation.storedBalance.amount;
    ledgerEntries = $reconciliation.checkedEntries } | ConvertTo-Json
