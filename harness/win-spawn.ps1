# =============================================================================
# harness/win-spawn.ps1 — spawn one sidecar on Windows, detached from the
# ssh session's job object.
#
# Every bash-shaped launch tried on this box died with its ssh session
# (job-object teardown; seeds 8, 9, 98). Start-Process creates the child
# through CreateProcess outside the caller's console session, which is the
# documented way out. Env is set on THIS process and inherited by the child.
# stdout/stderr must be different files on Windows; the sidecar's phase
# markers ride stderr.
#
#   powershell -ExecutionPolicy Bypass -File harness\win-spawn.ps1 `
#     -Peer 0 -Seed 98 -Nodes 3 -DurationMs 90000 -OpenN 2 -OwnedN 1 `
#     -Region eagle -Bridge wss://testnet.axona.net
# =============================================================================
param(
  [Parameter(Mandatory)][int]$Peer,
  [Parameter(Mandatory)][int]$Seed,
  [Parameter(Mandatory)][int]$Nodes,
  [Parameter(Mandatory)][int]$DurationMs,
  [int]$OpenN = 0, [int]$OwnedN = 0,
  [string]$Region = 'eagle',
  [string]$Bridge = 'wss://testnet.axona.net'
)
$repo = 'C:\Users\david\github\axona-relay'
$env:HOST = 'axona-win'; $env:OS_TAG = 'win32'
$env:PEER_IDX = "$Peer"; $env:NODES = "$Nodes"; $env:SEED = "$Seed"
$env:DURATION_MS = "$DurationMs"; $env:REGION = $Region; $env:BRIDGE = $Bridge
$env:LEDGER_DIR = 'harness/results'
if ($OpenN -gt 0) { $env:OPEN_N = "$OpenN" }
if ($OwnedN -gt 0) { $env:OWNED_N = "$OwnedN" }
New-Item -ItemType Directory -Force -Path "$repo\harness\results" | Out-Null
$p = Start-Process -FilePath 'node' `
  -ArgumentList 'harness/sidecar.mjs', '--peer', "$Peer" `
  -WorkingDirectory $repo -WindowStyle Hidden -PassThru `
  -RedirectStandardOutput "$repo\harness\results\sidecar-$Seed-$Peer.stdout.log" `
  -RedirectStandardError  "$repo\harness\results\sidecar-$Seed-$Peer.out"
Write-Output "win-spawn.ps1: peer $Peer pid $($p.Id)"
