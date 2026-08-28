# =============================================================================
# harness/win-run-sidecar.ps1 — the scheduled task's payload: run ONE sidecar
# synchronously in the task's own logon session.
#
# Invoked by win-spawn.ps1 through schtasks, never directly over ssh — the
# whole point is that the task session is not the ssh session, so the
# sidecar survives the disconnect that killed every direct launch shape
# (seeds 8/9/98/97: bash nohup ×3 and Start-Process all died with the
# session; the held-session probes prove node itself runs fine).
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
Set-Location $repo
$env:HOST = 'axona-win'
$env:PEER_IDX = "$Peer"; $env:NODES = "$Nodes"; $env:SEED = "$Seed"
$env:DURATION_MS = "$DurationMs"; $env:REGION = $Region; $env:BRIDGE = $Bridge
$env:LEDGER_DIR = 'harness/results'
if ($OpenN -gt 0) { $env:OPEN_N = "$OpenN" }
if ($OwnedN -gt 0) { $env:OWNED_N = "$OwnedN" }
New-Item -ItemType Directory -Force -Path "$repo\harness\results" | Out-Null
# Synchronous: the task session owns node for the whole window. Redirects go
# through cmd for RAW BYTES — PowerShell's own 2> writes UTF-16 and doubles
# every artifact's size while breaking naive greps.
cmd /c "node harness\sidecar.mjs --peer $Peer 2> harness\results\sidecar-$Seed-$Peer.out 1> harness\results\sidecar-$Seed-$Peer.stdout.log"
exit 0
