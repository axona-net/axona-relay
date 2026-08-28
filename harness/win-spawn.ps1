# =============================================================================
# harness/win-spawn.ps1 — spawn one sidecar on Windows via a one-shot
# scheduled task, the only launch that survives ssh disconnect on this box.
#
# Direct shapes all died with their session: bash nohup (three variants,
# seeds 8/9/98) and Start-Process (seed 97) — Windows OpenSSH tears down
# the session's process tree on disconnect. schtasks runs the payload in
# its OWN logon session; /SC ONCE with a past /ST plus an explicit /Run
# fires it immediately. The task name is unique per (seed, peer) and /F
# overwrites any prior definition.
#
#   powershell -ExecutionPolicy Bypass -File harness\win-spawn.ps1 `
#     -Peer 6 -Seed 10 -Nodes 8 -DurationMs 600000 -OpenN 4 -OwnedN 2
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
$name = "axona-harness-$Seed-$Peer"
$tr = "powershell -ExecutionPolicy Bypass -File $repo\harness\win-run-sidecar.ps1 -Peer $Peer -Seed $Seed -Nodes $Nodes -DurationMs $DurationMs -OpenN $OpenN -OwnedN $OwnedN -Region $Region -Bridge $Bridge"
schtasks /Create /F /TN $name /SC ONCE /ST 00:00 /TR $tr | Out-Null
schtasks /Run /TN $name | Out-Null
Write-Output "win-spawn: task $name started"
