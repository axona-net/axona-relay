# relayctl.ps1 — the one controller for the Windows relay fleet (SCM services).
#
# Every relay on this host is a Windows service, axona-relay-01 … -NN, run by
# relaysvc.exe (relaysvc.cs). The host's facts and layout live in
# hosts\<host>.json; this script reads them and never guesses.
#
#   powershell -NoProfile -ExecutionPolicy Bypass -File windows\relayctl.ps1 status
#   ... relayctl.ps1 install                    # build wrapper, create services (starts nothing)
#   ... relayctl.ps1 cutover -Kernel 4.100.0    # bare node.exe relays -> services, start-then-kill
#   ... relayctl.ps1 roll    -Kernel 4.101.0    # pull, install, then restart slot by slot behind a gate
#   ... relayctl.ps1 start|stop -Slot 7
#   ... relayctl.ps1 uninstall                  # stop and delete every service
#
# Every mutating command writes a transcript to relay-logs\relayctl-<UTC>.out on
# the host, so the result is read there, never from a buffered ssh stream.
# Status lines go to the host stream (Write-Host) so Status can return a bool.
# Exit code: 0 = done and verified, 1 = stopped by a gate or a check.

param(
  [Parameter(Position = 0, Mandatory = $true)]
  [ValidateSet('status', 'install', 'cutover', 'roll', 'start', 'stop', 'uninstall')]
  [string]$Command,
  [int]$Slot = 0,
  [string]$Kernel = '',
  [int]$AdvanceCap = 90,
  [string]$HostName = 'axona-win'
)
$ErrorActionPreference = 'Stop'

$Repo     = Split-Path -Parent $PSScriptRoot
$Manifest = Get-Content (Join-Path $Repo "hosts\$HostName.json") -Raw | ConvertFrom-Json
$Count    = [int]$Manifest.services.count
$Prefix   = $Manifest.services.name_prefix
$WrapDir  = $Manifest.services.wrapper_dir
$Wrapper  = $Manifest.services.wrapper
$LogDir   = $Manifest.services.log_dir
$RelayDir = $Manifest.relay.repo
$Csc      = $Manifest.facts.compiler

function SvcName([int]$i) { $Prefix + $i.ToString('00') }
function LogPath([int]$i) { Join-Path $LogDir ('svc-' + $i.ToString('00') + '.log') }
function Fail([string]$m) { Write-Output "ABORT: $m"; throw "relayctl: $m" }

# ---- process inventory -----------------------------------------------------
# A service relay is node.exe whose parent is the cmd.exe launched by a
# relaysvc.exe. Any other node.exe running src/index.js is a BARE relay.
function Inventory {
  $all = @(Get-CimInstance Win32_Process)
  $byPid = @{}; foreach ($p in $all) { $byPid[[int]$p.ProcessId] = $p }
  $svcNodes = @{}; $bare = @()
  foreach ($p in $all) {
    if ($p.Name -ne 'node.exe' -or $p.CommandLine -notmatch 'src[\\/]index\.js') { continue }
    $parent = $byPid[[int]$p.ParentProcessId]
    $grand  = if ($parent) { $byPid[[int]$parent.ParentProcessId] } else { $null }
    if ($grand -and $grand.Name -eq 'relaysvc.exe' -and $grand.CommandLine -match 'svc-(\d\d)\.log') {
      $svcNodes[[int]$Matches[1]] = $p
    } else { $bare += $p }
  }
  [pscustomobject]@{ Service = $svcNodes; Bare = $bare }
}

# ---- readiness gate (the Fleet Cadence Standard, same rule as fleet-cadence.sh)
$StateRe = 'state=([a-z]+) peers=\d+ synaptome=\d+ mesh\(open/bound\)=(\d+)/(\d+)'
function LastState([int]$i) {
  $log = LogPath $i
  if (-not (Test-Path $log)) { return $null }
  $hit = Select-String -Path $log -Pattern $StateRe -AllMatches -ErrorAction SilentlyContinue | Select-Object -Last 1
  if ($hit) { $hit.Matches[-1].Value } else { $null }
}
function LooseReady([string]$line) {
  if (-not $line) { return $false }
  if ($line -notmatch $StateRe) { return $false }
  $state = $Matches[1]; $open = [int]$Matches[2]; $bound = [int]$Matches[3]
  if ($state -eq 'open' -or $state -eq 'graduated') { return $true }
  return ($bound -ge 3 -and $open -ge ($bound - 1))
}
function AwaitReady([int]$i) {
  $deadline = (Get-Date).AddSeconds($AdvanceCap)
  $line = $null
  while ((Get-Date) -lt $deadline) {
    $svc = Get-Service (SvcName $i)
    if ($svc.Status -ne 'Running') { Start-Sleep 3; continue }
    $line = LastState $i
    if (LooseReady $line) { return $line }
    Start-Sleep 3
  }
  Fail ("slot {0}: not bridged+bonded within {1}s (last: {2})" -f $i, $AdvanceCap, $(if ($line) { $line } else { 'none' }))
}
function AssertKernel([int]$i, [string]$k) {
  if (-not (Select-String -Path (LogPath $i) -SimpleMatch "kernel v$k" -Quiet)) {
    Fail "slot ${i}: log has no 'kernel v$k' banner"
  }
}

# ---- checkout preparation -----------------------------------------------------
function VendoredKernel { (Get-Content (Join-Path $RelayDir 'vendor\axona-protocol\package.json') -Raw | ConvertFrom-Json).version }
function Prep([string]$k, [switch]$Pull) {
  if (-not $k) { Fail '-Kernel <x.y.z> is required' }
  if ($Pull) {
    # npm install on Windows rewrites package-lock.json (drops an optional
    # android entry), and that drift blocks the next ff-only pull. Discard it.
    & git -C $RelayDir checkout -- package-lock.json
    & git -C $RelayDir fetch origin $Manifest.relay.branch -q; if ($LASTEXITCODE) { Fail 'git fetch failed' }
    & git -C $RelayDir pull --ff-only origin $Manifest.relay.branch -q; if ($LASTEXITCODE) { Fail 'git pull --ff-only failed (diverged); resolve by hand' }
    Push-Location $RelayDir
    try { & npm.cmd install --no-audit --no-fund *> $null; if ($LASTEXITCODE) { Fail 'npm install failed' } } finally { Pop-Location }
  }
  $v = VendoredKernel
  if ($v -ne $k) { Fail "vendored kernel $v != -Kernel $k" }
  Push-Location $RelayDir
  try {
    & node -e 'const n=require("node-datachannel");const p=new n.PeerConnection("t",{iceServers:[]});p.createDataChannel("x");p.close();setTimeout(()=>process.exit(0),300);'
    if ($LASTEXITCODE) { Fail 'node-datachannel failed to load' }
  } finally { Pop-Location }
  Write-Output ("  checkout head {0}, vendored kernel {1}, node-datachannel loads" -f (& git -C $RelayDir rev-parse --short HEAD), $v)
}

# ---- wrapper build + service definitions ------------------------------------
function BuildWrapper {
  New-Item -ItemType Directory -Force -Path $WrapDir | Out-Null
  $src = Join-Path $PSScriptRoot 'relaysvc.cs'
  $want = (Get-FileHash $src -Algorithm SHA256).Hash
  $stamp = Join-Path $WrapDir 'relaysvc.cs.sha256'
  $have = if (Test-Path $stamp) { (Get-Content $stamp -Raw).Trim() } else { '' }
  if ((Test-Path $Wrapper) -and $have -eq $want) { Write-Output '  wrapper current'; return }
  $running = @(Get-Service "$Prefix*" -ErrorAction SilentlyContinue | Where-Object Status -eq 'Running')
  if ($running.Count -gt 0) { Fail "wrapper source changed but $($running.Count) service(s) are running; stop them first" }
  $refs = '/r:System.ServiceProcess.dll'
  & $Csc /nologo /optimize+ $refs "/out:$Wrapper" $src
  if ($LASTEXITCODE) { Fail 'csc failed to build relaysvc.exe' }
  Set-Content -Path $stamp -Value $want
  Write-Output "  wrapper built: $Wrapper"
}

function EnvBlock {
  $Manifest.relay.env.PSObject.Properties | ForEach-Object { '{0}={1}' -f $_.Name, $_.Value }
}

function DefineService([int]$i) {
  $name = SvcName $i
  $bin = '"{0}" "{1}" "{2}"' -f $Wrapper, $RelayDir, (LogPath $i)
  if (-not (Get-Service $name -ErrorAction SilentlyContinue)) {
    New-Service -Name $name -BinaryPathName $bin -DisplayName "Axona relay $($i.ToString('00'))" `
      -Description "Axona relay slot $i (region $($Manifest.relay.region)); managed by relayctl.ps1" -StartupType Automatic | Out-Null
  } else {
    # Not `sc.exe config binPath=`: Windows PowerShell 5.1 strips the embedded quotes.
    Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\$name" -Name ImagePath -Value $bin
  }
  & sc.exe config $name start= delayed-auto | Out-Null
  & sc.exe failure $name reset= 86400 actions= restart/10000/restart/30000/restart/60000 | Out-Null
  & sc.exe failureflag $name 1 | Out-Null
  $key = "HKLM:\SYSTEM\CurrentControlSet\Services\$name"
  New-ItemProperty -Path $key -Name Environment -PropertyType MultiString -Value ([string[]](EnvBlock)) -Force | Out-Null
}

# ---- commands -------------------------------------------------------------------
function Status {
  $inv = Inventory
  $running = 0; $kernels = @{}
  for ($i = 1; $i -le $Count; $i++) {
    $svc = Get-Service (SvcName $i) -ErrorAction SilentlyContinue
    $st = if ($svc) { [string]$svc.Status } else { 'absent' }
    $node = $inv.Service[$i]
    $k = $null
    if (Test-Path (LogPath $i)) {
      $m = Select-String -Path (LogPath $i) -Pattern 'kernel v(\d+\.\d+\.\d+)' | Select-Object -First 1
      if ($m) { $k = $m.Matches[0].Groups[1].Value }
    }
    if ($st -eq 'Running' -and $node) { $running++; if ($k) { $kernels[$k] = 1 + [int]$kernels[$k] } }
    Write-Host ('{0}  {1,-8} relay={2,-6} kernel={3,-8} {4}' -f (SvcName $i), $st, $(if ($node) { $node.ProcessId } else { '-' }), $(if ($k) { $k } else { '?' }), $(LastState $i))
  }
  $ks = ($kernels.GetEnumerator() | ForEach-Object { "$($_.Value)x$($_.Key)" }) -join ' '
  Write-Host ("SUMMARY services_live={0}/{1} bare={2} kernels=[{3}]" -f $running, $Count, $inv.Bare.Count, $ks)
  return ($running -eq $Count -and $inv.Bare.Count -eq 0)
}

function WithLock([scriptblock]$body) {
  $mutex = New-Object System.Threading.Mutex($false, 'Global\axona-relayctl')
  $got = $false
  try { $got = $mutex.WaitOne(0) } catch [System.Threading.AbandonedMutexException] { $got = $true }
  if (-not $got) { Fail 'another relayctl holds Global\axona-relayctl' }
  New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
  $out = Join-Path $LogDir ('relayctl-{0}.out' -f (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ'))
  Start-Transcript -Path $out | Out-Null
  Write-Output "TRANSCRIPT=$out"
  try { & $body; Write-Output 'RESULT=OK' }
  catch { Write-Output "RESULT=FAIL $($_.Exception.Message)"; $script:rc = 1 }
  finally { Stop-Transcript | Out-Null; $mutex.ReleaseMutex(); $mutex.Dispose() }
}

$script:rc = 0
switch ($Command) {
  'status' { if (-not (Status)) { $script:rc = 1 } }

  'install' { WithLock {
    BuildWrapper
    for ($i = 1; $i -le $Count; $i++) { DefineService $i }
    Write-Output "  $Count services defined; none started"
  } }

  'cutover' { WithLock {
    Prep $Kernel
    BuildWrapper
    $inv = Inventory
    $bare = @($inv.Bare)
    Write-Output ("  bare relays {0}, target services {1}" -f $bare.Count, $Count)
    for ($i = 1; $i -le $Count; $i++) {
      DefineService $i
      $svc = Get-Service (SvcName $i)
      if ($svc.Status -ne 'Running') { Start-Service (SvcName $i) }
      $line = AwaitReady $i
      AssertKernel $i $Kernel
      $retired = '-'
      if ($bare.Count -gt 0) { $b = $bare[0]; $bare = @($bare | Select-Object -Skip 1); Stop-Process -Id $b.ProcessId -Force; $retired = $b.ProcessId }
      Write-Output ("  slot {0}/{1} ready ({2}); retired bare pid {3}" -f $i, $Count, $line, $retired)
    }
    foreach ($b in $bare) { Stop-Process -Id $b.ProcessId -Force; Write-Output "  retired surplus bare pid $($b.ProcessId)" }
    Start-Sleep 5
    if (-not (Status)) { Fail 'post-cutover status is not services_live=count, bare=0' }
  } }

  'roll' { WithLock {
    Prep $Kernel -Pull
    BuildWrapper
    for ($i = 1; $i -le $Count; $i++) {
      Restart-Service (SvcName $i)
      $line = AwaitReady $i
      AssertKernel $i $Kernel
      Write-Output ("  slot {0}/{1} restarted on {2} ({3})" -f $i, $Count, $Kernel, $line)
    }
    if (-not (Status)) { Fail 'post-roll status is not services_live=count, bare=0' }
  } }

  'start' { WithLock { if ($Slot -lt 1) { Fail '-Slot required' }; Start-Service (SvcName $Slot); Write-Output "  $(AwaitReady $Slot)" } }
  'stop'  { WithLock { if ($Slot -lt 1) { Fail '-Slot required' }; Stop-Service (SvcName $Slot); Write-Output "  stopped $(SvcName $Slot)" } }

  'uninstall' { WithLock {
    Get-Service "$Prefix*" -ErrorAction SilentlyContinue | ForEach-Object {
      if ($_.Status -ne 'Stopped') { Stop-Service $_.Name -Force }
      & sc.exe delete $_.Name | Out-Null
      Write-Output "  deleted $($_.Name)"
    }
  } }
}
exit $script:rc
