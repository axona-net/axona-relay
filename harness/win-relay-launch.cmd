@echo off
REM win-relay-launch.cmd <region> <bridge> <arm:0|1> <logfile>
REM Launcher invoked by the schtasks task win-relay.sh creates. The env vars
REM live HERE (not inlined in schtasks /TR, which caps at 261 chars — the four
REM RELAY_* arm names overflow it). Arm B passes arm=1 to set the four
REM connection-quality env vars; Arm A passes 0 and they stay unset.
cd /d C:\Users\david\github\axona-relay
set RELAY_REGION=%~1
set BRIDGE_URL=%~2
set RELAY_TUI=0
if "%~3"=="1" (
  set RELAY_SYNAPTOME_MAINTAIN=1
  set RELAY_ADMISSION_GATE=1
  set RELAY_ATTEMPT_GUARD=1
  set RELAY_PRESENCE=1
)
node src\index.js >> %~4 2>&1
