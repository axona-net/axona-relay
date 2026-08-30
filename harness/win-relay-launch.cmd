@echo off
REM win-relay-launch.cmd <region> <bridge> <arm:0|1> <logfile> <armfix:0|1>
REM Launcher invoked by the schtasks task win-relay.sh creates. The env vars
REM live HERE (not inlined in schtasks /TR, which caps at 261 chars). arm=1 sets
REM the four connection-quality stack vars (Arm B); armfix=1 sets the routing
REM fix vars FINDK_SKIP_DEAD + SUB_TERMINAL_VERIFY (Arm C). Composes.
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
if "%~5"=="1" (
  set FINDK_SKIP_DEAD=1
  set SUB_TERMINAL_VERIFY=1
)
node src\index.js >> %~4 2>&1
