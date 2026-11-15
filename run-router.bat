@echo off
rem gateway-hub aggregate router - silent detached launcher
set "HERE=%~dp0"
if not exist "%HERE%python\python.exe" goto use_path
set "PYW=%HERE%python\pythonw.exe"
goto run
:use_path
set "PYW=pythonw.exe"
:run
start "" "%PYW%" "%HERE%router.py"
echo [OK] router started (port from config.json, default 8787). Logs: %HERE%router.log
