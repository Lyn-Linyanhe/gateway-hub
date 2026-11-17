@echo off
rem gateway-hub aggregate router - silent detached launcher
set "HERE=%~dp0"
if not exist "%HERE%python\pythonw.exe" goto use_path
set "PYW=%HERE%python\pythonw.exe"
goto run
:use_path
where pythonw.exe >nul 2>&1
if errorlevel 1 (
  echo [ERROR] pythonw.exe not found.
  exit /b 1
)
set "PYW=pythonw.exe"
:run
start "" "%PYW%" "%HERE%router.py"
if errorlevel 1 exit /b 1
echo [INFO] Launch requested. Service readiness is not verified. Logs: %HERE%router.log
