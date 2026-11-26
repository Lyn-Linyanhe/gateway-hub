@echo off
rem gateway-hub one-shot launcher: gateway + engine processes of enabled platforms
rem (ASCII only: cmd.exe parses .bat in the OEM codepage, UTF-8 Chinese breaks it)
chcp 65001 >nul
set "HERE=%~dp0"
if exist "%HERE%python\python.exe" goto embedded
where python.exe >nul 2>&1
if errorlevel 1 (
  echo [ERROR] python.exe not found.
  exit /b 1
)
python "%HERE%start_all.py"
exit /b %errorlevel%
:embedded
"%HERE%python\python.exe" "%HERE%start_all.py"
