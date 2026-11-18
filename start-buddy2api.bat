@echo off
setlocal
title Buddy2api / QwenWork launcher
rem Usage:
rem   start-buddy2api.bat [port] [path-to-Buddy2api]
rem Default port 8792; default engine path is ../../Buddy2api next to gateway-hub.
rem This launcher deliberately clears HTTP(S)_PROXY: a stale local proxy
rem (e.g. FlClash on 127.0.0.1:7890 that is no longer running) makes every
rem QwenWork request fail with "All connection attempts failed".
rem It also pins the repository's .venv, which contains wasmtime==49.0.0
rem required by the QwenWork chat encoder.

set "PORT=%~1"
if "%PORT%"=="" set "PORT=8792"

set "ENGINE=%~2"
if "%ENGINE%"=="" set "ENGINE=%~dp0..\..\Buddy2api"
for %%I in ("%ENGINE%") do set "ENGINE=%%~fI"

if not exist "%ENGINE%\server.py" (
  echo [ERROR] Buddy2api not found: "%ENGINE%"
  echo Usage: start-buddy2api.bat [port] [path-to-Buddy2api]
  exit /b 1
)
if not exist "%ENGINE%\.venv\Scripts\python.exe" (
  echo [ERROR] Missing "%ENGINE%\.venv\Scripts\python.exe"
  echo Create it with:
  echo   cd /d "%ENGINE%"
  echo   python -m venv .venv
  echo   .venv\Scripts\python.exe -m pip install -r requirements.txt
  exit /b 1
)

set "HTTP_PROXY="
set "HTTPS_PROXY="
set "ALL_PROXY="
set "NO_PROXY=gateway.qwenwork.cn,localhost,127.0.0.1,::1"

"%ENGINE%\.venv\Scripts\python.exe" -c "import wasmtime" >nul 2>&1
if errorlevel 1 (
  echo [ERROR] .venv is missing wasmtime. Install requirements.txt into it.
  exit /b 1
)

echo ========================================
echo  Buddy 2 API - QwenWork
echo  Engine: "%ENGINE%"
echo  Listen: http://127.0.0.1:%PORT%
echo  Python: .venv (wasmtime available)
echo  Proxy : cleared; gateway.qwenwork.cn connects directly
echo ========================================
echo.

cd /d "%ENGINE%"
".venv\Scripts\python.exe" server.py --port %PORT%
