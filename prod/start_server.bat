@echo off
REM 
set SCRIPT_DIR=%~dp0

REM 
cd /d "%SCRIPT_DIR%"

REM 
start "" cmd /k python "%SCRIPT_DIR%web.py"

REM
timeout /t 5 /nobreak >nul

REM
start http://127.0.0.1:1000