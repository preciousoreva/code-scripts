@echo off
REM Scheduled all-companies run: calls Django management command so runs appear in the dashboard.
REM Lock and logging are handled inside the management command; this script just activates venv and runs it.
REM Exit codes: 0 = success, 1 = pipeline failure, 2 = lock held (another run active).

setlocal
set "REPO=%~dp0"
set "REPO=%REPO:~0,-1%"

REM Optional: if lock already exists, exit 2 without starting Python (saves startup time on repeated scheduler triggers)
if exist "%REPO%\runtime\global_run.lock" (
    echo Another run is already active. Exiting.
    exit /b 2
)

cd /d "%REPO%"
if exist "%REPO%\.venv\Scripts\activate.bat" (
    call "%REPO%\.venv\Scripts\activate.bat"
)

python manage.py run_scheduled_all_companies --parallel 2
exit /b %ERRORLEVEL%
