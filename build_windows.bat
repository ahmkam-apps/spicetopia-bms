@echo off
:: ────────────────────────────────────────────────────────────────
::  Spicetopia ERP v2 — Windows EXE Builder
::  Run this ONCE on a Windows machine that has Python installed.
::  Produces:  dist\Spicetopia.exe
::  That single .exe can then be copied to any Windows PC.
:: ────────────────────────────────────────────────────────────────

cd /d "%~dp0"

echo.
echo ╔══════════════════════════════════════════╗
echo ║   SPICETOPIA — BUILD WINDOWS EXE         ║
echo ╚══════════════════════════════════════════╝
echo.

:: Check Python
where python >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found. Install from https://www.python.org/downloads/
    pause & exit /b 1
)
python --version

:: Install / upgrade build tools
echo.
echo Installing build dependencies...
pip install pyinstaller reportlab --quiet --upgrade
if errorlevel 1 (
    echo ERROR: pip install failed.
    pause & exit /b 1
)

:: Clean previous build
echo.
echo Cleaning previous build...
if exist build rmdir /s /q build
if exist dist  rmdir /s /q dist

:: Build
echo.
echo Building Spicetopia.exe — this takes 1-3 minutes...
echo.
pyinstaller spicetopia.spec
if errorlevel 1 (
    echo.
    echo BUILD FAILED. See errors above.
    pause & exit /b 1
)

echo.
echo ╔══════════════════════════════════════════╗
echo ║   BUILD COMPLETE                          ║
echo ╚══════════════════════════════════════════╝
echo.
echo   Your executable is at:
echo   %~dp0dist\Spicetopia.exe
echo.
echo   Copy Spicetopia.exe to any Windows PC — no Python needed.
echo   The .db file must still be accessible (OneDrive or local).
echo.
pause
