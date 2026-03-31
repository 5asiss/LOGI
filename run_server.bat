@echo off
chcp 65001 >nul
echo ========================================
echo   LOGI 서버 실행 (내 서버용)
echo ========================================
cd /d "%~dp0"

REM 서버 모드: 디버그 끔, localhost(127.0.0.1)에서만 접속
set FLASK_DEBUG=0
if "%PORT%"=="" set PORT=5000

echo 포트: %PORT%
echo 브라우저: http://localhost:%PORT%
echo 종료: Ctrl+C
echo.

python app.py

pause
