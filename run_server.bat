@echo off
chcp 65001 >nul
echo ========================================
echo   LOGI 서버 실행 (내 서버용)
echo ========================================
cd /d "%~dp0"

REM 서버 모드: 디버그 끔, 0.0.0.0으로 모든 IP에서 접속 가능
set FLASK_DEBUG=0
if "%PORT%"=="" set PORT=5001

echo 포트: %PORT%
echo 브라우저: http://localhost:%PORT%  또는  http://내서버IP:%PORT%
echo 종료: Ctrl+C
echo.

python app.py

pause
