@echo off
chcp 65001 >nul
echo 서버에서 받은 DB를 백업 폴더에 저장합니다.
echo (다운로드 폴더에 ledger_backup.db 가 있어야 합니다.)
echo.
set "DOWN=%USERPROFILE%\Downloads\ledger_backup.db"
if exist "%DOWN%" (
    python "%~dp0backup_before_deploy.py" "%DOWN%"
) else (
    echo 다운로드 폴더에 ledger_backup.db 가 없습니다.
    echo 먼저 사이트에서 로그인 후 "서버 DB 백업" 버튼으로 다운받으세요.
)
echo.
pause
