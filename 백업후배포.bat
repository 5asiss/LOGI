@echo off
chcp 65001 >nul
echo 배포 전 DB 백업 중...
python "%~dp0backup_before_deploy.py"
if errorlevel 1 (
    echo 백업 실패. 배포를 중단할까요? 일단 계속 진행합니다.
)
echo.
echo 백업 후 배포를 진행하세요 (git push 등).
pause
