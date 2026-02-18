#!/usr/bin/env python3
"""배포 전 DB 백업
- 인자 없이 실행: 로컬 ledger.db 를 백업폴더에 복사
- 인자에 파일경로: (서버에서 다운받은 파일) 해당 파일을 백업폴더에 저장
  예: python backup_before_deploy.py C:\\Users\\new\\Downloads\\ledger_backup.db
"""
import os
import sys
import shutil
from datetime import datetime

# 백업 루트: C:\Users\new\Documents\GitHub\백업db
BACKUP_ROOT = r"C:\Users\new\Documents\GitHub\백업db"
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LEDGER_DB = os.path.join(PROJECT_ROOT, "ledger.db")


def backup_db(source_path=None):
    """source_path가 있으면 그 파일을, 없으면 로컬 ledger.db를 백업"""
    os.makedirs(BACKUP_ROOT, exist_ok=True)
    folder_name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dest_dir = os.path.join(BACKUP_ROOT, folder_name)
    os.makedirs(dest_dir, exist_ok=True)
    dest_file = os.path.join(dest_dir, "ledger.db")

    if source_path:
        src = os.path.abspath(source_path)
        if not os.path.isfile(src):
            print(f"오류: 파일 없음 — {src}")
            return 1
        shutil.copy2(src, dest_file)
        print(f"서버에서 받은 DB 백업 완료: {dest_file}")
    else:
        if not os.path.isfile(LEDGER_DB):
            print(f"경고: ledger.db 없음 — {LEDGER_DB}")
            return 1
        shutil.copy2(LEDGER_DB, dest_file)
        print(f"로컬 DB 백업 완료: {dest_file}")
    return 0


if __name__ == "__main__":
    source = sys.argv[1].strip() if len(sys.argv) > 1 else None
    exit(backup_db(source))
