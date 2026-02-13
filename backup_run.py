#!/usr/bin/env python3
"""백업 실행 스크립트 - Windows 작업 스케줄러에서 오전 4시에 실행하도록 설정"""
import os
import sys

# 프로젝트 루트를 path에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# app 모듈에서 run_backup 가져오기 (DB 초기화 없이)
os.chdir(os.path.dirname(os.path.abspath(__file__)))

def run_backup_standalone():
    import shutil
    import zipfile
    from datetime import datetime, timezone, timedelta
    KST = timezone(timedelta(hours=9))
    now = datetime.now(KST)
    BACKUP_DIR = os.environ.get('BACKUP_DIR', r'c:\logi\backup')
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)
        today = now.strftime('%Y%m%d')
        ledger_src = os.path.join(PROJECT_ROOT, 'ledger.db')
        evidences_src = os.path.join(PROJECT_ROOT, 'static', 'evidences')
        evidences_dst = os.path.join(BACKUP_DIR, 'evidences')
        if os.path.exists(ledger_src):
            shutil.copy2(ledger_src, os.path.join(BACKUP_DIR, 'ledger.db'))
        if os.path.exists(evidences_src):
            if os.path.exists(evidences_dst):
                shutil.rmtree(evidences_dst)
            shutil.copytree(evidences_src, evidences_dst)
        for fname in ['.env.example', 'requirements.txt']:
            src = os.path.join(PROJECT_ROOT, fname)
            if os.path.exists(src):
                shutil.copy2(src, os.path.join(BACKUP_DIR, fname))
        zip_path = os.path.join(BACKUP_DIR, f'logi_backup_{today}.zip')
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            lb = os.path.join(BACKUP_DIR, 'ledger.db')
            if os.path.exists(lb):
                zf.write(lb, 'ledger.db')
            if os.path.exists(evidences_dst):
                for root, _, files in os.walk(evidences_dst):
                    for f in files:
                        fp = os.path.join(root, f)
                        zf.write(fp, os.path.join('evidences', os.path.relpath(fp, evidences_dst)))
            for fname in ['.env.example', 'requirements.txt']:
                bf = os.path.join(BACKUP_DIR, fname)
                if os.path.exists(bf):
                    zf.write(bf, fname)
        print(f"백업 완료: {zip_path}")
        return 0
    except Exception as e:
        print(f"백업 실패: {e}")
        return 1

if __name__ == '__main__':
    sys.exit(run_backup_standalone())
