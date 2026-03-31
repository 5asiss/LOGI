#!/usr/bin/env bash
# Render 등 PaaS: PORT 환경변수를 셸에서 확실히 넣어서 gunicorn 실행
set -e
PORT="${PORT:-5000}"
echo "Binding to 0.0.0.0:$PORT"
# --timeout: 앱 로딩(init_db, load_db_to_mem 등)이 느릴 수 있어 120초로 설정
exec gunicorn -w 1 --bind "0.0.0.0:$PORT" --timeout 120 --log-level info app:app
