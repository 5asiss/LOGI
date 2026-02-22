#!/usr/bin/env bash
# Render 등 PaaS: PORT 환경변수를 셸에서 확실히 넣어서 gunicorn 실행
set -e
PORT="${PORT:-10000}"
echo "Binding to 0.0.0.0:$PORT"
exec gunicorn -w 1 app:app --bind "0.0.0.0:$PORT"
