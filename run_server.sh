#!/bin/bash
# LOGI 서버 실행 (내 서버용, Linux/Mac)
cd "$(dirname "$0")"

export FLASK_DEBUG=0
export PORT="${PORT:-5001}"

echo "========================================"
echo "  LOGI 서버 실행 (포트: $PORT)"
echo "========================================"
echo "접속: http://localhost:$PORT  또는  http://서버IP:$PORT"
echo "종료: Ctrl+C"
echo ""

exec python app.py
