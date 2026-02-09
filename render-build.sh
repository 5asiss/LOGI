#!/usr/bin/env bash
# exit on error
set -o errexit

pip install -r requirements.txt

# instance 폴더가 없으면 생성 (SQLite용)
mkdir -p instance
mkdir -p uploads