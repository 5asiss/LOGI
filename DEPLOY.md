# 배포 시 보안 설정 안내

## 필수 환경변수 (서버 실행 전 설정)

| 변수명 | 설명 | 예시 |
|--------|------|------|
| `FLASK_SECRET_KEY` 또는 `SECRET_KEY` | 세션 암호화용 비밀키 (강한 랜덤 문자열 권장) | `openssl rand -hex 32` 로 생성 |
| `ADMIN_PW` | 관리자 비밀번호 (기본값 1234 사용 금지) | 강한 비밀번호로 변경 |
| `ADMIN_ID` | (선택) 관리자 아이디, 기본값 `admin` | 필요 시 변경 |

## 선택 환경변수

| 변수명 | 설명 |
|--------|------|
| `PORT` | 서버 포트, 기본 5000 |
| `FLASK_DEBUG` | `1`/`true` 일 때만 디버그 모드. **배포 시 미설정 또는 0** |
| `HTTPS` | `1`/`true` 이면 세션 쿠키에 Secure 플래그 적용 (HTTPS 사용 시 설정) |

## 배포 체크리스트

- [ ] `FLASK_SECRET_KEY` 설정 (미설정 시 기본 dev 키 사용됨)
- [ ] `ADMIN_PW` 를 1234 가 아닌 강한 비밀번호로 변경
- [ ] `FLASK_DEBUG` 를 설정하지 않거나 0으로 실행 (디버그 모드 비활성화)
- [ ] HTTPS 사용 시 리버스 프록시(Nginx 등) 뒤에서 실행하고 `HTTPS=1` 설정
- [ ] `ledger.db`, `static/evidences/` 등 데이터·업로드 경로 백업 및 권한 설정
- [ ] 방화벽에서 필요한 포트만 개방
- [ ] `ledger.db`가 git에 커밋되지 않도록 확인 (민감 데이터)
- [ ] Gunicorn 사용 시: `gunicorn -w 4 -b 0.0.0.0:5000 app:app`

## Linux 예시 (systemd 또는 실행 전)

```bash
export FLASK_SECRET_KEY="$(openssl rand -hex 32)"
export ADMIN_PW="your-strong-password"
# FLASK_DEBUG 는 설정하지 않음 (배포 모드)
python app.py
```

또는 `.env` 파일을 사용하는 경우, 서버 실행 전에 `export $(cat .env | xargs)` 등으로 로드 후 실행하세요. (`.env`는 반드시 .gitignore에 포함)
