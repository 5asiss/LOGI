# 배포 시 보안 설정 안내

---

## GitHub Desktop으로 웹 배포하기 (요약)

코드를 **수정 → GitHub Desktop에서 커밋 & 푸시**하면 웹 사이트가 자동으로 갱신되도록 할 수 있습니다.

### 1단계: GitHub 저장소 준비

- GitHub에서 이 프로젝트용 저장소를 만듭니다 (이미 있으면 생략).
- GitHub Desktop에서 **File → Clone repository** 또는 **Add → Add Existing Repository**로 이 폴더(`logi`)를 연결하고, 원격을 해당 GitHub 저장소로 설정합니다.

### 2단계: Render에서 서비스 연결

1. [Render](https://render.com)에 가입 후 로그인합니다.
2. **Dashboard → New + → Web Service**를 선택합니다.
3. **Connect a repository**에서 GitHub 계정을 연결하고, 이 프로젝트 저장소(`logi`)를 선택합니다.
4. 저장소 루트에 `render.yaml`이 있으면 **Blueprint**로 생성할 수 있습니다.  
   - **Blueprint** 사용: Dashboard에서 **New + → Blueprint** → 이 저장소 선택 → `render.yaml` 기준으로 서비스가 생성됩니다.  
   - **직접 설정**: Repository 연결 후 아래를 입력합니다.  
     - **Build Command**: `pip install -r requirements.txt`  
     - **Start Command**: `gunicorn app:app --bind 0.0.0.0:$PORT`

### 3단계: 환경변수 설정 (필수)

Render 대시보드에서 해당 Web Service → **Environment** 탭으로 이동해 다음을 설정합니다.

| 변수 | 필수 | 설명 |
|------|------|------|
| `FLASK_SECRET_KEY` | 예 | Render에서 **Generate** 버튼으로 자동 생성 권장 |
| `ADMIN_PW` | 예 | 관리자 로그인 비밀번호 (1234 말고 강한 비밀번호 사용) |
| `ADMIN_ID` | 선택 | 기본값 `admin` |
| `HTTPS` | 권장 | `1` 입력 시 세션 쿠키 Secure (Render는 HTTPS 제공) |

### 4단계: 배포 및 이후 흐름

- **Deploy** 버튼으로 첫 배포를 실행합니다. 배포가 끝나면 Render가 부여한 URL(예: `https://logi.onrender.com`)로 접속할 수 있습니다.
- **이후 코드 수정 시**: 로컬에서 수정 → **GitHub Desktop**에서 **Commit** → **Push origin** 하면 Render가 자동으로 재배포합니다. 별도 FTP나 서버 접속 없이 웹에 반영됩니다.

### 주의사항 (웹 배포)

- **데이터 유지**: Render 무료/스탠다드 플랜은 재배포 시 디스크가 초기화됩니다. `ledger.db`와 업로드 파일(`static/evidences/`)은 배포할 때마다 비워질 수 있으므로, 중요 데이터는 정기 백업하거나 Render **Disk**(유료)를 붙여 사용하세요.
- **비밀번호**: `.env`는 Git에 올라가지 않습니다. 웹 서버 비밀번호·시크릿은 반드시 Render **Environment**에서만 설정하세요.
- **무료 플랜**: 요청이 없을 때 서비스가 슬립 모드로 들어갑니다. 첫 접속 시 수십 초 정도 걸릴 수 있습니다.

---

## 필수 환경변수 (서버 실행 전 설정)

| 변수명 | 설명 | 예시 |
|--------|------|------|
| `FLASK_SECRET_KEY` 또는 `SECRET_KEY` | 세션 암호화용 비밀키 (강한 랜덤 문자열 권장) | `openssl rand -hex 32` 로 생성 |
| `ADMIN_PW` | 관리자 비밀번호 (기본값 1234 사용 금지) | 강한 비밀번호로 변경 |
| `ADMIN_ID` | (선택) 관리자 아이디, 기본값 `admin` | 필요 시 변경 |

## 선택 환경변수

| 변수명 | 설명 |
|--------|------|
| `PORT` | 서버 포트, 기본 5001 (Render 등에서는 자동 지정) |
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
- [ ] Gunicorn 사용 시: `gunicorn app:app --bind 0.0.0.0:$PORT` (Render 등에서는 반드시 `$PORT` 사용)

## Linux 예시 (systemd 또는 실행 전)

```bash
export FLASK_SECRET_KEY="$(openssl rand -hex 32)"
export ADMIN_PW="your-strong-password"
# FLASK_DEBUG 는 설정하지 않음 (배포 모드)
python app.py
```

또는 `.env` 파일을 사용하는 경우, 서버 실행 전에 `export $(cat .env | xargs)` 등으로 로드 후 실행하세요. (`.env`는 반드시 .gitignore에 포함)

---

## Render 배포 점검사항

Render에 올리기 전 아래 항목을 확인하세요.

### 필수 설정 (Render 대시보드 → Environment)

| 항목 | 설명 |
|------|------|
| **Build Command** | `pip install -r requirements.txt` |
| **Start Command** | `gunicorn app:app --bind 0.0.0.0:$PORT` |
| **FLASK_SECRET_KEY** | 반드시 설정 (미설정 시 기본 dev 키 사용됨). Render에서 "Generate" 가능 |
| **ADMIN_PW** | 1234가 아닌 강한 비밀번호로 설정 |

### 선택 설정

| 항목 | 설명 |
|------|------|
| **ADMIN_ID** | 기본값 `admin` 유지 또는 변경 |
| **FLASK_DEBUG** | 설정하지 않거나 비움 (배포 시 디버그 끄기) |
| **HTTPS** | Render는 기본 HTTPS이므로 `HTTPS=1` 설정 권장 (세션 쿠키 Secure) |

### 데이터 유의사항 (중요)

- **SQLite(`ledger.db`)**: Render의 파일시스템은 **재배포 시 초기화**됩니다. 무료/스탠다드 플랜에서는 배포할 때마다 장부·기사·업체 데이터가 사라질 수 있습니다.
  - **권장**: Render **Disk**(유료)를 붙여 DB·업로드 경로를 영구 디스크에 두거나, 정기 백업 후 복원 스크립트 사용.
- **업로드 파일(`static/evidences/`)**: 위와 동일하게 재배포 시 삭제됩니다. Disk 마운트 또는 외부 스토리지(S3 등) 연동을 고려하세요.

### render.yaml 사용 시

저장소 루트의 `render.yaml`을 사용하면 Blueprint로 서비스를 만들 수 있습니다. `ADMIN_PW`는 대시보드에서 직접 입력해야 하며, `FLASK_SECRET_KEY`는 Render가 자동 생성할 수 있습니다.
