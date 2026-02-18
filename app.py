from flask import Flask, render_template_string, request, jsonify, send_file, session, redirect, url_for, make_response
from werkzeug.utils import secure_filename
import pandas as pd

# .env 파일 로드 (python-dotenv)
try:
    from dotenv import load_dotenv  # type: ignore[reportMissingImports]
    load_dotenv()
except ImportError:
    pass
import html
import io
import json
import os
import re
import shutil
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from functools import wraps
from urllib.parse import quote, urlencode

# 한국시간(KST, UTC+9) 설정
KST = timezone(timedelta(hours=9))

def now_kst():
    """현재 한국시간 반환"""
    return datetime.now(KST)

# 백업 기본 경로 (Windows: C:\logi\backup, 그 외: ./backup)
if os.name == 'nt':
    BACKUP_BASE_DIR = r"C:\logi\backup"
else:
    BACKUP_BASE_DIR = os.path.join(os.getcwd(), "backup")

def backup_all(reason: str = "auto") -> None:
    """
    ledger.db + 통합장부 전체 엑셀을 백업 폴더에 저장.
    - 백업 경로: BACKUP_BASE_DIR/YYYYMMDD_HHMMSS_reason/
    """
    try:
        ts = now_kst().strftime('%Y%m%d_%H%M%S')
        base_dir = BACKUP_BASE_DIR
        os.makedirs(base_dir, exist_ok=True)
        folder_name = f"{ts}_{reason}"
        target_dir = os.path.join(base_dir, folder_name)
        os.makedirs(target_dir, exist_ok=True)

        # DB 백업
        db_src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ledger.db")
        if os.path.isfile(db_src):
            shutil.copy2(db_src, os.path.join(target_dir, f"ledger_{ts}.db"))

        # 통합장부 전체 엑셀 백업 (기존 /api/ledger_excel 로직과 동일한 데이터)
        conn = sqlite3.connect('ledger.db', timeout=15)
        conn.row_factory = sqlite3.Row
        try:
            all_rows = conn.execute("SELECT * FROM ledger ORDER BY id DESC").fetchall()
        finally:
            conn.close()

        col_keys = [c['k'] for c in FULL_COLUMNS]
        headers = ['id'] + [c['n'] for c in FULL_COLUMNS]
        rows = []
        for r in all_rows:
            d = dict(r)
            calc_vat_auto(d)
            driver_fixed = get_driver_fixed_type(drivers_db, d.get('d_name'), d.get('c_num'))
            if driver_fixed is not None:
                d['log_move'] = driver_fixed
            d_name = (d.get('d_name') or '').strip()
            c_num = (d.get('c_num') or '').strip()
            if d_name or c_num:
                driver_row = next((dr for dr in drivers_db if (dr.get('기사명') or '').strip() == d_name and (dr.get('차량번호') or '').strip() == c_num), None)
                if driver_row is not None:
                    d['memo2'] = driver_row.get('메모') or d.get('memo2') or ''
            row = [d.get('id', '')] + [d.get(k, '') or '' for k in col_keys]
            rows.append(row)

        df = pd.DataFrame(rows if rows else [[]], columns=headers)
        backup_xlsx = os.path.join(target_dir, f"통합장부_{ts}.xlsx")
        with pd.ExcelWriter(backup_xlsx, engine='openpyxl') as w:
            df.to_excel(w, index=False)
    except Exception as e:
        # 백업 실패는 서비스 동작을 막지 않도록 로그만 출력
        print(f"[backup_all error] {e}")

def calc_supply_value(r):
    """공급가액 = 수수료 + 선착불 + 업체운임"""
    return float(r.get('fee') or 0) + float(r.get('comm') or 0) + float(r.get('pre_post') or 0)

def calc_fee_total(r):
    """공급가액 (하위호환용, calc_supply_value와 동일)"""
    return calc_supply_value(r)

def calc_totals_with_vat(r):
    """업체/기사 운임·부가세·합계 반환. (supply_val, vat1, total1, fee_out, vat2, total2)"""
    supply_val = int(calc_supply_value(r))
    is_cash_client = (str(r.get('pay_method_client') or '').strip() == '현금')
    vat1 = 0 if is_cash_client else int(round(supply_val * 0.1))
    total1 = supply_val + vat1
    fee_out = int(float(r.get('fee_out') or 0))
    is_cash_driver = (str(r.get('pay_method_driver') or '').strip() == '현금')
    vat2 = 0 if is_cash_driver else int(round(fee_out * 0.1))
    total2 = fee_out + vat2
    return supply_val, vat1, total1, fee_out, vat2, total2

def calc_vat_auto(data):
    """부가세·합계 자동계산. 공급가액=수수료+선착불+업체운임, 부가세=공급가액*0.1, 합계=공급가액+부가세. 현금건이면 부가세=0"""
    def _f(k): return float(data.get(k) or 0)
    supply_val = calc_supply_value(data)
    is_cash_client = (str(data.get('pay_method_client') or '').strip() == '현금')
    data['sup_val'] = str(int(supply_val)) if supply_val != 0 else ''
    if supply_val != 0:
        v1 = 0 if is_cash_client else int(round(supply_val * 0.1))
        data['vat1'] = str(v1)
        data['total1'] = str(int(supply_val) + v1)
    else:
        data['vat1'] = ''; data['total1'] = ''
    # 기사: 기사운임 → 부가세, 합계 (현금이면 부가세=0)
    is_cash_driver = (str(data.get('pay_method_driver') or '').strip() == '현금')
    fo = _f('fee_out')
    if fo != 0:
        v2 = 0 if is_cash_driver else int(round(fo * 0.1))
        data['vat2'] = str(v2)
        data['total2'] = str(int(fo) + v2)
    # 순수입·부가세: total1 - total2 → net_profit, vat_final
    t1 = _f('total1')
    t2 = _f('total2')
    if t1 != 0 or t2 != 0:
        np = int(t1 - t2)
        data['net_profit'] = str(np)
        data['vat_final'] = str(int(round(np * 0.1)))
    return data

def safe_int(val, default=1):
    """사용자 입력을 안전하게 정수로 변환 (잘못된 입력 시 default 반환)"""
    try:
        return int(val) if val is not None and str(val).strip() else default
    except (ValueError, TypeError):
        return default

def to_kst_str(ts_val):
    """DB 타임스탬프(UTC 가정)를 한국시간 문자열로 변환"""
    if ts_val is None: return ''
    try:
        s = str(ts_val)[:19]
        if not s or len(s) < 19: return str(ts_val)
        dt = datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        dt_utc = dt.replace(tzinfo=timezone.utc)
        dt_kst = dt_utc.astimezone(KST)
        return dt_kst.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return str(ts_val)

def _norm_tax_chk(raw):
    """계산서 확인 표시용 정규화: '발행완료'만 True로 인식 (장부·정산 공통)"""
    return (str(raw or '').strip() == '발행완료')

def _norm_mail_done(raw):
    """우편확인 표시용 정규화: '확인완료'만 True로 인식 (장부·정산 공통)"""
    return (str(raw or '').strip() == '확인완료')

app = Flask(__name__)

# [배포 보안] 세션·관리자 정보는 환경변수 사용 (미설정 시 기본값은 로컬 전용)
app.secret_key = os.environ.get('FLASK_SECRET_KEY') or os.environ.get('SECRET_KEY') or 'dev-secret-change-in-production'
ADMIN_ID = os.environ.get('ADMIN_ID', 'admin')
ADMIN_PW = os.environ.get('ADMIN_PW', '1234')
# 배포 시 반드시 ADMIN_PW, FLASK_SECRET_KEY 환경변수 설정 권장
if not os.environ.get('FLASK_DEBUG'):
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    if os.environ.get('HTTPS', '').lower() in ('1', 'true', 'on'):
        app.config['SESSION_COOKIE_SECURE'] = True

# [수정/추가] 로그인 체크 데코레이터
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# 이미지 업로드 폴더 설정
UPLOAD_FOLDER = 'static/evidences'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# 은행명 → 은행코드 매핑 (미지급 기사 엑셀용)
BANK_NAME_TO_CODE = {
    "국민": "004", "국민은행": "004", "KB": "004", "kb": "004",
    "신한": "088", "신한은행": "088",
    "우리": "020", "우리은행": "020",
    "하나": "081", "하나은행": "081", "KEB": "081",
    "농협": "011", "NH농협": "011", "NH": "011", "농협은행": "011", "nh": "011",
    "기업": "003", "기업은행": "003", "IBK": "003",
    "산업": "002", "산업은행": "002", "KDB": "002",
    "수협": "007", "수협은행": "007", "수협 bank": "007",
    "SC제일": "023", "SC": "023", "제일": "023", "씨티": "027", "한국씨티": "027", "씨티은행": "027",
    "카카오": "090", "카카오뱅크": "090", "카뱅": "090", "kakaobank": "090",
    "케이뱅크": "089", "K뱅크": "089", "kbank": "089", "k뱅크": "089",
    "토스": "092", "토스뱅크": "092", "toss": "092",
    "우체국": "071", "우편": "071", "우편취급": "071",
    "대구": "031", "대구은행": "031", "부산": "032", "부산은행": "032",
    "광주": "034", "광주은행": "034", "전북": "037", "전북은행": "037",
    "경남": "039", "경남은행": "039", "제주": "035", "제주은행": "035",
    "새마을": "045", "새마을금고": "045", "신협": "048", "SAEMAEUL": "045",
    "쿼리": "042", "한국투자": "264", "미래에셋": "218", "키움": "207",
}
def get_bank_code(bank_name):
    """은행명(또는 일부)으로 은행코드 찾기. 없으면 빈 문자열."""
    if not bank_name or not str(bank_name).strip():
        return ""
    s = str(bank_name).strip().replace(" ", "").replace("　", "")
    for name, code in BANK_NAME_TO_CODE.items():
        if name in s or s in name:
            return code
    return ""

# --- [항목 정의 영역] ---
FULL_COLUMNS = [
    {"n": "비고", "k": "memo1"}, {"n": "요청내용", "k": "req_type"}, {"n": "구분", "k": "category"},
    {"n": "우편/문자/팩스 발송 주소,연락처", "k": "send_to"}, {"n": "완료", "k": "is_done1", "t": "checkbox"},
    {"n": "추가요청사항", "k": "req_add"}, {"n": "완료", "k": "is_done2", "t": "checkbox"},
    {"n": "오더일", "k": "order_dt", "t": "date"}, {"n": "배차일", "k": "dispatch_dt", "t": "datetime-local"},
    {"n": "노선", "k": "route"}, {"n": "기사명", "k": "d_name", "c": "driver-search"},
    {"n": "차량번호", "k": "c_num", "c": "driver-search"}, {"n": "검색용", "k": "search_num"},
    {"n": "연락처", "k": "d_phone", "c": "driver-search"}, {"n": "비고", "k": "memo2"},
    {"n": "사업자구분", "k": "pay_to"}, {"n": "업체명", "k": "client_name", "c": "client-search"},
    {"n": "담당자연락처", "k": "c_mgr_phone"}, {"n": "담당자", "k": "c_mgr_name"},
    {"n": "연락처", "k": "c_phone"}, {"n": "사업자번호", "k": "biz_num"},
    {"n": "사업장주소", "k": "biz_addr"}, {"n": "업종", "k": "biz_type1"},
    {"n": "업태", "k": "biz_type2"}, {"n": "메일주소", "k": "mail"},
    {"n": "도메인", "k": "domain"}, {"n": "사업자", "k": "biz_owner"},
    {"n": "발행구분", "k": "biz_issue"}, {"n": "업체비고", "k": "client_memo"},
    {"n": "결제참고사항", "k": "pay_memo"}, {"n": "결제예정일", "k": "pay_due_dt", "t": "date"},
    {"n": "개인/고정", "k": "log_move"},     {"n": "입금일", "k": "in_dt", "t": "date"},
    {"n": "업체 현금확인", "k": "pay_method_client", "t": "text"},
    {"n": "수수료", "k": "comm", "t": "number"}, {"n": "선착불", "k": "pre_post"},
    {"n": "업체운임", "k": "fee", "t": "number"}, {"n": "공급가액", "k": "sup_val", "t": "number"},
    {"n": "부가세", "k": "vat1", "t": "number"}, {"n": "합계", "k": "total1", "t": "number"},
    {"n": "입금자명", "k": "in_name"}, {"n": "입금내역", "k": "month_val"},
    {"n": "계산서발행일", "k": "tax_dt", "t": "date"}, {"n": "발행사업자", "k": "tax_biz"},
    {"n": "폰", "k": "tax_phone"}, {"n": "계좌번호", "k": "bank_acc"},
    {"n": "연락처", "k": "tax_contact"}, {"n": "사업자번호", "k": "tax_biz_num"},
    {"n": "사업자", "k": "tax_biz_name"},     {"n": "지급일", "k": "out_dt", "t": "date"},
    {"n": "기사 현금확인", "k": "pay_method_driver", "t": "text"},
    {"n": "기사운임", "k": "fee_out", "t": "number"}, {"n": "부가세", "k": "vat2", "t": "number"},
    {"n": "합계", "k": "total2", "t": "number"}, {"n": "작성일자", "k": "write_dt", "t": "date"},
    {"n": "발행일", "k": "issue_dt", "t": "date"}, {"n": "계산서확인", "k": "tax_chk", "t": "text"},
    {"n": "발행사업자", "k": "tax_biz2"}, {"n": "순수입", "k": "net_profit", "t": "number"},
    {"n": "부가세", "k": "vat_final", "t": "number"},
    {"n": "계산서사진", "k": "tax_img", "t": "text"},
    {"n": "운송장사진", "k": "ship_img", "t": "text"},
    {"n": "기사은행명", "k": "d_bank_name"}, 
    {"n": "기사예금주", "k": "d_bank_owner"},
    {"n": "운송우편확인", "k": "is_mail_done", "t": "text"},
    {"n": "우편확인일", "k": "mail_dt", "t": "date"},
    {"n": "업체 월말합산", "k": "month_end_client", "t": "checkbox"},
    {"n": "기사 월말합산", "k": "month_end_driver", "t": "checkbox"}
]

# 통합장부 수정 모달: 품명(라벨) 변경 {원래명 → 바꿀명}, 없으면 원래명 유지
EDIT_MODAL_LABELS = {
    "fee": "수금운임", "month_end_client": "업체 합산발행", "memo2": "콜명", "biz_owner": "매출결제처",
    "c_mgr_phone": "매출결제담당 연락처", "tax_chk": "매출계산서 비고", "client_name": "매출처명", "tax_phone": "폰넘버",
    "c_phone": "매출처연락처", "c_mgr_name": "매출처담당자", "biz_num": "매출처사업자번호", "biz_addr": "매출처사업자주소",
    "client_memo": "매출처 결제비고", "pay_memo": "매출처 결제일정", "pay_due_dt": "수금예정일", "in_dt": "수금일",
    "in_name": "매출처 입금자명", "tax_dt": "매출처 계산서발행일", "tax_biz": "매출사업자구분",
    "is_done1": "인수증 송달완료", "d_phone": "기사연락처", "out_dt": "지급일", "tax_biz2": "매입사업자구분",
    "fee_out": "지급운임", "month_end_driver": "기사합산발행", "tax_contact": "매입처연락처", "log_move": "개별/고정",
    "d_bank_owner": "매입처 예금주", "d_bank_name": "매입처 은행명", "bank_acc": "매입처 계좌번호",
    "tax_biz_name": "매입처 사업자명", "tax_biz_num": "매입처 사업자번호", "issue_dt": "공급자 계산서발행일",
    "tax_img": "매입처계산서", "ship_img": "매출처운송장", "is_mail_done": "운송장우편확인", "mail_dt": "우편전송일",
    "req_type": "인수증 요청사항", "category": "인수증송달방식", "req_add": "인수증비고",
}
# 왼쪽 열(1페이지) 순서 — 사용자 입력 순
EDIT_MODAL_LEFT_KEYS = [
    "memo1", "dispatch_dt", "order_dt", "pre_post", "fee", "sup_val", "vat1", "total1", "month_end_client",
    "memo2", "biz_owner", "c_mgr_phone", "tax_chk", "client_name", "tax_phone", "c_phone", "c_mgr_name",
    "biz_num", "biz_addr", "biz_type1", "biz_type2", "mail", "client_memo", "pay_memo", "pay_due_dt", "in_dt",
    "in_name", "tax_dt", "tax_biz",
]
# 오른쪽 열(2페이지) 순서 — 사용자 입력 순
EDIT_MODAL_RIGHT_KEYS = [
    "send_to", "is_done1", "route", "d_name", "d_phone", "c_num", "out_dt", "tax_biz2", "fee_out", "vat2", "total2",
    "month_end_driver", "tax_contact", "log_move", "d_bank_owner", "d_bank_name", "bank_acc", "tax_biz_name", "tax_biz_num",
    "issue_dt", "tax_img", "ship_img", "is_mail_done", "mail_dt", "req_type", "category", "req_add", "net_profit", "vat_final",
]

def ledger_edit_modal_columns():
    """수정 모달용: (왼쪽 컬럼 리스트, 오른쪽 컬럼 리스트). 각 항목은 (FULL_COLUMNS 항목, 표시품명). 나머지 키는 오른쪽 끝에."""
    by_key = {c["k"]: c for c in FULL_COLUMNS}
    used = set()

    def add_cols(keys):
        out = []
        for k in keys:
            if k in by_key:
                c = by_key[k]
                used.add(k)
                # 장부 수정 모달: month_end_driver는 항상 "기사합산발행" 표시
                label = "기사합산발행" if k == "month_end_driver" else EDIT_MODAL_LABELS.get(k, c["n"])
                out.append((c, label))
        return out

    left = add_cols(EDIT_MODAL_LEFT_KEYS)
    right = add_cols(EDIT_MODAL_RIGHT_KEYS)
    for c in FULL_COLUMNS:
        if c["k"] not in used:
            label = "기사합산발행" if c["k"] == "month_end_driver" else EDIT_MODAL_LABELS.get(c["k"], c["n"])
            right.append((c, label))
    return left, right

DRIVER_COLS = ["기사명", "차량번호", "연락처", "계좌번호", "사업자번호", "사업자", "개인/고정", "메모"]
CLIENT_COLS = ["사업자구분", "업체명", "발행구분", "사업자등록번호", "대표자명", "사업자주소", "업태", "종목", "메일주소", "담당자", "연락처", "결제특이사항", "비고"]
# 통합장부 - 기사 관련 컬럼 (연한 빨강 배경)
COL_KEYS_DRIVER = {'d_name', 'c_num', 'search_num', 'd_phone', 'memo2', 'bank_acc', 'tax_phone', 'tax_contact', 'tax_biz_num', 'tax_biz_name', 'out_dt', 'pay_method_driver', 'fee_out', 'vat2', 'total2', 'write_dt', 'issue_dt', 'tax_chk', 'tax_biz2', 'tax_img', 'ship_img', 'd_bank_name', 'd_bank_owner', 'log_move'}
# 통합장부 - 업체 관련 컬럼 (파랑 배경)
COL_KEYS_CLIENT = {'pay_to', 'client_name', 'c_mgr_phone', 'c_mgr_name', 'c_phone', 'biz_num', 'biz_addr', 'biz_type1', 'biz_type2', 'mail', 'domain', 'biz_owner', 'biz_issue', 'client_memo', 'pay_memo', 'pay_due_dt', 'in_dt', 'pay_method_client', 'comm', 'pre_post', 'fee', 'sup_val', 'vat1', 'total1', 'in_name', 'month_val', 'tax_dt', 'tax_biz'}
# 공급가액·부가세·합계 자동계산 필드 (입력 불가)
CALC_READONLY_KEYS = {'sup_val', 'vat1', 'total1', 'vat2', 'total2', 'net_profit', 'vat_final'}
# 순수입·부가세: 사용 안 함, 회색 처리
UNUSED_GRAY_KEYS = {'net_profit', 'vat_final'}

def ledger_col_class(k):
    """컬럼별 배경 클래스: 기사=연한빨강, 업체=파랑, 미사용=회색"""
    if k in UNUSED_GRAY_KEYS: return 'col-unused'
    if k in COL_KEYS_CLIENT: return 'col-client'
    if k in COL_KEYS_DRIVER: return 'col-driver'
    return ''

def _col_attr(k):
    cls = ledger_col_class(k)
    return f' class="{cls}"' if cls else ''

def ledger_input_attrs(c):
    """컬럼별 input 속성 (데이터 타입/형식 검증)"""
    t = c.get('t', 'text')
    k = c['k']
    base = f"name='{k}' id='{k}' class='{c.get('c', '')}' autocomplete='off'"
    if k in UNUSED_GRAY_KEYS:
        return f"type='text' {base} readonly style='background:#f5f5f5; color:#757575; cursor:not-allowed;' title='사용 안 함'"
    if k in CALC_READONLY_KEYS:
        return f"type='text' {base} readonly style='background:#f0f0f0;'"
    if t == 'number':
        return f"type='text' inputmode='decimal' pattern='^-?[0-9]*\\.?[0-9]*$' title='숫자만 입력 (소수점 가능)' {base} oninput=\"this.value=this.value.replace(/[^0-9.-]/g,'').replace(/(\\..*)\\./g,'$1')\""
    if t == 'date':
        return f"type='date' {base}"
    if t == 'datetime-local':
        return f"type='datetime-local' {base}"
    if t == 'checkbox':
        return f"type='checkbox' {base}"
    return f"type='text' {base}"

def init_db():
    conn = sqlite3.connect('ledger.db', timeout=15)
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass

    keys = [c['k'] for c in FULL_COLUMNS]
    cols_sql = ", ".join([f"'{k}' TEXT" for k in keys])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, {cols_sql})")

    cursor.execute("PRAGMA table_info(ledger)")
    existing_ledger_cols = [info[1] for info in cursor.fetchall()]
    for k in keys:
        if k not in existing_ledger_cols:
            try:
                cursor.execute(f"ALTER TABLE ledger ADD COLUMN '{k}' TEXT")
            except Exception:
                pass

    # 기사 테이블 컬럼 보강
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS drivers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            '기사명' TEXT, '차량번호' TEXT, '연락처' TEXT, '계좌번호' TEXT,
            '사업자번호' TEXT, '사업자' TEXT, '개인/고정' TEXT, '메모' TEXT,
            '은행명' TEXT, '예금주' TEXT
        )
    """)
    # 업체(clients) 테이블: 없으면 CLIENT_COLS로 생성, 있으면 누락 컬럼 추가
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='clients'")
    if cursor.fetchone():
        cursor.execute("PRAGMA table_info(clients)")
        existing_client_cols = [r[1] for r in cursor.fetchall()]
        for col in CLIENT_COLS:
            if col not in existing_client_cols:
                try:
                    cursor.execute(f"ALTER TABLE clients ADD COLUMN [{col}] TEXT")
                except Exception:
                    pass
    else:
        cols_clients = ", ".join([f"[{c}] TEXT" for c in CLIENT_COLS])
        cursor.execute(f"CREATE TABLE clients ({cols_clients})")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS activity_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        action TEXT,          -- 등록, 수정 등 행위
        target_id INTEGER,    -- 대상 장부 ID
        details TEXT          -- 변경 내용 요약
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS arrival_status (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        target_time TEXT,
        content TEXT,
        content_important TEXT,
        content_color TEXT,
        content_font TEXT,
        content_font_size TEXT,
        order_idx INTEGER DEFAULT 0
    )
    """)
    for col in ['content_important', 'content_color', 'content_font', 'content_font_size']:
        try:
            cursor.execute("PRAGMA table_info(arrival_status)")
            existing = [r[1] for r in cursor.fetchall()]
            if col not in existing:
                cursor.execute(f"ALTER TABLE arrival_status ADD COLUMN {col} TEXT")
        except Exception:
            pass

    conn.commit()
    conn.close()

init_db()
drivers_db = []; clients_db = []

def load_db_to_mem():
    global drivers_db, clients_db
    init_db()  # DB 삭제 후 재생성 시 테이블이 있도록 보장
    try:
        conn = sqlite3.connect('ledger.db', timeout=15)
        drivers_db = pd.read_sql("SELECT rowid as id, * FROM drivers", conn).fillna('').to_dict('records')
        clients_db = pd.read_sql("SELECT * FROM clients", conn).fillna('').to_dict('records')
        conn.close()
    except Exception:
        drivers_db = []
        clients_db = []

load_db_to_mem()

def get_driver_fixed_type(drivers_list, d_name, c_num):
    """기사명·차량번호에 해당하는 기사의 개인/고정 값을 반환 (기사관리와 연동)"""
    d_name_s = str(d_name or '').strip()
    c_num_s = str(c_num or '').strip()
    for d in drivers_list:
        if str(d.get('기사명', '')).strip() == d_name_s and str(d.get('차량번호', '')).strip() == c_num_s:
            return str(d.get('개인/고정', '')).strip()
    return None

BASE_HTML = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <title>sm logitek</title>
    <style>
        body { font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', sans-serif; margin: 12px; font-size: 12px; background: #eef1f6; color: #333; }
        .nav { background: #1a2a6c; padding: 12px 18px; border-radius: 8px; margin-bottom: 18px; display: flex; gap: 18px; justify-content: space-between; align-items: center; box-shadow: 0 2px 8px rgba(0,0,0,0.15); }
        .nav-links { display: flex; gap: 18px; flex-wrap: wrap; }
        .nav a { color: white; text-decoration: none; font-weight: bold; font-size: 14px; padding: 6px 10px; border-radius: 4px; }
        .nav a:hover { background: rgba(255,255,255,0.2); }
        .section { background: white; padding: 18px; border-radius: 8px; margin-bottom: 18px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); }
        .section h2 { font-size: 18px; margin: 0 0 14px 0; color: #1a2a6c; border-left: 4px solid #1a2a6c; padding-left: 10px; }
        .section h3 { font-size: 15px; margin: 0 0 12px 0; color: #2c3e50; }
        .scroll-x { overflow-x: auto; max-width: 100%; border: 1px solid #d0d7de; background: white; border-radius: 6px; }
        .scroll-x table { width: max-content; min-width: 100%; }
        .scroll-top { overflow-x: auto; overflow-y: hidden; max-height: 14px; margin-bottom: 4px; border: 1px solid #d0d7de; border-radius: 6px; background: #f6f8fa; box-sizing: border-box; }
        .scroll-top table { width: max-content; min-width: 100%; visibility: hidden; }
        .container { overflow: visible; }
        .section { overflow: visible; }
        .scroll-sticky-wrap { position: sticky; top: 0; left: 0; right: 0; z-index: 10; background: #eef1f6; padding-bottom: 6px; margin-bottom: 4px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); border-radius: 6px; }
        .scroll-sticky-wrap .scroll-top { margin-bottom: 4px; border-radius: 6px 6px 0 0; }
        .scroll-sticky-wrap .scroll-x { border-radius: 6px; }
        .page-ledger { padding-bottom: 36px; }
        .page-ledger #ledgerListScroll { scrollbar-width: none; -ms-overflow-style: none; max-height: 70vh; overflow-y: auto; overflow-x: auto; }
        .page-ledger #ledgerListScroll::-webkit-scrollbar { display: none; height: 0; }
        .page-ledger #ledgerListScroll table thead th { position: sticky; top: 0; z-index: 5; background: #f0f3f7; box-shadow: 0 1px 0 #dee2e6; }
        .page-ledger #ledgerListScroll table thead th:first-child { left: 0; z-index: 6; box-shadow: 2px 0 0 #dee2e6, 0 1px 0 #dee2e6; }
        .page-ledger #ledgerListScroll table tbody td:first-child { position: sticky; left: 0; z-index: 4; background: #fff; box-shadow: 2px 0 0 #dee2e6; }
        .page-ledger #ledgerListScrollTop,
        .page-ledger #ledgerListScrollBottom { height: 20px; min-height: 20px; max-height: 20px; overflow-x: auto; overflow-y: hidden; }
        .page-ledger #ledgerListScrollTop::-webkit-scrollbar,
        .page-ledger #ledgerListScrollBottom::-webkit-scrollbar { height: 10px; }
        .page-ledger #ledgerListScrollTop,
        .page-ledger #ledgerListScrollBottom { scrollbar-width: thin; }
        .page-settlement { padding-bottom: 36px; }
        .page-settlement #settlementScroll { scrollbar-width: none; -ms-overflow-style: none; max-height: 70vh; overflow-y: auto; overflow-x: auto; }
        .page-settlement #settlementScroll::-webkit-scrollbar { display: none; height: 0; }
        .page-settlement #settlementScroll table thead th { position: sticky; top: 0; z-index: 5; background: #f0f3f7; box-shadow: 0 1px 0 #dee2e6; }
        .page-settlement #settlementScroll table thead th:first-child { left: 0; z-index: 6; box-shadow: 2px 0 0 #dee2e6, 0 1px 0 #dee2e6; }
        .page-settlement #settlementScroll table tbody td:first-child { position: sticky; left: 0; z-index: 4; background: #fff; box-shadow: 2px 0 0 #dee2e6; }
        .page-settlement .scroll-top { height: 20px; min-height: 20px; max-height: 20px; overflow-x: auto; overflow-y: hidden; flex-shrink: 0; }
        .page-settlement .scroll-top::-webkit-scrollbar { height: 10px; }
        .page-settlement .scroll-top { scrollbar-width: thin; }
        .ledger-scrollbar-fix { position: fixed; bottom: 0; left: 0; right: 0; height: 28px; background: #f0f3f7; border-top: 2px solid #1a2a6c; z-index: 1000; overflow-x: auto; overflow-y: hidden; display: flex; align-items: center; }
        .ledger-scrollbar-fix-inner { height: 1px; min-width: 100%; flex-shrink: 0; }
        table { border-collapse: collapse; width: 100%; white-space: nowrap; font-size: 12px; }
        th, td { border: 1px solid #dee2e6; padding: 6px 8px; text-align: center; }
        th { background: #f0f3f7; position: sticky; top: 0; z-index: 5; font-weight: 600; color: #374151; }
        input[type="text"], input[type="number"], input[type="date"], input[type="datetime-local"] { width: 110px; border: 1px solid #d0d7de; padding: 6px 8px; font-size: 12px; border-radius: 4px; box-sizing: border-box; }
        input:focus { outline: none; border-color: #1a2a6c; box-shadow: 0 0 0 2px rgba(26,42,108,0.15); }
        /* 업체 입력란 - 연한 파랑 */
        input.client-search { background: #e3f2fd; border-color: #1976d2; }
        input.client-search::placeholder { color: #1565c0; }
        /* 기사 입력란 - 연한 빨강 */
        input.driver-search { background: #ffebee; border-color: #c62828; }
        input.driver-search::placeholder { color: #b71c1c; }
        /* 통합장부 - 기사/업체 컬럼 구분 (은은한 색상) */
        th.col-driver, td.col-driver { background: #ffebee !important; }
        td.col-driver input { background: #ffebee; }
        th.col-client, td.col-client { background: #e3f2fd !important; }
        td.col-client input { background: #e3f2fd; }
        th.col-unused, td.col-unused { background: #f5f5f5 !important; color: #757575; }
        .btn-save { background: #27ae60; color: white; padding: 12px 28px; border: none; border-radius: 6px; cursor: pointer; font-weight: bold; font-size: 14px; min-height: 44px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .btn-save:hover { background: #219a52; }
        .btn { padding: 10px 20px; border-radius: 6px; cursor: pointer; font-weight: 600; font-size: 13px; border: 1px solid #d0d7de; background: #f6f8fa; }
        .btn:hover { background: #eaeef2; }
        .btn-edit { padding: 8px 16px; border: none; border-radius: 5px; cursor: pointer; font-weight: 600; font-size: 12px; background: #1a2a6c; color: white; }
        .btn-edit:hover { background: #253a7c; }
        .btn-status { padding: 8px 14px; border: none; border-radius: 5px; cursor: pointer; font-weight: 600; color: white; font-size: 12px; min-height: 34px; }
        .bg-red { background: #e74c3c; } .bg-green { background: #2ecc71; } .bg-orange { background: #f39c12; } .bg-gray { background: #95a5a6; }
        .bg-blue { background: #3498db; }
        .search-bar { padding: 10px 12px; width: 300px; border: 2px solid #1a2a6c; border-radius: 6px; margin-bottom: 10px; font-size: 13px; }
        .stat-card { flex: 1; border: 1px solid #ddd; padding: 12px; border-radius: 8px; text-align: center; background: #fff; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
        .stat-val { font-size: 14px; font-weight: bold; color: #1a2a6c; margin-top: 5px; line-height: 1.4; }
        
        /* 검색 팝업 스타일 강화 (눈에 띄게 수정) */
        .search-results { 
            position: absolute; 
            z-index: 10001;
            background-color: white !important; 
            border: 2px solid #1a2a6c !important; 
            z-index: 999999 !important; /* 최상단 배치 */
            max-height: 250px; 
            overflow-y: auto; 
            display: none; 
            box-shadow: 0 8px 20px rgba(0,0,0,0.3);
            border-radius: 4px;
        }
        .search-item { 
            padding: 10px 15px; 
            cursor: pointer; 
            border-bottom: 1px solid #eee; 
            font-size: 13px;
            text-align: left;
            color: #333;
            background: white;
        }
        .search-item:hover { background-color: #ebf2ff; color: #1a2a6c; font-weight: bold; }
        .search-item:active { background-color: #c5d9ff; }
        .search-item { user-select: none; -webkit-user-select: none; }
        
        .quick-order-grid { display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; margin-bottom: 14px; }
        .quick-order-grid label { display: block; font-size: 12px; font-weight: 600; color: #374151; margin-bottom: 4px; }
        #imgModal { display:none; position:fixed; z-index:9999; left:0; top:0; width:100%; height:100%; background:rgba(0,0,0,0.8); text-align:center; }
        #imgModal img { max-width:90%; max-height:90%; margin-top:30px; border:3px solid white; }
        .multi-img-btns { display: flex; gap: 2px; justify-content: center; }
        .img-num-btn { width: 18px; height: 18px; font-size: 9px; padding: 0; cursor: pointer; border: 1px solid #ccc; background: white; }
        .img-num-btn.active { background: #2ecc71; color: white; }
        .memo-board { height: 140px; background: #dfe6e9; border: 2px dashed #b2bec3; position: relative; margin-bottom: 15px; border-radius: 5px; overflow: hidden; }
        .sticky-note { position: absolute; width: 160px; background: #fff9c4; border: 1px solid #fbc02d; padding: 8px; cursor: move; z-index: 100; box-shadow: 2px 2px 5px rgba(0,0,0,0.1); border-radius: 5px; }
        .draggable { cursor: grab; }
        .draggable:active { cursor: grabbing; }
        .dragging { opacity: 0.5; background: #e8f4fd !important; }
        .link-btn { font-size: 11px; padding: 6px 10px; border: 1px solid #d0d7de; background: #f6f8fa; color: #333; text-decoration: none; border-radius: 4px; }
        .link-btn:hover { background: #eaeef2; }
        .link-btn.has-file { background: #e3f2fd; border-color: #2196f3; color: #1976d2; font-weight: bold; }
        .pagination { display: flex; justify-content: center; gap: 8px; margin-top: 18px; flex-wrap: wrap; }
        .page-btn { padding: 8px 14px; border: 1px solid #d0d7de; background: white; cursor: pointer; text-decoration: none; color: #333; border-radius: 5px; font-size: 13px; font-weight: 500; }
        .page-btn:hover { background: #f0f3f7; }
        .page-btn.active { background: #1a2a6c; color: white; border-color: #1a2a6c; }
        .board-container { position: relative; width: 100%; min-height: 82vh; height: 82vh; background: #dfe6e9; background-image: radial-gradient(#b2bec3 1px, transparent 1px); background-size: 30px 30px; border-radius: 10px; overflow: visible; }
        .sticky-note { position: absolute; background: #fff9c4; border: 1px solid #fbc02d; box-shadow: 3px 3px 10px rgba(0,0,0,0.15); display: flex; flex-direction: column; overflow: hidden; resize: both; min-width: 120px; min-height: 100px; }
        .note-header { background: #fbc02d; padding: 6px; cursor: move; display: flex; justify-content: space-between; align-items: center; font-weight: bold; font-size: 12px; }
        .note-content { flex-grow: 1; border: none; background: transparent; padding: 10px; font-family: inherit; font-size: 13px; resize: none; width: 100%; height: 100%; box-sizing: border-box; }
        .note-delete-btn { cursor: pointer; color: red; font-weight: bold; padding: 0 5px; user-select: none; }
        .ctx-menu { display: none; position: fixed; z-index: 10000; background: white; border: 1px solid #ccc; border-radius: 6px; box-shadow: 0 4px 12px rgba(0,0,0,0.15); min-width: 120px; padding: 4px 0; }
        .ctx-menu.show { display: block; }
        .ctx-menu button { display: block; width: 100%; padding: 8px 16px; border: none; background: none; text-align: left; cursor: pointer; font-size: 13px; }
        .ctx-menu button:hover { background: #f0f3f7; }
        .ctx-menu button.del { color: #e74c3c; }
        /* 통합장부 버튼 글씨 검정 */
        .page-ledger .btn-edit,
        .page-ledger .btn-status,
        .page-ledger .btn-save,
        .page-ledger .btn { color: #000 !important; }
    </style>
    </head>
<body>
    <div class="nav">
        <div class="nav-links">
            <a href="/">통합장부입력</a>
            <a href="/arrival">도착현황</a>
            <a href="/settlement">정산관리</a>
            <a href="/statistics">통계분석</a>
            <a href="/manage_drivers">기사관리</a>
            <a href="/manage_clients">업체관리</a>
        </div>
        <div>
            <a href="/logout" style="background:#e74c3c; padding:5px 10px; border-radius:3px; color:white;">로그아웃</a>
        </div>
    </div>
    <div class="container">{{ content_body | safe }}</div>
    <div id="search-popup" class="search-results"></div>
    <div id="imgModal" onclick="this.style.display='none'"><span class="close">&times;</span><img id="modalImg"></div>
    <div id="ledgerCtxMenu" class="ctx-menu">
        <button type="button" data-action="recall">재호출</button>
        <button type="button" data-action="delete" class="del">삭제</button>
    </div>

  <script>
    let drivers = {{ drivers_json | safe }};
    let clients = {{ clients_json | safe }};
    let columnKeys = {{ col_keys | safe }};
    let columnKeysDriver = new Set({{ col_keys_driver | default('[]') | safe }});
    let columnKeysClient = new Set({{ col_keys_client | default('[]') | safe }});
    let lastLedgerData = [];
    let currentEditId = null;

    {% raw %}
    window.viewImg = function(src) {
        if(!src || src.includes('❌') || src === '/' || src.includes('None') || src == '') return;
        let path = (typeof src === 'string') ? src.trim() : '';
        if(path.includes(',')) path = path.split(',')[0].trim();
        if(path && path.startsWith('static')) {
            document.getElementById('modalImg').src = '/' + path;
            document.getElementById('imgModal').style.display = 'block';
        }
    };
    {% endraw %}

    function todayKST() { return new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Seoul' }); }
    function nowKSTLocal() { const s = new Date().toLocaleString('sv-SE', { timeZone: 'Asia/Seoul', hour12: false }); return s.replace(' ', 'T').slice(0, 16); }

    // 초성 추출 함수
    const getChosung = (str) => {
        const cho = ["ㄱ","ㄲ","ㄴ","ㄷ","ㄸ","ㄹ","ㅁ","ㅂ","ㅃ","ㅅ","ㅆ","ㅇ","ㅈ","ㅉ","ㅊ","ㅋ","ㅌ","ㅍ","ㅎ"];
        let res = "";
        for(let i=0; i<str.length; i++) {
            let code = str.charCodeAt(i) - 44032;
            if(code > -1 && code < 11172) res += cho[Math.floor(code/588)];
            else res += str.charAt(i);
        }
        return res;
    };

    // 검색 팝업 닫기 지연 타이머 (blur 시 클릭이 먼저 처리되도록)
    let searchPopupCloseTimer = null;
    document.getElementById('search-popup').addEventListener('mousedown', function(ev) {
        const item = ev.target.closest('.search-item');
        if (!item) return;
        ev.preventDefault();
        ev.stopPropagation();
        const idx = parseInt(item.getAttribute('data-index'), 10);
        const list = window._searchList;
        const targetId = this.getAttribute('data-search-target-id') || '';
        const type = this.getAttribute('data-search-type') || 'client';
        if (list && list[idx] != null && targetId && type) {
            fillData(JSON.stringify(list[idx]), type, targetId);
        }
        this.style.display = 'none';
        if (searchPopupCloseTimer) clearTimeout(searchPopupCloseTimer);
        searchPopupCloseTimer = null;
    });

    // 2. 실시간 입력 감지 및 팝업 표시 (이벤트 위임 + data-index로 클릭 안정화)
    document.addEventListener('input', function(e) {
        const vatSrc = ['comm','pre_post','fee','fee_out','pay_method_client','pay_method_driver'];
        if (e.target.form?.id === 'ledgerForm' && vatSrc.includes(e.target.name)) {
            if (typeof calcVatAutoForm === 'function') calcVatAutoForm();
        }
        if(e.target.classList.contains('driver-search') || e.target.classList.contains('client-search')) {
            if (searchPopupCloseTimer) { clearTimeout(searchPopupCloseTimer); searchPopupCloseTimer = null; }
            const isDriver = e.target.classList.contains('driver-search');
            const val = e.target.value.toLowerCase().trim();
            const db = isDriver ? drivers : clients;
            const popup = document.getElementById('search-popup');

            if(val.length < 1) { popup.style.display = 'none'; return; }

            const filtered = db.filter(item => {
                const target = isDriver ? (item.기사명 + (item.차량번호||'')) : (item.업체명||'');
                const targetLower = target.toLowerCase();
                return targetLower.includes(val) || getChosung(targetLower).includes(val);
            });

            if(filtered.length > 0) {
                window._searchList = filtered;
                popup.setAttribute('data-search-target-id', e.target.id);
                popup.setAttribute('data-search-type', isDriver ? 'driver' : 'client');
                const rect = e.target.getBoundingClientRect();
                popup.style.display = 'block'; 
                popup.style.width = Math.max(rect.width, 220) + 'px';
                popup.style.top = (window.scrollY + rect.bottom) + 'px'; 
                popup.style.left = (window.scrollX + rect.left) + 'px'; 
                popup.innerHTML = filtered.map((item, idx) => {
                    const label = isDriver ? `${item.기사명 || ''} [${item.차량번호 || ''}]` : (item.업체명 || '');
                    const safeLabel = (label || '').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
                    return `<div class="search-item" data-index="${idx}">${safeLabel}</div>`;
                }).join('');
            } else { popup.style.display = 'none'; }
        }
    });

    document.addEventListener('focusin', function(e) {
        if (e.target.classList && (e.target.classList.contains('driver-search') || e.target.classList.contains('client-search')))
            if (searchPopupCloseTimer) { clearTimeout(searchPopupCloseTimer); searchPopupCloseTimer = null; }
    });
    document.addEventListener('focusout', function(e) {
        if (e.target.classList && (e.target.classList.contains('driver-search') || e.target.classList.contains('client-search'))) {
            searchPopupCloseTimer = setTimeout(function() {
                document.getElementById('search-popup').style.display = 'none';
                searchPopupCloseTimer = null;
            }, 180);
        }
    });

    // 3. 데이터 자동 입력 (상세 칸까지 완벽 대응 및 q_ 인식)
    window.fillData = function(itemStr, type, targetInputId) {
        // 문자열 데이터를 객체로 안전하게 변환
        const item = JSON.parse(itemStr.replace(/&quot;/g, '"').replace(/&#39;/g, "'"));
        const isQuick = targetInputId.startsWith('q_');
        const prefix = isQuick ? 'q_' : '';
        
        if(type === 'driver') {
            const nameField = document.getElementById(prefix + 'd_name');
            const numField = document.getElementById(prefix + 'c_num');
            if(nameField) nameField.value = item.기사명 || '';
            if(numField) numField.value = item.차량번호 || '';

            if(!isQuick) { // 상세 장부: 기사관리 10개 항목 전체(기사명·차량번호·연락처·은행명·계좌번호·예금주·사업자번호·사업자·개인고정·메모) 불러와 기입
                if(document.getElementById('d_phone')) document.getElementById('d_phone').value = item.연락처 || '';
                if(document.getElementById('d_bank_name')) document.getElementById('d_bank_name').value = item.은행명 || '';
                if(document.getElementById('bank_acc')) document.getElementById('bank_acc').value = item.계좌번호 || '';
                if(document.getElementById('d_bank_owner')) document.getElementById('d_bank_owner').value = item.예금주 || '';
                if(document.getElementById('tax_biz_num')) document.getElementById('tax_biz_num').value = item.사업자번호 || '';
                if(document.getElementById('tax_biz_name')) document.getElementById('tax_biz_name').value = item.사업자 || '';
                if(document.getElementById('log_move')) document.getElementById('log_move').value = item['개인/고정'] || '';
                if(document.getElementById('memo1')) document.getElementById('memo1').value = item.메모 || '';
            }
        } else {
            const clientField = document.getElementById(prefix + 'client_name');
            if(clientField) clientField.value = item.업체명 || '';

            if(!isQuick) { // 상세 장부 입력창: 업체관리 탭 전부(사업자구분~비고) 불러와 기입
                if(document.getElementById('pay_to')) document.getElementById('pay_to').value = item.사업자구분 || '';
                if(document.getElementById('c_mgr_phone')) document.getElementById('c_mgr_phone').value = item.연락처 || '';
                if(document.getElementById('c_mgr_name')) document.getElementById('c_mgr_name').value = item.담당자 || '';
                if(document.getElementById('c_phone')) document.getElementById('c_phone').value = item.연락처 || '';
                if(document.getElementById('biz_num')) document.getElementById('biz_num').value = item.사업자등록번호 || '';
                if(document.getElementById('biz_addr')) document.getElementById('biz_addr').value = item.사업자주소 || '';
                if(document.getElementById('biz_type1')) document.getElementById('biz_type1').value = item.종목 || '';
                if(document.getElementById('biz_type2')) document.getElementById('biz_type2').value = item.업태 || '';
                if(document.getElementById('mail')) document.getElementById('mail').value = item.메일주소 || '';
                if(document.getElementById('biz_owner')) document.getElementById('biz_owner').value = item.대표자명 || '';
                if(document.getElementById('biz_issue')) document.getElementById('biz_issue').value = item.발행구분 || '';
                if(document.getElementById('client_memo')) document.getElementById('client_memo').value = item.비고 || '';
                if(document.getElementById('pay_memo')) document.getElementById('pay_memo').value = item.결제특이사항 || '';
            }
        }
        document.getElementById('search-popup').style.display = 'none';
    };

        function calcVatAutoForm() {
            const form = document.getElementById('ledgerForm');
            if (!form) return;
            const get = (k) => parseFloat(form.elements[k]?.value || 0) || 0;
            const supplyVal = get('comm') + get('pre_post') + get('fee');
            const isCashClient = (form.elements['pay_method_client']?.value || '').trim() === '현금';
            const isCashDriver = (form.elements['pay_method_driver']?.value || '').trim() === '현금';
            const elSup = form.elements['sup_val'];
            if (elSup) elSup.value = supplyVal !== 0 ? Math.floor(supplyVal) : '';
            if (supplyVal !== 0) {
                const v1 = isCashClient ? 0 : Math.round(supplyVal * 0.1);
                const el1 = form.elements['vat1']; const el2 = form.elements['total1'];
                if (el1) el1.value = v1; if (el2) el2.value = Math.floor(supplyVal) + v1;
            } else {
                const el1 = form.elements['vat1']; const el2 = form.elements['total1'];
                if (el1) el1.value = ''; if (el2) el2.value = '';
            }
            const fo = get('fee_out');
            if (fo !== 0) {
                const v2 = isCashDriver ? 0 : Math.round(fo * 0.1);
                const el3 = form.elements['vat2']; const el4 = form.elements['total2'];
                if (el3) el3.value = v2; if (el4) el4.value = Math.floor(fo) + v2;
            }
            const t1 = get('total1'); const t2 = get('total2');
            if (t1 !== 0 || t2 !== 0) {
                const np = Math.floor(t1 - t2);
                const el5 = form.elements['net_profit']; const el6 = form.elements['vat_final'];
                if (el5) el5.value = np; if (el6) el6.value = Math.round(np * 0.1);
            }
        }

        function validateLedgerForm(formId) {
            const form = document.getElementById(formId);
            if (!form) return false;
            const isQuick = (formId === 'quickOrderForm');
            const numFields = isQuick ? ['q_fee','q_fee_out'] : ['comm','pre_post','fee','sup_val','vat1','total1','fee_out','vat2','total2','net_profit','vat_final'];
            const dateFields = isQuick ? ['q_order_dt'] : ['order_dt','pay_due_dt','in_dt','tax_dt','out_dt','write_dt','issue_dt'];
            const dtFields = isQuick ? ['q_dispatch_dt'] : ['dispatch_dt'];
            const dateRe = /^\\d{4}-\\d{2}-\\d{2}$/;
            const dtRe = /^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}/;
            for (const name of numFields) {
                const el = form.elements[name] || form.elements[name.replace('q_','')];
                if (!el || el.type==='checkbox') continue;
                const v = (el.value || '').trim();
                if (v && isNaN(parseFloat(v))) { alert(el.id + ' (숫자): 올바른 숫자를 입력하세요.'); el.focus(); return false; }
            }
            for (const name of (isQuick ? [] : dateFields)) {
                const el = form.elements[name];
                if (!el) continue;
                const v = (el.value || '').trim();
                if (v && !dateRe.test(v)) { alert(el.id + ' (날짜): YYYY-MM-DD 형식으로 입력하세요.'); el.focus(); return false; }
            }
            for (const name of (isQuick ? [] : dtFields)) {
                const el = form.elements[name];
                if (!el) continue;
                const v = (el.value || '').trim();
                if (v && !dtRe.test(v)) { alert(el.id + ' (날짜시간): 올바른 형식으로 입력하세요.'); el.focus(); return false; }
            }
            return true;
        }

        function saveLedger(formId) {
            if (!validateLedgerForm(formId)) return;
            const form = document.getElementById(formId);
            const formData = new FormData(form);
            const data = {};
            const isQuick = (formId === 'quickOrderForm');
            formData.forEach((v, k) => {
                const key = isQuick ? k.replace('q_', '') : k;
                const input = form.elements[k]; 
                if (input && input.type === 'checkbox') {
                    if (key === 'month_end_client' || key === 'month_end_driver') data[key] = input.checked ? '1' : '';
                    else data[key] = input.checked ? "✅" : "❌";
                } else data[key] = v;
            });
            if(isQuick) {
                // 빠른오더: 업체명·기사명 초성검색과 동일하게 등록 — 선택한 업체/기사 전체 데이터 보내서 장부·업체관리/기사관리 동기화
                const client = clients.find(c => c.업체명 === data.client_name);
                if(client) {
                    data.pay_to = client.사업자구분 || ''; data.biz_issue = client.발행구분 || '';
                    data.biz_num = client.사업자등록번호 || ''; data.biz_owner = client.대표자명 || '';
                    data.biz_addr = client.사업자주소 || ''; data.biz_type2 = client.업태 || ''; data.biz_type1 = client.종목 || '';
                    data.mail = client.메일주소 || ''; data.c_mgr_name = client.담당자 || ''; data.c_phone = client.연락처 || '';
                    data.pay_memo = client.결제특이사항 || ''; data.client_memo = client.비고 || '';
                }
                const driver = drivers.find(d => d.기사명 === data.d_name && d.차량번호 === data.c_num);
                if(driver) {
                    data.d_phone = driver.연락처 || ''; data.d_bank_name = driver.은행명 || ''; data.bank_acc = driver.계좌번호 || '';
                    data.d_bank_owner = driver.예금주 || ''; data.tax_biz_num = driver.사업자번호 || ''; data.tax_biz_name = driver.사업자 || '';
                    data.log_move = driver['개인/고정'] || ''; data.memo1 = driver.메모 || '';
                }
                data.order_dt = data.order_dt || (typeof todayKST === 'function' ? todayKST() : new Date().toISOString().split('T')[0]);
                // 배차일: 기본값 없이 공란으로 두어 사용자가 직접 날짜/시간 선택
            }
            if (currentEditId) data['id'] = currentEditId;
            fetch('/api/save_ledger', {
                method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data)
            }).then(r => r.json()).then(res => {
                if(res.status === 'success') {
                    alert('장부가 등록되었습니다.'); 
                    currentEditId = null; 
                    form.reset(); 
                    loadLedgerList(); 
                    fetch('/api/load_db_mem').then(r => r.json()).then(db => { drivers = db.drivers; clients = db.clients; });
                } else { alert(res.message || '저장 중 오류가 발생했습니다.'); }
            }).catch(() => alert('저장 중 오류가 발생했습니다.'));
        }

        // 빠른 기간 설정 함수 (한국시간 기준)
function setDateRange(days) {
    const endStr = typeof todayKST === 'function' ? todayKST() : new Date().toISOString().split('T')[0];
    const end = new Date(endStr + 'T12:00:00');
    const start = new Date(end);
    start.setDate(start.getDate() - days);
    const startStr = start.toISOString().split('T')[0];
    document.getElementById('startDate').value = startStr;
    document.getElementById('endDate').value = endStr;
    loadLedgerList();
}

function loadLedgerList() {
    const body = document.getElementById('ledgerBody');
    if (!body) return; 
    
    const urlParams = new URLSearchParams(window.location.search);
    const page = urlParams.get('page') || 1;
    var perPageFromUrl = urlParams.get('per_page');
    if (perPageFromUrl && ['20','50','100'].indexOf(perPageFromUrl) >= 0) {
        var sel = document.getElementById('ledgerPerPage');
        if (sel) sel.value = perPageFromUrl;
    }
    var searchEl = document.getElementById('ledgerSearch');
    if (urlParams.get('q') && searchEl) searchEl.value = urlParams.get('q');
    const start = document.getElementById('startDate').value;
    const end = document.getElementById('endDate').value;
    const q = (searchEl && searchEl.value) ? searchEl.value.trim() : '';
    
    const monthClient = document.getElementById('filterMonthEndClient') && document.getElementById('filterMonthEndClient').checked ? '1' : '';
    const monthDriver = document.getElementById('filterMonthEndDriver') && document.getElementById('filterMonthEndDriver').checked ? '1' : '';
    var perPageEl = document.getElementById('ledgerPerPage');
    var perPage = (perPageEl && [20,50,100].indexOf(parseInt(perPageEl.value,10)) >= 0) ? perPageEl.value : '20';
    let url = `/api/get_ledger?page=${page}&per_page=${perPage}&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`;
    if(monthClient) url += '&month_end_client=1';
    if(monthDriver) url += '&month_end_driver=1';
    if(q) url += '&q=' + encodeURIComponent(q);
    fetch(url)
        .then(r => r.json())
        .then(res => {
            lastLedgerData = res.data;
            var ordered = applyLedgerSavedOrder(lastLedgerData, getLedgerOrderKey());
            renderTableRows(ordered);
            lastLedgerData = ordered;
            if (typeof renderPagination === 'function') renderPagination(res.total_pages, res.current_page, 'ledger');
            if (typeof window.updateLedgerScrollBarWidth === 'function') requestAnimationFrame(function() { window.updateLedgerScrollBarWidth(); });
        });
}
        function getLedgerOrderKey() {
            var urlParams = new URLSearchParams(window.location.search);
            var page = urlParams.get('page') || '1';
            var start = document.getElementById('startDate') ? document.getElementById('startDate').value || '' : '';
            var end = document.getElementById('endDate') ? document.getElementById('endDate').value || '' : '';
            var monthClient = document.getElementById('filterMonthEndClient') && document.getElementById('filterMonthEndClient').checked ? '1' : '';
            var monthDriver = document.getElementById('filterMonthEndDriver') && document.getElementById('filterMonthEndDriver').checked ? '1' : '';
            var perPageEl = document.getElementById('ledgerPerPage');
            var perPage = (perPageEl && [20,50,100].indexOf(parseInt(perPageEl.value,10)) >= 0) ? perPageEl.value : '20';
            var q = (document.getElementById('ledgerSearch') && document.getElementById('ledgerSearch').value) ? document.getElementById('ledgerSearch').value.trim() : '';
            return 'ledger_order_' + [page, perPage, start, end, monthClient, monthDriver, q].join('_');
        }
        function applyLedgerSavedOrder(data, key) {
            if (!key || !data || !data.length) return data;
            try {
                var saved = localStorage.getItem(key);
                if (!saved) return data;
                var orderIds = JSON.parse(saved);
                if (!Array.isArray(orderIds) || orderIds.length === 0) return data;
                var byId = {};
                data.forEach(function(r) { byId[r.id] = r; });
                var ordered = [];
                orderIds.forEach(function(id) { if (byId[id]) { ordered.push(byId[id]); delete byId[id]; } });
                Object.keys(byId).forEach(function(id) { ordered.push(byId[id]); });
                return ordered.length ? ordered : data;
            } catch (e) { return data; }
        }
        function saveLedgerOrder() {
            var body = document.getElementById('ledgerBody');
            if (!body) return;
            var rows = body.querySelectorAll('tr.draggable');
            var ids = [];
            for (var i = 0; i < rows.length; i++) { var id = parseInt(rows[i].getAttribute('data-id'), 10); if (!isNaN(id)) ids.push(id); }
            if (ids.length === 0) return;
            try { localStorage.setItem(getLedgerOrderKey(), JSON.stringify(ids)); } catch (e) {}
        }

        function renderTableRows(data) {
    const body = document.getElementById('ledgerBody');
    if (!body) return;
    body.innerHTML = data.map(item => `
        <tr class="draggable" draggable="true" data-id="${item.id}">
            <td style="white-space:nowrap;">
                <span class="order-no" style="display:inline-block; font-weight:700; color:#1a2a6c; margin-right:8px; font-size:12px;" title="고유오더번호">n${String(item.id).padStart(2, '0')}</span>
                <button class="btn-edit" onclick="editEntry(${item.id})">수정</button>
                <button class="btn-status" style="margin-left:4px; font-size:10px; padding:5px 8px; ${(item.pay_method_client || '').trim() === '현금' ? 'background:#e67e22; color:white;' : 'background:#ebf2ff; color:#1a2a6c;'}" onclick="changeStatus(${item.id}, 'pay_method_client', '${(item.pay_method_client || '').trim() === '현금' ? '이체' : '현금'}')">업체: ${(item.pay_method_client || '').trim() || '이체'}</button>
                <button class="btn-status" style="margin-left:4px; font-size:10px; padding:5px 8px; ${(item.pay_method_driver || '').trim() === '현금' ? 'background:#e67e22; color:white;' : 'background:#ffebee; color:#b71c1c;'}" onclick="changeStatus(${item.id}, 'pay_method_driver', '${(item.pay_method_driver || '').trim() === '현금' ? '이체' : '현금'}')">기사: ${(item.pay_method_driver || '').trim() || '이체'}</button>
            </td>
            ${columnKeys.map(key => {
                let val = item[key] || '';
                let tdCls = '';
                if (['net_profit','vat_final'].includes(key)) tdCls = ' class="col-unused"';
                else if (typeof columnKeysClient !== 'undefined' && columnKeysClient.has && columnKeysClient.has(key)) tdCls = ' class="col-client"';
                else if (typeof columnKeysDriver !== 'undefined' && columnKeysDriver.has && columnKeysDriver.has(key)) tdCls = ' class="col-driver"';
                // 공급가액(sup_val): 수수료+선착불+업체운임 (API에서 calc_vat_auto 적용됨)
                if(key === 'sup_val') {
                    let feeNum = parseFloat(item.fee) || 0;
                    let commNum = parseFloat(item.comm) || 0;
                    let preNum = parseFloat(item.pre_post) || 0;
                    val = (feeNum + commNum + preNum).toLocaleString();
                }
                // 기사운임은 합산 없이 그대로 표기
                if(key === 'tax_img' || key === 'ship_img') {
                    let paths = val.split(',').map(p => p.trim());
                    let btns = '<div style="display:flex; gap:2px; justify-content:center;">';
                    for(let i=0; i<5; i++) {
                        let p = (paths[i] && paths[i].startsWith('static')) ? paths[i] : '';
                        let safe = p ? p.replace(/'/g, "\\'") : '';
                        if(p) btns += `<button class="img-num-btn active" onclick="viewImg('${safe}')">${i+1}</button>`;
                        else btns += `<button class="img-num-btn" style="cursor:default; color:#ccc;">${i+1}</button>`;
                    }
                    return `<td${tdCls}>${btns}</div></td>`;
                    
                }
                if(['in_dt','tax_dt','out_dt','mail_dt','issue_dt'].includes(key)) {
                    let label = key==='in_dt'?'입금일':key==='tax_dt'?'계산서발행일':key==='out_dt'?'지급일':key==='mail_dt'?'우편확인일':'기사계산서발행일';
                    let today = new Date().toISOString().slice(0,10);
                    // 계산서·우편확인: 정산관리 연동 — 셀은 날짜 유무로 표시(날짜 있음→확인완료, 없음→미확인), 버튼 클릭으로 적용/미적용
                    let displayVal = val || '';
                    let hasVal = false;
                    let onclickStr, btnLabel;
                    if(key==='tax_dt') {
                        hasVal = !!(item.tax_dt || '').toString().trim();
                        displayVal = (item.tax_dt || '').toString().trim();
                        let toggleVal = hasVal ? "''" : "'"+today+"'";
                        onclickStr = `changeStatus(${item.id}, 'tax_dt', ${toggleVal})`;
                        btnLabel = hasVal ? '확인완료' : '미확인';
                    } else if(key==='mail_dt') {
                        hasVal = !!(item.mail_dt || '').toString().trim();
                        displayVal = (item.mail_dt || '').toString().trim();
                        let toggleVal = hasVal ? "''" : "'"+today+"'";
                        onclickStr = `changeStatus(${item.id}, 'mail_dt', ${toggleVal})`;
                        btnLabel = hasVal ? '확인완료' : '미확인';
                    } else if(key==='issue_dt') {
                        hasVal = !!(item.issue_dt || val || '').toString().trim();
                        displayVal = (item.issue_dt || val || '').toString().trim();
                        let toggleVal = hasVal ? "''" : "'"+today+"'";
                        onclickStr = `changeStatus(${item.id}, 'issue_dt', ${toggleVal})`;
                        btnLabel = hasVal ? '확인완료' : '미확인';
                    } else {
                        hasVal = !!displayVal;
                        let toggleVal = hasVal ? "''" : "'"+today+"'";
                        onclickStr = `changeStatus(${item.id}, '${key}', ${toggleVal})`;
                        btnLabel = key==='in_dt' ? (hasVal?'수금완료':'미확인') : (hasVal?'지급완료':'미확인');
                    }
                    let btnHtml = `<button class="btn-status ${hasVal?'bg-green':'bg-orange'}" style="font-size:10px; padding:3px 6px;" onclick="${onclickStr}">${btnLabel}</button>`;
                    let taxBizSpan = (key==='tax_dt' && (item.tax_biz||'').trim()) ? `<span style="font-size:10px; color:#666;">${(item.tax_biz||'').trim()}</span>` : '';
                    let taxBiz2Span = (key==='issue_dt' && (item.tax_biz2||'').trim()) ? `<span style="font-size:10px; color:#666;">${(item.tax_biz2||'').trim()}</span>` : '';
                    return `<td${tdCls}><div style="display:flex; flex-direction:column; align-items:center; gap:2px;"><span style="font-size:10px; color:#1976d2; font-weight:600;">${displayVal||''}</span>${btnHtml}${taxBizSpan}${taxBiz2Span}<span style="font-size:9px; color:#888;">${label}</span></div></td>`;
                }
                if(key === 'month_end_client' || key === 'month_end_driver') {
                    let checked = (val === '1' || val === 'Y');
                    return `<td${tdCls} style="text-align:center;">${checked ? '✓' : ''}</td>`;
                }
                if(key === 'fee' || key === 'fee_out') {
                    let disp = (val === null || val === undefined || val === '') ? '' : (typeof val === 'number' ? val : parseFloat(val));
                    let fmt = (disp !== '' && !isNaN(disp)) ? Number(disp).toLocaleString() : '';
                    return `<td${tdCls} style="text-align:right;">${fmt}</td>`;
                }
                if(key === 'client_memo') {
                    let safeVal = (val === null || val === undefined) ? '' : String(val).replace(/</g, '&lt;').replace(/>/g, '&gt;');
                    return `<td${tdCls}>${safeVal}</td>`;
                }
                if(key === 'memo2') {
                    let safeVal = (val === null || val === undefined) ? '' : String(val).replace(/</g, '&lt;').replace(/>/g, '&gt;');
                    return `<td${tdCls}>${safeVal}</td>`;
                }
                return `<td${tdCls}>${val}</td>`;
            }).join('')}
        </tr>
    `).join('');
    initDraggable();
}
        function renderPagination(totalPages, currentPage, type) {
            const container = document.getElementById(type + 'Pagination');
            if (!container) return;
            const blockSize = 10;
            const block = Math.floor((currentPage - 1) / blockSize);
            const start = block * blockSize + 1;
            const end = Math.min(block * blockSize + blockSize, totalPages);
            let html = "";
            const urlParams = new URLSearchParams(window.location.search);
            var perPageEl = document.getElementById('ledgerPerPage');
            if (perPageEl && type === 'ledger') urlParams.set('per_page', perPageEl.value);
            if (type === 'ledger') {
                var searchEl = document.getElementById('ledgerSearch');
                var q = (searchEl && searchEl.value) ? searchEl.value.trim() : '';
                if (q) urlParams.set('q', q); else urlParams.delete('q');
            }
            if (block > 0) {
                urlParams.set('page', (block - 1) * blockSize + 1);
                html += `<a href="?${urlParams.toString()}" class="page-btn">이전</a>`;
            }
            for (let i = start; i <= end; i++) {
                urlParams.set('page', i);
                const activeClass = i == currentPage ? "active" : "";
                html += `<a href="?${urlParams.toString()}" class="page-btn ${activeClass}">${i}</a>`;
            }
            if (end < totalPages) {
                urlParams.set('page', end + 1);
                html += `<a href="?${urlParams.toString()}" class="page-btn">다음</a>`;
            }
            container.innerHTML = html;
        }

        function initDraggable() {
            var body = document.getElementById('ledgerBody');
            if (!body) return;
            if (!body._dragBound) {
                body._dragBound = true;
                body.addEventListener('dragstart', function(e) {
                    var t = e.target.closest('tr.draggable');
                    if (t) t.classList.add('dragging');
                }, false);
                body.addEventListener('dragend', function(e) {
                    var t = e.target.closest('tr.draggable');
                    if (t) t.classList.remove('dragging');
                    if (typeof saveLedgerOrder === 'function') saveLedgerOrder();
                }, false);
                body.addEventListener('dragover', function(e) {
                    e.preventDefault();
                    var dragging = body.querySelector('tr.dragging');
                    if (!dragging) return;
                    var after = getDragAfterElement(body, e.clientY);
                    if (after) body.insertBefore(dragging, after);
                    else body.appendChild(dragging);
                }, false);
            }
            initLedgerContextMenu();
        }
        let ledgerCtxRowId = null;
        function initLedgerContextMenu() {
            const body = document.getElementById('ledgerBody');
            const menu = document.getElementById('ledgerCtxMenu');
            if(!body || !menu) return;
            body.oncontextmenu = (e) => {
                const tr = e.target.closest('tr.draggable');
                if(!tr) return;
                e.preventDefault();
                ledgerCtxRowId = parseInt(tr.getAttribute('data-id'), 10);
                if(isNaN(ledgerCtxRowId)) return;
                var editBtn = tr.querySelector('.btn-edit');
                if(editBtn) {
                    var r = editBtn.getBoundingClientRect();
                    menu.style.left = (r.right + 6) + 'px';
                    menu.style.top = r.top + 'px';
                } else {
                    menu.style.left = e.clientX + 'px';
                    menu.style.top = e.clientY + 'px';
                }
                menu.classList.add('show');
            };
            menu.querySelectorAll('button').forEach(btn => {
                btn.onclick = () => {
                    if(ledgerCtxRowId == null) return;
                    const action = btn.getAttribute('data-action');
                    menu.classList.remove('show');
                    if(action === 'delete') {
                        if(!confirm('이 오더를 삭제하시겠습니까?')) return;
                        fetch('/api/delete_ledger/' + ledgerCtxRowId, { method: 'POST' })
                            .then(r => r.json()).then(res => { if(res.status === 'success') loadLedgerList(); else alert(res.message || '삭제 실패'); })
                            .catch(() => alert('삭제 중 오류가 발생했습니다.'));
                    } else if(action === 'recall') {
                        fetch('/api/recall_ledger/' + ledgerCtxRowId, { method: 'POST' })
                            .then(r => r.json()).then(res => { if(res.status === 'success') loadLedgerList(); else alert(res.message || '재호출 실패'); })
                            .catch(() => alert('재호출 중 오류가 발생했습니다.'));
                    }
                    ledgerCtxRowId = null;
                };
            });
            if(!window._ledgerCtxClickBound) {
                window._ledgerCtxClickBound = true;
                document.addEventListener('click', () => { menu.classList.remove('show'); ledgerCtxRowId = null; });
            }
        }

        function getDragAfterElement(container, y) {
            const draggableElements = [...container.querySelectorAll('.draggable:not(.dragging)')];
            return draggableElements.reduce((closest, child) => {
                const box = child.getBoundingClientRect();
                const offset = y - box.top - box.height / 2;
                if (offset < 0 && offset > closest.offset) return { offset: offset, element: child };
                else return closest;
            }, { offset: Number.NEGATIVE_INFINITY }).element;
        }

        var filterLedgerTid;
        function filterLedger() {
            clearTimeout(filterLedgerTid);
            filterLedgerTid = setTimeout(function() {
                var searchEl = document.getElementById('ledgerSearch');
                var query = (searchEl && searchEl.value) ? searchEl.value.trim() : '';
                var urlParams = new URLSearchParams(window.location.search);
                urlParams.set('page', '1');
                urlParams.set('q', query);
                if (query) {
                    history.replaceState(null, '', '?' + urlParams.toString());
                } else {
                    urlParams.delete('q');
                    history.replaceState(null, '', (urlParams.toString() ? '?' + urlParams.toString() : window.location.pathname));
                }
                loadLedgerList();
            }, 300);
        }

        window.editEntry = function(id) {
            const formLegacy = document.querySelector('#ledgerForm');
            if (formLegacy) {
                const item = lastLedgerData.find(d => d.id === id);
                if (!item) return;
                currentEditId = id; 
                const saveBtn = formLegacy.querySelector('.btn-save');
                if (saveBtn) saveBtn.innerText = '장부 내용 수정 완료';
                const dtKeys = ['dispatch_dt'];
                columnKeys.forEach(key => {
                    const input = formLegacy.querySelector('[name="'+key+'"]');
                    if (!input) return;
                    if (input.type === 'checkbox') { input.checked = (item[key] === "✅" || item[key] === "1" || item[key] === "Y"); return; }
                    let val = item[key] || '';
                    if (dtKeys.includes(key) && val && val.includes(' ')) val = val.replace(' ', 'T').slice(0, 16);
                    input.value = val;
                });
                if (typeof calcVatAutoForm === 'function') calcVatAutoForm();
                window.scrollTo(0, formLegacy.offsetTop - 50);
                return;
            }
            const modal = document.getElementById('ledgerEditModal');
            const editForm = document.getElementById('ledgerEditForm');
            if (!modal || !editForm) return;
            const item = lastLedgerData.find(d => d.id === id);
            if (item) {
                fillLedgerEditForm(item);
                document.getElementById('ledgerEditId').value = id;
                modal.style.display = 'block';
            } else {
                fetch('/api/get_ledger_row/' + id).then(r => r.json()).then(row => {
                    if (row.error) return;
                    fillLedgerEditForm(row);
                    document.getElementById('ledgerEditId').value = id;
                    modal.style.display = 'block';
                }).catch(function() {});
            }
        };
        function fillLedgerEditForm(item) {
            const dtKeys = ['dispatch_dt'];
            columnKeys.forEach(key => {
                const input = document.querySelector('#ledgerEditForm [name="'+key+'"]');
                if (!input || input.name === 'id') return;
                if (input.type === 'checkbox') {
                    input.checked = (item[key] === "✅" || item[key] === "1" || item[key] === "Y");
                    return;
                }
                let val = item[key] || '';
                if (dtKeys.includes(key) && val && val.includes(' ')) val = val.replace(' ', 'T').slice(0, 16);
                input.value = val;
            });
        }
        window.closeLedgerEditModal = function() {
            const modal = document.getElementById('ledgerEditModal');
            if (modal) modal.style.display = 'none';
        };
        function submitLedgerEdit() {
            const form = document.getElementById('ledgerEditForm');
            if (!form) return;
            const data = {};
            form.querySelectorAll('[name]').forEach(function(inp) {
                if (inp.name === 'id') { data.id = inp.value; return; }
                if (inp.type === 'checkbox') data[inp.name] = (inp.name === 'month_end_client' || inp.name === 'month_end_driver') ? (inp.checked ? '1' : '') : (inp.checked ? '✅' : '❌');
                else data[inp.name] = inp.value || '';
            });
            if (!data.id) { alert('수정 대상이 없습니다.'); return; }
            fetch('/api/save_ledger', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data) })
                .then(r => r.json())
                .then(function(res) {
                    if (res.status === 'success') {
                        closeLedgerEditModal();
                        if (typeof loadLedgerList === 'function') loadLedgerList();
                    } else alert(res.message || '저장 실패');
                })
                .catch(function() { alert('저장 중 오류'); });
        }

        window.changeStatus = function(id, key, val) {
            fetch('/api/update_status', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({id: id, key: key, value: val}) }).then(() => location.reload());
        };

        function addMemo() {
            const board = document.getElementById('memoBoard'); if(!board) return;
            const note = document.createElement('div'); note.className = 'sticky-note'; note.style.left = '50px'; note.style.top = '20px';
            note.innerHTML = `<div style="font-size:10px; font-weight:bold; border-bottom:1px solid #fbc02d; margin-bottom:5px;">퀵 메모 <span style="float:right; cursor:pointer;" onclick="this.parentElement.remove()">×</span></div>
                              <input type="text" placeholder="기사명/차량번호" style="width:100%; border:none; background:transparent; font-size:10px; border-bottom:1px solid #eee;">
                              <input type="text" placeholder="도착지" style="width:100%; border:none; background:transparent; font-size:10px; border-bottom:1px solid #eee;">
                              <input type="text" placeholder="도착시간" style="width:100%; border:none; background:transparent; font-size:10px;">`;
            board.appendChild(note); dragElement(note);
        }

        function dragElement(elmnt) {
            let p1=0, p2=0, p3=0, p4=0;
            elmnt.onmousedown = (e) => { if(e.target.tagName === 'INPUT') return; e.preventDefault(); p3=e.clientX; p4=e.clientY; document.onmouseup=()=>document.onmousemove=null; document.onmousemove=(e)=>{ e.preventDefault(); p1=p3-e.clientX; p2=p4-e.clientY; p3=e.clientX; p4=e.clientY; elmnt.style.top=(elmnt.offsetTop-p2)+"px"; elmnt.style.left=(elmnt.offsetLeft-p1)+"px"; }; };
        }

        window.viewOrderLog = function(orderId) {
            var orderNo = 'n' + String(orderId).padStart(2, '0');
            var titleEl = document.getElementById('logModalTitle');
            if (titleEl) titleEl.textContent = '📋 오더 수정 상세 이력 (시간순) · ' + orderNo;
    fetch(`/api/get_order_logs/${orderId}`)
        .then(r => r.json())
        .then(logs => {
            const tbody = document.getElementById('logContent');
            // 스타일을 직접 주입하여 글자가 잘리지 않고 크게 나오도록 함
            if (logs.length === 0) { 
                tbody.innerHTML = '<tr><td colspan="3" style="text-align:center; padding:50px; font-size:16px; color:#999;">기록된 변경 사항이 없습니다.</td></tr>'; 
            } else {
                tbody.innerHTML = logs.map(log => `
                    <tr style="border-bottom:2px solid #eee;">
                        <td style="padding:15px; text-align:center; font-family:monospace; font-size:14px; color:#666;">${log.timestamp}</td>
                        <td style="padding:15px; text-align:center;"><span style="background:#1a2a6c; color:white; padding:4px 10px; border-radius:4px; font-weight:bold; font-size:13px;">${log.action}</span></td>
                        <td style="padding:15px; font-size:15px; line-height:1.6; color:#000; word-break:break-all; white-space:normal;">${log.details}</td>
                    </tr>`).join('');
            }
            // 모달 창 크기 조절을 위한 스타일 수정
            const modalInner = document.querySelector('#logModal > div');
            modalInner.style.width = '95%';
            modalInner.style.maxWidth = '1200px';
            document.getElementById('logModal').style.display = 'block';
        });
};

        window.closeLogModal = function() { document.getElementById('logModal').style.display = 'none'; };
        window.onload = function() {
            if (window.location.pathname !== '/') return;
            var defer = window.requestIdleCallback ? function(fn) { window.requestIdleCallback(fn, { timeout: 100 }); } : function(fn) { setTimeout(fn, 0); };
            function runLedgerInit() {
                const urlParams = new URLSearchParams(window.location.search);
                const editId = urlParams.get('edit_id');
                if (editId) {
                    fetch('/api/get_ledger_row/' + editId)
                        .then(r => r.json())
                        .then(row => {
                            if (row.error) { defer(loadLedgerList); return; }
                            lastLedgerData = [row];
                            if (typeof editEntry === 'function') editEntry(parseInt(editId));
                            const form = document.querySelector('#ledgerForm');
                            if (form) form.scrollIntoView({ behavior: 'smooth', block: 'start' });
                            defer(loadLedgerList);
                        })
                        .catch(function() { defer(loadLedgerList); });
                } else {
                    defer(loadLedgerList);
                }
            }
            fetch('/api/load_db_mem').then(r => r.json()).then(function(d) {
                drivers = d.drivers || [];
                clients = d.clients || [];
                runLedgerInit();
            }).catch(runLedgerInit);
        }

        var ledgerFormScrollId = 'ledgerFormScroll';
        var ledgerListScrollIds = ['ledgerListScrollTop', 'ledgerListScroll', 'ledgerListScrollBottom', 'ledgerScrollBarFix'];
        let ledgerScrollPending = false, ledgerScrollSource = null, ledgerScrollIsList = false;
        function applyLedgerScroll() {
            ledgerScrollPending = false;
            var src = ledgerScrollSource; var isList = ledgerScrollIsList; ledgerScrollSource = null;
            if (!src) return;
            var left = src.scrollLeft;
            if (isList) {
                for (var i = 0; i < ledgerListScrollIds.length; i++) {
                    var el = document.getElementById(ledgerListScrollIds[i]);
                    if (el && el !== src && el.scrollLeft !== left) el.scrollLeft = left;
                }
            }
        }
        function syncLedgerScroll(sourceEl) {
            var id = sourceEl && sourceEl.id;
            ledgerScrollSource = sourceEl;
            ledgerScrollIsList = ledgerListScrollIds.indexOf(id) !== -1;
            if (ledgerScrollPending) return;
            ledgerScrollPending = true;
            requestAnimationFrame(applyLedgerScroll);
        }
        function bindLedgerScroll() {
            var formEl = document.getElementById(ledgerFormScrollId);
            if (formEl) formEl.addEventListener('scroll', function(e){ syncLedgerScroll(e.target); }, { passive: true });
            for (var i = 0; i < ledgerListScrollIds.length; i++) {
                var el = document.getElementById(ledgerListScrollIds[i]);
                if (el) el.addEventListener('scroll', function(e){ syncLedgerScroll(e.target); }, { passive: true });
            }
        }
        function updateLedgerListScrollWidths() {
            var listEl = document.getElementById('ledgerListScroll');
            var topEl = document.getElementById('ledgerListScrollTop');
            var botEl = document.getElementById('ledgerListScrollBottom');
            if (!listEl || !topEl) return;
            var mainTbl = listEl.querySelector('table');
            var topTbl = topEl.querySelector('table');
            if (!mainTbl || !topTbl) return;
            var mainRow = mainTbl.querySelector('thead tr');
            var topRow = topTbl.querySelector('tr');
            var botTbl = botEl ? botEl.querySelector('table') : null;
            var botRow = botTbl ? botTbl.querySelector('tr') : null;
            if (!mainRow || !topRow || mainRow.cells.length !== topRow.cells.length) return;
            for (var i = 0; i < mainRow.cells.length; i++) {
                var w = mainRow.cells[i].offsetWidth;
                topRow.cells[i].style.width = topRow.cells[i].style.minWidth = w + 'px';
                if (botRow && botRow.cells[i]) botRow.cells[i].style.width = botRow.cells[i].style.minWidth = w + 'px';
            }
            topEl.style.width = listEl.clientWidth + 'px';
            if (botEl) botEl.style.width = listEl.clientWidth + 'px';
        }
        function updateLedgerScrollBarWidth() {
            var inner = document.getElementById('ledgerScrollBarFixInner');
            var listEl = document.getElementById('ledgerListScroll');
            if (!inner) return;
            inner.style.width = (listEl && listEl.scrollWidth) ? listEl.scrollWidth + 'px' : '100px';
            updateLedgerListScrollWidths();
        }
        var ledgerResizeTimer = 0;
        window.addEventListener('resize', function() {
            if (ledgerResizeTimer) clearTimeout(ledgerResizeTimer);
            ledgerResizeTimer = setTimeout(function() { updateLedgerScrollBarWidth(); ledgerResizeTimer = 0; }, 200);
        });
        document.addEventListener('DOMContentLoaded', function() {
            bindLedgerScroll();
            updateLedgerScrollBarWidth();
        });
        window.updateLedgerScrollBarWidth = updateLedgerScrollBarWidth;
    </script>
</body>
</html>
"""

# [수정/추가] 로그인 페이지 HTML
LOGIN_HTML = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <title>관리자 로그인 -에스엠 로지스  </title>
    <style>
        body { font-family: 'Malgun Gothic', sans-serif; background: #f0f2f5; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
        .login-box { background: white; padding: 30px; border-radius: 10px; box-shadow: 0 4px 10px rgba(0,0,0,0.1); width: 320px; }
        h2 { text-align: center; color: #1a2a6c; margin-bottom: 20px; }
        input { width: 100%; padding: 12px; margin: 8px 0; border: 1px solid #ddd; border-radius: 5px; box-sizing: border-box; }
        button { width: 100%; padding: 12px; background: #1a2a6c; color: white; border: none; border-radius: 5px; cursor: pointer; font-size: 16px; font-weight: bold; }
        .error { color: #e74c3c; font-size: 12px; text-align: center; margin-top: 10px; }
    </style>
</head>
<body>
    <div class="login-box">
        <h2>sm logitek</h2>
        <form method="post">
            <input type="text" name="username" placeholder="아이디" required autofocus>
            <input type="password" name="password" placeholder="비밀번호" required>
            <button type="submit">로그인</button>
        </form>
        {% if error %}<div class="error">{{ error }}</div>{% endif %}
    </div>
</body>
</html>
"""

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')
        if username == ADMIN_ID and password == ADMIN_PW:
            session['logged_in'] = True
            # 로그인 시 자동 백업
            try:
                backup_all("login")
            except Exception as e:
                print(f"[backup_all login error] {e}")
            return redirect(url_for('index'))
        else:
            error = "아이디 또는 비밀번호가 올바르지 않습니다."
    return render_template_string(LOGIN_HTML, error=error)

@app.errorhandler(500)
def handle_internal_error(e):
    """예상치 못한 서버 오류 발생 시에도 자동 백업 시도"""
    try:
        backup_all("error500")
    except Exception as ex:
        print(f"[backup_all 500 error] {ex}")
    # 기존 Flask 기본 500 응답 유지
    return jsonify({"status": "error", "message": "서버 오류가 발생했습니다."}), 500

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    # 로그아웃 시 자동 백업
    try:
        backup_all("logout")
    except Exception as e:
        print(f"[backup_all logout error] {e}")
    return redirect(url_for('login'))

@app.route('/')
@login_required 
def index():
    col_keys_json = json.dumps([c['k'] for c in FULL_COLUMNS])
    col_keys_driver_json = json.dumps(list(COL_KEYS_DRIVER))
    col_keys_client_json = json.dumps(list(COL_KEYS_CLIENT))
    left_cols, right_cols = ledger_edit_modal_columns()
    ledger_edit_left_html = "".join([f'<div style="display:flex; flex-direction:column;"><label style="font-size:12px; margin-bottom:4px;">{label}</label><input {ledger_input_attrs(c)}></div>' for c, label in left_cols])
    ledger_edit_right_html = "".join([f'<div style="display:flex; flex-direction:column;"><label style="font-size:12px; margin-bottom:4px;">{label}</label><input {ledger_input_attrs(c)}></div>' for c, label in right_cols])
    content = f"""
    <div class="page-ledger">
        <div class="section" style="background:#fffbf0; border:2px solid #fbc02d;">
        <h3>⚡ 빠른 오더 입력 (초성 검색 가능)</h3>
        <p style="margin:0 0 10px 0; font-size:11px; color:#666;"><span style="background:#e3f2fd; padding:2px 6px; border-radius:3px;">파랑</span> = 업체 &nbsp; <span style="background:#ffebee; padding:2px 6px; border-radius:3px;">연한 빨강</span> = 기사</p>
        <form id="quickOrderForm">
            <div class="quick-order-grid">
                <div><label>업체명</label><input type="text" name="q_client_name" id="q_client_name" class="client-search" placeholder="초성(예:ㅇㅅㅁ)" autocomplete="off"></div>
                <div><label>노선</label><input type="text" name="q_route" id="q_route"></div>
                <div><label>업체운임</label><input type="text" inputmode="decimal" pattern="^-?[0-9]*\.?[0-9]*$" title="숫자만 입력" name="q_fee" id="q_fee" oninput="this.value=this.value.replace(/[^0-9.-]/g,'').replace(/(\..*)\./g,'$1')"></div>
                <div><label>기사명</label><input type="text" name="q_d_name" id="q_d_name" class="driver-search" placeholder="기사초성" autocomplete="off"></div>
                <div><label>차량번호</label><input type="text" name="q_c_num" id="q_c_num" class="driver-search" autocomplete="off"></div>
                <div><label>기사운임</label><input type="text" inputmode="decimal" pattern="^-?[0-9]*\.?[0-9]*$" title="숫자만 입력" name="q_fee_out" id="q_fee_out" oninput="this.value=this.value.replace(/[^0-9.-]/g,'').replace(/(\..*)\./g,'$1')"></div>
            </div>
            <div style="text-align:right;"><button type="button" class="btn-save" style="background:#e67e22;" onclick="saveLedger('quickOrderForm')">장부 즉시 등록</button></div>
        </form>
    </div>
    <div class="section">
        <h3>1. 장부 목록 및 오더 검색</h3>
        <div style="background:#f0f3f7; padding:16px; border-radius:8px; margin-bottom:16px; display:flex; gap:12px; align-items:center; flex-wrap:wrap;">
            <strong>📅 오더일 조회:</strong>
            <input type="date" id="startDate" class="search-bar" style="width:140px; margin:0;"> ~ 
            <input type="date" id="endDate" class="search-bar" style="width:140px; margin:0;">
            <button type="button" class="btn-edit" onclick="loadLedgerList()">조회</button>
            <div style="border-left:1px solid #ccc; height:24px; margin:0 8px;"></div>
            <button type="button" class="btn-status bg-blue" style="background:#ebf2ff; color:#1a2a6c; border:1px solid #1a2a6c;" onclick="setDateRange(7)">1주일</button>
            <button type="button" class="btn-status bg-blue" style="background:#ebf2ff; color:#1a2a6c; border:1px solid #1a2a6c;" onclick="setDateRange(30)">1달</button>
            <button type="button" class="btn" onclick="location.href='/'">전체보기</button>
        </div>
        <div style="display:flex; align-items:center; gap:12px; margin-bottom:10px; flex-wrap:wrap;">
            <label style="display:inline-flex; align-items:center; gap:6px; cursor:pointer;"><input type="checkbox" id="filterMonthEndClient" onchange="loadLedgerList()"> 업체 월말합산만</label>
            <label style="display:inline-flex; align-items:center; gap:6px; cursor:pointer;"><input type="checkbox" id="filterMonthEndDriver" onchange="loadLedgerList()"> 기사 월말합산만</label>
            <span style="margin-left:8px;">출력</span>
            <select id="ledgerPerPage" onchange="var sel=document.getElementById('ledgerPerPage'); var q=new URLSearchParams(location.search); q.set('per_page',sel.value); q.set('page','1'); history.replaceState(null,'', '?'+q.toString()); loadLedgerList();" style="padding:6px 10px; border:1px solid #d0d7de; border-radius:4px; font-size:13px;">
                <option value="20" selected>20</option>
                <option value="50">50</option>
                <option value="100">100</option>
            </select>
            <span style="font-size:12px; color:#666;">개씩</span>
        </div>
        <div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap; margin-bottom:10px;">
        <input type="text" id="ledgerSearch" class="search-bar" placeholder="오더고유번호(n01), 기사명, 업체명, 차량번호, 노선 등 검색..." onkeyup="filterLedger()">
        <a href="/settlement" style="color:#1a2a6c; font-weight:600; text-decoration:none; white-space:nowrap;">정산관리 바로가기 →</a>
        <button type="button" class="btn-edit" onclick="var s=document.getElementById('startDate').value; var e=document.getElementById('endDate').value; var c=document.getElementById('filterMonthEndClient').checked?'1':''; var d=document.getElementById('filterMonthEndDriver').checked?'1':''; var u='/api/ledger_excel?start='+encodeURIComponent(s)+'&end='+encodeURIComponent(e)+'&month_end_client='+c+'&month_end_driver='+d; window.location.href=u;">엑셀 다운로드</button>
        <a href="/api/ledger_excel" class="btn-status bg-green" style="text-decoration:none; padding:6px 12px; border-radius:4px;">전체 목록 다운로드</a>
        <form id="ledgerUploadForm" style="display:inline;" enctype="multipart/form-data">
            <input type="file" name="file" accept=".xlsx,.xls" style="font-size:12px;">
            <button type="button" class="btn-save" onclick="var f=document.getElementById('ledgerUploadForm'); var fd=new FormData(f); fetch('/api/ledger_upload', {{method:'POST', body:fd}}).then(r=>r.json()).then(res=>{{if(res.status==='success'){{alert(res.message||'반영 완료'); loadLedgerList(); if(confirm('통계 페이지에서 확인할까요?')) location.href='/statistics';}}else alert(res.message||'업로드 실패');}}).catch(()=>alert('업로드 중 오류'));">엑셀 업로드</button>
        </form>
        <button type="button" class="btn-status" style="background:#c53030; color:white; border:none; padding:6px 12px; border-radius:4px; cursor:pointer; font-weight:600;" onclick="if(confirm('장부 내역을 전체 삭제합니다. 복구할 수 없습니다. 정말 진행할까요?')) fetch('/api/ledger_delete_all', {{method:'POST'}}).then(r=>r.json()).then(res=>{{if(res.status==='success'){{alert(res.message); if(typeof loadLedgerList==='function') loadLedgerList();}}else alert(res.message||'삭제 실패');}}).catch(()=>alert('삭제 중 오류'));" title="ledger 테이블 전체 삭제">엑셀 장부전체삭제</button>
        <a href="/api/download-db" class="btn-status bg-orange" style="text-decoration:none; padding:6px 12px; border-radius:4px;" title="배포 전 서버 DB 백업">📥 서버 DB 백업</a>
        </div>
        <div class="scroll-sticky-wrap">
        <div class="scroll-top" id="ledgerListScrollTop"><table><thead><tr><th>관리</th>{"".join([f"<th{_col_attr(c['k'])}>{c['n']}</th>" for c in FULL_COLUMNS])}</tr></thead><tbody><tr>{"<td>-</td>" * (1 + len(FULL_COLUMNS))}</tr></tbody></table></div>
        <div class="scroll-x scroll-x-ledger" id="ledgerListScroll"><table><thead><tr><th>관리</th>{"".join([f"<th{_col_attr(c['k'])}>{c['n']}</th>" for c in FULL_COLUMNS])}</tr></thead><tbody id="ledgerBody"></tbody></table></div>
        <div class="scroll-top" id="ledgerListScrollBottom" style="margin-top:4px;"><table><thead><tr><th>관리</th>{"".join([f"<th{_col_attr(c['k'])}>{c['n']}</th>" for c in FULL_COLUMNS])}</tr></thead><tbody><tr>{"<td>-</td>" * (1 + len(FULL_COLUMNS))}</tr></tbody></table></div>
        </div>
        <div id="ledgerPagination" class="pagination"></div>
    </div>
    <div id="ledgerScrollBarFix" class="ledger-scrollbar-fix"><div id="ledgerScrollBarFixInner" class="ledger-scrollbar-fix-inner"></div></div>
    <div id="ledgerEditModal" style="display:none; position:fixed; z-index:9999; left:0; top:0; width:100%; height:100%; background:rgba(0,0,0,0.5);">
        <style>#ledgerEditForm input {{ width:100%; padding:6px 8px; font-size:12px; box-sizing:border-box; }}</style>
        <div style="background:white; width:96%; max-width:1100px; margin:20px auto; padding:20px; border-radius:10px; max-height:90vh; overflow:hidden; display:flex; flex-direction:column;">
            <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:2px solid #1a2a6c; padding-bottom:10px; margin-bottom:12px; flex-shrink:0;">
                <h3 style="margin:0; color:#1a2a6c; font-size:18px;">📝 장부 수정</h3>
                <button type="button" onclick="closeLedgerEditModal()" style="background:none; border:none; font-size:24px; cursor:pointer; color:#999;">&times;</button>
            </div>
            <form id="ledgerEditForm" style="overflow-y:auto; flex:1;">
                <input type="hidden" name="id" id="ledgerEditId" value="">
                <div style="display:grid; grid-template-columns: 1fr 1fr; gap:16px 24px; align-content:start;">
                    <div style="display:flex; flex-direction:column; gap:8px;">{ledger_edit_left_html}</div>
                    <div style="display:flex; flex-direction:column; gap:8px;">{ledger_edit_right_html}</div>
                </div>
                <div style="margin-top:16px; padding-top:12px; border-top:1px solid #eee; display:flex; gap:10px;">
                    <button type="button" class="btn-save" onclick="submitLedgerEdit()">저장</button>
                    <button type="button" class="btn" onclick="closeLedgerEditModal()">취소</button>
                </div>
            </form>
        </div>
    </div>
    <div id="logModal" style="display:none; position:fixed; z-index:9999; left:0; top:0; width:100%; height:100%; background:rgba(0,0,0,0.6);">
        <div style="background:white; width:95%; max-width:1200px; margin:30px auto; padding:25px; border-radius:10px; box-shadow:0 5px 25px rgba(0,0,0,0.4);">
            <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:3px solid #1a2a6c; padding-bottom:12px; margin-bottom:15px;">
                <h3 id="logModalTitle" style="margin:0; color:#1a2a6c; font-size:18px;">📋 오더 수정 상세 이력 (시간순)</h3>
                <button onclick="closeLogModal()" style="background:none; border:none; font-size:28px; cursor:pointer; color:#999;">&times;</button>
            </div>
            <div style="max-height:70vh; overflow-y:auto; border:1px solid #eee;">
                <table style="width:100%; border-collapse:collapse; font-size:14px; table-layout: fixed;">
                    <thead>
                        <tr style="background:#f8f9fa; position: sticky; top: 0; z-index: 10;">
                            <th style="padding:12px; border:1px solid #dee2e6; width:180px;">수정일시</th>
                            <th style="padding:12px; border:1px solid #dee2e6; width:100px;">작업분류</th>
                            <th style="padding:12px; border:1px solid #dee2e6;">수정 및 변경 상세 내용</th>
                        </tr>
                    </thead>
                    <tbody id="logContent" style="word-break: break-all; white-space: pre-wrap;"></tbody>
                </table>
            </div>
            <div style="text-align:right; margin-top:15px;">
                <button onclick="closeLogModal()" style="padding:8px 20px; background:#6c757d; color:white; border:none; border-radius:5px; cursor:pointer;">닫기</button>
            </div>
        </div>
    </div>
    </div>
    """
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps([]), clients_json=json.dumps([]), col_keys=col_keys_json, col_keys_driver=col_keys_driver_json, col_keys_client=col_keys_client_json)
@app.route('/settlement')
@login_required 
def settlement():
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    # 쿼리 파라미터는 항상 문자열로 취급 (숫자 1개만 입력해도 오류 없음)
    def _arg_str(key, default=''):
        return str(request.args.get(key) or default).strip()
    q_status = _arg_str('status')
    q_name = _arg_str('name')
    q_c_num = _arg_str('c_num')
    q_start = _arg_str('start')
    q_end = _arg_str('end')
    page = max(1, safe_int(_arg_str('page') or '1', 1))
    per_page_arg = safe_int(_arg_str('per_page') or '20', 20)
    per_page = per_page_arg if per_page_arg in (20, 50, 100) else 20
    
    # 성능: 날짜·이름 필터를 SQL로 적용해 조회량 감소
    query = "SELECT * FROM ledger"
    params = []
    conditions = []
    if q_start:
        conditions.append("order_dt >= ?")
        params.append(q_start)
    if q_end:
        conditions.append("order_dt <= ?")
        params.append(q_end)
    if q_name:
        name_part = q_name
        add_id_condition = False
        id_val = None
        # n01, n02 형식 또는 숫자만(01, 02, 12 등) 입력 시 id 조건 추가
        if name_part.lower().startswith('n'):
            num_str = name_part[1:].lstrip('0') or '0'
            if num_str.isdigit():
                try:
                    id_val = int(num_str)
                    add_id_condition = True
                except (ValueError, TypeError):
                    pass
        elif name_part.isdigit():
            try:
                id_val = int(name_part)
                add_id_condition = True
            except (ValueError, TypeError):
                pass
        like_part = "(client_name LIKE ? OR d_name LIKE ?)"
        like_params = [f"%{name_part}%", f"%{name_part}%"]
        if add_id_condition and id_val is not None:
            conditions.append("(" + like_part + " OR id = ?)")
            params.extend(like_params + [id_val])
        else:
            conditions.append(like_part)
            params.extend(like_params)
    if q_c_num:
        conditions.append("COALESCE(c_num,'') LIKE ?")
        params.append(f"%{q_c_num}%")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY dispatch_dt DESC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    
    filtered_rows = []
    today = now_kst()
    today_naive = today.replace(tzinfo=None)  # naive용 비교 (DB 날짜는 timezone 없음)
    
    for row in rows:
        in_dt = row['in_dt']; out_dt = row['out_dt']; pay_due_dt = row['pay_due_dt']
        pre_post = row['pre_post']; dispatch_dt_str = row['dispatch_dt']
        order_dt = row['order_dt'] or "" # 날짜 필터를 위한 변수
        tax_img = row['tax_img'] or ""; ship_img = row['ship_img'] or ""
        
        # 1. 미수 상태 판별 로직 복구
        misu_status = "미수"; misu_color = "bg-red"
        if in_dt:
            misu_status = "수금완료"; misu_color = "bg-green"
        else:
            is_over_30 = False
            if dispatch_dt_str:
                try:
                    d_dt = datetime.fromisoformat(dispatch_dt_str.replace(' ', 'T'))
                    if today_naive > d_dt + timedelta(days=30): is_over_30 = True
                except Exception:
                    pass
            
            is_due_passed = False
            if pay_due_dt:
                try:
                    p_due = datetime.strptime(pay_due_dt, "%Y-%m-%d")
                    if today.date() > p_due.date(): is_due_passed = True
                except Exception:
                    pass
            
            # 조건부 미수 판단 (선착불/결제예정일 없는 초기 상태)
            if not pre_post and not in_dt and not pay_due_dt:
                if is_over_30: misu_status = "미수"; misu_color = "bg-red"
                else: misu_status = "조건부미수금"; misu_color = "bg-blue"
            elif is_due_passed or pre_post:
                misu_status = "미수"; misu_color = "bg-red"

        # 2. 지급 상태 판별 로직 복구
        pay_status = "미지급"; pay_color = "bg-red"
        if out_dt:
            pay_status = "지급완료"; pay_color = "bg-green"
        else:
            has_tax_img = any('static' in p for p in tax_img.split(','))
            has_ship_img = any('static' in p for p in ship_img.split(','))
            # 수금완료 + 서류구비 완료 시에만 진짜 '미지급', 아니면 '조건부'
            if in_dt and has_tax_img and has_ship_img:
                pay_status = "미지급"; pay_color = "bg-red"
            else:
                pay_status = "조건부미지급"; pay_color = "bg-blue"

        # 3. 검색 필터 적용 (날짜/이름/상태 통합 필터)
        # 날짜 기간 필터
        if q_start and order_dt < q_start: continue
        if q_end and order_dt > q_end: continue
        
        # 이름 필터 (업체/기사명 + 오더고유번호 n01, n02 또는 숫자만 01, 12 등)
        if q_name:
            q = q_name.lower()
            in_client = q in str(row['client_name'] or '').lower()
            in_driver = q in str(row['d_name'] or '').lower()
            order_no = ('n' + str(row['id']).zfill(2)).lower()
            match_order = (q == order_no or q in order_no or order_no in q)
            match_id = False
            if q.isdigit():
                try:
                    match_id = (row['id'] == int(q))
                except (ValueError, TypeError):
                    pass
            if not (in_client or in_driver or match_order or match_id):
                continue

        # 차량번호 전용 필터 (Row는 .get 없음 → 인덱스 접근)
        if q_c_num and q_c_num.lower() not in str(row['c_num'] or '').lower():
            continue

        # 상태 필터
        if q_status:
            if q_status == 'misu_all' and in_dt: continue
            if q_status == 'pay_all' and out_dt: continue
            if q_status == 'misu_only' and misu_status != '미수': continue
            if q_status == 'cond_misu' and misu_status != '조건부미수금': continue
            if q_status == 'pay_only' and pay_status != '미지급': continue
            if q_status == 'cond_pay' and pay_status != '조건부미지급': continue
            if q_status == 'done_in' and not in_dt: continue
            if q_status == 'done_out' and not out_dt: continue

        row_data = dict(row)
        row_data['m_st'] = misu_status; row_data['m_cl'] = misu_color
        row_data['p_st'] = pay_status; row_data['p_cl'] = pay_color
        filtered_rows.append(row_data)

    total_pages = max(1, (len(filtered_rows) + per_page - 1) // per_page)
    page = min(max(1, page), total_pages)
    start = (page - 1) * per_page
    end = start + per_page
    page_data = filtered_rows[start:end]

    client_by_name = {str(c.get('업체명') or '').strip(): c for c in clients_db if (c.get('업체명') or '').strip()}
    table_rows = ""
    for row in page_data:
        # 토글 변수 설정 (데이터가 있으면 공백으로 보내서 미수/미지급 처리)
        in_dt_toggle = f"'{today.strftime('%Y-%m-%d')}'" if not row['in_dt'] else "''"
        out_dt_toggle = f"'{today.strftime('%Y-%m-%d')}'" if not row['out_dt'] else "''"
        # 계산서·우편확인: 장부와 동일하게 날짜 유무로 버튼 눌림 상태 판단 (날짜 있음 → 녹색 적용, 없음 → 주황 미적용)
        tax_dt_val = (row.get('tax_dt') or '').strip() if row.get('tax_dt') else ''
        tax_chk_ok = bool(tax_dt_val) or _norm_tax_chk(row.get('tax_chk'))
        tax_chk_toggle = "''" if tax_chk_ok else "'발행완료'"
        mail_dt_val = (row.get('mail_dt') or '').strip() if row.get('mail_dt') else ''
        mail_dt_toggle = f"'{today.strftime('%Y-%m-%d')}'" if not mail_dt_val else "''"

        in_dt_val = row.get('in_dt') or ''
        out_dt_val = row.get('out_dt') or ''
        in_dt_span = f'<span style="font-size:10px; color:#1976d2;">{in_dt_val}</span>' if in_dt_val else ''
        tax_dt_span = f'<span style="font-size:10px; color:#1976d2;">{tax_dt_val}</span>' if tax_dt_val else ''
        out_dt_span = f'<span style="font-size:10px; color:#1976d2;">{out_dt_val}</span>' if out_dt_val else ''
        mail_dt_span = f'<span style="font-size:10px; color:#1976d2;">{mail_dt_val}</span>' if mail_dt_val else ''
        tax_label = '계산서 발행확인' if tax_chk_ok else '미발행'
        misu_btn = f'<div style="display:flex; flex-direction:column; align-items:center; gap:2px;"><input type="date" value="{in_dt_val}" style="font-size:10px; width:95px; padding:2px;" onchange="changeStatus({row["id"]}, \'in_dt\', this.value)">{in_dt_span}<button class="btn-status {row["m_cl"]}" onclick="changeStatus({row["id"]}, \'in_dt\', {in_dt_toggle})">{row["m_st"]}</button></div>'
        tax_issued_btn = f'<button class="btn-status {"bg-green" if tax_chk_ok else "bg-orange"}" onclick="changeStatus({row["id"]}, \'tax_chk\', {tax_chk_toggle})">{tax_label}</button>'
        tax_biz_val = (row.get('tax_biz') or '').strip()
        tax_biz_span = f'<span style="font-size:10px; color:#666;">{tax_biz_val}</span>' if tax_biz_val else ''
        tax_cell = f'<div style="display:flex; flex-direction:column; align-items:center; gap:2px;"><input type="date" value="{tax_dt_val}" style="font-size:10px; width:95px; padding:2px;" onchange="changeStatus({row["id"]}, \'tax_dt\', this.value)">{tax_dt_span}<div>{tax_issued_btn}</div>{tax_biz_span}</div>'
        pay_btn = f'<div style="display:flex; flex-direction:column; align-items:center; gap:2px;"><input type="date" value="{out_dt_val}" style="font-size:10px; width:95px; padding:2px;" onchange="changeStatus({row["id"]}, \'out_dt\', this.value)">{out_dt_span}<button class="btn-status {row["p_cl"]}" onclick="changeStatus({row["id"]}, \'out_dt\', {out_dt_toggle})">{row["p_st"]}</button></div>'
        
        mail_ok = bool(mail_dt_val) or _norm_mail_done(row.get('is_mail_done'))
        mail_val = '확인완료' if mail_ok else '미확인'
        mail_color = "bg-green" if mail_ok else "bg-orange"
        mail_btn = f'<div style="display:flex; flex-direction:column; align-items:center; gap:2px;"><input type="date" value="{mail_dt_val}" style="font-size:10px; width:95px; padding:2px;" onchange="changeStatus({row["id"]}, \'mail_dt\', this.value)">{mail_dt_span}<button class="btn-status {mail_color}" onclick="changeStatus({row["id"]}, \'mail_dt\', {mail_dt_toggle})">{mail_val}</button></div>'

        issue_dt_val = (row.get('issue_dt') or '').strip()
        issue_dt_toggle = f"'{today.strftime('%Y-%m-%d')}'" if not issue_dt_val else "''"
        issue_dt_span = f'<span style="font-size:10px; color:#1976d2;">{issue_dt_val}</span>' if issue_dt_val else ''
        tax_biz2_val = (row.get('tax_biz2') or '').strip()
        tax_biz2_span = f'<span style="font-size:10px; color:#666;">{tax_biz2_val}</span>' if tax_biz2_val else ''
        issue_confirmed = bool(issue_dt_val)
        issue_btn = f'<div style="display:flex; flex-direction:column; align-items:center; gap:2px;"><input type="date" value="{issue_dt_val}" style="font-size:10px; width:95px; padding:2px;" onchange="changeStatus({row["id"]}, \'issue_dt\', this.value)">{issue_dt_span}<button class="btn-status {"bg-green" if issue_confirmed else "bg-orange"}" onclick="changeStatus({row["id"]}, \'issue_dt\', {issue_dt_toggle})">{"확인완료" if issue_confirmed else "미확인"}</button>{tax_biz2_span}</div>'

        me_c = (str(row.get('month_end_client') or '').strip() in ('1', 'Y'))
        me_d = (str(row.get('month_end_driver') or '').strip() in ('1', 'Y'))
        rid = row['id']
        month_end_client_cell = f'<input type="checkbox" {"checked" if me_c else ""} onchange="fetch(\'/api/update_status\', {{method:\'POST\', headers:{{\'Content-Type\':\'application/json\'}}, body: JSON.stringify({{id:{rid}, key:\'month_end_client\', value: this.checked ? \'1\' : \'\'}})}}).then(r=>r.json()).then(res=>{{if(res.status===\'success\') location.reload(); else alert(res.message||\'반영 실패\');}});">'
        month_end_driver_cell = f'<input type="checkbox" {"checked" if me_d else ""} onchange="fetch(\'/api/update_status\', {{method:\'POST\', headers:{{\'Content-Type\':\'application/json\'}}, body: JSON.stringify({{id:{rid}, key:\'month_end_driver\', value: this.checked ? \'1\' : \'\'}})}}).then(r=>r.json()).then(res=>{{if(res.status===\'success\') location.reload(); else alert(res.message||\'반영 실패\');}});">'

        def make_direct_links(ledger_id, img_type, raw_paths):
            paths = [p.strip() for p in (raw_paths or "").split(',')] if raw_paths else []
            links_html = '<div style="display:flex; gap:3px; justify-content:center;">'
            for i in range(1, 6):
                has_file = len(paths) >= i and paths[i-1].startswith('static')
                css_class = "link-btn has-file" if has_file else "link-btn"
                links_html += f'<a href="/upload_evidence/{ledger_id}?type={img_type}&seq={i}" target="_blank" class="{css_class}">{i}</a>'
            links_html += '</div>'
            return links_html

        fee_display = int(calc_supply_value(row))
        is_cash_client = (str(row.get('pay_method_client') or '').strip() == '현금')
        is_cash_driver = (str(row.get('pay_method_driver') or '').strip() == '현금')
        vat1 = 0 if is_cash_client else int(round(fee_display * 0.1))
        total1 = fee_display + vat1
        fee_out_val = int(float(row.get('fee_out') or 0))
        vat2 = 0 if is_cash_driver else int(round(fee_out_val * 0.1))
        total2 = fee_out_val + vat2
        order_no = "n" + str(row['id']).zfill(2)
        _esc_attr = lambda x: (str(x) or '').replace('"', '&quot;')[:200]
        has_tax = '1' if any('static' in p for p in (row.get('tax_img') or '').split(',')) else '0'
        has_ship = '1' if any('static' in p for p in (row.get('ship_img') or '').split(',')) else '0'
        me_c = '1' if (str(row.get('month_end_client') or '').strip() in ('1', 'Y')) else '0'
        me_d = '1' if (str(row.get('month_end_driver') or '').strip() in ('1', 'Y')) else '0'
        _tax_chk_val = '발행완료' if tax_chk_ok else ''
        _mail_val = '확인완료' if mail_ok else '미확인'
        table_rows += f"""<tr class="data-row" data-order-no="{order_no}" data-client-name="{_esc_attr(row.get('client_name'))}" data-tax-chk="{_esc_attr(_tax_chk_val)}" data-order-dt="{row.get('order_dt') or ''}" data-route="{_esc_attr(row.get('route'))}" data-d-name="{_esc_attr(row.get('d_name'))}" data-c-num="{_esc_attr(row.get('c_num'))}" data-supply="{fee_display}" data-vat1="{vat1}" data-total1="{total1}" data-m-st="{row['m_st']}" data-fee-out="{fee_out_val}" data-vat2="{vat2}" data-total2="{total2}" data-p-st="{row['p_st']}" data-mail="{_esc_attr(_mail_val)}" data-issue-dt="{row.get('issue_dt') or ''}" data-has-tax="{has_tax}" data-has-ship="{has_ship}" data-me-c="{me_c}" data-me-d="{me_d}">
            <td style="white-space:nowrap;">
                <span class="order-no" style="display:inline-block; font-weight:700; color:#1a2a6c; margin-right:8px; font-size:12px;" title="고유오더번호">{order_no}</span>
                <button class="btn-log" onclick="viewOrderLog({row['id']})" style="background:#6c757d; color:white; border:none; padding:2px 5px; cursor:pointer; font-size:11px; border-radius:3px;">로그</button>
            </td>
            <td>{row['client_name']}</td><td>{tax_cell}</td><td>{row['order_dt']}</td><td>{row['route']}</td><td>{row['d_name']}</td><td>{row['c_num']}</td><td>{fee_display:,}</td><td>{vat1:,}</td><td>{total1:,}</td><td>{misu_btn}</td><td>{fee_out_val:,}</td><td>{vat2:,}</td><td>{total2:,}</td><td>{pay_btn}</td><td>{mail_btn}</td><td>{issue_btn}</td><td>{make_direct_links(row['id'], 'tax', row['tax_img'])}</td><td>{make_direct_links(row['id'], 'ship', row['ship_img'])}</td><td style="text-align:center;">{month_end_client_cell}</td><td style="text-align:center;">{month_end_driver_cell}</td></tr>"""
    
    def _settlement_query(page_num=None):
        qdict = {'status': q_status, 'name': q_name, 'start': q_start, 'end': q_end, 'per_page': per_page}
        if q_c_num:
            qdict['c_num'] = q_c_num
        if page_num is not None:
            qdict['page'] = page_num
        return urlencode(qdict)

    _block = 10
    _block_idx = (page - 1) // _block
    _start = _block_idx * _block + 1
    _end = min(_block_idx * _block + _block, total_pages)
    _parts = []
    if _block_idx > 0:
        _parts.append(f'<a href="/settlement?{_settlement_query((_block_idx - 1) * _block + 1)}" class="page-btn">이전</a>')
    for i in range(_start, _end + 1):
        _parts.append(f'<a href="/settlement?{_settlement_query(i)}" class="page-btn {"active" if i==page else ""}">{i}</a>')
    if _end < total_pages:
        _parts.append(f'<a href="/settlement?{_settlement_query(_end + 1)}" class="page-btn">다음</a>')
    pagination_html = "".join(_parts)

    _qe = html.escape
    content = f"""<div class="section page-settlement"><h2>정산 관리 (기간 및 실시간 필터)</h2>
    <form class="filter-box" method="get" style="display: flex; gap: 10px; align-items: center; flex-wrap: wrap;">
        <strong>📅 오더일:</strong>
        <input type="date" name="start" value="{_qe(q_start)}"> ~ 
        <input type="date" name="end" value="{_qe(q_end)}">
        <strong>🔍 필터:</strong>
        <select name="status">
            <option value="">전체상태</option>
            <option value="misu_only" {'selected' if q_status=='misu_only' else ''}>미수</option>
            <option value="misu_all" {'selected' if q_status=='misu_all' else ''}>미수금 전체</option>
            <option value="cond_misu" {'selected' if q_status=='cond_misu' else ''}>조건부미수</option>
            <option value="pay_only" {'selected' if q_status=='pay_only' else ''}>미지급</option>
            <option value="pay_all" {'selected' if q_status=='pay_all' else ''}>미지급 전체</option>
            <option value="cond_pay" {'selected' if q_status=='cond_pay' else ''}>조건부미지급</option>
            <option value="done_in" {'selected' if q_status=='done_in' else ''}>수금완료</option>
            <option value="done_out" {'selected' if q_status=='done_out' else ''}>지급완료</option>
        </select>
        <input type="text" name="name" value="{_qe(q_name)}" placeholder="오더고유번호(n01)·숫자(01)·업체/기사 검색">
        <strong>🚗 차량번호:</strong>
        <input type="text" name="c_num" value="{_qe(q_c_num)}" placeholder="차량번호만 검색" style="width:100px;">
        <span style="margin-left:8px;">출력</span>
        <select name="per_page" onchange="this.form.submit()" style="padding:6px 10px; border:1px solid #d0d7de; border-radius:4px; font-size:13px;">
            <option value="20" {"selected" if per_page==20 else ""}>20</option>
            <option value="50" {"selected" if per_page==50 else ""}>50</option>
            <option value="100" {"selected" if per_page==100 else ""}>100</option>
        </select>
        <span style="font-size:12px; color:#666;">개씩</span>
        <button type="submit" class="btn-save">조회</button>
        <button type="button" onclick="location.href='/settlement'" class="btn-status bg-gray">초기화</button>
    </form>
    <div style="margin: 15px 0;">
        <a href="/export_misu_info?{urlencode({'status': q_status, 'name': q_name, 'c_num': q_c_num, 'start': q_start, 'end': q_end})}" class="btn-status bg-red" style="text-decoration:none;">미수금 업체정보 엑셀</a>
        <a href="/export_pay_info?{urlencode({'status': q_status, 'name': q_name, 'c_num': q_c_num, 'start': q_start, 'end': q_end})}" class="btn-status bg-orange" style="text-decoration:none; margin-left:5px;">미지급 기사정보 엑셀</a>
        <a href="/export_tax_not_issued?{urlencode({'status': q_status, 'name': q_name, 'c_num': q_c_num, 'start': q_start, 'end': q_end})}" class="btn-status bg-gray" style="text-decoration:none; margin-left:5px;">세금계산서 미발행 엑셀</a>
    </div>
    <div class="scroll-sticky-wrap">
    <div class="scroll-top" id="settlementScrollTop"><table><thead><tr><th>로그</th><th>업체명</th><th>계산서</th><th>오더일</th><th>노선</th><th>기사명</th><th>차량번호</th><th>공급가액</th><th>부가세</th><th>합계</th><th>수금상태</th><th>기사운임</th><th>부가세</th><th>합계</th><th>지급상태</th><th>우편확인</th><th>기사계산서발행일</th><th>기사계산서</th><th>운송장</th><th>업체월말</th><th>기사월말</th></tr></thead><tbody><tr><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td></tr></tbody></table></div>
    <div class="scroll-x" id="settlementScroll"><table id="settlementTable"><thead><tr><th data-sort="order-no" style="cursor:pointer;" title="클릭 시 오름/내림차순">로그 ↕</th><th data-sort="client-name" style="cursor:pointer;" title="클릭 시 오름/내림차순">업체명 ↕</th><th data-sort="tax-chk" style="cursor:pointer;" title="클릭 시 오름/내림차순">계산서 ↕</th><th data-sort="order-dt" style="cursor:pointer;" title="클릭 시 오름/내림차순">오더일 ↕</th><th data-sort="route" style="cursor:pointer;" title="클릭 시 오름/내림차순">노선 ↕</th><th data-sort="d-name" style="cursor:pointer;" title="클릭 시 오름/내림차순">기사명 ↕</th><th data-sort="c-num" style="cursor:pointer;" title="클릭 시 오름/내림차순">차량번호 ↕</th><th data-sort="supply" style="cursor:pointer;" title="클릭 시 오름/내림차순">공급가액 ↕</th><th data-sort="vat1" style="cursor:pointer;" title="클릭 시 오름/내림차순">부가세 ↕</th><th data-sort="total1" style="cursor:pointer;" title="클릭 시 오름/내림차순">합계 ↕</th><th data-sort="m-st" style="cursor:pointer;" title="클릭 시 오름/내림차순">수금상태 ↕</th><th data-sort="fee-out" style="cursor:pointer;" title="클릭 시 오름/내림차순">기사운임 ↕</th><th data-sort="vat2" style="cursor:pointer;" title="클릭 시 오름/내림차순">부가세 ↕</th><th data-sort="total2" style="cursor:pointer;" title="클릭 시 오름/내림차순">합계 ↕</th><th data-sort="p-st" style="cursor:pointer;" title="클릭 시 오름/내림차순">지급상태 ↕</th><th data-sort="mail" style="cursor:pointer;" title="클릭 시 오름/내림차순">우편확인 ↕</th><th data-sort="issue-dt" style="cursor:pointer;" title="클릭 시 오름/내림차순">기사계산서발행일 ↕</th><th data-sort="has-tax" style="cursor:pointer;" title="클릭 시 오름/내림차순">기사계산서 ↕</th><th data-sort="has-ship" style="cursor:pointer;" title="클릭 시 오름/내림차순">운송장 ↕</th><th data-sort="me-c" style="cursor:pointer;" title="클릭 시 오름/내림차순">업체월말 ↕</th><th data-sort="me-d" style="cursor:pointer;" title="클릭 시 오름/내림차순">기사월말 ↕</th></tr></thead><tbody>{table_rows}</tbody></table></div>
    <div class="scroll-top" id="settlementScrollBottom" style="margin-top:4px;"><table><thead><tr><th>로그</th><th>업체명</th><th>계산서</th><th>오더일</th><th>노선</th><th>기사명</th><th>차량번호</th><th>공급가액</th><th>부가세</th><th>합계</th><th>수금상태</th><th>기사운임</th><th>부가세</th><th>합계</th><th>지급상태</th><th>우편확인</th><th>기사계산서발행일</th><th>기사계산서</th><th>운송장</th><th>업체월말</th><th>기사월말</th></tr></thead><tbody><tr><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td></tr></tbody></table></div>
    </div>
    <div class="pagination">{pagination_html}</div></div>
    <div id="settlementScrollBarFix" class="ledger-scrollbar-fix"><div id="settlementScrollBarFixInner" class="ledger-scrollbar-fix-inner"></div></div>
    <div id="logModal" style="display:none; position:fixed; z-index:9999; left:0; top:0; width:100%; height:100%; background:rgba(0,0,0,0.6);">
        <div style="background:white; width:90%; max-width:800px; margin:50px auto; padding:20px; border-radius:10px; box-shadow:0 5px 15px rgba(0,0,0,0.3);">
            <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:2px solid #1a2a6c; padding-bottom:10px; margin-bottom:15px;">
                <h3 id="logModalTitle" style="margin:0; color:#1a2a6c;">📋 오더 변경 이력</h3>
                <button onclick="closeLogModal()" style="background:none; border:none; font-size:24px; cursor:pointer;">&times;</button>
            </div>
            <div style="max-height:500px; overflow-y:auto;"><table style="width:100%; border-collapse:collapse; font-size:13px;"><thead><tr style="background:#f4f4f4;"><th style="padding:10px; border:1px solid #ddd; width:30%;">일시</th><th style="padding:10px; border:1px solid #ddd; width:15%;">작업</th><th style="padding:10px; border:1px solid #ddd;">상세내용</th></tr></thead><tbody id="logContent"></tbody></table></div>
            <div style="text-align:right; margin-top:15px;"><button onclick="closeLogModal()" style="padding:8px 20px; background:#6c757d; color:white; border:none; border-radius:5px; cursor:pointer;">닫기</button></div>
        </div>
    </div>
    <script>
    window.viewOrderLog = function(orderId) {{
        var orderNo = 'n' + String(orderId).padStart(2, '0');
        var titleEl = document.getElementById('logModalTitle');
        if (titleEl) titleEl.textContent = '📋 오더 변경 이력 · ' + orderNo;
        fetch('/api/get_order_logs/' + orderId)
            .then(function(r) {{ return r.json(); }})
            .then(function(logs) {{
                var tbody = document.getElementById('logContent');
                if (!logs || logs.length === 0) {{
                    tbody.innerHTML = '<tr><td colspan="3" style="text-align:center; padding:50px; font-size:16px; color:#999;">기록된 변경 사항이 없습니다.</td></tr>';
                }} else {{
                    tbody.innerHTML = logs.map(function(log) {{
                        return '<tr style="border-bottom:2px solid #eee;"><td style="padding:15px; text-align:center; font-family:monospace; font-size:14px; color:#666;">' + log.timestamp + '</td><td style="padding:15px; text-align:center;"><span style="background:#1a2a6c; color:white; padding:4px 10px; border-radius:4px; font-weight:bold; font-size:13px;">' + (log.action || '') + '</span></td><td style="padding:15px; font-size:15px; line-height:1.6; color:#000; word-break:break-all; white-space:normal;">' + (log.details || '') + '</td></tr>';
                    }}).join('');
                }}
                document.getElementById('logModal').style.display = 'block';
            }});
    }};
    window.closeLogModal = function() {{ var el = document.getElementById('logModal'); if (el) el.style.display = 'none'; }};
    (function() {{
        const topEl = document.getElementById('settlementScrollTop');
        const mainEl = document.getElementById('settlementScroll');
        const botEl = document.getElementById('settlementScrollBottom');
        const barEl = document.getElementById('settlementScrollBarFix');
        const barInner = document.getElementById('settlementScrollBarFixInner');
        if (!topEl || !mainEl) return;
        function matchWidth() {{
            const mainTbl = mainEl.querySelector('table');
            const topTbl = topEl.querySelector('table');
            const botTbl = botEl ? botEl.querySelector('table') : null;
            if (mainTbl && topTbl) {{
                const mainRow = mainTbl.querySelector('thead tr') || mainTbl.querySelector('tr');
                const topRow = topTbl.querySelector('tr');
                const botRow = botTbl ? botTbl.querySelector('tr') : null;
                if (mainRow && topRow && mainRow.cells.length === topRow.cells.length) {{
                    for (let i = 0; i < mainRow.cells.length; i++) {{
                        const w = mainRow.cells[i].offsetWidth;
                        topRow.cells[i].style.width = w + 'px';
                        topRow.cells[i].style.minWidth = w + 'px';
                        if (botRow && botRow.cells[i]) {{ botRow.cells[i].style.width = w + 'px'; botRow.cells[i].style.minWidth = w + 'px'; }}
                    }}
                }}
            }}
            const w = mainEl.clientWidth;
            topEl.style.width = w + 'px';
            if (botEl) botEl.style.width = w + 'px';
            if (barInner) barInner.style.width = (mainEl.scrollWidth || 100) + 'px';
        }}
        var settleScrollPending = false, settleScrollSrc = null;
        function applySettleScroll() {{
            settleScrollPending = false;
            var src = settleScrollSrc; settleScrollSrc = null;
            if (!src) return;
            var left = src.scrollLeft;
            if (topEl.scrollLeft !== left) topEl.scrollLeft = left;
            if (mainEl.scrollLeft !== left) mainEl.scrollLeft = left;
            if (botEl && botEl.scrollLeft !== left) botEl.scrollLeft = left;
            if (barEl && barEl.scrollLeft !== left) barEl.scrollLeft = left;
        }}
        function syncSettle(e) {{
            settleScrollSrc = e.target;
            if (settleScrollPending) return;
            settleScrollPending = true;
            requestAnimationFrame(applySettleScroll);
        }}
        var scrollOpt = {{ passive: true }};
        topEl.addEventListener('scroll', syncSettle, scrollOpt);
        mainEl.addEventListener('scroll', syncSettle, scrollOpt);
        if (botEl) botEl.addEventListener('scroll', syncSettle, scrollOpt);
        if (barEl) barEl.addEventListener('scroll', syncSettle, scrollOpt);
        var settleResizeTimer = 0;
        window.addEventListener('resize', function() {{
            if (settleResizeTimer) clearTimeout(settleResizeTimer);
            settleResizeTimer = setTimeout(function() {{ matchWidth(); settleResizeTimer = 0; }}, 200);
        }});
        setTimeout(matchWidth, 80);
    }})();
    (function() {{
        var tbl = document.getElementById("settlementTable");
        if (!tbl) return;
        var sortCol = null;
        var sortAsc = true;
        var numericKeys = ["supply", "vat1", "total1", "fee-out", "vat2", "total2", "has-tax", "has-ship", "me-c", "me-d"];
        function sortSettleTable(colKey) {{
            var tbody = tbl.querySelector("tbody");
            if (!tbody) return;
            var dataRows = [].slice.call(tbody.querySelectorAll("tr.data-row"));
            dataRows.sort(function(a, b) {{
                var va = (a.getAttribute(colKey) || "").trim();
                var vb = (b.getAttribute(colKey) || "").trim();
                var key = colKey.replace("data-", "");
                if (numericKeys.indexOf(key) !== -1) {{
                    var na = parseFloat(va) || 0;
                    var nb = parseFloat(vb) || 0;
                    return sortAsc ? na - nb : nb - na;
                }}
                if (va < vb) return sortAsc ? -1 : 1;
                if (va > vb) return sortAsc ? 1 : -1;
                return 0;
            }});
            dataRows.forEach(function(tr) {{ tbody.appendChild(tr); }});
        }}
        tbl.querySelectorAll("thead th[data-sort]").forEach(function(th) {{
            th.addEventListener("click", function() {{
                var key = th.getAttribute("data-sort") || "";
                var colKey = "data-" + key;
                if (sortCol === colKey) sortAsc = !sortAsc;
                else {{ sortCol = colKey; sortAsc = true; }}
                sortSettleTable(colKey);
            }});
        }});
    }})();
    </script>
    """
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys="[]")
def _statistics_date_range(period_months, today):
    """기간(1/3/6개월)에 따른 start, end 날짜 반환 (오늘 기준 과거 N개월 ~ 오늘)"""
    end_d = today.date()
    if period_months == 1:
        start_d = end_d - timedelta(days=30)
    elif period_months == 3:
        start_d = end_d - timedelta(days=90)
    else:  # 6
        start_d = end_d - timedelta(days=180)
    return start_d.strftime('%Y-%m-%d'), end_d.strftime('%Y-%m-%d')


@app.route('/statistics')
@login_required 
def statistics():
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    now = now_kst()
    today = now
    # 기간: period(1/3/6개월) 또는 start/end 직접 입력. 선택한 기간은 쿠키로 유지
    q_period = request.args.get('period', '').strip() or request.cookies.get('stats_period', '').strip()
    q_start = request.args.get('start', '')
    q_end = request.args.get('end', '')
    if q_period in ('1', '3', '6'):
        months = int(q_period)
        q_start, q_end = _statistics_date_range(months, today)
    elif not q_start or not q_end:
        # 기본: 저장된 기간이 있으면 사용, 없으면 1개월
        saved = request.cookies.get('stats_period', '1').strip()
        if saved in ('1', '3', '6'):
            q_start, q_end = _statistics_date_range(int(saved), today)
            q_period = saved
        else:
            q_period = '1'
            q_start, q_end = _statistics_date_range(1, today)
    else:
        q_period = ''  # 사용자 지정 기간이면 period 비움
    q_client = request.args.get('client', '').strip()
    q_driver = request.args.get('driver', '').strip()
    q_c_num = request.args.get('c_num', '').strip()
    q_status = request.args.get('status', '')
    q_month_client = request.args.get('month_end_client', '')
    q_month_driver = request.args.get('month_end_driver', '')
    q_in_start = request.args.get('in_start', '').strip()
    q_in_end = request.args.get('in_end', '').strip()
    q_out_start = request.args.get('out_start', '').strip()
    q_out_end = request.args.get('out_end', '').strip()
    q_dispatch_start = request.args.get('dispatch_start', '').strip()
    q_dispatch_end = request.args.get('dispatch_end', '').strip()

    rows = conn.execute("SELECT * FROM ledger").fetchall(); conn.close()
    filtered_rows = []
    today = now_kst()
    today_naive = today.replace(tzinfo=None) if getattr(today, 'tzinfo', None) else today

    # 기사관리(기사현황)에서 개인/고정="고정"인 차량번호 목록 (차량번호 기준 필터)
    fixed_c_nums = {str(d.get('차량번호', '')).strip() for d in drivers_db if str(d.get('개인/고정', '')).strip() == '고정'}
    fixed_c_nums.discard('')  # 빈 문자열 제외

    for row in rows:
        r = dict(row)
        order_dt = (r.get('order_dt', '') or "")[:10]  # YYYY-MM-DD만 사용 (업로드·DB 혼용 대응)
        # 기간/업체/기사 필터
        if q_start and q_end and not (q_start <= order_dt <= q_end): continue
        if q_client and q_client not in str(r.get('client_name', '')): continue
        if q_driver and q_driver not in str(r.get('d_name', '')): continue
        if q_c_num and q_c_num not in str(r.get('c_num', '')): continue
        if q_month_client and (str(r.get('month_end_client') or '').strip() not in ('1', 'Y')): continue
        if q_month_driver and (str(r.get('month_end_driver') or '').strip() not in ('1', 'Y')): continue

        # 정산 상태 판별 — 정산관리와 동일 로직 (미수 필터 시 미수만 정확히 표시)
        in_dt = r.get('in_dt'); out_dt = r.get('out_dt')
        pay_due_dt = r.get('pay_due_dt'); pre_post = r.get('pre_post')
        dispatch_dt_str = r.get('dispatch_dt')
        tax_img = r.get('tax_img') or ""; ship_img = r.get('ship_img') or ""

        misu_status = "미수"
        if in_dt:
            misu_status = "수금완료"
        else:
            is_over_30 = False
            if dispatch_dt_str:
                try:
                    d_dt = datetime.fromisoformat(str(dispatch_dt_str).replace(' ', 'T'))
                    if today_naive > d_dt + timedelta(days=30): is_over_30 = True
                except Exception:
                    pass
            is_due_passed = False
            if pay_due_dt:
                try:
                    p_due = datetime.strptime(str(pay_due_dt), "%Y-%m-%d")
                    if today.date() > p_due.date(): is_due_passed = True
                except Exception:
                    pass
            if not pre_post and not pay_due_dt:
                misu_status = "미수" if is_over_30 else "조건부미수금"
            elif is_due_passed or pre_post:
                misu_status = "미수"
        m_st = "조건부미수" if misu_status == "조건부미수금" else misu_status

        p_st = "지급완료" if out_dt else ("조건부미지급" if not in_dt else "미지급")
        if not out_dt and in_dt:
            has_tax_img = any('static' in p for p in (tax_img or '').split(','))
            has_ship_img = any('static' in p for p in (ship_img or '').split(','))
            if has_tax_img and has_ship_img:
                p_st = "미지급"
            else:
                p_st = "조건부미지급"
        # 고정 여부: 기사관리 차량번호 기준 (해당 차량번호의 장부 데이터만)
        c_num = str(r.get('c_num', '')).strip()
        d_type = "직영" if c_num in fixed_c_nums else "일반"

        # 세부 상태 필터
        if q_status:
            if q_status in ["미수", "조건부미수", "수금완료"] and q_status != m_st: continue
            if q_status in ["미지급", "조건부미지급", "지급완료"] and q_status != p_st: continue
            if q_status == "고정" and c_num not in fixed_c_nums: continue
            if q_status in ["직영", "일반"] and q_status != d_type: continue

        # 수금완료 변경일(입금일) 기준 조회
        if q_in_start or q_in_end:
            in_dt_val = (in_dt or '')[:10] if in_dt else ''
            if not in_dt_val: continue
            if q_in_start and in_dt_val < q_in_start: continue
            if q_in_end and in_dt_val > q_in_end: continue
        # 지급완료 변경일(지급일) 기준 조회
        if q_out_start or q_out_end:
            out_dt_val = (out_dt or '')[:10] if out_dt else ''
            if not out_dt_val: continue
            if q_out_start and out_dt_val < q_out_start: continue
            if q_out_end and out_dt_val > q_out_end: continue
        # 배차일 기준 조회
        if q_dispatch_start or q_dispatch_end:
            dispatch_dt_val = (dispatch_dt_str or '')[:10] if dispatch_dt_str else ''
            if not dispatch_dt_val: continue
            if q_dispatch_start and dispatch_dt_val < q_dispatch_start: continue
            if q_dispatch_end and dispatch_dt_val > q_dispatch_end: continue

        r['m_st'] = m_st; r['p_st'] = p_st; r['d_type'] = d_type
        # 정산관리에 있는 항목(미수/지급 상태, 공급가·부가세·합계)은 정산관리와 동일한 계산으로 값 설정
        fee, vat1, total1, fo, vat2, total2 = calc_totals_with_vat(r)
        r['fee'] = fee; r['vat1'] = vat1; r['total1'] = total1
        r['fee_out'] = fo; r['vat2'] = vat2; r['total2'] = total2
        # 그 외 항목은 통합장부(ledger) 원본 값 유지
        filtered_rows.append(r)

    stats_total_count = len(filtered_rows)
    df = pd.DataFrame(filtered_rows)
    summary_monthly = ""; summary_daily = ""
    full_settlement_client = ""; full_settlement_driver = ""
    q_client_enc = quote(q_client, safe='') if q_client else ''
    q_driver_enc = quote(q_driver, safe='') if q_driver else ''
    q_c_num_enc = quote(q_c_num, safe='') if q_c_num else ''
    q_status_enc = quote(q_status, safe='') if q_status else ''
    q_month_client_enc = '&month_end_client=1' if q_month_client else ''
    q_month_driver_enc = '&month_end_driver=1' if q_month_driver else ''

    if not df.empty:
        # fee, vat1, total1, fee_out, vat2, total2 는 위에서 정산관리와 동일 계산으로 이미 설정됨
        df['fee_out'] = pd.to_numeric(df['fee_out'], errors='coerce').fillna(0)
        
        # 월별 요약: 매출=업체합계(total1), 지출=기사합계(total2), 부가세 표시
        df['month'] = df['order_dt'].str[:7]
        m_grp = df.groupby('month').agg({'fee':'sum', 'vat1':'sum', 'total1':'sum', 'fee_out':'sum', 'vat2':'sum', 'total2':'sum', 'id':'count'}).sort_index(ascending=False)
        for month, v in m_grp.iterrows():
            summary_monthly += f"<tr><td>{month}</td><td>{int(v['id'])}건</td><td>{int(v['fee']):,}</td><td>{int(v['vat1']):,}</td><td>{int(v['total1']):,}</td><td>{int(v['fee_out']):,}</td><td>{int(v['vat2']):,}</td><td>{int(v['total2']):,}</td><td>{int(v['total1']-v['total2']):,}</td></tr>"

        # 일별 실적 (최근 15일): 매출=업체합계, 지출=기사합계, 부가세 표시
        d_grp = df.groupby('order_dt').agg({'fee':'sum', 'vat1':'sum', 'total1':'sum', 'fee_out':'sum', 'vat2':'sum', 'total2':'sum', 'id':'count'}).sort_index(ascending=False).head(15)
        for date, v in d_grp.iterrows():
            summary_daily += f"<tr><td>{date}</td><td>{int(v['id'])}</td><td>{int(v['fee']):,}</td><td>{int(v['vat1']):,}</td><td>{int(v['total1']):,}</td><td>{int(v['fee_out']):,}</td><td>{int(v['vat2']):,}</td><td>{int(v['total2']):,}</td></tr>"

        # 업체 정산 데이터 조립: 업체별 "수신 [업체명] 정산서" 형식, 오더일|노선|공급가액|부가세|합계 (미수란 미표기, 오더일 오름차순)
        for client_name, grp in df.sort_values(by=['client_name', 'order_dt'], ascending=[True, True]).groupby('client_name'):
            cname = str(client_name or '').strip() or '(업체명 없음)'
            cname_attr = cname.replace('&', '&amp;').replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
            cname_display = cname.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            grp_fee_sum = int(grp['fee'].sum())
            grp_vat_sum = int(grp['vat1'].sum())
            grp_total_sum = int(grp['total1'].sum())
            full_settlement_client += f"""
            <div class="client-settle-card" data-client="{cname_attr}">
                <div class="client-settle-title">수신 「{cname_display}」 정산서</div>
                <table class="client-settle-table"><thead><tr><th>오더일</th><th>노선</th><th>공급가액</th><th>부가세</th><th>합계</th></tr></thead><tbody>"""
            for _, r in grp.iterrows():
                fee_val = int(r['fee'])
                vat_val = int(r['vat1'])
                total_val = int(r['total1'])
                full_settlement_client += f"<tr><td>{r['order_dt']}</td><td>{r.get('route','')}</td><td style='text-align:right;'>{fee_val:,}</td><td style='text-align:right;'>{vat_val:,}</td><td style='text-align:right;'>{total_val:,}</td></tr>"
            full_settlement_client += f"<tr class='client-sum-row'><td colspan='2'>합계</td><td style='text-align:right; font-weight:bold;'>{grp_fee_sum:,}</td><td style='text-align:right; font-weight:bold;'>{grp_vat_sum:,}</td><td style='text-align:right; font-weight:bold;'>{grp_total_sum:,}</td></tr></tbody></table></div>"

        # 기사 정산 데이터 조립: 업체와 동일 카드 형식, 상단 "기사 정산서 [기사명]님", 테이블은 오더일·노선·기사운임·부가세·합계만 (기사명·업체명·입금일 제외)
        full_settlement_driver = "<div class='client-settle-sections'>"
        for name, group in df.groupby('d_name'):
            group = group.sort_values(by='order_dt', ascending=True)
            grp_sum = int(group['fee_out'].sum())
            grp_vat = int(group['vat2'].sum())
            grp_total = int(group['total2'].sum())
            name_attr = (name or '').replace('&', '&amp;').replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
            name_display = (name or '').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            full_settlement_driver += f"<div class='client-settle-card' data-driver='{name_attr}'><div class='client-settle-title'>기사 정산서 {name_display}님</div><table class='client-settle-table'><thead><tr><th>오더일</th><th>노선</th><th>기사운임</th><th>부가세</th><th>합계</th></tr></thead><tbody>"
            for _, r in group.iterrows():
                fee_out_val = int(r['fee_out'])
                vat_val = int(r['vat2'])
                total_val = int(r['total2'])
                full_settlement_driver += f"<tr><td>{r['order_dt']}</td><td>{r['route']}</td><td style='text-align:right;'>{fee_out_val:,}</td><td style='text-align:right;'>{vat_val:,}</td><td style='text-align:right;'>{total_val:,}</td></tr>"
            full_settlement_driver += f"<tr class='client-sum-row'><td colspan='2'>합계</td><td style='text-align:right; font-weight:bold;'>{grp_sum:,}</td><td style='text-align:right; font-weight:bold;'>{grp_vat:,}</td><td style='text-align:right; font-weight:bold;'>{grp_total:,}</td></tr></tbody></table></div>"
        full_settlement_driver += "</div>"

    # 이체확인 테이블: 오더일 기준 오름차순(최근 아래), 운임별 정렬·소계
    stats_transfer_table = ""
    if not df.empty:
        df_sorted = df.sort_values(by=['order_dt', 'total1'], ascending=[True, True])
        sum_total1 = int(df_sorted['total1'].sum())
        sum_total2 = int(df_sorted['total2'].sum())
        def _esc(s):
            if s is None or (isinstance(s, float) and pd.isna(s)): return ''
            s = str(s).strip()
            return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')
        rows_html = ""
        grp_t1 = 0
        grp_t2 = 0
        prev_total1 = None
        for _, r in df_sorted.iterrows():
            total1_val = int(r.get('total1', 0))
            if prev_total1 is not None and prev_total1 != total1_val and (grp_t1 or grp_t2):
                rows_html += f"<tr class=\"summary-row\" style='background:#e8f0e8; font-weight:bold;'><td colspan='2'>운임별 소계</td><td style='text-align:right;'>{grp_t1:,}</td><td style='text-align:right;'>{grp_t2:,}</td><td colspan='9'></td></tr>"
                grp_t1 = 0
                grp_t2 = 0
            prev_total1 = total1_val
            order_dt = _esc(r.get('order_dt'))
            dispatch_dt = _esc(r.get('dispatch_dt'))
            m_st = _esc(r.get('m_st', ''))
            in_dt = _esc(r.get('in_dt', ''))
            p_st = _esc(r.get('p_st', ''))
            out_dt = _esc(r.get('out_dt', ''))
            total2_val = int(r.get('total2', 0))
            in_name = _esc(r.get('in_name', ''))
            month_val = _esc(r.get('month_val', ''))
            route = _esc(r.get('route', ''))
            d_name = _esc(r.get('d_name', ''))
            c_num = _esc(r.get('c_num', ''))
            d_phone = _esc(r.get('d_phone', ''))
            tax_biz_name = _esc(r.get('tax_biz_name', ''))
            memo_parts = [_esc(r.get('memo1', '')), _esc(r.get('memo2', '')), _esc(r.get('req_add', ''))]
            memo_str = ' / '.join(p for p in memo_parts if p)
            cell_misu = f"<div style='text-align:right;'><span style='font-size:10px; color:#666;'>수금상태: {m_st}</span><br><span style='font-size:10px; color:#666;'>변경일: {in_dt or '-'}</span><br><strong>{total1_val:,}</strong></div>"
            cell_pay = f"<div style='text-align:right;'><span style='font-size:10px; color:#666;'>지급상태: {p_st}</span><br><span style='font-size:10px; color:#666;'>변경일: {out_dt or '-'}</span><br><strong>{total2_val:,}</strong></div>"
            rows_html += f"<tr class=\"data-row\" data-order-dt=\"{order_dt}\" data-dispatch-dt=\"{dispatch_dt}\" data-total1=\"{total1_val}\" data-total2=\"{total2_val}\" data-in-name=\"{_esc(in_name)}\" data-route=\"{_esc(route)}\" data-d-name=\"{_esc(d_name)}\" data-c-num=\"{c_num}\"><td>{order_dt}</td><td>{dispatch_dt}</td><td>{cell_misu}</td><td>{cell_pay}</td><td>{in_name}</td><td>{month_val}</td><td>{route}</td><td>{d_name}</td><td>{c_num}</td><td>{d_phone}</td><td>{tax_biz_name}</td><td style='text-align:right;'>{total2_val:,}</td><td style='text-align:left; max-width:120px;'>{memo_str}</td></tr>"
            grp_t1 += total1_val
            grp_t2 += total2_val
        if grp_t1 or grp_t2:
            rows_html += f"<tr class=\"summary-row\" style='background:#e8f0e8; font-weight:bold;'><td colspan='2'>운임별 소계</td><td style='text-align:right;'>{grp_t1:,}</td><td style='text-align:right;'>{grp_t2:,}</td><td colspan='9'></td></tr>"
        stats_transfer_table = f"""
        <div class="section" style="margin-top:20px;">
            <h3>💳 이체확인 (총 {stats_total_count}개)</h3>
            <div style="margin-bottom:10px;"><a href="/api/statistics_transfer_excel?start={q_start}&amp;end={q_end}&amp;client={q_client_enc}&amp;driver={q_driver_enc}&amp;c_num={q_c_num_enc}&amp;status={q_status_enc}&amp;month_end_client={q_month_client or ''}&amp;month_end_driver={q_month_driver or ''}&amp;in_start={q_in_start}&amp;in_end={q_in_end}&amp;out_start={q_out_start}&amp;out_end={q_out_end}&amp;dispatch_start={q_dispatch_start}&amp;dispatch_end={q_dispatch_end}" class="btn-status bg-green" style="text-decoration:none; cursor:pointer;">📥 엑셀 다운로드</a></div>
            <div class="table-scroll stats-transfer-scroll">
            <table class="client-settle-table" id="statsTransferTable">
                <thead><tr><th data-sort="order-dt" style="cursor:pointer;" title="클릭 시 오름/내림차순">오더일 ↕</th><th data-sort="dispatch-dt" style="cursor:pointer;" title="클릭 시 오름/내림차순">배차일 ↕</th><th data-sort="total1" style="cursor:pointer;" title="클릭 시 오름/내림차순">업체운임합계 ↕</th><th data-sort="total2" style="cursor:pointer;" title="클릭 시 오름/내림차순">기사운임합계 ↕</th><th>입금자명</th><th>입금내역</th><th data-sort="route" style="cursor:pointer;">노선 ↕</th><th data-sort="d-name" style="cursor:pointer;">기사명 ↕</th><th>차량번호</th><th>연락처</th><th>사업자</th><th>기사운임합계</th><th>특이사항</th></tr></thead>
                <tbody>{rows_html}</tbody>
                <tfoot><tr style="background:#d0e8d0; font-weight:bold;"><td colspan="2">총합계</td><td style="text-align:right;">{sum_total1:,}</td><td style="text-align:right;">{sum_total2:,}</td><td colspan="9"></td></tr></tfoot>
            </table>
            </div>
        </div>"""
    else:
        stats_transfer_table = ""

    # 기사사업자별계산서: 사업자번호, 사업자, 지급일 + 기사명·차량번호·금액 (통계와 동일 필터 데이터)
    driver_biz_rows_html = ""
    driver_biz_list = []
    if not df.empty:
        for _, r in df.iterrows():
            biz_num = str(r.get('tax_biz_num', '') or '').strip()
            biz_name = str(r.get('tax_biz_name', '') or '').strip()
            out_dt_val = (r.get('out_dt') or '')[:10] if r.get('out_dt') else ''
            d_name_val = str(r.get('d_name', '') or '').strip()
            c_num_val = str(r.get('c_num', '') or '').strip()
            dispatch_dt_val = str(r.get('dispatch_dt', '') or '')[:19] if r.get('dispatch_dt') else ''  # YYYY-MM-DD 또는 YYYY-MM-DDTHH:MM
            order_dt_val = str(r.get('order_dt', '') or '')[:10] if r.get('order_dt') else ''
            route_val = str(r.get('route', '') or '').strip()
            fee_out_val = int(r.get('fee_out', 0) or 0)
            vat2_val = int(r.get('vat2', 0) or 0)
            total2_val = int(r.get('total2', 0) or 0)
            driver_biz_list.append({
                '사업자번호': biz_num, '사업자': biz_name, '지급일': out_dt_val,
                '기사명': d_name_val, '차량번호': c_num_val, '배차일': dispatch_dt_val, '오더일': order_dt_val, '노선': route_val,
                '기사운임': fee_out_val, '부가세': vat2_val, '합계': total2_val,
            })
        driver_biz_list.sort(key=lambda x: ((x['지급일'] or '9999'), x['사업자'], x['오더일']))
        for b in driver_biz_list:
            def _besc(s):
                if s is None: return ''
                s = str(s).strip()
                return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')
            dispatch_display = (b['배차일'] or '')[:10] if b['배차일'] else ''
            driver_biz_rows_html += f"<tr class=\"biz-row\" data-biz-num=\"{_besc(b['사업자번호'])}\" data-biz-name=\"{_besc(b['사업자'])}\" data-out-dt=\"{_besc(b['지급일'])}\" data-d-name=\"{_besc(b['기사명'])}\" data-c-num=\"{_besc(b['차량번호'])}\" data-dispatch-dt=\"{_besc(b['배차일'])}\" data-order-dt=\"{_besc(b['오더일'])}\"><td>{_besc(b['사업자번호'])}</td><td>{_besc(b['사업자'])}</td><td>{_besc(b['지급일'])}</td><td>{_besc(b['기사명'])}</td><td>{_besc(b['차량번호'])}</td><td>{dispatch_display}</td><td>{_besc(b['오더일'])}</td><td>{_besc(b['노선'])}</td><td style='text-align:right;'>{b['기사운임']:,}</td><td style='text-align:right;'>{b['부가세']:,}</td><td style='text-align:right;'>{b['합계']:,}</td></tr>"
    biz_table_count = len(driver_biz_list)
    stats_biz_section = f"""
        <div class="section" style="margin-top:28px;">
            <h3>📋 기사사업자별계산서 <span style="font-size:0.9em; color:#555;">(총 {biz_table_count}건)</span></h3>
            <div style="margin-bottom:10px; display:flex; flex-wrap:wrap; gap:10px; align-items:center;">
                <input type="text" id="bizTableSearch" placeholder="사업자번호, 사업자명, 지급일, 기사명, 차량번호 등 검색" style="width:320px; padding:8px 12px; border:1px solid #cbd5e1; border-radius:6px;">
                <a href="/api/statistics_biz_settlement_excel?start={q_start}&amp;end={q_end}&amp;client={q_client_enc}&amp;driver={q_driver_enc}&amp;c_num={q_c_num_enc}&amp;status={q_status_enc}&amp;month_end_client={q_month_client or ''}&amp;month_end_driver={q_month_driver or ''}&amp;in_start={q_in_start}&amp;in_end={q_in_end}&amp;out_start={q_out_start}&amp;out_end={q_out_end}&amp;dispatch_start={q_dispatch_start}&amp;dispatch_end={q_dispatch_end}" class="btn-status bg-green" style="text-decoration:none;">📥 엑셀 다운로드</a>
                <button type="button" onclick="captureBizSettle()" class="btn-status bg-orange">🖼️ 정산서 양식 이미지 저장</button>
            </div>
            <div class="table-scroll stats-transfer-scroll" id="bizSettleZone">
            <table class="client-settle-table" id="statsBizTable">
                <thead><tr><th>사업자번호</th><th>사업자</th><th>지급일</th><th>기사명</th><th>차량번호</th><th data-sort="dispatch-dt" style="cursor:pointer;" title="클릭 시 배차일 기준 오름/내림차순">배차일 ↕</th><th data-sort="order-dt" style="cursor:pointer;" title="클릭 시 오더일 기준 오름/내림차순">오더일 ↕</th><th>노선</th><th style="text-align:right;">기사운임</th><th style="text-align:right;">부가세</th><th style="text-align:right;">합계</th></tr></thead>
                <tbody>{driver_biz_rows_html}</tbody>
            </table>
            </div>
        </div>"""
    if df.empty:
        stats_biz_section = """
        <div class="section" style="margin-top:28px;">
            <h3>📋 기사사업자별계산서 <span style="font-size:0.9em; color:#555;">(총 0건)</span></h3>
            <div style="margin-bottom:10px;"><input type="text" id="bizTableSearch" placeholder="사업자번호, 사업자명, 지급일, 기사명, 차량번호 등 검색" style="width:320px; padding:8px 12px; border:1px solid #cbd5e1; border-radius:6px;" disabled></div>
            <div class="table-scroll stats-transfer-scroll" id="bizSettleZone"><table class="client-settle-table" id="statsBizTable"><thead><tr><th>사업자번호</th><th>사업자</th><th>지급일</th><th>기사명</th><th>차량번호</th><th>배차일 ↕</th><th>오더일 ↕</th><th>노선</th><th>기사운임</th><th>부가세</th><th>합계</th></tr></thead><tbody></tbody></table></div>
        </div>"""

    # 기간 버튼(1/3/6개월) 클릭 시 다른 필터 유지용 쿼리
    _parts = []
    if q_client: _parts.append('client=' + quote(q_client, safe=''))
    if q_driver: _parts.append('driver=' + quote(q_driver, safe=''))
    if q_c_num: _parts.append('c_num=' + quote(q_c_num, safe=''))
    if q_status: _parts.append('status=' + quote(q_status, safe=''))
    if q_month_client: _parts.append('month_end_client=1')
    if q_month_driver: _parts.append('month_end_driver=1')
    if q_in_start: _parts.append('in_start=' + quote(q_in_start, safe=''))
    if q_in_end: _parts.append('in_end=' + quote(q_in_end, safe=''))
    if q_out_start: _parts.append('out_start=' + quote(q_out_start, safe=''))
    if q_out_end: _parts.append('out_end=' + quote(q_out_end, safe=''))
    if q_dispatch_start: _parts.append('dispatch_start=' + quote(q_dispatch_start, safe=''))
    if q_dispatch_end: _parts.append('dispatch_end=' + quote(q_dispatch_end, safe=''))
    q_extra = '&' + '&'.join(_parts) if _parts else ''

    content = f"""
    <style>
        .summary-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 25px; }}
        .table-scroll {{ max-height: 350px; overflow-y: auto; background: white; border: 1px solid #ddd; border-radius: 5px; }}
        .stats-transfer-scroll {{ max-height: 700px; overflow-y: auto; background: white; border: 1px solid #ddd; border-radius: 5px; }}
        .tab-btn {{ padding: 12px 25px; cursor: pointer; border: none; background: #eee; font-weight: bold; font-size: 14px; border-radius: 5px 5px 0 0; }}
        .tab-btn.active {{ background: #1a2a6c; color: white; }}
        .tab-content {{ display: none; border: 1px solid #ddd; padding: 20px; background: white; border-radius: 0 0 5px 5px; }}
        .tab-content.active {{ display: block; }}
        #printArea {{ position: fixed; left: -9999px; top: 0; background: white; width: 850px; }}
        .client-settle-sections {{ display: flex; flex-direction: column; gap: 24px; }}
        .client-settle-card {{ background: #fafbfc; border: 1px solid #e2e8f0; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }}
        .client-settle-title {{ background: linear-gradient(135deg, #1a2a6c 0%, #2c3e7a 100%); color: white; padding: 14px 20px; font-size: 16px; font-weight: bold; letter-spacing: 0.5px; }}
        .client-settle-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
        .client-settle-table th {{ background: #f1f5f9; padding: 10px 12px; text-align: center; border-bottom: 2px solid #e2e8f0; font-weight: 600; color: #334155; }}
        .client-settle-table td {{ padding: 10px 12px; border-bottom: 1px solid #e2e8f0; }}
        .client-settle-table .client-sum-row {{ background: #e8f0e8; font-weight: bold; }}
        .settle-footer-msg {{ margin-top: 28px; padding-top: 16px; border-top: 2px solid #e2e8f0; text-align: right; font-size: 16px; font-weight: bold; color: #1a2a6c; letter-spacing: 2px; }}
    </style>

    <div id="printArea"><div id="printContent"></div></div>

    <div class="section">
        <h2 style="color:#1a2a6c; margin-bottom:20px; border-left:5px solid #1a2a6c; padding-left:10px;">📈 에스엠 로지텍 정산 센터 <span style="font-size:0.85em; color:#555;">(총 {stats_total_count}개)</span></h2>
        
        <form method="get" id="statsFilterForm" style="background:#f8f9fa; padding:20px; border-radius:10px; display:flex; gap:12px; flex-wrap:wrap; align-items:center; border:1px solid #dee2e6;">
            <strong>📅 기간:</strong>
            <span style="display:inline-flex; gap:6px; align-items:center;">
                <a href="/statistics?period=1{q_extra}" class="tab-btn {'active' if q_period=='1' else ''}" style="padding:8px 14px; text-decoration:none; color:inherit;">1개월</a>
                <a href="/statistics?period=3{q_extra}" class="tab-btn {'active' if q_period=='3' else ''}" style="padding:8px 14px; text-decoration:none; color:inherit;">3개월</a>
                <a href="/statistics?period=6{q_extra}" class="tab-btn {'active' if q_period=='6' else ''}" style="padding:8px 14px; text-decoration:none; color:inherit;">6개월</a>
            </span>
            <span style="color:#666; font-size:12px;">또는</span>
            <input type="date" name="start" value="{q_start}"> ~ <input type="date" name="end" value="{q_end}">
            <strong>🏢 업체:</strong> <input type="text" name="client" value="{q_client}" style="width:100px;">
            <strong>🚚 기사:</strong> <input type="text" name="driver" value="{q_driver}" style="width:100px;">
            <strong>🚗 차량번호:</strong> <input type="text" name="c_num" value="{q_c_num}" placeholder="차량번호 검색" style="width:100px;">
            <strong>🔍 상태:</strong>
            <select name="status">
                <option value="">전체보기</option>
                <option value="미수" {'selected' if q_status=='미수' else ''}>미수</option>
                <option value="조건부미수" {'selected' if q_status=='조건부미수' else ''}>조건부미수</option>
                <option value="수금완료" {'selected' if q_status=='수금완료' else ''}>수금완료</option>
                <option value="지급완료" {'selected' if q_status=='지급완료' else ''}>지급완료</option>
                <option value="미지급" {'selected' if q_status=='미지급' else ''}>미지급</option>
                <option value="조건부미지급" {'selected' if q_status=='조건부미지급' else ''}>조건부미지급</option>
                <option value="고정" {'selected' if q_status=='고정' else ''}>고정</option>
            </select>
            <label style="display:inline-flex; align-items:center; gap:4px;"><input type="checkbox" name="month_end_client" value="1" {'checked' if q_month_client else ''}> 업체 월말합산</label>
            <label style="display:inline-flex; align-items:center; gap:4px;"><input type="checkbox" name="month_end_driver" value="1" {'checked' if q_month_driver else ''}> 기사 월말합산</label>
            <div style="flex-basis:100%; height:0;"></div>
            <strong>💰 수금완료 변경일:</strong> <input type="date" name="in_start" value="{q_in_start}" title="입금일 시작"> ~ <input type="date" name="in_end" value="{q_in_end}" title="입금일 종료">
            <strong>💸 지급완료 변경일:</strong> <input type="date" name="out_start" value="{q_out_start}" title="지급일 시작"> ~ <input type="date" name="out_end" value="{q_out_end}" title="지급일 종료">
            <strong>🚛 배차일:</strong> <input type="date" name="dispatch_start" value="{q_dispatch_start}" title="배차일 시작"> ~ <input type="date" name="dispatch_end" value="{q_dispatch_end}" title="배차일 종료">
            <button type="submit" class="btn-save">데이터 조회</button>
            <button type="button" onclick="location.href='/export_stats'+window.location.search" class="btn-status bg-green">엑셀 다운로드</button>
        </form>

        <div class="summary-grid" style="margin-top:25px;">
            <div class="section"><h3>📅 월별 수익 요약</h3><div class="table-scroll"><table><thead><tr><th>연월</th><th>건수</th><th>공급가액</th><th>부가세</th><th>매출(합계)</th><th>기사운임</th><th>부가세</th><th>지출(합계)</th><th>수익</th></tr></thead><tbody>{summary_monthly}</tbody></table></div></div>
            <div class="section"><h3>📆 최근 일별 요약</h3><div class="table-scroll"><table><thead><tr><th>날짜</th><th>건수</th><th>공급가액</th><th>부가세</th><th>매출(합계)</th><th>기사운임</th><th>부가세</th><th>지출(합계)</th></tr></thead><tbody>{summary_daily}</tbody></table></div></div>
        </div>

        {stats_transfer_table}

        <div style="margin-top:30px;">
            <button class="tab-btn active" onclick="openSettleTab(event, 'clientZone')">🏢 업체별 정산 관리 (총 {stats_total_count}개)</button>
            <button class="tab-btn" onclick="openSettleTab(event, 'driverZone')">🚚 기사별 정산 관리 (총 {stats_total_count}개)</button>
        </div>

        <div id="clientZone" class="tab-content active">
            <div style="display:flex; justify-content:space-between; margin-bottom:15px; align-items:center; flex-wrap:wrap; gap:8px;">
                <h4 style="margin:0;">🧾 업체별 상세 매출 및 수금 현황</h4>
                <div style="display:flex; gap:8px;">
                    <a href="/export_custom_settlement?type=client{'' if not q_start else '&start='+q_start}{'' if not q_end else '&end='+q_end}{'' if not q_client else '&client='+q_client_enc}{'' if not q_status else '&status='+q_status_enc}{q_month_client_enc}{q_month_driver_enc}" class="btn-status bg-green" style="text-decoration:none;">📥 업체 정산 엑셀</a>
                    <button onclick="captureSettle('clientZone')" class="btn-status bg-orange">🖼️ 업체 정산서 이미지 저장</button>
                </div>
            </div>
            <div class="table-scroll" id="raw_client"><div class="client-settle-sections">{full_settlement_client}</div><div class="settle-footer-msg">에스엠 로지텍 발신</div></div>
        </div>

        <div id="driverZone" class="tab-content">
            <div style="display:flex; justify-content:space-between; margin-bottom:15px; align-items:center; flex-wrap:wrap; gap:8px;">
                <h4 style="margin:0;">🧾 기사별 상세 지출 및 지급 현황</h4>
                <div style="display:flex; gap:8px;">
                    <a href="/export_custom_settlement?type=driver{'' if not q_start else '&start='+q_start}{'' if not q_end else '&end='+q_end}{'' if not q_driver else '&driver='+q_driver_enc}{'' if not q_status else '&status='+q_status_enc}{q_month_client_enc}{q_month_driver_enc}" class="btn-status bg-green" style="text-decoration:none;">📥 기사 정산 엑셀</a>
                    <button onclick="captureSettle('driverZone')" class="btn-status bg-orange">🖼️ 기사 정산서 이미지 저장</button>
                </div>
            </div>
            <div class="table-scroll" id="raw_driver">{full_settlement_driver}</div>
        </div>

        {stats_biz_section}
    </div>

    <script>
        function openSettleTab(e, n) {{
            const contents = document.getElementsByClassName("tab-content");
            for (let c of contents) c.classList.remove("active");
            const btns = document.getElementsByClassName("tab-btn");
            for (let b of btns) b.classList.remove("active");
            document.getElementById(n).classList.add("active");
            e.currentTarget.classList.add("active");
        }}
        (function() {{
            var tbl = document.getElementById("statsTransferTable");
            if (!tbl) return;
            var sortCol = null;
            var sortAsc = true;
            function sortTable(colKey) {{
                var tbody = tbl.querySelector("tbody");
                if (!tbody) return;
                var dataRows = [].slice.call(tbody.querySelectorAll("tr.data-row"));
                var summaryRows = [].slice.call(tbody.querySelectorAll("tr.summary-row"));
                dataRows.sort(function(a, b) {{
                    var va = a.getAttribute(colKey) || "";
                    var vb = b.getAttribute(colKey) || "";
                    if (colKey === "data-total1" || colKey === "data-total2") {{
                        var na = parseFloat(va) || 0;
                        var nb = parseFloat(vb) || 0;
                        return sortAsc ? na - nb : nb - na;
                    }}
                    if (va < vb) return sortAsc ? -1 : 1;
                    if (va > vb) return sortAsc ? 1 : -1;
                    return 0;
                }});
                dataRows.forEach(function(tr) {{ tbody.appendChild(tr); }});
                summaryRows.forEach(function(tr) {{ tbody.appendChild(tr); }});
            }}
            tbl.querySelectorAll("thead th[data-sort]").forEach(function(th) {{
                th.addEventListener("click", function() {{
                    var key = th.getAttribute("data-sort") || "";
                    var colKey = "data-" + key;
                    if (sortCol === colKey) sortAsc = !sortAsc;
                    else {{ sortCol = colKey; sortAsc = true; }}
                    sortTable(colKey);
                }});
            }});
        }})();

        (function() {{
            var bizSearch = document.getElementById("bizTableSearch");
            var bizTable = document.getElementById("statsBizTable");
            if (bizSearch && bizTable) {{
                bizSearch.addEventListener("input", function() {{
                    var q = (this.value || "").trim().toLowerCase();
                    var rows = bizTable.querySelectorAll("tbody tr.biz-row");
                    for (var i = 0; i < rows.length; i++) {{
                        var tr = rows[i];
                        var text = (tr.getAttribute("data-biz-num") || "") + " " + (tr.getAttribute("data-biz-name") || "") + " " + (tr.getAttribute("data-out-dt") || "") + " " + (tr.getAttribute("data-d-name") || "") + " " + (tr.getAttribute("data-c-num") || "") + " " + (tr.textContent || "");
                        tr.style.display = q === "" || text.toLowerCase().indexOf(q) !== -1 ? "" : "none";
                    }}
                }});
            }}
            if (bizTable) {{
                var bizSortCol = null, bizSortAsc = true;
                function sortBizTable(colKey) {{
                    var tbody = bizTable.querySelector("tbody");
                    if (!tbody) return;
                    var rows = [].slice.call(tbody.querySelectorAll("tr.biz-row"));
                    rows.sort(function(a, b) {{
                        var va = a.getAttribute(colKey) || "";
                        var vb = b.getAttribute(colKey) || "";
                        if (va < vb) return bizSortAsc ? -1 : 1;
                        if (va > vb) return bizSortAsc ? 1 : -1;
                        return 0;
                    }});
                    rows.forEach(function(tr) {{ tbody.appendChild(tr); }});
                }}
                bizTable.querySelectorAll("thead th[data-sort]").forEach(function(th) {{
                    th.addEventListener("click", function() {{
                        var key = th.getAttribute("data-sort") || "";
                        var colKey = "data-" + key;
                        if (bizSortCol === colKey) bizSortAsc = !bizSortAsc;
                        else {{ bizSortCol = colKey; bizSortAsc = true; }}
                        sortBizTable(colKey);
                    }});
                }});
            }}
        }})();

        async function captureBizSettle() {{
            if (typeof html2canvas === 'undefined') {{
                await new Promise(function(resolve, reject) {{
                    var s = document.createElement('script');
                    s.src = 'https://cdn.jsdelivr.net/npm/html2canvas@1.4.1/dist/html2canvas.min.js';
                    s.onload = resolve; s.onerror = reject;
                    document.head.appendChild(s);
                }});
            }}
            var targetEl = document.getElementById('bizSettleZone');
            if (!targetEl) {{ alert('대상 영역을 찾을 수 없습니다.'); return; }}
            var wrap = document.createElement('div');
            wrap.style.cssText = 'position:fixed; left:0; top:0; z-index:99999; padding:24px; background:#fff; font-family: Malgun Gothic, sans-serif; max-width:95vw; box-sizing:border-box;';
            wrap.innerHTML = '<div style="text-align:right; margin-bottom:12px; font-size:13px; color:#64748b;">출력일: ' + new Date().toLocaleDateString('ko-KR', {{ timeZone: 'Asia/Seoul' }}) + '</div><h4 style="margin:0 0 16px 0; color:#1a2a6c;">기사사업자별계산서</h4>';
            wrap.appendChild(targetEl.cloneNode(true));
            document.body.appendChild(wrap);
            try {{
                var canvas = await html2canvas(wrap, {{ scale: 2, backgroundColor: '#ffffff', useCORS: true, allowTaint: true }});
                var link = document.createElement('a');
                link.download = '기사사업자별계산서_' + new Date().getTime() + '.png';
                link.href = canvas.toDataURL('image/png');
                link.click();
            }} catch (e) {{ alert('이미지 저장 중 오류: ' + (e && e.message ? e.message : String(e))); }}
            finally {{ document.body.removeChild(wrap); }}
        }}

        async function captureSettle(zoneId) {{
            if (typeof html2canvas === 'undefined') {{
                await new Promise(function(resolve, reject) {{
                    var s = document.createElement('script');
                    s.src = 'https://cdn.jsdelivr.net/npm/html2canvas@1.4.1/dist/html2canvas.min.js';
                    s.onload = resolve; s.onerror = reject;
                    document.head.appendChild(s);
                }});
            }}
            const area = document.getElementById('printArea');
            const printContent = document.getElementById('printContent');
            const isDriver = (zoneId === 'driverZone');
            const targetId = isDriver ? 'raw_driver' : 'raw_client';
            const fileName = isDriver ? '기사정산서_' + new Date().getTime() + '.png' : '업체정산서_' + new Date().getTime() + '.png';
            const targetEl = document.getElementById(targetId);
            let bodyHtml;
            if (isDriver) {{
                bodyHtml = targetEl.innerHTML;
                printContent.innerHTML = `
                    <div style="padding:40px; background:white; font-family: 'Malgun Gothic', sans-serif; max-width:800px;">
                        <div style="text-align:right; margin-bottom:20px; font-size:13px; color:#64748b;">출력일: ${{new Date().toLocaleDateString('ko-KR', {{ timeZone: 'Asia/Seoul' }})}}</div>
                        <div style="margin-bottom:30px;">${{bodyHtml}}</div>
                    </div>
                `;
            }} else {{
                bodyHtml = targetEl.innerHTML;
                printContent.innerHTML = `
                    <div style="padding:40px; background:white; font-family: 'Malgun Gothic', sans-serif; max-width:800px;">
                        <div style="text-align:right; margin-bottom:20px; font-size:13px; color:#64748b;">출력일: ${{new Date().toLocaleDateString('ko-KR', {{ timeZone: 'Asia/Seoul' }})}}</div>
                        <div style="margin-bottom:30px;">${{bodyHtml}}</div>
                    </div>
                `;
            }}
            
            area.style.left = '0';
            try {{
                const canvas = await html2canvas(area, {{ scale: 2, backgroundColor: "#ffffff" }});
                const link = document.createElement('a');
                link.download = fileName;
                link.href = canvas.toDataURL('image/png');
                link.click();
            }} catch (e) {{ alert("이미지 저장 중 오류가 발생했습니다."); }}
            finally {{ area.style.left = '-9999px'; }}
        }}
    </script>
    """
    resp = make_response(render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys="[]"))
    if q_period in ('1', '3', '6'):
        resp.set_cookie('stats_period', q_period, max_age=365*24*60*60)
    return resp

@app.route('/api/statistics_transfer_excel')
@login_required
def statistics_transfer_excel():
    """통계분석 이체확인 테이블과 동일 조건으로 엑셀 다운로드"""
    q_start = request.args.get('start', '')
    q_end = request.args.get('end', '')
    q_client = request.args.get('client', '').strip()
    q_driver = request.args.get('driver', '').strip()
    q_c_num = request.args.get('c_num', '').strip()
    q_status = request.args.get('status', '')
    q_month_client = request.args.get('month_end_client', '')
    q_month_driver = request.args.get('month_end_driver', '')
    q_in_start = request.args.get('in_start', '').strip()
    q_in_end = request.args.get('in_end', '').strip()
    q_out_start = request.args.get('out_start', '').strip()
    q_out_end = request.args.get('out_end', '').strip()
    q_dispatch_start = request.args.get('dispatch_start', '').strip()
    q_dispatch_end = request.args.get('dispatch_end', '').strip()
    conn = sqlite3.connect('ledger.db', timeout=15)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ledger").fetchall()
    conn.close()
    fixed_c_nums = {str(d.get('차량번호', '')).strip() for d in drivers_db if str(d.get('개인/고정', '')).strip() == '고정'}
    fixed_c_nums.discard('')
    filtered = []
    for row in rows:
        r = dict(row)
        order_dt = r.get('order_dt', '') or ''
        if q_start and q_end and not (q_start <= order_dt <= q_end):
            continue
        if q_client and q_client not in str(r.get('client_name', '')):
            continue
        if q_driver and q_driver not in str(r.get('d_name', '')):
            continue
        if q_c_num and q_c_num not in str(r.get('c_num', '')):
            continue
        if q_month_client and (str(r.get('month_end_client') or '').strip() not in ('1', 'Y')):
            continue
        if q_month_driver and (str(r.get('month_end_driver') or '').strip() not in ('1', 'Y')):
            continue
        in_dt = r.get('in_dt')
        out_dt = r.get('out_dt')
        if q_in_start or q_in_end:
            in_dt_val = (in_dt or '')[:10] if in_dt else ''
            if not in_dt_val: continue
            if q_in_start and in_dt_val < q_in_start: continue
            if q_in_end and in_dt_val > q_in_end: continue
        if q_out_start or q_out_end:
            out_dt_val = (out_dt or '')[:10] if out_dt else ''
            if not out_dt_val: continue
            if q_out_start and out_dt_val < q_out_start: continue
            if q_out_end and out_dt_val > q_out_end: continue
        if q_dispatch_start or q_dispatch_end:
            dispatch_dt_val = (r.get('dispatch_dt') or '')[:10] if r.get('dispatch_dt') else ''
            if not dispatch_dt_val: continue
            if q_dispatch_start and dispatch_dt_val < q_dispatch_start: continue
            if q_dispatch_end and dispatch_dt_val > q_dispatch_end: continue
        m_st = "수금완료" if in_dt else ("조건부미수" if not r.get('pre_post') and not r.get('pay_due_dt') else "미수")
        p_st = "지급완료" if out_dt else ("조건부미지급" if not in_dt else "미지급")
        c_num = str(r.get('c_num', '')).strip()
        d_type = "직영" if c_num in fixed_c_nums else "일반"
        if q_status:
            if q_status in ["미수", "조건부미수", "수금완료"] and q_status != m_st:
                continue
            if q_status in ["미지급", "조건부미지급", "지급완료"] and q_status != p_st:
                continue
            if q_status == "고정" and c_num not in fixed_c_nums:
                continue
            if q_status in ["직영", "일반"] and q_status != d_type:
                continue
        fee, vat1, total1, fee_out, vat2, total2 = calc_totals_with_vat(r)
        memo_parts = [str(r.get('memo1', '') or '').strip(), str(r.get('memo2', '') or '').strip(), str(r.get('req_add', '') or '').strip()]
        memo_str = ' / '.join(p for p in memo_parts if p)
        filtered.append({
            '오더일': r.get('order_dt', ''),
            '배차일': r.get('dispatch_dt', ''),
            '수금상태': m_st,
            '입금일': in_dt or '',
            '지급상태': p_st,
            '지급일': out_dt or '',
            '업체운임합계': total1,
            '기사운임합계': total2,
            '입금자명': r.get('in_name', ''),
            '입금내역': r.get('month_val', ''),
            '노선': r.get('route', ''),
            '기사명': r.get('d_name', ''),
            '차량번호': r.get('c_num', ''),
            '연락처': r.get('d_phone', ''),
            '사업자': r.get('tax_biz_name', ''),
            '특이사항': memo_str,
        })
    df = pd.DataFrame(filtered)
    if df.empty:
        df = pd.DataFrame(columns=['오더일', '배차일', '수금상태', '입금일', '지급상태', '지급일', '업체운임합계', '기사운임합계', '입금자명', '입금내역', '노선', '기사명', '차량번호', '연락처', '사업자', '특이사항'])
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name='이체확인')
    out.seek(0)
    fname = f"이체확인_{now_kst().strftime('%Y%m%d_%H%M')}.xlsx"
    return send_file(out, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=fname)

@app.route('/api/statistics_biz_settlement_excel')
@login_required
def statistics_biz_settlement_excel():
    """통계 기사사업자별계산서 테이블과 동일 조건으로 엑셀 다운로드 (사업자번호, 사업자, 지급일, 기사명, 차량번호, 오더일, 노선, 기사운임, 부가세, 합계)"""
    q_start = request.args.get('start', '')
    q_end = request.args.get('end', '')
    q_client = request.args.get('client', '').strip()
    q_driver = request.args.get('driver', '').strip()
    q_c_num = request.args.get('c_num', '').strip()
    q_status = request.args.get('status', '')
    q_month_client = request.args.get('month_end_client', '')
    q_month_driver = request.args.get('month_end_driver', '')
    q_in_start = request.args.get('in_start', '').strip()
    q_in_end = request.args.get('in_end', '').strip()
    q_out_start = request.args.get('out_start', '').strip()
    q_out_end = request.args.get('out_end', '').strip()
    q_dispatch_start = request.args.get('dispatch_start', '').strip()
    q_dispatch_end = request.args.get('dispatch_end', '').strip()
    fixed_c_nums = {str(d.get('차량번호', '')).strip() for d in drivers_db if str(d.get('개인/고정', '')).strip() == '고정'}
    fixed_c_nums.discard('')
    conn = sqlite3.connect('ledger.db', timeout=15)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ledger").fetchall()
    conn.close()
    filtered = []
    for row in rows:
        r = dict(row)
        order_dt = r.get('order_dt', '') or ''
        if q_start and q_end and not (q_start <= order_dt <= q_end):
            continue
        if q_client and q_client not in str(r.get('client_name', '')):
            continue
        if q_driver and q_driver not in str(r.get('d_name', '')):
            continue
        if q_c_num and q_c_num not in str(r.get('c_num', '')):
            continue
        if q_month_client and (str(r.get('month_end_client') or '').strip() not in ('1', 'Y')):
            continue
        if q_month_driver and (str(r.get('month_end_driver') or '').strip() not in ('1', 'Y')):
            continue
        in_dt = r.get('in_dt')
        out_dt = r.get('out_dt')
        if q_in_start or q_in_end:
            in_dt_val = (in_dt or '')[:10] if in_dt else ''
            if not in_dt_val: continue
            if q_in_start and in_dt_val < q_in_start: continue
            if q_in_end and in_dt_val > q_in_end: continue
        if q_out_start or q_out_end:
            out_dt_val = (out_dt or '')[:10] if out_dt else ''
            if not out_dt_val: continue
            if q_out_start and out_dt_val < q_out_start: continue
            if q_out_end and out_dt_val > q_out_end: continue
        if q_dispatch_start or q_dispatch_end:
            dispatch_dt_val = (r.get('dispatch_dt') or '')[:10] if r.get('dispatch_dt') else ''
            if not dispatch_dt_val: continue
            if q_dispatch_start and dispatch_dt_val < q_dispatch_start: continue
            if q_dispatch_end and dispatch_dt_val > q_dispatch_end: continue
        m_st = "수금완료" if in_dt else ("조건부미수" if not r.get('pre_post') and not r.get('pay_due_dt') else "미수")
        p_st = "지급완료" if out_dt else ("조건부미지급" if not in_dt else "미지급")
        c_num = str(r.get('c_num', '')).strip()
        d_type = "직영" if c_num in fixed_c_nums else "일반"
        if q_status:
            if q_status in ["미수", "조건부미수", "수금완료"] and q_status != m_st:
                continue
            if q_status in ["미지급", "조건부미지급", "지급완료"] and q_status != p_st:
                continue
            if q_status == "고정" and c_num not in fixed_c_nums:
                continue
            if q_status in ["직영", "일반"] and q_status != d_type:
                continue
        fee_out, vat2, total2 = calc_totals_with_vat(r)[3:6]
        dispatch_dt = str(r.get('dispatch_dt', '') or '')[:19] if r.get('dispatch_dt') else ''
        filtered.append({
            '사업자번호': str(r.get('tax_biz_num', '') or '').strip(),
            '사업자': str(r.get('tax_biz_name', '') or '').strip(),
            '지급일': (out_dt or '')[:10] if out_dt else '',
            '기사명': str(r.get('d_name', '') or '').strip(),
            '차량번호': str(r.get('c_num', '') or '').strip(),
            '배차일': dispatch_dt[:10] if dispatch_dt else '',
            '오더일': (order_dt or '')[:10] if order_dt else '',
            '노선': str(r.get('route', '') or '').strip(),
            '기사운임': fee_out,
            '부가세': vat2,
            '합계': total2,
        })
    df = pd.DataFrame(filtered)
    if df.empty:
        df = pd.DataFrame(columns=['사업자번호', '사업자', '지급일', '기사명', '차량번호', '배차일', '오더일', '노선', '기사운임', '부가세', '합계'])
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name='기사사업자별계산서')
    out.seek(0)
    fname = f"기사사업자별계산서_{now_kst().strftime('%Y%m%d_%H%M')}.xlsx"
    return send_file(out, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=fname)

@app.route('/export_custom_settlement')
@login_required 
def export_custom_settlement():
    t = request.args.get('type', 'client'); s = request.args.get('start',''); e = request.args.get('end','')
    c = request.args.get('client',''); d = request.args.get('driver',''); st = request.args.get('status', '')
    month_client = request.args.get('month_end_client', '')
    month_driver = request.args.get('month_end_driver', '')
    fixed_c_nums = {str(dr.get('차량번호', '')).strip() for dr in drivers_db if str(dr.get('개인/고정', '')).strip() == '고정'}
    fixed_c_nums.discard('')
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ledger").fetchall(); conn.close()
    filtered_data = []
    for row in rows:
        r = dict(row)
        in_dt = r['in_dt']; out_dt = r['out_dt']; p_due = r['pay_due_dt']; pre = r['pre_post']
        o_dt = r['order_dt'] or ""; t_img = r['tax_img'] or ""; s_img = r['ship_img'] or ""
        m_st = "조건부미수금" if not pre and not in_dt and not p_due else ("수금완료" if in_dt else "미수")
        p_st = "지급완료" if out_dt else ("미지급" if in_dt and any('static' in p for p in t_img.split(',')) and any('static' in p for p in s_img.split(',')) else "조건부미지급")
        c_num = str(r.get('c_num', '')).strip()
        d_type = "직영" if c_num in fixed_c_nums else "일반"
        if s and e and not (s <= o_dt <= e): continue
        if c and c not in str(r['client_name']): continue
        if d and d not in str(r['d_name']): continue
        if month_client and (str(r.get('month_end_client') or '').strip() not in ('1', 'Y')): continue
        if month_driver and (str(r.get('month_end_driver') or '').strip() not in ('1', 'Y')): continue
        st_map = {'미수':'misu_only', '조건부미수':'cond_misu', '수금완료':'done_in', '지급완료':'done_out', '미지급':'pay_only', '조건부미지급':'cond_pay'}
        st_use = st_map.get(st, st)
        if st_use == 'misu_all' and in_dt: continue
        if st_use == 'misu_only' and m_st != '미수': continue
        if st_use == 'cond_misu' and m_st != '조건부미수금': continue
        if st_use == 'pay_all' and out_dt: continue
        if st_use == 'pay_only' and p_st != '미지급': continue
        if st_use == 'cond_pay' and p_st != '조건부미지급': continue
        if st_use == 'done_in' and not in_dt: continue
        if st_use == 'done_out' and not out_dt: continue
        if st == '고정' and c_num not in fixed_c_nums: continue
        if st in ['직영', '일반'] and d_type != st: continue
        r['m_st'] = m_st; r['p_st'] = p_st
        filtered_data.append(r)
    df = pd.DataFrame(filtered_data)
    if df.empty: return "데이터가 없습니다."
    group_col = 'client_name' if t == 'client' else 'd_name'
    excel_list = []
    if t == 'client':
        for name, group in df.groupby(group_col):
            grp_fee, grp_vat, grp_total = 0, 0, 0
            for idx, row in group.sort_values(by='order_dt', ascending=True).iterrows():
                fee, vat1, total1, _, _, _ = calc_totals_with_vat(row)
                excel_list.append({'구분': name, '오더일': row['order_dt'], '노선': row['route'], '공급가액': fee, '부가세': vat1, '합계': total1})
                grp_fee += fee; grp_vat += vat1; grp_total += total1
            excel_list.append({'구분': f'[{name}] 합계', '오더일': '-', '노선': '-', '공급가액': grp_fee, '부가세': grp_vat, '합계': grp_total})
            excel_list.append({})
    else:
        df['fee_out'] = pd.to_numeric(df['fee_out'], errors='coerce').fillna(0)
        for name, group in df.groupby(group_col):
            grp_fee, grp_vat, grp_total = 0, 0, 0
            for idx, row in group.sort_values(by='order_dt', ascending=True).iterrows():
                _, _, _, fee_out, vat2, total2 = calc_totals_with_vat(row)
                excel_list.append({'구분': name, '업체명': row.get('client_name',''), '오더일': row['order_dt'], '노선': row['route'], '기사운임': fee_out, '부가세': vat2, '합계': total2, '지급상태': row.get('p_st', '')})
                grp_fee += fee_out; grp_vat += vat2; grp_total += total2
            excel_list.append({'구분': f'[{name}] 합계', '업체명': '-', '오더일': '-', '노선': '-', '기사운임': grp_fee, '부가세': grp_vat, '합계': grp_total, '지급상태': '-'})
            excel_list.append({})
    result_df = pd.DataFrame(excel_list)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w: result_df.to_excel(w, index=False)
    out.seek(0); return send_file(out, as_attachment=True, download_name=f"{t}_settlement.xlsx")

@app.route('/export_misu_info')
@login_required 
def export_misu_info():
    q_st = request.args.get('status', ''); q_name = request.args.get('name', '')
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ledger").fetchall(); conn.close()
    export_data = []
    for row in rows:
        row_dict = dict(row)
        in_dt = row_dict['in_dt']; pay_due_dt = row_dict['pay_due_dt']; pre_post = row_dict['pre_post']
        m_status = "조건부미수금" if not pre_post and not in_dt and not pay_due_dt else ("수금완료" if in_dt else "미수")
        if q_st == 'misu_all' and in_dt: pass
        elif q_st == 'misu_only' and m_status == '미수': pass
        elif q_st == 'cond_misu' and m_status == '조건부미수금': pass
        elif not q_st and not in_dt: pass
        else: continue
        if q_name and q_name not in str(row_dict['client_name']): continue
        export_data.append({'거래처명': row_dict['client_name'], '사업자번호': row_dict['biz_num'], '대표자': row_dict['biz_owner'], '메일': row_dict['mail'], '연락처': row_dict['c_phone'], '노선': row_dict['route'], '공급가액': int(calc_supply_value(row_dict)), '오더일': row_dict['order_dt'], '결제예정일': row_dict['pay_due_dt']})
    df = pd.DataFrame(export_data)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w: df.to_excel(w, index=False)
    out.seek(0); return send_file(out, as_attachment=True, download_name="misu_client_info.xlsx")

@app.route('/export_tax_not_issued')
@login_required
def export_tax_not_issued():
    """정산관리 - 세금계산서 미발행 건 엑셀. 업체운임/부가세/합계는 통합장부 기준. 부가세0건은 별계로 하단 표시"""
    q_name = request.args.get('name', '')
    q_start = request.args.get('start', ''); q_end = request.args.get('end', '')
    client_by_name = {str(c.get('업체명') or '').strip(): c for c in clients_db if (c.get('업체명') or '').strip()}
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ledger").fetchall(); conn.close()
    cols = ['사업자구분', '결제특이사항', '발행구분', '사업자등록번호', '대표자명', '사업자주소', '업태', '종목', '메일주소', '오더일', '노선', '업체명', '공급가액', '부가세', '합계']
    export_with_vat = []   # 부가세 있는 건 (발행 대상)
    export_no_vat = []     # 부가세 없는 건 (별계, 계산서 발행 불필요)
    for row in rows:
        r = dict(row)
        tax_chk = (r.get('tax_chk') or '').strip()
        if tax_chk == '발행완료':
            continue
        order_dt = r.get('order_dt') or ''
        if q_start and order_dt < q_start: continue
        if q_end and order_dt > q_end: continue
        if q_name and q_name not in str(r.get('client_name') or '') and q_name not in str(r.get('d_name') or ''):
            continue
        fee, vat1, total1, _, _, _ = calc_totals_with_vat(r)
        cname = str(r.get('client_name') or '').strip()
        client = client_by_name.get(cname, {})
        row_data = {
            '사업자구분': client.get('사업자구분', ''),
            '결제특이사항': client.get('결제특이사항', ''),
            '발행구분': client.get('발행구분', ''),
            '사업자등록번호': client.get('사업자등록번호', '') or r.get('biz_num', ''),
            '대표자명': client.get('대표자명', '') or r.get('biz_owner', ''),
            '사업자주소': client.get('사업자주소', '') or r.get('biz_addr', ''),
            '업태': client.get('업태', ''),
            '종목': client.get('종목', ''),
            '메일주소': client.get('메일주소', '') or r.get('mail', ''),
            '오더일': order_dt,
            '노선': r.get('route', ''),
            '업체명': cname or r.get('client_name', ''),
            '공급가액': fee,
            '부가세': vat1,
            '합계': total1,
        }
        if vat1 > 0:
            export_with_vat.append(row_data)
        else:
            export_no_vat.append(row_data)
    excel_rows = []
    if export_with_vat:
        df_main = pd.DataFrame(export_with_vat).sort_values(by=['업체명', '오더일', '노선', '공급가액'], ascending=[True, True, True, True], na_position='last')
        for _, r in df_main.iterrows():
            excel_rows.append({c: r[c] for c in cols})
        sum_fee = int(df_main['공급가액'].sum())
        sum_vat = int(df_main['부가세'].sum())
        sum_total = int(df_main['합계'].sum())
        excel_rows.append({c: ('총합계' if c == '업체명' else sum_fee if c == '공급가액' else sum_vat if c == '부가세' else sum_total if c == '합계' else '') for c in cols})
    excel_rows.append({})
    if export_no_vat:
        excel_rows.append({c: ('[부가세 없는 건 - 계산서 발행 불필요]' if c == '업체명' else '') for c in cols})
        df_no = pd.DataFrame(export_no_vat).sort_values(by=['업체명', '오더일', '노선', '공급가액'], ascending=[True, True, True, True], na_position='last')
        for _, r in df_no.iterrows():
            excel_rows.append({c: r[c] for c in cols})
    df_out = pd.DataFrame(excel_rows if excel_rows else [{}], columns=cols)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        df_out.to_excel(w, index=False)
    out.seek(0)
    return send_file(out, as_attachment=True, download_name="tax_not_issued.xlsx")

@app.route('/export_pay_info')
@login_required 
def export_pay_info():
    q_st = request.args.get('status', ''); q_name = request.args.get('name', '')
    # 기사(기사명+차량번호)별 은행정보 보조 (ledger에 없을 때 사용)
    driver_bank = {}
    for d in drivers_db:
        key = (str(d.get('기사명') or '').strip(), str(d.get('차량번호') or '').strip())
        if key[0] or key[1]:
            driver_bank[key] = {
                '은행명': str(d.get('은행명') or '').strip(),
                '예금주': str(d.get('예금주') or d.get('사업자') or '').strip(),
                '계좌번호': str(d.get('계좌번호') or '').strip(),
            }
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ledger").fetchall(); conn.close()
    # 미지급 건만 수집 후, (기사명, 차량번호, 은행명, 예금주, 계좌번호) 기준으로 묶어 금액 합산
    raw_list = []
    for row in rows:
        row_dict = dict(row)
        in_dt = row_dict['in_dt']; out_dt = row_dict['out_dt']
        tax_img = row_dict['tax_img'] or ""; ship_img = row_dict['ship_img'] or ""
        has_tax = any('static' in p for p in tax_img.split(','))
        has_ship = any('static' in p for p in ship_img.split(','))
        p_status = "지급완료" if out_dt else ("미지급" if in_dt and has_tax and has_ship else "조건부미지급")
        if q_st == 'pay_all' and out_dt: pass
        elif q_st == 'pay_only' and p_status == '미지급': pass
        elif q_st == 'cond_pay' and p_status == '조건부미지급': pass
        elif not q_st and not out_dt: pass
        else: continue
        if q_name and q_name not in str(row_dict['d_name']): continue
        d_name = str(row_dict.get('d_name') or '').strip()
        c_num = str(row_dict.get('c_num') or '').strip()
        bank_name = str(row_dict.get('d_bank_name') or '').strip()
        owner = str(row_dict.get('d_bank_owner') or row_dict.get('tax_biz_name') or '').strip()
        acc = str(row_dict.get('bank_acc') or '').strip()
        if not bank_name or not owner or not acc:
            info = driver_bank.get((d_name, c_num), {})
            if not bank_name: bank_name = info.get('은행명', '')
            if not owner: owner = info.get('예금주', '')
            if not acc: acc = info.get('계좌번호', '')
        try:
            amt = int(float(row_dict.get('fee_out') or 0))
        except (TypeError, ValueError):
            amt = 0
        raw_list.append({'기사명': d_name, '은행명': bank_name, '예금주': owner, '계좌번호': acc, '금액': amt})
    # 동일 (기사명, 은행명, 예금주, 계좌번호)별 금액 합산
    agg = defaultdict(int)
    for r in raw_list:
        key = (r['기사명'], r['은행명'], r['예금주'], r['계좌번호'])
        agg[key] += r['금액']
    # 엑셀 출력: 기사명, 기사운임, 계좌번호, 예금주, 은행명, 은행코드 순
    export_data = []
    for (d_name, bank_name, owner, acc), total in agg.items():
        code = get_bank_code(bank_name)
        export_data.append({
            '기사명': d_name or '(미기재)',
            '기사운임': total,
            '계좌번호': acc or '(미기재)',
            '예금주': owner or '(미기재)',
            '은행명': bank_name or '(미기재)',
            '은행코드': code,
        })
    df = pd.DataFrame(export_data)
    if df.empty:
        df = pd.DataFrame(columns=['기사명', '기사운임', '계좌번호', '예금주', '은행명', '은행코드'])
    else:
        df = df[['기사명', '기사운임', '계좌번호', '예금주', '은행명', '은행코드']]
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        df.to_excel(w, index=False)
    out.seek(0)
    return send_file(out, as_attachment=True, download_name="pay_driver_info.xlsx")

@app.route('/export_stats')
@login_required 
def export_stats():
    s = request.args.get('start',''); e = request.args.get('end','')
    c = request.args.get('client',''); d = request.args.get('driver','')
    c_num_param = request.args.get('c_num', '').strip()
    st = request.args.get('status', '')
    month_client = request.args.get('month_end_client', '')
    month_driver = request.args.get('month_end_driver', '')
    in_start = request.args.get('in_start', '').strip()
    in_end = request.args.get('in_end', '').strip()
    out_start = request.args.get('out_start', '').strip()
    out_end = request.args.get('out_end', '').strip()
    
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ledger").fetchall(); conn.close()
    
    # 기사관리에서 개인/고정="고정"인 차량번호 목록 (차량번호 기준)
    fixed_c_nums = {str(dr.get('차량번호', '')).strip() for dr in drivers_db if str(dr.get('개인/고정', '')).strip() == '고정'}
    fixed_c_nums.discard('')
    export_data = []

    for row in rows:
        r = dict(row)
        # 상태 계산 로직 (통계 함수와 동일하게 적용)
        in_dt = r.get('in_dt'); out_dt = r.get('out_dt')
        if in_start or in_end:
            in_dt_val = (in_dt or '')[:10] if in_dt else ''
            if not in_dt_val: continue
            if in_start and in_dt_val < in_start: continue
            if in_end and in_dt_val > in_end: continue
        if out_start or out_end:
            out_dt_val = (out_dt or '')[:10] if out_dt else ''
            if not out_dt_val: continue
            if out_start and out_dt_val < out_start: continue
            if out_end and out_dt_val > out_end: continue
        m_st = "수금완료" if in_dt else ("조건부미수" if not r.get('pre_post') and not r.get('pay_due_dt') else "미수")
        p_st = "지급완료" if out_dt else ("조건부미지급" if not in_dt else "미지급")
        c_num = str(r.get('c_num', '')).strip()
        d_type = "직영" if c_num in fixed_c_nums else "일반"

        # 필터링
        if s and e and not (s <= (r['order_dt'] or "") <= e): continue
        if c and c not in str(r['client_name']): continue
        if d and d not in str(r['d_name']): continue
        if c_num_param and c_num_param not in str(r.get('c_num', '')): continue
        if month_client and (str(r.get('month_end_client') or '').strip() not in ('1', 'Y')): continue
        if month_driver and (str(r.get('month_end_driver') or '').strip() not in ('1', 'Y')): continue
        if st:
            if st in ["미수", "조건부미수", "수금완료"] and st != m_st: continue
            if st in ["미지급", "조건부미지급", "지급완료"] and st != p_st: continue
            if st == "고정" and c_num not in fixed_c_nums: continue
            if st in ["직영", "일반"] and st != d_type: continue

        fee, vat1, total1, fee_out, vat2, total2 = calc_totals_with_vat(r)
        export_data.append({
            '오더일': r['order_dt'], '업체명': r['client_name'], '노선': r['route'],
            '기사명': r['d_name'], '공급가액': fee, '부가세': vat1, '매출(합계)': total1, '수금상태': m_st,
            '기사운임': fee_out, '기사부가세': vat2, '지출(기사합계)': total2, '지급상태': p_st, '기사구분': d_type
        })
        
    df = pd.DataFrame(export_data)
    if not df.empty:
        sum_row = {'오더일': '합계', '업체명': '', '노선': '', '기사명': '', '공급가액': int(df['공급가액'].sum()), '부가세': int(df['부가세'].sum()), '매출(합계)': int(df['매출(합계)'].sum()), '수금상태': '', '기사운임': int(df['기사운임'].sum()), '기사부가세': int(df['기사부가세'].sum()), '지출(기사합계)': int(df['지출(기사합계)'].sum()), '지급상태': '', '기사구분': ''}
        df = pd.concat([df, pd.DataFrame([sum_row])], ignore_index=True)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name='통계데이터')
    out.seek(0)
    return send_file(out, as_attachment=True, download_name=f"SM_Logis_Stats_{now_kst().strftime('%y%m%d')}.xlsx")

@app.route('/upload_evidence/<int:ledger_id>', methods=['GET', 'POST'])
def upload_evidence(ledger_id):
    """기사가 링크로 접속해 계산서/운송장 사진 업로드 (로그인 불필요)"""
    target_type = request.args.get('type', 'all')
    try:
        seq_val = int(request.args.get('seq', 1) or 1)
        target_seq = str(max(1, min(5, seq_val)))
    except (ValueError, TypeError):
        target_seq = '1'
    if request.method == 'POST':
        tax_file, ship_file = request.files.get('tax_file'), request.files.get('ship_file')
        conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT tax_img, ship_img FROM ledger WHERE id = ?", (ledger_id,)).fetchone()
        if not row:
            conn.close()
            return "해당 장부를 찾을 수 없습니다.", 404
        row = dict(row)
        def update_p(old, new, seq):
            plist = [p.strip() for p in old.split(',')] if old else [""] * 5
            while len(plist) < 5: plist.append("")
            idx = max(0, min(4, int(seq) - 1)) if str(seq).isdigit() else 0
            plist[idx] = new
            return ",".join(plist)
        if tax_file and tax_file.filename:
            safe_name = secure_filename(tax_file.filename) or "upload.jpg"
            path = os.path.join(UPLOAD_FOLDER, f"tax_{ledger_id}_{target_seq}_{safe_name}")
            tax_file.save(path); conn.execute("UPDATE ledger SET tax_img = ? WHERE id = ?", (update_p(row['tax_img'] or "", path, target_seq), ledger_id))
        if ship_file and ship_file.filename:
            safe_name = secure_filename(ship_file.filename) or "upload.jpg"
            path = os.path.join(UPLOAD_FOLDER, f"ship_{ledger_id}_{target_seq}_{safe_name}")
            ship_file.save(path); conn.execute("UPDATE ledger SET ship_img = ? WHERE id = ?", (update_p(row['ship_img'] or "", path, target_seq), ledger_id))
        conn.commit(); conn.close(); return "<h3>업로드 완료</h3><script>setTimeout(()=>location.reload(), 1000);</script>"

    seq_btns = []
    for i in range(1, 6):
        active_cls = 'active' if str(i) == target_seq else ''
        href = f"/upload_evidence/{ledger_id}?type={target_type}&seq={i}"
        seq_btns.append(f"<button class=\"seq-btn {active_cls}\" onclick=\"location.href='{href}'\">{i}번</button>")
    seq_btns_html = "".join(seq_btns)

    title_text = "기사계산서" if target_type == "tax" else "운송장"
    script = (
        "<script>async function processAndUpload(){"
        "const s=document.getElementById('status'); const fileInput = document.getElementById('file_input');"
        "if(!fileInput.files[0]) { alert('파일을 선택해주세요.'); return; }"
        "s.innerText='압축 및 전송중...';"
        "const compress=(f)=>new Promise((r)=>{const reader=new FileReader(); reader.readAsDataURL(f); reader.onload=(e)=>{const img=new Image(); img.src=e.target.result; img.onload=()=>{const cvs=document.createElement('canvas'); let w=img.width,h=img.height; if(w>1200){h*=1200/w;w=1200} cvs.width=w;cvs.height=h; cvs.getContext('2d').drawImage(img,0,0,w,h); cvs.toBlob((b)=>r(b),'image/jpeg',0.7)}}});"
        "const fd=new FormData(); const type='" + target_type + "';"
        "(async ()=>{ const fileBlob = await compress(fileInput.files[0]); fd.append(type === 'tax' ? 'tax_file' : 'ship_file', fileBlob, 'upload.jpg');"
        "fetch(location.href,{method:'POST',body:fd}).then(r=>r.text()).then(t=>{document.body.innerHTML=t; if(window.opener) window.opener.location.reload(); });})();"
        "}</script>"
    )

    html = (
        '<meta name="viewport" content="width=device-width,initial-scale=1.0">'
        '<style>body{padding:20px; text-align:center; font-family:sans-serif;} .seq-btns{display:flex; gap:10px; justify-content:center; margin-bottom:20px;} .seq-btn{padding:10px 15px; border:1px solid #ccc; background:white; cursor:pointer;} .seq-btn.active{background:#007bff; color:white; border-color:#007bff; font-weight:bold;} button[type="button"]{width:100%; padding:15px; background:#28a745; color:white; border:none; border-radius:5px; font-weight:bold; cursor:pointer; margin-top:10px;}</style>'
        f'<h3>증빙 업로드 - {title_text}</h3>'
        f'<div class="seq-btns">{seq_btns_html}</div>'
        f'<p>현재 선택된 슬롯: <b>{target_seq}번</b></p>'
        f"<form id=\"uploadForm\">파일 선택: <input type='file' id='file_input' accept='image/*' style='margin-bottom:10px;'><button type=\"button\" onclick=\"processAndUpload()\">전송하기</button></form><div id=\"status\"></div>"
        + script
    )
    return html

def sanitize_ledger_value(k, v):
    """컬럼 타입에 맞게 값 정제 (잘못된 형식이면 빈 문자열)"""
    if v is None: return ''
    v = str(v).strip()
    col = next((c for c in FULL_COLUMNS if c['k'] == k), None)
    if not col: return v
    t = col.get('t', 'text')
    if t == 'number':
        if not v: return ''
        try:
            float(v)
            return v
        except (ValueError, TypeError):
            return ''
    if t == 'date':
        if not v: return ''
        if re.match(r'^\d{4}-\d{2}-\d{2}$', v):
            return v
        # 엑셀 등에서 "2024-02-18 00:00:00" 또는 "2024/02/18" 형태로 들어온 경우
        if re.match(r'^\d{4}-\d{2}-\d{2}[ T]', v):
            return v[:10]
        if re.match(r'^\d{4}/\d{2}/\d{2}', v):
            return v[:10].replace('/', '-')
        return ''
    if t == 'datetime-local':
        if not v: return ''
        if re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}', v) or re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}', v):
            return v.replace(' ', 'T')[:16] if ' ' in v else v[:16]
        return ''
    return v

@app.route('/api/save_ledger', methods=['POST'])
@login_required 
def save_ledger_api():
    raw = request.json or {}
    if not isinstance(raw, dict):
        return jsonify({"status": "error", "message": "invalid request"}), 400
    keys = [c['k'] for c in FULL_COLUMNS]
    data = {k: sanitize_ledger_value(k, raw.get(k, '')) for k in keys}
    if 'id' in raw and raw['id']:
        data['id'] = raw['id']
    calc_vat_auto(data)
    conn = sqlite3.connect('ledger.db', timeout=15)
    cursor = conn.cursor()
    
    keys = [c['k'] for c in FULL_COLUMNS]
    if 'id' in data and data['id']:
        try:
            target_id = int(data['id'])
            if target_id <= 0:
                return jsonify({"status": "error", "message": "invalid id"}), 400
        except (ValueError, TypeError):
            return jsonify({"status": "error", "message": "invalid id"}), 400
        action_type = "수정"
        sql = ", ".join([f"[{k}] = ?" for k in keys])
        vals = [data.get(k, '') for k in keys] + [target_id]
        cursor.execute(f"UPDATE ledger SET {sql} WHERE id = ?", vals)
    else:
        action_type = "신규등록"
        placeholders = ", ".join(['?'] * len(keys))
        cursor.execute(f"INSERT INTO ledger ({', '.join([f'[{k}]' for k in keys])}) VALUES ({placeholders})", 
                       [data.get(k, '') for k in keys])
        target_id = cursor.lastrowid

    details = f"업체:{data.get('client_name')}, 노선:{data.get('route')}, 공급가액:{int(calc_supply_value(data))}, 기사운임:{data.get('fee_out', '')}"
    cursor.execute("INSERT INTO activity_logs (action, target_id, details) VALUES (?, ?, ?)",
                   (action_type, target_id, details))

    # 기사명·차량번호 있으면 기사관리 10개 항목 전체(연락처·은행명·계좌번호·예금주·사업자번호·사업자·개인고정·메모) 저장
    if data.get('d_name') and data.get('c_num'):
        d_vals = (
            data.get('d_phone',''), data.get('bank_acc',''), data.get('tax_biz_num',''),
            data.get('tax_biz_name',''), data.get('memo1',''), 
            data.get('d_bank_name',''), data.get('d_bank_owner',''), 
            str(data.get('log_move','')).strip(),
            data.get('d_name'), data.get('c_num')
        )
        cursor.execute("SELECT 1 FROM drivers WHERE 기사명 = ? AND 차량번호 = ?", (data.get('d_name'), data.get('c_num')))
        if cursor.fetchone():
            cursor.execute("UPDATE drivers SET 연락처=?, 계좌번호=?, 사업자번호=?, 사업자=?, 메모=?, 은행명=?, 예금주=?, [개인/고정]=? WHERE 기사명=? AND 차량번호=?", d_vals)
        else:
            cursor.execute("INSERT INTO drivers (연락처, 계좌번호, 사업자번호, 사업자, 메모, 은행명, 예금주, [개인/고정], 기사명, 차량번호) VALUES (?,?,?,?,?,?,?,?,?,?)", d_vals)

    # 업체명 있으면 업체관리(clients) 13개 탭 전부 동기화 — 사업자구분·업체명·발행구분·사업자등록번호·대표자명·사업자주소·업태·종목·메일주소·담당자·연락처·결제특이사항·비고
    if data.get('client_name'):
        c_name = str(data.get('client_name', '')).strip()
        c_vals = (
            str(data.get('pay_to', '')).strip(),
            str(data.get('biz_issue', '')).strip(),
            str(data.get('biz_num', '')).strip(),
            str(data.get('biz_owner', '')).strip(),
            str(data.get('biz_addr', '')).strip(),
            str(data.get('biz_type2', '')).strip(),
            str(data.get('biz_type1', '')).strip(),
            str(data.get('mail', '')).strip(),
            str(data.get('c_mgr_name', '')).strip(),
            str(data.get('c_phone', '')).strip(),
            str(data.get('pay_memo', '')).strip(),
            str(data.get('client_memo', '')).strip(),
            c_name
        )
        cursor.execute("SELECT rowid FROM clients WHERE 업체명 = ?", (c_name,))
        row = cursor.fetchone()
        if row:
            cursor.execute("""UPDATE clients SET [사업자구분]=?, [발행구분]=?, [사업자등록번호]=?, [대표자명]=?, [사업자주소]=?, [업태]=?, [종목]=?, [메일주소]=?, [담당자]=?, [연락처]=?, [결제특이사항]=?, [비고]=? WHERE 업체명=?""", c_vals)
        else:
            cursor.execute("""INSERT INTO clients ([사업자구분], [업체명], [발행구분], [사업자등록번호], [대표자명], [사업자주소], [업태], [종목], [메일주소], [담당자], [연락처], [결제특이사항], [비고])
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (c_vals[0], c_name, c_vals[1], c_vals[2], c_vals[3], c_vals[4], c_vals[5], c_vals[6], c_vals[7], c_vals[8], c_vals[9], c_vals[10], c_vals[11]))

    conn.commit()
    conn.close()
    load_db_to_mem()
    return jsonify({"status": "success"})

@app.route('/api/get_order_logs/<int:order_id>')
@login_required
def get_order_logs(order_id):
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    logs = conn.execute("""
        SELECT timestamp, action, details 
        FROM activity_logs 
        WHERE target_id = ? 
        ORDER BY timestamp DESC
    """, (order_id,)).fetchall()
    conn.close()
    result = []
    for l in logs:
        d = dict(l)
        d['timestamp'] = to_kst_str(d.get('timestamp'))
        result.append(d)
    return jsonify(result)

@app.route('/api/get_logs')
@login_required
def get_logs():
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    logs = conn.execute("SELECT * FROM activity_logs ORDER BY id DESC LIMIT 50").fetchall()
    conn.close()
    result = []
    for l in logs:
        d = dict(l)
        if 'timestamp' in d:
            d['timestamp'] = to_kst_str(d['timestamp'])
        result.append(d)
    return jsonify(result)

@app.route('/api/load_db_mem')
@login_required 
def api_load_db_mem(): load_db_to_mem(); return jsonify({"drivers": drivers_db, "clients": clients_db})

@app.route('/api/get_ledger')
@login_required 
def get_ledger():
    page = max(1, safe_int(request.args.get('page'), 1))
    per_page_arg = safe_int(request.args.get('per_page'), 20)
    per_page = per_page_arg if per_page_arg in (20, 50, 100) else 20
    start_dt = request.args.get('start', '')
    end_dt = request.args.get('end', '')
    month_end_client = request.args.get('month_end_client', '')
    month_end_driver = request.args.get('month_end_driver', '')
    q_search = (request.args.get('q') or '').strip()
    
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    
    # 기본 쿼리
    query = "SELECT * FROM ledger"
    params = []
    conditions = []
    
    # 날짜 필터링
    if start_dt and end_dt:
        conditions.append(" order_dt BETWEEN ? AND ?")
        params.extend([start_dt, end_dt])
    # 월말합산 필터
    if month_end_client:
        conditions.append(" (month_end_client = '1' OR month_end_client = 'Y')")
    if month_end_driver:
        conditions.append(" (month_end_driver = '1' OR month_end_driver = 'Y')")
    # 검색어: 전체 오더에서 검색 후 페이지네이션
    if q_search:
        q_like = "%" + q_search.replace("%", "\\%").replace("_", "\\_") + "%"
        q_parts = ["client_name LIKE ?", "d_name LIKE ?", "COALESCE(c_num,'') LIKE ?", "route LIKE ?", "COALESCE(memo1,'') LIKE ?", "COALESCE(memo2,'') LIKE ?", "COALESCE(req_add,'') LIKE ?"]
        params.extend([q_like] * 7)
        id_added = False
        if q_search.lower().startswith('n'):
            num_str = q_search[1:].lstrip('0') or '0'
            if num_str.isdigit():
                try:
                    q_parts.append("id = ?")
                    params.append(int(num_str))
                    id_added = True
                except (ValueError, TypeError):
                    pass
        if not id_added and q_search.isdigit():
            try:
                q_parts.append("id = ?")
                params.append(int(q_search))
            except (ValueError, TypeError):
                pass
        conditions.append(" (" + " OR ".join(q_parts) + ")")
    base_where = " WHERE " + " AND ".join(conditions) if conditions else ""
    total_count = conn.execute("SELECT COUNT(*) FROM ledger" + base_where, params).fetchone()[0]
    total_pages = max(1, (total_count + per_page - 1) // per_page)
    start_idx = (page - 1) * per_page
    query += base_where + " ORDER BY id DESC LIMIT ? OFFSET ?"
    params.extend([per_page, start_idx])
    rows = conn.execute(query, params).fetchall()
    page_rows = []
    for r in rows:
        d = dict(r)
        # 계산서/우편확인: 장부·정산 동일 표시를 위해 정규화
        d['tax_chk'] = '발행완료' if _norm_tax_chk(r['tax_chk']) else ''
        d['is_mail_done'] = '확인완료' if _norm_mail_done(r['is_mail_done']) else '미확인'
        calc_vat_auto(d)
        # 개인/고정: 기사관리(기사현황)와 연동하여 해당 기사 값 표시
        driver_fixed = get_driver_fixed_type(drivers_db, d.get('d_name'), d.get('c_num'))
        if driver_fixed is not None:
            d['log_move'] = driver_fixed
        # 비고(memo2): 기사관리 기사 비고(메모)와 연동 — 목록 표시 시 해당 기사의 메모 표시
        d_name = (d.get('d_name') or '').strip()
        c_num = (d.get('c_num') or '').strip()
        if d_name or c_num:
            driver_row = next((dr for dr in drivers_db if (dr.get('기사명') or '').strip() == d_name and (dr.get('차량번호') or '').strip() == c_num), None)
            if driver_row is not None:
                d['memo2'] = driver_row.get('메모') or d.get('memo2') or ''
        # 업체비고: 업체관리(clients) 비고와 연동 — 목록 표시 시 해당 업체의 비고 표시
        c_name = (d.get('client_name') or '').strip()
        if c_name:
            client_row = next((c for c in clients_db if (c.get('업체명') or '').strip() == c_name), None)
            if client_row is not None:
                d['client_memo'] = client_row.get('비고') or d.get('client_memo') or ''
        page_rows.append(d)
    conn.close()
    return jsonify({"data": page_rows, "total_pages": total_pages, "current_page": page})


@app.route('/api/ledger_excel')
@login_required
def ledger_excel():
    """통합장부 전체(또는 현재 필터) 엑셀 다운로드"""
    start_dt = request.args.get('start', '')
    end_dt = request.args.get('end', '')
    month_end_client = request.args.get('month_end_client', '')
    month_end_driver = request.args.get('month_end_driver', '')
    conn = sqlite3.connect('ledger.db', timeout=15)
    conn.row_factory = sqlite3.Row
    query = "SELECT * FROM ledger"
    params = []
    conditions = []
    if start_dt and end_dt:
        conditions.append(" order_dt BETWEEN ? AND ?")
        params.extend([start_dt, end_dt])
    if month_end_client:
        conditions.append(" (month_end_client = '1' OR month_end_client = 'Y')")
    if month_end_driver:
        conditions.append(" (month_end_driver = '1' OR month_end_driver = 'Y')")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY id DESC"
    all_rows = conn.execute(query, params).fetchall()
    conn.close()
    # 한글 헤더 + 계산 보정
    col_keys = [c['k'] for c in FULL_COLUMNS]
    headers = ['id'] + [c['n'] for c in FULL_COLUMNS]
    rows = []
    for r in all_rows:
        d = dict(r)
        calc_vat_auto(d)
        driver_fixed = get_driver_fixed_type(drivers_db, d.get('d_name'), d.get('c_num'))
        if driver_fixed is not None:
            d['log_move'] = driver_fixed
        d_name = (d.get('d_name') or '').strip()
        c_num = (d.get('c_num') or '').strip()
        if d_name or c_num:
            driver_row = next((dr for dr in drivers_db if (dr.get('기사명') or '').strip() == d_name and (dr.get('차량번호') or '').strip() == c_num), None)
            if driver_row is not None:
                d['memo2'] = driver_row.get('메모') or d.get('memo2') or ''
        row = [d.get('id', '')] + [d.get(k, '') or '' for k in col_keys]
        rows.append(row)
    df = pd.DataFrame(rows if rows else [[]], columns=headers)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        df.to_excel(w, index=False)
    out.seek(0)
    fname = f"통합장부_{now_kst().strftime('%Y%m%d_%H%M')}.xlsx"
    return send_file(out, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=fname)


@app.route('/api/ledger_upload', methods=['POST'])
@login_required
def ledger_upload():
    """통합장부 엑셀 업로드 — 다운로드 양식(id + 한글 헤더)과 동일한 엑셀을 업로드하여 반영"""
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "파일이 없습니다."}), 400
    file = request.files['file']
    if not file or file.filename == '':
        return jsonify({"status": "error", "message": "파일을 선택해 주세요."}), 400
    if not file.filename.lower().endswith(('.xlsx', '.xls')):
        return jsonify({"status": "error", "message": "엑셀 파일(.xlsx, .xls)만 업로드 가능합니다."}), 400
    try:
        df = pd.read_excel(file, engine='openpyxl').fillna('')
    except Exception as e:
        return jsonify({"status": "error", "message": f"엑셀 읽기 오류: {str(e)}"}), 400
    # 헤더 매핑: 한글(c['n']) -> 키(c['k']), id -> id
    header_to_key = {'id': 'id'}
    for c in FULL_COLUMNS:
        header_to_key[c['n']] = c['k']
    keys = [c['k'] for c in FULL_COLUMNS]
    conn = sqlite3.connect('ledger.db', timeout=15)
    cursor = conn.cursor()
    inserted = updated = 0
    today_str = now_kst().strftime('%Y-%m-%d')
    for _, row in df.iterrows():
        data = {}
        for col in df.columns:
            key = header_to_key.get(str(col).strip())
            if key:
                val = row.get(col, '')
                if val is None or (isinstance(val, float) and pd.isna(val)): val = ''
                if pd.isna(val): val = ''
                # 엑셀 날짜 셀: datetime이면 YYYY-MM-DD로, Excel 시리얼 숫자면 날짜로 변환 후 저장 (NaT 제외)
                if key == 'order_dt' and val != '':
                    if hasattr(val, 'strftime') and not pd.isna(val):
                        try:
                            val = val.strftime('%Y-%m-%d')
                        except (ValueError, TypeError):
                            val = ''
                    elif hasattr(val, 'strftime') and pd.isna(val):
                        val = ''
                    elif isinstance(val, (int, float)) and not pd.isna(val) and 0 < float(val) < 100000:
                        try:
                            val = (datetime(1899, 12, 30) + timedelta(days=float(val))).strftime('%Y-%m-%d')
                        except Exception:
                            val = str(val)
                    else:
                        val = str(val) if not pd.isna(val) else ''
                elif key != 'id' and key != 'order_dt':
                    val = str(val) if val != '' and not pd.isna(val) else ''
                data[key] = sanitize_ledger_value(key, str(val)) if key != 'id' else str(val).strip()
        if not data:
            continue
        # 오더일: 업로드에 입력된 값 유지. 비어 있을 때만 오늘로 설정 (통계 기간 필터용)
        od = (data.get('order_dt') or '').strip()
        if not od:
            data['order_dt'] = today_str
        else:
            data['order_dt'] = str(od)[:10].replace('/', '-')
        calc_vat_auto(data)
        lid = data.get('id', '')
        try:
            target_id = int(float(str(lid).strip())) if lid else 0
        except (ValueError, TypeError):
            target_id = 0
        if target_id > 0:
            cursor.execute("SELECT 1 FROM ledger WHERE id = ?", (target_id,))
            if cursor.fetchone():
                sql = ", ".join([f"[{k}] = ?" for k in keys])
                vals = [data.get(k, '') for k in keys] + [target_id]
                cursor.execute(f"UPDATE ledger SET {sql} WHERE id = ?", vals)
                updated += 1
                continue
        # 신규 삽입 (id 없거나 DB에 없음)
        data.pop('id', None)
        placeholders = ", ".join(['?'] * len(keys))
        cursor.execute(f"INSERT INTO ledger ({', '.join([f'[{k}]' for k in keys])}) VALUES ({placeholders})", [data.get(k, '') for k in keys])
        inserted += 1
    conn.commit()
    conn.close()
    load_db_to_mem()
    return jsonify({"status": "success", "message": f"반영 완료: 신규 {inserted}건, 수정 {updated}건. 통계 페이지를 새로고침하면 반영됩니다."})


@app.route('/api/get_ledger_row/<int:row_id>')
@login_required
def get_ledger_row(row_id):
    """단일 장부 행 조회 (정산관리 → 통합장부입력 연동용)"""
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM ledger WHERE id = ?", (row_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "not found"}), 404
    d = dict(row)
    # 계산서/우편확인: 장부·정산 동일 표시를 위해 저장값 정규화하여 반환
    d['tax_chk'] = '발행완료' if _norm_tax_chk(row['tax_chk']) else ''
    d['is_mail_done'] = '확인완료' if _norm_mail_done(row['is_mail_done']) else '미확인'
    calc_vat_auto(d)
    # 개인/고정: 기사관리와 연동
    driver_fixed = get_driver_fixed_type(drivers_db, d.get('d_name'), d.get('c_num'))
    if driver_fixed is not None:
        d['log_move'] = driver_fixed
    # 비고(memo2): 기사관리 기사 비고(메모)와 연동
    d_name = (d.get('d_name') or '').strip()
    c_num = (d.get('c_num') or '').strip()
    if d_name or c_num:
        driver_row = next((dr for dr in drivers_db if (dr.get('기사명') or '').strip() == d_name and (dr.get('차량번호') or '').strip() == c_num), None)
        if driver_row is not None:
            d['memo2'] = driver_row.get('메모') or d.get('memo2') or ''
    return jsonify(d)


@app.route('/api/ledger_delete_all', methods=['POST'])
@login_required
def ledger_delete_all_api():
    """장부(ledger) 테이블 전체 삭제 — 복구 불가"""
    conn = sqlite3.connect('ledger.db', timeout=15)
    count = conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
    conn.execute("DELETE FROM ledger")
    conn.execute("INSERT INTO activity_logs (action, target_id, details) VALUES (?, ?, ?)", ("장부전체삭제", 0, f"장부 {count}건 전체 삭제"))
    conn.commit()
    conn.close()
    load_db_to_mem()
    return jsonify({"status": "success", "message": f"장부 {count}건이 전체 삭제되었습니다."})


@app.route('/api/delete_ledger/<int:row_id>', methods=['POST', 'DELETE'])
@login_required
def delete_ledger_api(row_id):
    """장부 행 삭제"""
    conn = sqlite3.connect('ledger.db', timeout=15)
    cur = conn.execute("SELECT id FROM ledger WHERE id = ?", (row_id,)).fetchone()
    if not cur:
        conn.close()
        return jsonify({"status": "error", "message": "not found"}), 404
    conn.execute("DELETE FROM ledger WHERE id = ?", (row_id,))
    conn.execute("INSERT INTO activity_logs (action, target_id, details) VALUES (?, ?, ?)", ("삭제", row_id, f"장부 ID {row_id} 삭제"))
    conn.commit()
    conn.close()
    return jsonify({"status": "success"})


@app.route('/api/recall_ledger/<int:row_id>', methods=['POST'])
@login_required
def recall_ledger_api(row_id):
    """오더 재호출: 동일 내용으로 현재 시간 기준 새 행 생성"""
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM ledger WHERE id = ?", (row_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({"status": "error", "message": "not found"}), 404
    keys = [c['k'] for c in FULL_COLUMNS]
    now = now_kst()
    order_dt = now.strftime('%Y-%m-%d')
    dispatch_dt = now.strftime('%Y-%m-%dT%H:%M')
    r = dict(row)
    data = {k: r.get(k, '') for k in keys}
    data['order_dt'] = order_dt
    data['dispatch_dt'] = dispatch_dt
    data['in_dt'] = ''
    data['out_dt'] = ''
    data['tax_chk'] = ''
    data['tax_dt'] = ''
    data['issue_dt'] = ''
    data['tax_img'] = ''
    data['ship_img'] = ''
    data['is_mail_done'] = ''
    data['mail_dt'] = ''
    conn = sqlite3.connect('ledger.db', timeout=15)
    cursor = conn.cursor()
    placeholders = ", ".join(['?'] * len(keys))
    cursor.execute(f"INSERT INTO ledger ({', '.join([f'[{k}]' for k in keys])}) VALUES ({placeholders})", [data.get(k, '') for k in keys])
    new_id = cursor.lastrowid
    cursor.execute("INSERT INTO activity_logs (action, target_id, details) VALUES (?, ?, ?)", ("재호출", new_id, f"원본 ID {row_id} → 신규 ID {new_id}"))
    conn.commit()
    conn.close()
    return jsonify({"status": "success", "id": new_id})


# update_status에서 허용할 컬럼명 화이트리스트 (SQL injection 방지)
ALLOWED_STATUS_KEYS = {c['k'] for c in FULL_COLUMNS}

@app.route('/api/update_status', methods=['POST'])
@login_required 
def update_status():
    data = request.json or {}
    key = data.get('key')
    if key not in ALLOWED_STATUS_KEYS:
        return jsonify({"status": "error", "message": "invalid key"}), 400
    try:
        row_id = int(data.get('id', 0))
        if row_id <= 0:
            return jsonify({"status": "error", "message": "invalid id"}), 400
    except (ValueError, TypeError):
        return jsonify({"status": "error", "message": "invalid id"}), 400
    conn = sqlite3.connect('ledger.db', timeout=15)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    display_name = next((col['n'] for col in FULL_COLUMNS if col['k'] == key), key)
    val = data.get('value')
    # 계산서/우편확인: 장부·정산 연동을 위해 DB에는 항상 동일한 값만 저장
    if key == 'tax_chk':
        val = '발행완료' if (val and str(val).strip() == '발행완료') else ''
    if key == 'is_mail_done':
        val = '확인완료' if (val and str(val).strip() == '확인완료') else '미확인'
    cursor.execute(f"UPDATE ledger SET [{key}] = ? WHERE id = ?", (val, row_id))
    # 개인/고정: 기사관리와 연동 — 장부에서 변경 시 해당 기사의 기사관리(개인/고정)도 동기화
    if key == 'log_move':
        row = cursor.execute("SELECT d_name, c_num FROM ledger WHERE id = ?", (row_id,)).fetchone()
        if row and (row[0] or row[1]):
            cursor.execute("UPDATE drivers SET [개인/고정] = ? WHERE 기사명 = ? AND 차량번호 = ?",
                           (str(data.get('value', '')).strip(), row[0] or '', row[1] or ''))
            if cursor.rowcount == 0:
                cursor.execute("INSERT INTO drivers (기사명, 차량번호, [개인/고정]) VALUES (?, ?, ?)",
                               (row[0] or '', row[1] or '', str(data.get('value', '')).strip()))
    # 계산서 발행완료 시 계산서발행일(tax_dt) 동시 설정, 취소 시 비움
    if key == 'tax_chk':
        tax_dt_val = now_kst().strftime('%Y-%m-%d') if (val == '발행완료') else ''
        cursor.execute("UPDATE ledger SET tax_dt = ? WHERE id = ?", (tax_dt_val, row_id))
    # tax_dt 변경 시 tax_chk 연동 (날짜 있음 → 발행완료, 없음 → '')
    if key == 'tax_dt':
        v = data.get('value')
        tax_chk_val = '발행완료' if (v and str(v).strip()) else ''
        cursor.execute("UPDATE ledger SET tax_chk = ? WHERE id = ?", (tax_chk_val, row_id))
    # mail_dt 변경 시 is_mail_done 연동 (날짜 있음 → 확인완료, 없음 → 미확인)
    if key == 'mail_dt':
        v = data.get('value')
        mail_done_val = '확인완료' if (v and str(v).strip()) else '미확인'
        cursor.execute("UPDATE ledger SET is_mail_done = ? WHERE id = ?", (mail_done_val, row_id))
    if key in ('pay_method_client', 'pay_method_driver'):
        row = cursor.execute("SELECT * FROM ledger WHERE id = ?", (row_id,)).fetchone()
        if row:
            d = dict(row)
            calc_vat_auto(d)
            for k in ('vat1', 'total1', 'vat2', 'total2', 'net_profit', 'vat_final'):
                cursor.execute(f"UPDATE ledger SET [{k}] = ? WHERE id = ?", (d.get(k, ''), row_id))
    # 업체운임(fee)·기사운임(fee_out) 변경 시 공급가액·부가세·합계 재계산
    if key in ('fee', 'fee_out'):
        row = cursor.execute("SELECT * FROM ledger WHERE id = ?", (row_id,)).fetchone()
        if row:
            d = dict(row)
            calc_vat_auto(d)
            for k in ('sup_val', 'vat1', 'total1', 'vat2', 'total2', 'net_profit', 'vat_final'):
                cursor.execute(f"UPDATE ledger SET [{k}] = ? WHERE id = ?", (d.get(k, ''), row_id))
    # 업체비고(client_memo): 장부목록에서 변경 시 업체관리(clients) 비고와 연동
    if key == 'client_memo':
        row = cursor.execute("SELECT client_name FROM ledger WHERE id = ?", (row_id,)).fetchone()
        if row and (row[0] or '').strip():
            cursor.execute("UPDATE clients SET [비고] = ? WHERE 업체명 = ?",
                           (str(data.get('value', '')).strip(), (row[0] or '').strip()))
    # 비고(memo2): 장부목록에서 변경 시 기사관리(기사 비고/메모)와 연동
    if key == 'memo2':
        row = cursor.execute("SELECT d_name, c_num FROM ledger WHERE id = ?", (row_id,)).fetchone()
        if row and (row[0] or row[1]):
            cursor.execute("UPDATE drivers SET [메모] = ? WHERE 기사명 = ? AND 차량번호 = ?",
                           (str(data.get('value', '')).strip(), row[0] or '', row[1] or ''))
            if cursor.rowcount == 0:
                cursor.execute("INSERT INTO drivers (기사명, 차량번호, [메모]) VALUES (?, ?, ?)",
                               (row[0] or '', row[1] or '', str(data.get('value', '')).strip()))
    log_details = f"[{display_name}] 항목이 '{data.get('value')}'(으)로 변경됨"
    cursor.execute("INSERT INTO activity_logs (action, target_id, details) VALUES (?, ?, ?)",
                   ("상태변경", row_id, log_details))
    conn.commit()
    conn.close()
    if key in ('log_move', 'client_memo', 'memo2'):
        load_db_to_mem()
    return jsonify({"status": "success"})

@app.route('/api/clients_excel')
@login_required
def clients_excel():
    """업체관리 전체 목록 엑셀 다운로드 (업로드 양식과 동일)"""
    conn = sqlite3.connect('ledger.db', timeout=15)
    df = pd.read_sql("SELECT * FROM clients ORDER BY 업체명", conn)
    conn.close()
    df = df.fillna('')
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        df.to_excel(w, index=False)
    out.seek(0)
    fname = f"업체관리_전체목록_{now_kst().strftime('%Y%m%d_%H%M')}.xlsx"
    return send_file(out, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=fname)


@app.route('/export_clients')
@login_required
def export_clients():
    """업체관리 - 비고, 사업자구분, 결제특이사항, 발행구분, 사업자등록번호, 대표자명, 사업자주소, 업태, 종목, 메일주소, 오더일, 노선, 업체운임 순 엑셀"""
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    # 업체명별 최신 오더 1건 (오더일, 노선, 업체운임)
    ledger_rows = conn.execute(
        "SELECT client_name, order_dt, route, fee, comm, pre_post FROM ledger WHERE client_name IS NOT NULL AND client_name != '' ORDER BY id DESC"
    ).fetchall()
    conn.close()
    latest_order = {}
    for r in ledger_rows:
        cname = (r[0] or '').strip()
        if cname and cname not in latest_order:
            fee_total = int(calc_supply_value({'fee': r[3], 'comm': r[4], 'pre_post': r[5]}))
            latest_order[cname] = {'오더일': r[1] or '', '노선': r[2] or '', '공급가액': fee_total}
    # 컬럼 순서: 비고, 사업자구분, 결제특이사항, 발행구분, 사업자등록번호, 대표자명, 사업자주소, 업태, 종목, 메일주소, 오더일, 노선, 공급가액
    export_cols = ['비고', '사업자구분', '결제특이사항', '발행구분', '사업자등록번호', '대표자명', '사업자주소', '업태', '종목', '메일주소', '오더일', '노선', '공급가액']
    export_data = []
    for c in clients_db:
        cname = (c.get('업체명') or '').strip()
        order_info = latest_order.get(cname, {'오더일': '', '노선': '', '공급가액': ''})
        row = {
            '비고': c.get('비고', ''),
            '사업자구분': c.get('사업자구분', ''),
            '결제특이사항': c.get('결제특이사항', ''),
            '발행구분': c.get('발행구분', ''),
            '사업자등록번호': c.get('사업자등록번호', ''),
            '대표자명': c.get('대표자명', ''),
            '사업자주소': c.get('사업자주소', ''),
            '업태': c.get('업태', ''),
            '종목': c.get('종목', ''),
            '메일주소': c.get('메일주소', ''),
            '오더일': order_info['오더일'],
            '노선': order_info['노선'],
            '공급가액': order_info['공급가액'],
        }
        export_data.append(row)
    df = pd.DataFrame(export_data, columns=export_cols)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        df.to_excel(w, index=False)
    out.seek(0)
    return send_file(out, as_attachment=True, download_name="clients.xlsx")

@app.route('/api/delete_client/<int:row_id>', methods=['POST', 'DELETE'])
@login_required
def api_delete_client(row_id):
    conn = sqlite3.connect('ledger.db', timeout=15)
    conn.execute("DELETE FROM clients WHERE rowid = ?", (row_id,))
    conn.commit(); conn.close()
    load_db_to_mem()
    return jsonify({"status": "success"})

@app.route('/api/update_client/<int:row_id>', methods=['POST'])
@login_required
def api_update_client(row_id):
    data = request.json or {}
    conn = sqlite3.connect('ledger.db', timeout=15)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(clients)")
    cols = [r[1] for r in cursor.fetchall() if r[1] != 'id']
    updates = []
    vals = []
    for c in cols:
        if c in data:
            updates.append(f'[{c}] = ?')
            vals.append(str(data.get(c, '')))
    if not updates:
        conn.close()
        return jsonify({"status": "error", "message": "수정할 데이터 없음"}), 400
    vals.append(row_id)
    cursor.execute(f"UPDATE clients SET {', '.join(updates)} WHERE rowid = ?", vals)
    conn.commit(); conn.close()
    load_db_to_mem()
    return jsonify({"status": "success"})

@app.route('/manage_clients', methods=['GET', 'POST'])
@login_required 
def manage_clients():
    global clients_db
    load_db_to_mem()  # DB 삭제 후에도 현재 DB 기준으로 목록 표시
    if request.method == 'POST' and 'file' in request.files:
        file = request.files['file']
        if file.filename != '':
            conn_up = None
            try:
                if file.filename.lower().endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(file, engine='openpyxl')
                else:
                    df = pd.read_csv(io.StringIO(file.stream.read().decode("utf-8-sig")))
                df = df.fillna('').astype(str)
                conn_up = sqlite3.connect('ledger.db', timeout=15)
                # 기존 데이터 유지: 업로드 파일은 추가·수정만 반영 (업체명 기준 병합)
                try:
                    existing = pd.read_sql("SELECT * FROM clients", conn_up)
                except Exception:
                    existing = pd.DataFrame()
                if len(existing) == 0:
                    merge_df = df
                else:
                    key_col = '업체명'
                    if key_col not in df.columns:
                        merge_df = df
                    else:
                        existing[key_col] = existing[key_col].astype(str).str.strip()
                        df[key_col] = df[key_col].astype(str).str.strip()
                        by_name = existing.set_index(key_col).to_dict('index')
                        for _, r in df.iterrows():
                            key = (r.get(key_col) or '').strip() or None
                            if key is None:
                                continue
                            row_dict = {c: r.get(c, '') if c in r else by_name.get(key, {}).get(c, '') for c in existing.columns}
                            by_name[key] = row_dict
                        merge_df = pd.DataFrame(list(by_name.values()), columns=existing.columns)
                merge_df.to_sql('clients', conn_up, if_exists='replace', index=False)
                conn_up.commit()
                load_db_to_mem()
            except Exception as e:
                return f"업로드 오류: {str(e)}"
            finally:
                if conn_up is not None:
                    conn_up.close()
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    try:
        clients_with_id = conn.execute("SELECT rowid as id, * FROM clients").fetchall()
    except sqlite3.OperationalError:
        clients_with_id = []
    conn.close()
    clients_with_id = [dict(r) for r in clients_with_id]
    q_search = (request.args.get('q') or '').strip()
    if q_search:
        q_lower = q_search.lower()
        clients_filtered = [r for r in clients_with_id if any(q_lower in str(r.get(c, '')).lower() for c in CLIENT_COLS)]
    else:
        clients_filtered = clients_with_id
    clients_filtered = sorted(clients_filtered, key=lambda r: (str(r.get('업체명') or '')).strip())
    total_full = len(clients_with_id)
    total_clients = len(clients_filtered)
    page = max(1, safe_int(request.args.get('page'), 1))
    per_page_arg = safe_int(request.args.get('per_page'), 50)
    per_page = per_page_arg if per_page_arg in (20, 50, 100) else 50
    total_pages = max(1, (total_clients + per_page - 1) // per_page)
    page = min(page, total_pages)
    start_idx = (page - 1) * per_page
    page_clients = clients_filtered[start_idx:start_idx + per_page]
    q_enc = quote(q_search, safe='') if q_search else ''
    q_param = f'&q={q_enc}' if q_enc else ''
    _block = 10
    _block_idx = (page - 1) // _block
    _start = _block_idx * _block + 1
    _end = min(_block_idx * _block + _block, total_pages)
    _parts = []
    if _block_idx > 0:
        _parts.append(f'<a href="/manage_clients?page={(_block_idx - 1) * _block + 1}&per_page={per_page}{q_param}" class="page-btn">이전</a>')
    for i in range(_start, _end + 1):
        _parts.append(f'<a href="/manage_clients?page={i}&per_page={per_page}{q_param}" class="page-btn {"active" if i == page else ""}">{i}</a>')
    if _end < total_pages:
        _parts.append(f'<a href="/manage_clients?page={_end + 1}&per_page={per_page}{q_param}" class="page-btn">다음</a>')
    pagination_client_html = "".join(_parts)
    def _client_row(r):
        cid = r.get('id', '')
        search = ' '.join([str(r.get(c, '')).lower() for c in CLIENT_COLS])
        btns = f'<td style="white-space:nowrap;"><button type="button" class="btn-edit" onclick="editClient({cid})" style="padding:4px 8px; font-size:11px; margin-right:4px;">수정</button><button type="button" class="btn-status bg-red" onclick="deleteClient({cid})" style="padding:4px 8px; font-size:11px;">삭제</button></td>'
        cells = ''.join([f'<td>{r.get(c, "")}</td>' for c in CLIENT_COLS])
        return f'<tr class="filter-row" data-id="{cid}" data-search="{search}">{btns}{cells}</tr>'
    rows_html = "".join([_client_row(r) for r in page_clients])
    content = f"""<div class="section"><h2>업체 관리</h2>
    <div style="margin-bottom:15px; display:flex; align-items:center; gap:12px; flex-wrap:wrap;">
        <a href="/api/clients_excel" class="btn-status bg-green" style="text-decoration:none; padding:6px 12px;">전체 목록 다운로드</a>
        <form method="post" enctype="multipart/form-data" style="display:inline;">
            <input type="file" name="file" accept=".xlsx,.xls,.csv"> <button type="submit" class="btn-save">엑셀 업로드</button>
        </form>
        <span style="font-size:12px; color:#666;">다운로드한 엑셀 양식에 맞춰 수정 후 업로드하면 반영됩니다.</span>
    </div>
    <div style="margin-bottom:12px; display:flex; align-items:center; gap:12px; flex-wrap:wrap;">
        <form method="get" action="/manage_clients" style="display:inline-flex; align-items:center; gap:8px;">
            <input type="hidden" name="per_page" value="{per_page}">
            <input type="text" name="q" id="clientFilter" value="{q_search.replace("&", "&amp;").replace('"', "&quot;").replace("<", "&lt;")}" placeholder="업체명, 사업자번호, 대표자명 등 검색 (전체 DB 검색)" style="width:300px; padding:8px 12px; border:1px solid #cbd5e1; border-radius:6px;">
            <button type="submit" class="btn-edit" style="padding:8px 14px;">검색</button>
        </form>
        <span style="margin-left:8px;">출력</span>
        <select onchange="var q=document.getElementById('clientFilter').value; location.href='/manage_clients?page=1&per_page='+this.value+(q ? '&q='+encodeURIComponent(q) : '')" style="padding:6px 10px; border:1px solid #d0d7de; border-radius:4px; font-size:13px;">
            <option value="20" {"selected" if per_page == 20 else ""}>20</option>
            <option value="50" {"selected" if per_page == 50 else ""}>50</option>
            <option value="100" {"selected" if per_page == 100 else ""}>100</option>
        </select>
        <span style="font-size:12px; color:#666;">개씩</span>
        <span style="font-size:12px; color:#64748b;">{"검색 결과 " + str(total_clients) + "개 (전체 " + str(total_full) + "개)" if q_search else "(총 " + str(total_full) + "개)"}</span>
    </div>
    <div class="scroll-top" id="clientScrollTop"><table><thead><tr><th>관리</th>{"".join([f"<th>{c}</th>" for c in CLIENT_COLS])}</tr></thead><tbody><tr><td>-</td>{"".join(["<td>-</td>" for _ in CLIENT_COLS])}</tr></tbody></table></div>
    <div class="scroll-x" id="clientScroll"><table><thead><tr><th>관리</th>{"".join([f"<th>{c}</th>" for c in CLIENT_COLS])}</tr></thead><tbody id="clientTableBody">{rows_html}</tbody></table></div>
    <div class="pagination" style="margin-top:12px;">{pagination_client_html}</div>
    <div id="clientEditModal" style="display:none; position:fixed; z-index:9999; left:0; top:0; width:100%; height:100%; background:rgba(0,0,0,0.5);">
        <div style="background:white; max-width:600px; margin:40px auto; padding:24px; border-radius:10px; max-height:90vh; overflow-y:auto;">
            <h3 style="margin:0 0 20px 0;">업체 수정</h3>
            <form id="clientEditForm" onsubmit="event.preventDefault(); const id=this.dataset.editId; const d={{}}; this.querySelectorAll('[name]').forEach(inp=>d[inp.name]=inp.value); fetch('/api/update_client/'+id, {{method:'POST', headers:{{'Content-Type':'application/json'}}, body:JSON.stringify(d)}}).then(r=>r.json()).then(res=>{{if(res.status==='success'){{document.getElementById('clientEditModal').style.display='none';location.reload();}}else alert(res.message);}});">
                {"".join([f'<div style="margin-bottom:10px;"><label style="display:block; font-size:12px; margin-bottom:4px;">{c}</label><input type="text" name="{c}" style="width:100%; padding:8px; border:1px solid #ddd; border-radius:4px;"></div>' for c in CLIENT_COLS])}
                <div style="margin-top:20px; display:flex; gap:10px;"><button type="submit" class="btn-save">저장</button><button type="button" class="btn" onclick="document.getElementById('clientEditModal').style.display='none'">취소</button></div>
            </form>
        </div>
    </div>
    <script>
    function editClient(id) {{
        const row = document.querySelector('#clientTableBody tr[data-id="'+id+'"]');
        if(!row) return;
        const cells = row.querySelectorAll('td');
        const cols = {json.dumps(CLIENT_COLS)};
        const data = {{}};
        cols.forEach((c, i) => {{ data[c] = (cells[i+1] && cells[i+1].innerText) || ''; }});
        const form = document.getElementById('clientEditForm');
        if(form) {{ cols.forEach(c => {{ const inp = form.querySelector('[name="'+c+'"]'); if(inp) inp.value = data[c] || ''; }}); form.dataset.editId = id; document.getElementById('clientEditModal').style.display = 'block'; }}
    }}
    function deleteClient(id) {{ if(!confirm('이 업체를 삭제하시겠습니까?')) return; fetch('/api/delete_client/' + id, {{method:'POST'}}).then(r=>r.json()).then(res=>{{if(res.status==='success')location.reload();else alert(res.message||'삭제 실패');}}).catch(()=>alert('삭제 중 오류')); }}
    (function() {{
        const topEl = document.getElementById('clientScrollTop');
        const mainEl = document.getElementById('clientScroll');
        if (!topEl || !mainEl) return;
        function matchWidth() {{
            const mainTbl = mainEl.querySelector('table');
            const topTbl = topEl.querySelector('table');
            if (mainTbl && topTbl) {{
                const mainRow = mainTbl.querySelector('thead tr') || mainTbl.querySelector('tr');
                const topRow = topTbl.querySelector('tr');
                if (mainRow && topRow && mainRow.cells.length === topRow.cells.length) {{
                    for (let i = 0; i < mainRow.cells.length; i++) {{
                        const w = mainRow.cells[i].offsetWidth;
                        topRow.cells[i].style.width = w + 'px';
                        topRow.cells[i].style.minWidth = w + 'px';
                    }}
                }}
            }}
            topEl.style.width = mainEl.clientWidth + 'px';
        }}
        let syncing = false;
        function sync(src) {{ if(syncing) return; syncing = true; const left = src.scrollLeft; if(topEl.scrollLeft !== left) topEl.scrollLeft = left; if(mainEl.scrollLeft !== left) mainEl.scrollLeft = left; requestAnimationFrame(()=>{{syncing=false;}}); }}
        topEl.addEventListener('scroll', () => sync(topEl));
        mainEl.addEventListener('scroll', () => sync(mainEl));
        setTimeout(matchWidth, 80);
        window.addEventListener('resize', matchWidth);
    }})();
    </script></div>"""
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys="[]")
# --- [도착현황 라우트 및 API] ---
@app.route('/arrival')
@login_required
def arrival():
    conn = sqlite3.connect('ledger.db', timeout=15); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM arrival_status ORDER BY order_idx ASC, id ASC").fetchall(); conn.close()
    items = [dict(r) for r in rows]
    items_json = json.dumps(items, ensure_ascii=False)

    content = f"""
    <div class="section">
        <h2>🚚 도착현황</h2>
        <div style="margin-bottom:20px; padding:18px; background:#f8fafc; border-radius:8px; border:1px solid #e2e8f0;">
            <div style="display:grid; grid-template-columns: 1fr 1fr; gap:12px; margin-bottom:12px;">
                <div>
                    <label style="display:block; font-size:11px; color:#64748b; margin-bottom:4px;">내용</label>
                    <input type="text" id="arrivalContent" placeholder="예: 서울→부산 12톤 김기사" style="width:100%; padding:8px 12px; border:1px solid #cbd5e1; border-radius:6px;">
                </div>
                <div>
                    <label style="display:block; font-size:11px; color:#1a2a6c; font-weight:bold; margin-bottom:4px;">중요내용</label>
                    <input type="text" id="arrivalContentImportant" placeholder="강조할 중요 문구" style="width:100%; padding:8px 12px; border:1px solid #1a2a6c; border-radius:6px;">
                </div>
            </div>
            <div style="display:flex; flex-wrap:wrap; gap:12px; align-items:flex-end;">
                <div>
                    <label style="display:block; font-size:11px; color:#64748b; margin-bottom:4px;">도착 예정 시간</label>
                    <input type="datetime-local" id="arrivalTargetTime" style="width:200px; padding:8px 12px; border:1px solid #cbd5e1; border-radius:6px;">
                </div>
                <div>
                    <label style="display:block; font-size:11px; color:#64748b; margin-bottom:4px;">색상</label>
                    <input type="color" id="arrivalContentColor" value="#1a2a6c" style="width:44px; height:36px; padding:2px; border:1px solid #cbd5e1; border-radius:6px; cursor:pointer;">
                </div>
                <button onclick="addArrivalItem()" class="btn-save" style="padding:8px 18px;">추가</button>
            </div>
        </div>
        <div class="arrival-list" id="arrivalList"></div>
    </div>
    <style>
        .arrival-item {{ background:white; border:1px solid #e2e8f0; border-radius:8px; padding:14px 16px; margin-bottom:10px; display:flex; align-items:flex-start; gap:14px; box-shadow:0 1px 3px rgba(0,0,0,0.05); cursor:pointer; }}
        .arrival-item.expired {{ background:#fef2f2; border-color:#fecaca; }}
        .arrival-item.editing {{ border-color:#1a2a6c; box-shadow:0 0 0 2px rgba(26,42,108,0.2); }}
        .arrival-item .countdown {{ font-size:20px; font-weight:700; color:#1a2a6c; min-width:140px; flex-shrink:0; }}
        .arrival-item .countdown.warn {{ color:#dc2626; }}
        .arrival-item .countdown.done {{ color:#64748b; font-size:14px; }}
        .arrival-item .countdown.paused {{ color:#94a3b8; font-style:italic; }}
        .arrival-item .content-area {{ flex:1; word-break:break-all; line-height:1.5; }}
        .arrival-item .content-display {{ padding:4px 8px; margin:-4px -8px; border-radius:4px; }}
        .arrival-item .content-display:hover {{ background:#f1f5f9; }}
        .arrival-item .content-edit {{ width:100%; border:1px solid #1a2a6c; padding:6px 10px; border-radius:4px; font-size:13px; min-height:36px; }}
        .arrival-item .edit-panel {{ display:none; margin-top:10px; padding:10px; background:#f8fafc; border-radius:6px; border:1px solid #e2e8f0; }}
        .arrival-item.editing .edit-panel {{ display:block; }}
        .arrival-item .meta {{ font-size:11px; color:#94a3b8; margin-top:6px; }}
        .arrival-item .del-btn {{ color:#ef4444; cursor:pointer; padding:4px 8px; font-size:12px; flex-shrink:0; }}
        .arrival-item .del-btn:hover {{ text-decoration:underline; }}
    </style>
    <script>
        let arrivalItems = {items_json};
        let editingArrivalId = null;

        function getSortedArrivalItems() {{
            const now = new Date();
            return [...arrivalItems].sort((a, b) => {{
                const ta = a.target_time ? new Date(a.target_time.replace(' ', 'T')) : null;
                const tb = b.target_time ? new Date(b.target_time.replace(' ', 'T')) : null;
                const aPast = !ta || ta <= now;
                const bPast = !tb || tb <= now;
                if (aPast && !bPast) return 1;
                if (!aPast && bPast) return -1;
                if (aPast && bPast) return (tb || 0) - (ta || 0);
                return ta - tb;
            }});
        }}

        function openArrivalEdit(id) {{
            if (editingArrivalId === id) return;
            closeArrivalEdit();
            editingArrivalId = id;
            const item = arrivalItems.find(i => i.id == id);
            if (!item) return;
            const row = document.getElementById('arrival-row-' + id);
            if (row) row.classList.add('editing');
            const cd = document.getElementById('cd-' + id);
            if (cd) cd.classList.add('paused');
            document.getElementById('edit-time-' + id).value = (item.target_time || '').replace(' ', 'T');
            document.getElementById('edit-content-' + id).value = item.content || '';
            document.getElementById('edit-important-' + id).value = item.content_important || '';
            document.getElementById('edit-color-' + id).value = item.content_color || '#1a2a6c';
        }}

        function closeArrivalEdit() {{
            if (editingArrivalId) {{
                const row = document.getElementById('arrival-row-' + editingArrivalId);
                if (row) row.classList.remove('editing');
                const cd = document.getElementById('cd-' + editingArrivalId);
                if (cd) cd.classList.remove('paused');
                editingArrivalId = null;
            }}
            updateAllCountdowns();
        }}

        function renderArrivalList() {{
            const list = document.getElementById('arrivalList');
            if (arrivalItems.length === 0) {{
                list.innerHTML = '<p style="color:#94a3b8; padding:30px; text-align:center;">등록된 항목이 없습니다. 위에서 시간과 내용을 입력 후 추가해 주세요.</p>';
                return;
            }}
            const sorted = getSortedArrivalItems();
            list.innerHTML = sorted.map(item => {{
                const targetTime = item.target_time || '';
                const content = item.content || '';
                const contentImportant = item.content_important || '';
                const contentColor = item.content_color || '#1a2a6c';
                const contentEsc = content.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
                const importantEsc = contentImportant.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
                const id = item.id;
                return `<div class="arrival-item" data-id="${{id}}" id="arrival-row-${{id}}" onclick="openArrivalEdit(${{id}})">
                    <div class="countdown" id="cd-${{id}}" data-target="${{targetTime}}"></div>
                    <div class="content-area">
                        <div class="content-display" id="content-display-${{id}}" style="color:${{contentColor}};">${{contentEsc || '(내용 없음)'}}</div>
                        <div id="important-wrap-${{id}}" style="margin-top:4px;">${{contentImportant ? '<span class=\"content-display\" id=\"important-display-' + id + '\" style=\"color:' + contentColor + '; font-weight:bold;\">★ ' + importantEsc + '</span>' : ''}}</div>
                        <div class="edit-panel" onclick="event.stopPropagation()">
                            <div style="margin-bottom:8px;">
                                <label style="font-size:11px; color:#64748b;">시간</label>
                                <input type="datetime-local" id="edit-time-${{id}}" class="content-edit" onblur="saveArrivalTime(${{id}}, this.value)">
                            </div>
                            <div style="margin-bottom:8px;">
                                <label style="font-size:11px; color:#64748b;">내용</label>
                                <input type="text" id="edit-content-${{id}}" class="content-edit" onblur="saveArrivalContent(${{id}}, this.value)">
                            </div>
                            <div style="margin-bottom:8px;">
                                <label style="font-size:11px; color:#64748b;">중요내용</label>
                                <input type="text" id="edit-important-${{id}}" class="content-edit" onblur="saveArrivalImportant(${{id}}, this.value)">
                            </div>
                            <div style="display:flex; gap:12px; align-items:center; margin-bottom:8px;">
                                <div>
                                    <label style="font-size:11px; color:#64748b;">색상</label>
                                    <input type="color" id="edit-color-${{id}}" style="width:40px; height:32px; margin-left:4px; cursor:pointer;" onchange="saveArrivalStyle(${{id}})">
                                </div>
                            </div>
                            <button type="button" onclick="closeArrivalEdit()" style="padding:6px 12px; background:#94a3b8; color:white; border:none; border-radius:4px; cursor:pointer; font-size:12px;">닫기</button>
                        </div>
                        <div class="meta" id="meta-${{id}}"></div>
                    </div>
                    <span class="del-btn" onclick="event.stopPropagation(); deleteArrivalItem(${{id}})">삭제</span>
                </div>`;
            }}).join('');

            updateAllCountdowns();
        }}

        function reorderArrivalList() {{
            const list = document.getElementById('arrivalList');
            if (!list || list.querySelector('.arrival-item') === null) return;
            const sorted = getSortedArrivalItems();
            sorted.forEach(item => {{
                const row = document.getElementById('arrival-row-' + item.id);
                if (row) list.appendChild(row);
            }});
        }}

        function updateAllCountdowns() {{
            const now = new Date();
            arrivalItems.forEach(item => {{
                const el = document.getElementById('cd-' + item.id);
                const metaEl = document.getElementById('meta-' + item.id);
                if (!el) return;
                if (editingArrivalId === item.id) {{
                    el.textContent = '편집중';
                    el.className = 'countdown paused';
                    return;
                }}
                const targetStr = item.target_time;
                if (!targetStr) {{
                    el.textContent = '-';
                    el.className = 'countdown done';
                    if (metaEl) metaEl.textContent = '시간 미지정';
                    return;
                }}  
                const target = new Date(targetStr.replace(' ', 'T'));
                const diff = target - now;
                if (diff <= 0) {{
                    el.textContent = '도착 완료';
                    el.className = 'countdown done';
                    const parent = el.closest('.arrival-item');
                    if (parent) parent.classList.add('expired');
                    if (metaEl) metaEl.textContent = '예정: ' + formatDateTime(target);
                }} else {{
                    const h = Math.floor(diff / 3600000);
                    const m = Math.floor((diff % 3600000) / 60000);
                    el.textContent = (h > 0 ? h + '시간 ' : '') + m + '분';
                    el.className = 'countdown' + (h < 1 ? ' warn' : '');
                    const parent = el.closest('.arrival-item');
                    if (parent) parent.classList.remove('expired');
                    if (metaEl) metaEl.textContent = '예정: ' + formatDateTime(target);
                }}
            }});
        }}

        function formatDateTime(d) {{
            return d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0') + ' ' +
                String(d.getHours()).padStart(2,'0') + ':' + String(d.getMinutes()).padStart(2,'0');
        }}

        setInterval(function() {{
            if (editingArrivalId) return;
            updateAllCountdowns();
            reorderArrivalList();
        }}, 60000);

        function addArrivalItem() {{
            const targetTime = document.getElementById('arrivalTargetTime').value;
            const content = document.getElementById('arrivalContent').value.trim();
            const contentImportant = document.getElementById('arrivalContentImportant').value.trim();
            const contentColor = document.getElementById('arrivalContentColor').value || '#1a2a6c';
            fetch('/api/arrival/add', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{ target_time: targetTime || null, content: content, content_important: contentImportant, content_color: contentColor }})
            }}).then(r => r.json()).then(res => {{
                if (res.status === 'success') {{
                    arrivalItems.push({{ id: res.id, target_time: targetTime || null, content: content, content_important: contentImportant, content_color: contentColor, order_idx: arrivalItems.length }});
                    document.getElementById('arrivalTargetTime').value = '';
                    document.getElementById('arrivalContent').value = '';
                    document.getElementById('arrivalContentImportant').value = '';
                    renderArrivalList();
                }}
            }});
        }}

        function saveArrivalTime(id, value) {{
            const targetTime = value ? value.replace('T', ' ') : null;
            fetch('/api/arrival/update', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{ id: id, target_time: targetTime }})
            }}).then(r => r.json()).then(res => {{
                if (res.status === 'success') {{
                    const item = arrivalItems.find(i => i.id == id);
                    if (item) item.target_time = targetTime;
                    updateAllCountdowns();
                }}
            }});
        }}

        function saveArrivalContent(id, value) {{
            fetch('/api/arrival/update', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{ id: id, content: value }})
            }}).then(r => r.json()).then(res => {{
                if (res.status === 'success') {{
                    const item = arrivalItems.find(i => i.id == id);
                    if (item) item.content = value;
                    const displayEl = document.getElementById('content-display-' + id);
                    if (displayEl) displayEl.textContent = value || '(내용 없음)';
                }}
            }});
        }}

        function saveArrivalImportant(id, value) {{
            fetch('/api/arrival/update', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{ id: id, content_important: value }})
            }}).then(r => r.json()).then(res => {{
                if (res.status === 'success') {{
                    const item = arrivalItems.find(i => i.id == id);
                    if (item) item.content_important = value;
                    const wrap = document.getElementById('important-wrap-' + id);
                    if (wrap) {{
                        const esc = (value || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
                        const color = item.content_color || '#1a2a6c';
                        wrap.innerHTML = value ? '<span class=\"content-display\" id=\"important-display-' + id + '\" style=\"color:' + color + '; font-weight:bold;\">★ ' + esc + '</span>' : '';
                    }}
                }}
            }});
        }}

        function saveArrivalStyle(id) {{
            const color = document.getElementById('edit-color-' + id).value;
            fetch('/api/arrival/update', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{ id: id, content_color: color }})
            }}).then(r => r.json()).then(res => {{
                if (res.status === 'success') {{
                    const item = arrivalItems.find(i => i.id == id);
                    if (item) item.content_color = color;
                    const displayEl = document.getElementById('content-display-' + id);
                    const importantEl = document.getElementById('important-display-' + id);
                    if (displayEl) displayEl.style.color = color;
                    if (importantEl) importantEl.style.color = color;
                }}
            }});
        }}

        function deleteArrivalItem(id) {{
            if (!confirm('이 항목을 삭제할까요?')) return;
            fetch('/api/arrival/delete/' + id, {{ method: 'POST' }}).then(r => r.json()).then(res => {{
                if (res.status === 'success') {{
                    arrivalItems = arrivalItems.filter(i => i.id != id);
                    renderArrivalList();
                }}
            }});
        }}

        renderArrivalList();
    </script>"""
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys="[]")

@app.route('/api/arrival/add', methods=['POST'])
@login_required
def arrival_add():
    d = request.json or {}
    target_time = d.get('target_time') or None
    content = d.get('content') or ''
    content_important = d.get('content_important') or ''
    content_color = d.get('content_color') or '#1a2a6c'
    content_font = d.get('content_font') or 'Malgun Gothic'
    content_font_size = d.get('content_font_size') or '16px'
    conn = sqlite3.connect('ledger.db', timeout=15)
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(MAX(order_idx), -1) + 1 FROM arrival_status")
    next_idx = cursor.fetchone()[0]
    cursor.execute("INSERT INTO arrival_status (target_time, content, content_important, content_color, content_font, content_font_size, order_idx) VALUES (?, ?, ?, ?, ?, ?, ?)", (target_time, content, content_important, content_color, content_font, content_font_size, next_idx))
    rid = cursor.lastrowid
    conn.commit(); conn.close()
    return jsonify({"status": "success", "id": rid})

@app.route('/api/arrival/update', methods=['POST'])
@login_required
def arrival_update():
    d = request.json or {}
    try:
        nid = int(d.get('id', 0))
        if nid <= 0:
            return jsonify({"status": "error", "message": "invalid id"}), 400
    except (ValueError, TypeError):
        return jsonify({"status": "error", "message": "invalid id"}), 400
    conn = sqlite3.connect('ledger.db', timeout=15)
    cursor = conn.cursor()
    cursor.execute("SELECT content, content_important, content_color, content_font, content_font_size, target_time FROM arrival_status WHERE id=?", (nid,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return jsonify({"status": "error", "message": "not found"}), 404
    content, content_important, content_color, content_font, content_font_size, target_time = row[0] or '', row[1] or '', row[2] or '#1a2a6c', row[3] or 'Malgun Gothic', row[4] or '16px', row[5]
    if 'content' in d:
        content = d.get('content', '')
    if 'content_important' in d:
        content_important = d.get('content_important', '')
    if 'content_color' in d:
        content_color = d.get('content_color', '#1a2a6c')
    if 'content_font' in d:
        content_font = d.get('content_font', 'Malgun Gothic')
    if 'content_font_size' in d:
        content_font_size = d.get('content_font_size', '16px')
    if 'target_time' in d:
        target_time = d.get('target_time')
    conn.execute("UPDATE arrival_status SET content=?, content_important=?, content_color=?, content_font=?, content_font_size=?, target_time=? WHERE id=?", (content, content_important, content_color, content_font, content_font_size, target_time, nid))
    conn.commit(); conn.close()
    return jsonify({"status": "success"})

@app.route('/api/arrival/delete/<int:id>', methods=['POST'])
@login_required
def arrival_delete(id):
    conn = sqlite3.connect('ledger.db', timeout=15); conn.execute("DELETE FROM arrival_status WHERE id=?", (id,)); conn.commit(); conn.close()
    return jsonify({"status": "success"})

@app.route('/api/drivers_excel')
@login_required
def drivers_excel():
    """기사관리 전체 목록 엑셀 다운로드 (업로드 양식과 동일)"""
    conn = sqlite3.connect('ledger.db', timeout=15)
    df = pd.read_sql("SELECT * FROM drivers ORDER BY 기사명, 차량번호", conn)
    conn.close()
    df = df.fillna('')
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        df.to_excel(w, index=False)
    out.seek(0)
    fname = f"기사관리_전체목록_{now_kst().strftime('%Y%m%d_%H%M')}.xlsx"
    return send_file(out, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=fname)


@app.route('/manage_drivers', methods=['GET', 'POST'])
@login_required 
def manage_drivers():
    global drivers_db
    load_db_to_mem()  # DB 삭제 후에도 현재 DB 기준으로 목록 표시
    err_msg = ""
    if request.method == 'POST' and 'file' in request.files:
        file = request.files['file']
        if file.filename != '':
            conn_up = None
            try:
                if file.filename.lower().endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(file, engine='openpyxl')
                else:
                    df = pd.read_csv(io.StringIO(file.stream.read().decode("utf-8-sig")))
                df = df.fillna('').astype(str)
                conn_up = sqlite3.connect('ledger.db', timeout=15)
                # 기존 데이터 유지: 업로드 파일은 추가·수정만 반영 (기사명+차량번호 기준 병합)
                try:
                    existing = pd.read_sql("SELECT * FROM drivers", conn_up)
                except Exception:
                    existing = pd.DataFrame()
                if len(existing) == 0:
                    merge_df = df
                else:
                    k1, k2 = '기사명', '차량번호'
                    if k1 not in df.columns or k2 not in df.columns:
                        merge_df = df
                    else:
                        existing[k1] = existing[k1].astype(str).str.strip()
                        existing[k2] = existing[k2].astype(str).str.strip()
                        df[k1] = df[k1].astype(str).str.strip()
                        df[k2] = df[k2].astype(str).str.strip()
                        by_key = {}
                        for _, r in existing.iterrows():
                            by_key[(r[k1], r[k2])] = r.to_dict()
                        for _, r in df.iterrows():
                            key = ((r.get(k1) or '').strip(), (r.get(k2) or '').strip())
                            if key == ('', ''):
                                continue
                            row_dict = {c: r.get(c, '') if c in r else by_key.get(key, {}).get(c, '') for c in existing.columns}
                            by_key[key] = row_dict
                        merge_df = pd.DataFrame(list(by_key.values()), columns=existing.columns)
                if 'id' in merge_df.columns:
                    merge_df = merge_df.drop(columns=['id'], errors='ignore')
                merge_df.to_sql('drivers', conn_up, if_exists='replace', index=False)
                conn_up.commit()
                load_db_to_mem()
            except Exception as e:
                err_msg = f"<p style='color:red; margin-bottom:15px;'>업로드 오류: {str(e)}<br>엑셀(.xlsx, .xls) 또는 CSV 파일만 업로드 가능합니다.</p>"
            finally:
                if conn_up is not None:
                    conn_up.close()
    
    DISPLAY_DRIVER_COLS = ["기사명", "차량번호", "연락처", "은행명", "계좌번호", "예금주", "사업자번호", "사업자", "개인/고정", "메모"]
    def _driver_col_label(c): return "기사 비고" if c == "메모" else c
    q_search = (request.args.get('q') or '').strip()
    if q_search:
        q_lower = q_search.lower()
        drivers_filtered = [r for r in drivers_db if any(q_lower in str(r.get(c, '')).lower() for c in DISPLAY_DRIVER_COLS)]
    else:
        drivers_filtered = drivers_db
    drivers_filtered = sorted(drivers_filtered, key=lambda r: (str(r.get('기사명') or '')).strip())
    total_full = len(drivers_db)
    total_drivers = len(drivers_filtered)
    page = max(1, safe_int(request.args.get('page'), 1))
    per_page_arg = safe_int(request.args.get('per_page'), 50)
    per_page = per_page_arg if per_page_arg in (20, 50, 100) else 50
    total_pages = max(1, (total_drivers + per_page - 1) // per_page)
    page = min(page, total_pages)
    start_idx = (page - 1) * per_page
    page_drivers = drivers_filtered[start_idx:start_idx + per_page]
    q_enc = quote(q_search, safe='') if q_search else ''
    q_param = f'&q={q_enc}' if q_enc else ''
    _block = 10
    _block_idx = (page - 1) // _block
    _start = _block_idx * _block + 1
    _end = min(_block_idx * _block + _block, total_pages)
    _parts = []
    if _block_idx > 0:
        _parts.append(f'<a href="/manage_drivers?page={(_block_idx - 1) * _block + 1}&per_page={per_page}{q_param}" class="page-btn">이전</a>')
    for i in range(_start, _end + 1):
        _parts.append(f'<a href="/manage_drivers?page={i}&per_page={per_page}{q_param}" class="page-btn {"active" if i == page else ""}">{i}</a>')
    if _end < total_pages:
        _parts.append(f'<a href="/manage_drivers?page={_end + 1}&per_page={per_page}{q_param}" class="page-btn">다음</a>')
    pagination_driver_html = "".join(_parts)
    def _driver_row(r):
        did = r.get('id', '')
        search = ' '.join([str(r.get(c, '')).lower() for c in DISPLAY_DRIVER_COLS])
        btns = f'<td style="white-space:nowrap;"><button type="button" class="btn-edit" onclick="editDriver({did})" style="padding:4px 8px; font-size:11px; margin-right:4px;">수정</button><button type="button" class="btn-status bg-red" onclick="deleteDriver({did})" style="padding:4px 8px; font-size:11px;">삭제</button></td>'
        cells = ''.join([f'<td>{r.get(c, "")}</td>' for c in DISPLAY_DRIVER_COLS])
        return f'<tr class="filter-row" data-id="{did}" data-search="{search}">{btns}{cells}</tr>'
    rows_html = "".join([_driver_row(r) for r in page_drivers])
    content = f"""<div class="section"><h2>🚚 기사 관리 (은행/계좌 정보)</h2>
    {err_msg}
    <div style="margin-bottom:15px; display:flex; align-items:center; gap:12px; flex-wrap:wrap;">
        <a href="/api/drivers_excel" class="btn-status bg-green" style="text-decoration:none; padding:6px 12px;">전체 목록 다운로드</a>
        <form method="post" enctype="multipart/form-data" style="display:inline;">
            <input type="file" name="file" accept=".xlsx,.xls,.csv"> <button type="submit" class="btn-save">엑셀 업로드</button>
        </form>
        <span style="font-size:12px; color:#666;">다운로드한 엑셀 양식에 맞춰 수정 후 업로드하면 반영됩니다.</span>
    </div>
    <div style="margin-bottom:12px; display:flex; align-items:center; gap:12px; flex-wrap:wrap;">
        <form method="get" action="/manage_drivers" style="display:inline-flex; align-items:center; gap:8px;">
            <input type="hidden" name="per_page" value="{per_page}">
            <input type="text" name="q" id="driverFilter" value="{q_search.replace("&", "&amp;").replace('"', "&quot;").replace("<", "&lt;")}" placeholder="기사명, 차량번호, 연락처 등 검색 (전체 DB 검색)" style="width:300px; padding:8px 12px; border:1px solid #cbd5e1; border-radius:6px;">
            <button type="submit" class="btn-edit" style="padding:8px 14px;">검색</button>
        </form>
        <span style="margin-left:8px;">출력</span>
        <select onchange="var q=document.getElementById('driverFilter').value; location.href='/manage_drivers?page=1&per_page='+this.value+(q ? '&q='+encodeURIComponent(q) : '')" style="padding:6px 10px; border:1px solid #d0d7de; border-radius:4px; font-size:13px;">
            <option value="20" {"selected" if per_page == 20 else ""}>20</option>
            <option value="50" {"selected" if per_page == 50 else ""}>50</option>
            <option value="100" {"selected" if per_page == 100 else ""}>100</option>
        </select>
        <span style="font-size:12px; color:#666;">개씩</span>
        <span style="font-size:12px; color:#64748b;">{"검색 결과 " + str(total_drivers) + "명 (전체 " + str(total_full) + "명)" if q_search else "(총 " + str(total_full) + "명)"}</span>
    </div>
    <div class="scroll-top" id="driverScrollTop"><table><thead><tr><th>관리</th>{"".join([f"<th>{_driver_col_label(c)}</th>" for c in DISPLAY_DRIVER_COLS])}</tr></thead><tbody><tr><td>-</td>{"".join(["<td>-</td>" for _ in DISPLAY_DRIVER_COLS])}</tr></tbody></table></div>
    <div class="scroll-x" id="driverScroll"><table><thead><tr><th>관리</th>{"".join([f"<th>{_driver_col_label(c)}</th>" for c in DISPLAY_DRIVER_COLS])}</tr></thead><tbody id="driverTableBody">{rows_html}</tbody></table></div>
    <div class="scroll-top" id="driverScrollBottom" style="margin-top:4px;"><table><thead><tr><th>관리</th>{"".join([f"<th>{_driver_col_label(c)}</th>" for c in DISPLAY_DRIVER_COLS])}</tr></thead><tbody><tr><td>-</td>{"".join(["<td>-</td>" for _ in DISPLAY_DRIVER_COLS])}</tr></tbody></table></div>
    <div class="pagination" style="margin-top:12px;">{pagination_driver_html}</div>
    <div id="driverEditModal" style="display:none; position:fixed; z-index:9999; left:0; top:0; width:100%; height:100%; background:rgba(0,0,0,0.5);">
        <div style="background:white; max-width:600px; margin:40px auto; padding:24px; border-radius:10px; max-height:90vh; overflow-y:auto;">
            <h3 style="margin:0 0 20px 0;">기사 수정</h3>
            <form id="driverEditForm" onsubmit="event.preventDefault(); const id=this.dataset.editId; const d={{}}; this.querySelectorAll('[name]').forEach(inp=>d[inp.name]=inp.value); fetch('/api/update_driver/'+id, {{method:'POST', headers:{{'Content-Type':'application/json'}}, body:JSON.stringify(d)}}).then(r=>r.json()).then(res=>{{if(res.status==='success'){{document.getElementById('driverEditModal').style.display='none';location.reload();}}else alert(res.message);}});">
                {"".join([f'<div style="margin-bottom:10px;"><label style="display:block; font-size:12px; margin-bottom:4px;">{_driver_col_label(c)}</label><input type="text" name="{c}" style="width:100%; padding:8px; border:1px solid #ddd; border-radius:4px;"></div>' for c in DISPLAY_DRIVER_COLS])}
                <div style="margin-top:20px; display:flex; gap:10px;"><button type="submit" class="btn-save">저장</button><button type="button" class="btn" onclick="document.getElementById('driverEditModal').style.display='none'">취소</button></div>
            </form>
        </div>
    </div>
    <script>
    function editDriver(id) {{
        const row = document.querySelector('#driverTableBody tr[data-id="'+id+'"]');
        if(!row) return;
        const cells = row.querySelectorAll('td');
        const cols = {json.dumps(DISPLAY_DRIVER_COLS)};
        const form = document.getElementById('driverEditForm');
        if(form) {{ cols.forEach((c, i) => {{ const inp = form.querySelector('[name="'+c+'"]'); if(inp) inp.value = (cells[i+1] && cells[i+1].innerText) || ''; }}); form.dataset.editId = id; document.getElementById('driverEditModal').style.display = 'block'; }}
    }}
    function deleteDriver(id) {{ if(!confirm('이 기사를 삭제하시겠습니까?')) return; fetch('/api/delete_driver/' + id, {{method:'POST'}}).then(r=>r.json()).then(res=>{{if(res.status==='success')location.reload();else alert(res.message||'삭제 실패');}}).catch(()=>alert('삭제 중 오류')); }}
    (function() {{
        const topEl = document.getElementById('driverScrollTop');
        const mainEl = document.getElementById('driverScroll');
        const botEl = document.getElementById('driverScrollBottom');
        if (!topEl || !mainEl) return;
        function matchWidth() {{
            const mainTbl = mainEl.querySelector('table');
            const topTbl = topEl.querySelector('table');
            const botTbl = botEl ? botEl.querySelector('table') : null;
            if (mainTbl && topTbl) {{
                const mainRow = mainTbl.querySelector('thead tr') || mainTbl.querySelector('tr');
                const topRow = topTbl.querySelector('tr');
                const botRow = botTbl ? botTbl.querySelector('tr') : null;
                if (mainRow && topRow && mainRow.cells.length === topRow.cells.length) {{
                    for (let i = 0; i < mainRow.cells.length; i++) {{
                        const w = mainRow.cells[i].offsetWidth;
                        topRow.cells[i].style.width = w + 'px';
                        topRow.cells[i].style.minWidth = w + 'px';
                        if (botRow && botRow.cells[i]) {{ botRow.cells[i].style.width = w + 'px'; botRow.cells[i].style.minWidth = w + 'px'; }}
                    }}
                }}
            }}
            const w = mainEl.clientWidth;
            topEl.style.width = w + 'px';
            if (botEl) botEl.style.width = w + 'px';
        }}
        let syncing = false;
        function sync(src) {{ if(syncing) return; syncing = true; const left = src.scrollLeft; if(topEl.scrollLeft !== left) topEl.scrollLeft = left; if(mainEl.scrollLeft !== left) mainEl.scrollLeft = left; if(botEl && botEl.scrollLeft !== left) botEl.scrollLeft = left; requestAnimationFrame(()=>{{syncing=false;}}); }}
        topEl.addEventListener('scroll', () => sync(topEl));
        mainEl.addEventListener('scroll', () => sync(mainEl));
        if(botEl) botEl.addEventListener('scroll', () => sync(botEl));
        setTimeout(matchWidth, 80);
        window.addEventListener('resize', matchWidth);
    }})();
    </script></div>"""
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys="[]")

@app.route('/api/delete_driver/<int:driver_id>', methods=['POST', 'DELETE'])
@login_required
def api_delete_driver(driver_id):
    conn = sqlite3.connect('ledger.db', timeout=15)
    conn.execute("DELETE FROM drivers WHERE rowid = ?", (driver_id,))
    conn.commit(); conn.close()
    load_db_to_mem()
    return jsonify({"status": "success"})

@app.route('/api/update_driver/<int:driver_id>', methods=['POST'])
@login_required
def api_update_driver(driver_id):
    data = request.json or {}
    conn = sqlite3.connect('ledger.db', timeout=15)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(drivers)")
    cols = [r[1] for r in cursor.fetchall() if r[1] != 'id' and r[1] != 'rowid']
    updates = []
    vals = []
    for c in cols:
        if c in data:
            updates.append(f'[{c}] = ?')
            vals.append(str(data.get(c, '')))
    if not updates:
        conn.close()
        return jsonify({"status": "error", "message": "수정할 데이터 없음"}), 400
    vals.append(driver_id)
    cursor.execute(f"UPDATE drivers SET {', '.join(updates)} WHERE rowid = ?", vals)
    conn.commit(); conn.close()
    load_db_to_mem()
    return jsonify({"status": "success"})


@app.route("/download-all")
def download_all():
    return send_file("backup.tar.gz", as_attachment=True)


@app.route("/download-db")
@app.route("/api/download-db")  # 두 경로 모두 지원 (서버에 따라 다를 수 있음)
@login_required
def download_db():
    """배포 전 서버 DB 백업용 — ledger.db 다운로드 (로그인 필요)"""
    import os
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ledger.db")
    if not os.path.isfile(db_path):
        return jsonify({"status": "error", "message": "DB 파일이 없습니다."}), 404
    return send_file(db_path, as_attachment=True, download_name="ledger_backup.db")


# 배포 시 FLASK_DEBUG=0 설정. 개발 시 기본으로 수정 시 서버 자동 재시작(use_reloader)
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    use_debug = os.environ.get('FLASK_DEBUG', '1').lower() in ('1', 'true', 'on', 'yes')
    app.run(debug=use_debug, use_reloader=use_debug, host='0.0.0.0', port=port)