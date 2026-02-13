from flask import Flask, render_template_string, request, jsonify, send_file, session, redirect, url_for
from werkzeug.utils import secure_filename
import pandas as pd

# .env 파일 로드 (python-dotenv)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
import io
import json
import sqlite3
import os
from datetime import datetime, timedelta, timezone
from functools import wraps

# 한국시간(KST, UTC+9) 설정
KST = timezone(timedelta(hours=9))

def now_kst():
    """현재 한국시간 반환"""
    return datetime.now(KST)

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
from collections import defaultdict

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
    {"n": "결제처", "k": "pay_to"}, {"n": "업체명", "k": "client_name", "c": "client-search"},
    {"n": "담당자연락처", "k": "c_mgr_phone"}, {"n": "담당자", "k": "c_mgr_name"},
    {"n": "연락처", "k": "c_phone"}, {"n": "사업자번호", "k": "biz_num"},
    {"n": "사업장주소", "k": "biz_addr"}, {"n": "업종", "k": "biz_type1"},
    {"n": "업태", "k": "biz_type2"}, {"n": "메일주소", "k": "mail"},
    {"n": "도메인", "k": "domain"}, {"n": "사업자", "k": "biz_owner"},
    {"n": "결제참고사항", "k": "pay_memo"}, {"n": "결제예정일", "k": "pay_due_dt", "t": "date"},
    {"n": "장부이동내역", "k": "log_move"}, {"n": "입금일", "k": "in_dt", "t": "date"},
    {"n": "수수료", "k": "comm", "t": "number"}, {"n": "선착불", "k": "pre_post"},
    {"n": "업체운임", "k": "fee", "t": "number"}, {"n": "공급가액", "k": "sup_val", "t": "number"},
    {"n": "부가세", "k": "vat1", "t": "number"}, {"n": "합계", "k": "total1", "t": "number"},
    {"n": "입금자명", "k": "in_name"}, {"n": "월구분", "k": "month_val"},
    {"n": "계산서발행일", "k": "tax_dt", "t": "date"}, {"n": "발행사업자", "k": "tax_biz"},
    {"n": "폰", "k": "tax_phone"}, {"n": "계좌번호", "k": "bank_acc"},
    {"n": "연락처", "k": "tax_contact"}, {"n": "사업자번호", "k": "tax_biz_num"},
    {"n": "사업자", "k": "tax_biz_name"}, {"n": "지급일", "k": "out_dt", "t": "date"},
    {"n": "기사운임", "k": "fee_out", "t": "number"}, {"n": "부가세", "k": "vat2", "t": "number"},
    {"n": "합계", "k": "total2", "t": "number"}, {"n": "작성일자", "k": "write_dt", "t": "date"},
    {"n": "발행일", "k": "issue_dt", "t": "date"}, {"n": "계산서확인", "k": "tax_chk", "t": "text"},
    {"n": "발행사업자", "k": "tax_biz2"}, {"n": "순수입", "k": "net_profit", "t": "number"},
    {"n": "부가세", "k": "vat_final", "t": "number"},
    {"n": "계산서사진", "k": "tax_img", "t": "text"},
    {"n": "운송장사진", "k": "ship_img", "t": "text"},
    {"n": "기사은행명", "k": "d_bank_name"}, 
    {"n": "기사예금주", "k": "d_bank_owner"},
    {"n": "운송우편확인", "k": "is_mail_done", "t": "text"}
]

DRIVER_COLS = ["기사명", "차량번호", "연락처", "계좌번호", "사업자번호", "사업자", "개인/고정", "메모"]
CLIENT_COLS = ["사업자구분", "업체명", "발행구분", "사업자등록번호", "대표자명", "사업자주소", "업태", "종목", "메일주소", "담당자", "연락처", "결제특이사항", "비고"]

def init_db():
    conn = sqlite3.connect('ledger.db')
    cursor = conn.cursor()

    keys = [c['k'] for c in FULL_COLUMNS]
    cols_sql = ", ".join([f"'{k}' TEXT" for k in keys])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, {cols_sql})")

    cursor.execute("PRAGMA table_info(ledger)")
    existing_ledger_cols = [info[1] for info in cursor.fetchall()]
    for k in keys:
        if k not in existing_ledger_cols:
            try: cursor.execute(f"ALTER TABLE ledger ADD COLUMN '{k}' TEXT")
            except: pass

    # 기사 테이블 컬럼 보강
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS drivers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            '기사명' TEXT, '차량번호' TEXT, '연락처' TEXT, '계좌번호' TEXT,
            '사업자번호' TEXT, '사업자' TEXT, '개인/고정' TEXT, '메모' TEXT,
            '은행명' TEXT, '예금주' TEXT
        )
    """)
    # (이하 생략 - 기존 activity_logs, clients 유지)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS activity_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        action TEXT,          -- 등록, 수정 등 행위
        target_id INTEGER,    -- 대상 장부 ID
        details TEXT          -- 변경 내용 요약
    )
    """)


    conn.commit()
    conn.close()

init_db()
drivers_db = []; clients_db = []

def load_db_to_mem():
    global drivers_db, clients_db
    conn = sqlite3.connect('ledger.db')
    drivers_db = pd.read_sql("SELECT * FROM drivers", conn).fillna('').to_dict('records')
    clients_db = pd.read_sql("SELECT * FROM clients", conn).fillna('').to_dict('records')
    conn.close()

load_db_to_mem()

BASE_HTML = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <title>sm logitek</title>
    <script src="https://cdn.jsdelivr.net/npm/html2canvas@1.4.1/dist/html2canvas.min.js"></script>
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
        table { border-collapse: collapse; width: 100%; white-space: nowrap; font-size: 12px; }
        th, td { border: 1px solid #dee2e6; padding: 6px 8px; text-align: center; }
        th { background: #f0f3f7; position: sticky; top: 0; z-index: 5; font-weight: 600; color: #374151; }
        input[type="text"], input[type="number"], input[type="date"], input[type="datetime-local"] { width: 110px; border: 1px solid #d0d7de; padding: 6px 8px; font-size: 12px; border-radius: 4px; box-sizing: border-box; }
        input:focus { outline: none; border-color: #1a2a6c; box-shadow: 0 0 0 2px rgba(26,42,108,0.15); }
        /* 업체 입력란 - 연한 파랑 */
        input.client-search { background: #e8f4fc; border-color: #1976d2; }
        input.client-search::placeholder { color: #1565c0; }
        /* 기사 입력란 - 연한 초록 */
        input.driver-search { background: #e6f4ea; border-color: #2e7d32; }
        input.driver-search::placeholder { color: #1b5e20; }
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

  <script>
    let drivers = {{ drivers_json | safe }};
    let clients = {{ clients_json | safe }};
    let columnKeys = {{ col_keys | safe }};
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

    // 2. 실시간 입력 감지 및 팝업 표시 (좌표 계산 및 데이터 전달 수정)
    document.addEventListener('input', function(e) {
        if(e.target.classList.contains('driver-search') || e.target.classList.contains('client-search')) {
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
                const rect = e.target.getBoundingClientRect();
                popup.style.display = 'block'; 
                popup.style.width = rect.width + 'px';
                // 좌표 보정: 스크롤 위치를 포함하여 입력창 바로 아래에 배치
                popup.style.top = (window.scrollY + rect.bottom) + 'px'; 
                popup.style.left = (window.scrollX + rect.left) + 'px'; 
                
                popup.innerHTML = filtered.map(item => {
                    const label = isDriver ? `${item.기사명} [${item.차량번호 || ''}]` : (item.업체명 || '');
                    // 중요: 데이터를 안전하게 문자열화 (따옴표 오류 방지)
                    const itemData = JSON.stringify(item).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
                    return `<div class="search-item" onclick="fillData('${itemData}', '${isDriver ? 'driver' : 'client'}', '${e.target.id}')">${label}</div>`;
                }).join('');
            } else { popup.style.display = 'none'; }
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

            if(!isQuick) { // 상세 장부 입력창일 때만 추가 정보 자동 기입
                if(document.getElementById('d_phone')) document.getElementById('d_phone').value = item.연락처 || '';
                if(document.getElementById('bank_acc')) document.getElementById('bank_acc').value = item.계좌번호 || '';
                if(document.getElementById('d_bank_name')) document.getElementById('d_bank_name').value = item.은행명 || '';
                if(document.getElementById('d_bank_owner')) document.getElementById('d_bank_owner').value = item.예금주 || item.사업자 || '';
            }
        } else {
            const clientField = document.getElementById(prefix + 'client_name');
            if(clientField) clientField.value = item.업체명 || '';

            if(!isQuick) { // 상세 장부 입력창일 때만 추가 정보 자동 기입
                if(document.getElementById('c_phone')) document.getElementById('c_phone').value = item.연락처 || '';
                if(document.getElementById('biz_num')) document.getElementById('biz_num').value = item.사업자등록번호 || '';
                if(document.getElementById('biz_addr')) document.getElementById('biz_addr').value = item.사업자주소 || '';
                if(document.getElementById('biz_owner')) document.getElementById('biz_owner').value = item.대표자명 || '';
            }
        }
        document.getElementById('search-popup').style.display = 'none';
    };

        function saveLedger(formId) {
            const form = document.getElementById(formId);
            const formData = new FormData(form);
            const data = {};
            const isQuick = (formId === 'quickOrderForm');
            formData.forEach((v, k) => {
                const key = isQuick ? k.replace('q_', '') : k;
                const input = form.elements[k]; 
                if (input && input.type === 'checkbox') data[key] = input.checked ? "✅" : "❌";
                else data[key] = v;
            });
            if(isQuick) {
                const client = clients.find(c => c.업체명 === data.client_name);
                if(client) {
                    data.c_phone = client.연락처 || ''; data.biz_num = client.사업자등록번호 || ''; 
                    data.biz_addr = client.사업자주소 || ''; data.biz_owner = client.대표자명 || '';
                }
                const driver = drivers.find(d => d.기사명 === data.d_name && d.차량번호 === data.c_num);
                if(driver) {
                    data.d_phone = driver.연락처 || ''; data.bank_acc = driver.계좌번호 || ''; 
                    data.tax_biz_num = driver.사업자번호 || ''; data.tax_biz_name = driver.사업자 || '';
                }
                data.order_dt = data.order_dt || (typeof todayKST === 'function' ? todayKST() : new Date().toISOString().split('T')[0]);
                data.dispatch_dt = data.dispatch_dt || (typeof nowKSTLocal === 'function' ? nowKSTLocal() : new Date().toISOString().slice(0,16));
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
                } else { alert('저장 중 오류가 발생했습니다.'); }
            });
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
    const start = document.getElementById('startDate').value;
    const end = document.getElementById('endDate').value;
    
    // 날짜 쿼리 스트링 추가
    fetch(`/api/get_ledger?page=${page}&start=${start}&end=${end}`)
        .then(r => r.json())
        .then(res => {
            lastLedgerData = res.data;
            renderTableRows(res.data);
            if (typeof renderPagination === 'function') renderPagination(res.total_pages, res.current_page, 'ledger');
        });
}

        function renderTableRows(data) {
    const body = document.getElementById('ledgerBody');
    if (!body) return;
    body.innerHTML = data.map(item => `
        <tr class="draggable" draggable="true" data-id="${item.id}">
            <td style="white-space:nowrap;">
                <button class="btn-edit" onclick="editEntry(${item.id})">수정</button>
            </td>
            ${columnKeys.map(key => {
                let val = item[key] || '';
                // 업체운임: 수수료·선착불에 금액이 있으면 합산 표기, 없으면 업체운임만
                if(key === 'fee') {
                    let feeNum = parseFloat(item.fee) || 0;
                    let commNum = parseFloat(item.comm) || 0;
                    let preNum = parseFloat(item.pre_post) || 0;
                    if (commNum || preNum) {
                        val = (feeNum + commNum + preNum).toLocaleString();
                    } else {
                        val = val ? (isNaN(parseFloat(val)) ? val : parseFloat(val).toLocaleString()) : '';
                    }
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
                    return `<td>${btns}</div></td>`;
                }
                return `<td>${val}</td>`;
            }).join('')}
        </tr>
    `).join('');
    initDraggable();
}
        function renderPagination(totalPages, currentPage, type) {
            const container = document.getElementById(type + 'Pagination');
            if (!container) return;
            let html = "";
            const urlParams = new URLSearchParams(window.location.search);
            for (let i = 1; i <= totalPages; i++) {
                urlParams.set('page', i);
                const activeClass = i == currentPage ? "active" : "";
                html += `<a href="?${urlParams.toString()}" class="page-btn ${activeClass}">${i}</a>`;
            }
            container.innerHTML = html;
        }

        function initDraggable() {
            const body = document.getElementById('ledgerBody');
            if(!body) return;
            const draggables = document.querySelectorAll('.draggable');
            draggables.forEach(draggable => {
                draggable.addEventListener('dragstart', () => draggable.classList.add('dragging'));
                draggable.addEventListener('dragend', () => draggable.classList.remove('dragging'));
            });
            body.addEventListener('dragover', e => {
                e.preventDefault();
                const dragging = document.querySelector('.dragging');
                const afterElement = getDragAfterElement(body, e.clientY);
                if (afterElement == null) body.appendChild(dragging);
                else body.insertBefore(dragging, afterElement);
            });
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

        function filterLedger() {
            const query = document.getElementById('ledgerSearch').value.toLowerCase();
            const filtered = lastLedgerData.filter(item => Object.values(item).some(val => String(val).toLowerCase().includes(query)));
            renderTableRows(filtered);
        }

        window.editEntry = function(id) {
            const item = lastLedgerData.find(d => d.id === id);
            if (!item) return;
            currentEditId = id; 
            document.querySelector('#ledgerForm .btn-save').innerText = '장부 내용 수정 완료';
            columnKeys.forEach(key => {
                const input = document.querySelector(`#ledgerForm [name="${key}"]`);
                if (input) { if (input.type === 'checkbox') input.checked = (item[key] === "✅"); else input.value = item[key] || ''; }
            });
            window.scrollTo(0, document.querySelector('#ledgerForm').offsetTop - 50);
        };

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
            const urlParams = new URLSearchParams(window.location.search);
            const editId = urlParams.get('edit_id');
            if (editId) {
                fetch('/api/get_ledger_row/' + editId)
                    .then(r => r.json())
                    .then(row => {
                        if (row.error) { loadLedgerList(); return; }
                        lastLedgerData = [row];
                        if (typeof editEntry === 'function') editEntry(parseInt(editId));
                        const form = document.querySelector('#ledgerForm');
                        if (form) form.scrollIntoView({ behavior: 'smooth', block: 'start' });
                        loadLedgerList();
                    })
                    .catch(() => loadLedgerList());
            } else {
                loadLedgerList();
            }
        }
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
            return redirect(url_for('index'))
        else:
            error = "아이디 또는 비밀번호가 올바르지 않습니다."
    return render_template_string(LOGIN_HTML, error=error)

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('login'))

@app.route('/')
@login_required 
def index():
    col_keys_json = json.dumps([c['k'] for c in FULL_COLUMNS])
    content = f"""
        <div class="section" style="background:#fffbf0; border:2px solid #fbc02d;">
        <h3>⚡ 빠른 오더 입력 (초성 검색 가능)</h3>
        <p style="margin:0 0 10px 0; font-size:11px; color:#666;"><span style="background:#e8f4fc; padding:2px 6px; border-radius:3px;">파란 배경</span> = 업체 입력란 &nbsp; <span style="background:#e6f4ea; padding:2px 6px; border-radius:3px;">초록 배경</span> = 기사 입력란</p>
        <form id="quickOrderForm">
            <div class="quick-order-grid">
                <div><label>업체명</label><input type="text" name="q_client_name" id="q_client_name" class="client-search" placeholder="초성(예:ㅇㅅㅁ)" autocomplete="off"></div>
                <div><label>노선</label><input type="text" name="q_route" id="q_route"></div>
                <div><label>업체운임</label><input type="number" name="q_fee" id="q_fee"></div>
                <div><label>기사명</label><input type="text" name="q_d_name" id="q_d_name" class="driver-search" placeholder="기사초성" autocomplete="off"></div>
                <div><label>차량번호</label><input type="text" name="q_c_num" id="q_c_num" class="driver-search" autocomplete="off"></div>
                <div><label>기사운임</label><input type="number" name="q_fee_out" id="q_fee_out"></div>
            </div>
            <div style="text-align:right;"><button type="button" class="btn-save" style="background:#e67e22;" onclick="saveLedger('quickOrderForm')">장부 즉시 등록</button></div>
        </form>
    </div>
    <div class="section">
        <h3>1. 장부 상세 데이터 입력</h3>
        <p style="margin:0 0 10px 0; font-size:11px; color:#666;"><span style="background:#e8f4fc; padding:2px 6px; border-radius:3px;">파란 배경</span> = 업체 관련 &nbsp; <span style="background:#e6f4ea; padding:2px 6px; border-radius:3px;">초록 배경</span> = 기사 관련</p>
        <form id="ledgerForm">
            <div class="scroll-x">
                <table>
                    <thead>
                        <tr><th>관리</th>{"".join([f"<th>{c['n']}</th>" for c in FULL_COLUMNS])}</tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>-</td>
                            # 모든 input에 id 속성을 추가하여 자바스크립트가 데이터를 채울 수 있게 합니다.
                            {"".join([f"<td><input type='{c.get('t', 'text')}' name='{c['k']}' id='{c['k']}' class='{c.get('c', '')}' autocomplete='off'></td>" for c in FULL_COLUMNS])}
                        </tr>
                    </tbody>
                </table>
            </div>
            <div style="text-align:right; margin-top:15px;"><button type="button" class="btn-save" onclick="saveLedger('ledgerForm')">상세 저장 및 추가 ↓</button></div>
        </form>
    </div>
    <div class="section">
        <h3>2. 장부 목록 및 오더 검색</h3>
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
        <input type="text" id="ledgerSearch" class="search-bar" placeholder="기사명, 업체명, 노선 등 검색..." onkeyup="filterLedger()">
        <div class="scroll-x"><table><thead><tr><th>관리</th>{"".join([f"<th>{c['n']}</th>" for c in FULL_COLUMNS])}</tr></thead><tbody id="ledgerBody"></tbody></table></div>
        <div id="ledgerPagination" class="pagination"></div>
    </div>
    
    <div id="logModal" style="display:none; position:fixed; z-index:9999; left:0; top:0; width:100%; height:100%; background:rgba(0,0,0,0.6);">
        <div style="background:white; width:95%; max-width:1200px; margin:30px auto; padding:25px; border-radius:10px; box-shadow:0 5px 25px rgba(0,0,0,0.4);">
            <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:3px solid #1a2a6c; padding-bottom:12px; margin-bottom:15px;">
                <h3 style="margin:0; color:#1a2a6c; font-size:18px;">📋 오더 수정 상세 이력 (시간순)</h3>
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
    """
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys=col_keys_json)
@app.route('/settlement')
@login_required 
def settlement():
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    # 시작일, 종료일 검색 값을 URL에서 가져옵니다.
    q_status = request.args.get('status', ''); q_name = request.args.get('name', '')
    q_start = request.args.get('start', ''); q_end = request.args.get('end', '')
    page = max(1, safe_int(request.args.get('page'), 1))
    per_page = 50
    
    rows = conn.execute("SELECT * FROM ledger ORDER BY dispatch_dt DESC").fetchall(); conn.close()
    
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
                except: pass
            
            is_due_passed = False
            if pay_due_dt:
                try:
                    p_due = datetime.strptime(pay_due_dt, "%Y-%m-%d")
                    if today.date() > p_due.date(): is_due_passed = True
                except: pass
            
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
        
        # 이름 필터
        if q_name and (q_name not in str(row['client_name'] or '') and q_name not in str(row['d_name'] or '')): continue

        # 상태 필터

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

    total_pages = (len(filtered_rows) + per_page - 1) // per_page
    start = (page - 1) * per_page
    end = start + per_page
    page_data = filtered_rows[start:end]

    table_rows = ""
    for row in page_data:
        # 토글 변수 설정 (데이터가 있으면 공백으로 보내서 미수/미지급 처리)
        in_dt_toggle = f"'{today.strftime('%Y-%m-%d')}'" if not row['in_dt'] else "''"
        out_dt_toggle = f"'{today.strftime('%Y-%m-%d')}'" if not row['out_dt'] else "''"

        misu_btn = f'<button class="btn-status {row["m_cl"]}" onclick="changeStatus({row["id"]}, \'in_dt\', {in_dt_toggle})">{row["m_st"]}</button>'
        tax_issued_btn = f'<button class="btn-status {"bg-green" if row["tax_chk"]=="발행완료" else "bg-orange"}" onclick="changeStatus({row["id"]}, \'tax_chk\', \'발행완료\')">{row["tax_chk"] if row["tax_chk"] else "미발행"}</button>'
        pay_btn = f'<button class="btn-status {row["p_cl"]}" onclick="changeStatus({row["id"]}, \'out_dt\', {out_dt_toggle})">{row["p_st"]}</button>'
        
        mail_val = row.get('is_mail_done', '미확인')
        mail_color = "bg-green" if mail_val == "확인완료" else "bg-orange"
        mail_btn = f'<button class="btn-status {mail_color}" onclick="changeStatus({row["id"]}, \'is_mail_done\', \'확인완료\')">{mail_val if mail_val else "미확인"}</button>'

        def make_direct_links(ledger_id, img_type, raw_paths):
            paths = [p.strip() for p in (raw_paths or "").split(',')] if raw_paths else []
            links_html = '<div style="display:flex; gap:3px; justify-content:center;">'
            for i in range(1, 6):
                has_file = len(paths) >= i and paths[i-1].startswith('static')
                css_class = "link-btn has-file" if has_file else "link-btn"
                links_html += f'<a href="/upload_evidence/{ledger_id}?type={img_type}&seq={i}" target="_blank" class="{css_class}">{i}</a>'
            links_html += '</div>'
            return links_html

        table_rows += f"""<tr>
            <td style="white-space:nowrap;">
                <a href="/?edit_id={row['id']}" class="btn-edit" style="display:inline-block; margin-right:4px; text-decoration:none;">장부입력</a>
                <button class="btn-log" onclick="viewOrderLog({row['id']})" style="background:#6c757d; color:white; border:none; padding:2px 5px; cursor:pointer; font-size:11px; border-radius:3px;">로그</button>
            </td>
            <td>{row['client_name']}</td><td>{tax_issued_btn}</td><td>{row['order_dt']}</td><td>{row['route']}</td><td>{row['d_name']}</td><td>{row['c_num']}</td><td>{row['fee']}</td><td>{misu_btn}</td><td>{row['fee_out']}</td><td>{pay_btn}</td><td>{mail_btn}</td><td>{make_direct_links(row['id'], 'tax', row['tax_img'])}</td><td>{make_direct_links(row['id'], 'ship', row['ship_img'])}</td></tr>"""
    
    pagination_html = "".join([f'<a href="/settlement?status={q_status}&name={q_name}&start={q_start}&end={q_end}&page={i}" class="page-btn {"active" if i==page else ""}">{i}</a>' for i in range(1, total_pages+1)])

    content = f"""<div class="section"><h2>정산 관리 (기간 및 실시간 필터)</h2>
    <form class="filter-box" method="get" style="display: flex; gap: 10px; align-items: center; flex-wrap: wrap;">
        <strong>📅 오더일:</strong>
        <input type="date" name="start" value="{q_start}"> ~ 
        <input type="date" name="end" value="{q_end}">
        <strong>🔍 필터:</strong>
        <select name="status">
            <option value="">전체상태</option>
            <option value="misu_all" {'selected' if q_status=='misu_all' else ''}>미수금 전체</option>
            <option value="cond_misu" {'selected' if q_status=='cond_misu' else ''}>조건부미수</option>
            <option value="pay_all" {'selected' if q_status=='pay_all' else ''}>미지급 전체</option>
            <option value="cond_pay" {'selected' if q_status=='cond_pay' else ''}>조건부미지급</option>
            <option value="done_in" {'selected' if q_status=='done_in' else ''}>수금완료</option>
            <option value="done_out" {'selected' if q_status=='done_out' else ''}>지급완료</option>
        </select>
        <input type="text" name="name" value="{q_name}" placeholder="업체/기사 검색">
        <button type="submit" class="btn-save">조회</button>
        <button type="button" onclick="location.href='/settlement'" class="btn-status bg-gray">초기화</button>
    </form>
    <div style="margin: 15px 0;">
        <a href="/export_misu_info?status={q_status}&name={q_name}&start={q_start}&end={q_end}" class="btn-status bg-red" style="text-decoration:none;">미수금 업체정보 엑셀</a>
        <a href="/export_pay_info?status={q_status}&name={q_name}&start={q_start}&end={q_end}" class="btn-status bg-orange" style="text-decoration:none; margin-left:5px;">미지급 기사정보 엑셀</a>
        <a href="/export_tax_not_issued?status={q_status}&name={q_name}&start={q_start}&end={q_end}" class="btn-status bg-gray" style="text-decoration:none; margin-left:5px;">세금계산서 미발행 엑셀</a>
    </div>
    <div class="scroll-x"><table><thead><tr><th>로그</th><th>업체명</th><th>계산서</th><th>오더일</th><th>노선</th><th>기사명</th><th>차량번호</th><th>업체운임</th><th>수금상태</th><th>기사운임</th><th>지급상태</th><th>우편확인</th><th>기사계산서</th><th>운송장</th></tr></thead><tbody>{table_rows}</tbody></table></div>
    <div class="pagination">{pagination_html}</div></div>

    <div id="logModal" style="display:none; position:fixed; z-index:9999; left:0; top:0; width:100%; height:100%; background:rgba(0,0,0,0.6);">
        <div style="background:white; width:90%; max-width:800px; margin:50px auto; padding:20px; border-radius:10px; box-shadow:0 5px 15px rgba(0,0,0,0.3);">
            <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:2px solid #1a2a6c; padding-bottom:10px; margin-bottom:15px;">
                <h3 style="margin:0; color:#1a2a6c;">📋 오더 변경 이력</h3>
                <button onclick="closeLogModal()" style="background:none; border:none; font-size:24px; cursor:pointer;">&times;</button>
            </div>
            <div style="max-height:500px; overflow-y:auto;"><table style="width:100%; border-collapse:collapse; font-size:13px;"><thead><tr style="background:#f4f4f4;"><th style="padding:10px; border:1px solid #ddd; width:30%;">일시</th><th style="padding:10px; border:1px solid #ddd; width:15%;">작업</th><th style="padding:10px; border:1px solid #ddd;">상세내용</th></tr></thead><tbody id="logContent"></tbody></table></div>
            <div style="text-align:right; margin-top:15px;"><button onclick="closeLogModal()" style="padding:8px 20px; background:#6c757d; color:white; border:none; border-radius:5px; cursor:pointer;">닫기</button></div>
        </div>
    </div>
    """
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys="[]")
@app.route('/statistics')
@login_required 
def statistics():
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    # 1. 모든 필터 파라미터 정의
    q_start = request.args.get('start', '')
    q_end = request.args.get('end', '')
    q_client = request.args.get('client', '').strip()
    q_driver = request.args.get('driver', '').strip()
    q_status = request.args.get('status', '')
    
    rows = conn.execute("SELECT * FROM ledger").fetchall(); conn.close()
    filtered_rows = []
    
    # 기사관리(기사현황)에서 개인/고정="고정"인 차량번호 목록 (차량번호 기준 필터)
    fixed_c_nums = {str(d.get('차량번호', '')).strip() for d in drivers_db if str(d.get('개인/고정', '')).strip() == '고정'}
    fixed_c_nums.discard('')  # 빈 문자열 제외

    for row in rows:
        r = dict(row)
        order_dt = r.get('order_dt', '') or ""
        # 기간/업체/기사 필터
        if q_start and q_end and not (q_start <= order_dt <= q_end): continue
        if q_client and q_client not in str(r.get('client_name', '')): continue
        if q_driver and q_driver not in str(r.get('d_name', '')): continue

        # 정산 상태 판별 로직 복구
        in_dt = r.get('in_dt'); out_dt = r.get('out_dt')
        m_st = "수금완료" if in_dt else ("조건부미수" if not r.get('pre_post') and not r.get('pay_due_dt') else "미수")
        p_st = "지급완료" if out_dt else ("조건부미지급" if not in_dt else "미지급")
        # 고정 여부: 기사관리 차량번호 기준 (해당 차량번호의 장부 데이터만)
        c_num = str(r.get('c_num', '')).strip()
        d_type = "직영" if c_num in fixed_c_nums else "일반"

        # 세부 상태 필터
        if q_status:
            if q_status in ["미수", "조건부미수", "수금완료"] and q_status != m_st: continue
            if q_status in ["미지급", "조건부미지급", "지급완료"] and q_status != p_st: continue
            if q_status == "고정" and c_num not in fixed_c_nums: continue
            if q_status in ["직영", "일반"] and q_status != d_type: continue

        r['m_st'] = m_st; r['p_st'] = p_st; r['d_type'] = d_type
        filtered_rows.append(r)

    df = pd.DataFrame(filtered_rows)
    summary_monthly = ""; summary_daily = ""
    full_settlement_client = ""; full_settlement_driver = ""

    if not df.empty:
        df['fee'] = pd.to_numeric(df['fee'], errors='coerce').fillna(0)
        df['fee_out'] = pd.to_numeric(df['fee_out'], errors='coerce').fillna(0)
        
        # 월별 요약 테이블
        df['month'] = df['order_dt'].str[:7]
        m_grp = df.groupby('month').agg({'fee':'sum', 'fee_out':'sum', 'id':'count'}).sort_index(ascending=False)
        for month, v in m_grp.iterrows():
            summary_monthly += f"<tr><td>{month}</td><td>{v['id']}건</td><td>{int(v['fee']):,}</td><td>{int(v['fee_out']):,}</td><td>{int(v['fee']-v['fee_out']):,}</td></tr>"

        # 일별 실적 (최근 15일)
        d_grp = df.groupby('order_dt').agg({'fee':'sum', 'fee_out':'sum', 'id':'count'}).sort_index(ascending=False).head(15)
        for date, v in d_grp.iterrows():
            summary_daily += f"<tr><td>{date}</td><td>{v['id']}</td><td>{int(v['fee']):,}</td><td>{int(v['fee_out']):,}</td></tr>"

        # 업체 정산 데이터 조립: 업체별 "수신 [업체명] 정산서" 형식, 오더일|노선|업체운임|미수
        for client_name, grp in df.sort_values(by=['client_name', 'order_dt'], ascending=[True, False]).groupby('client_name'):
            cname = str(client_name or '').strip() or '(업체명 없음)'
            cname_attr = cname.replace('&', '&amp;').replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
            cname_display = cname.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            grp_fee_sum = int(grp['fee'].sum())
            full_settlement_client += f"""
            <div class="client-settle-card" data-client="{cname_attr}">
                <div class="client-settle-title">수신 「{cname_display}」 정산서</div>
                <table class="client-settle-table"><thead><tr><th>오더일</th><th>노선</th><th>업체운임</th><th>미수</th></tr></thead><tbody>"""
            for _, r in grp.iterrows():
                fee_val = int(float(r.get('fee', 0) or 0))
                full_settlement_client += f"<tr><td>{r['order_dt']}</td><td>{r.get('route','')}</td><td style='text-align:right;'>{fee_val:,}</td><td>{r['m_st']}</td></tr>"
            full_settlement_client += f"<tr class='client-sum-row'><td colspan='2'>합계</td><td style='text-align:right; font-weight:bold;'>{grp_fee_sum:,}</td><td>-</td></tr></tbody></table></div>"

        # 기사 정산 데이터 조립: 기사명, 업체명, 입금일, 오더일, 노선, 기사운임, 지급상태 + 기사별 소계 + 총합계
        driver_grand_total = 0
        for name, group in df.groupby('d_name'):
            for _, r in group.iterrows():
                in_dt = r.get('in_dt') or ''
                full_settlement_driver += f"<tr><td>{name}</td><td>{r.get('client_name', '')}</td><td>{in_dt}</td><td>{r['order_dt']}</td><td>{r['route']}</td><td style='text-align:right;'>{int(r['fee_out']):,}</td><td>{r['p_st']}</td></tr>"
            grp_sum = int(group['fee_out'].sum())
            driver_grand_total += grp_sum
            full_settlement_driver += f"<tr style='background:#e8f0e8; font-weight:bold;'><td colspan='5'>[{name}] 소계</td><td style='text-align:right;'>{grp_sum:,}</td><td>-</td></tr>"
        if not df.empty and full_settlement_driver:
            full_settlement_driver += f"<tr style='background:#1a2a6c; color:white; font-weight:bold; font-size:14px;'><td colspan='5'>총합계</td><td style='text-align:right;'>{driver_grand_total:,}</td><td>-</td></tr>"

    content = f"""
    <style>
        .summary-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 25px; }}
        .table-scroll {{ max-height: 350px; overflow-y: auto; background: white; border: 1px solid #ddd; border-radius: 5px; }}
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
        <h2 style="color:#1a2a6c; margin-bottom:20px; border-left:5px solid #1a2a6c; padding-left:10px;">📈 에스엠 로지텍 정산 센터</h2>
        
        <form method="get" style="background:#f8f9fa; padding:20px; border-radius:10px; display:flex; gap:12px; flex-wrap:wrap; align-items:center; border:1px solid #dee2e6;">
            <strong>📅 기간:</strong> <input type="date" name="start" value="{q_start}"> ~ <input type="date" name="end" value="{q_end}">
            <strong>🏢 업체:</strong> <input type="text" name="client" value="{q_client}" style="width:100px;">
            <strong>🚚 기사:</strong> <input type="text" name="driver" value="{q_driver}" style="width:100px;">
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
            <button type="submit" class="btn-save">데이터 조회</button>
            <button type="button" onclick="location.href='/export_stats'+window.location.search" class="btn-status bg-green">엑셀 다운로드</button>
        </form>

        <div class="summary-grid" style="margin-top:25px;">
            <div class="section"><h3>📅 월별 수익 요약</h3><div class="table-scroll"><table><thead><tr><th>연월</th><th>건수</th><th>매출</th><th>지출</th><th>수익</th></tr></thead><tbody>{summary_monthly}</tbody></table></div></div>
            <div class="section"><h3>📆 최근 일별 요약</h3><div class="table-scroll"><table><thead><tr><th>날짜</th><th>건수</th><th>매출</th><th>지출</th></tr></thead><tbody>{summary_daily}</tbody></table></div></div>
        </div>

        <div style="margin-top:30px;">
            <button class="tab-btn active" onclick="openSettleTab(event, 'clientZone')">🏢 업체별 정산 관리</button>
            <button class="tab-btn" onclick="openSettleTab(event, 'driverZone')">🚚 기사별 정산 관리</button>
        </div>

        <div id="clientZone" class="tab-content active">
            <div style="display:flex; justify-content:space-between; margin-bottom:15px; align-items:center;">
                <h4 style="margin:0;">🧾 업체별 상세 매출 및 수금 현황</h4>
                <button onclick="captureSettle('clientZone')" class="btn-status bg-orange">🖼️ 업체 정산서 이미지 저장</button>
            </div>
            <div class="table-scroll" id="raw_client"><div class="client-settle-sections">{full_settlement_client}</div><div class="settle-footer-msg">에스엠 로지텍 발신</div></div>
        </div>

        <div id="driverZone" class="tab-content">
            <div style="display:flex; justify-content:space-between; margin-bottom:15px; align-items:center;">
                <h4 style="margin:0;">🧾 기사별 상세 지출 및 지급 현황</h4>
                <button onclick="captureSettle('driverZone')" class="btn-status bg-orange">🖼️ 기사 정산서 이미지 저장</button>
            </div>
            <div class="table-scroll" id="raw_driver"><table><thead><tr style="background:#f2f2f2;"><th>기사명</th><th>업체명</th><th>입금일</th><th>오더일</th><th>노선</th><th>기사운임</th><th>지급상태</th></tr></thead><tbody>{full_settlement_driver}</tbody></table></div>
        </div>
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

        async function captureSettle(zoneId) {{
            const area = document.getElementById('printArea');
            const printContent = document.getElementById('printContent');
            const isDriver = (zoneId === 'driverZone');
            const targetId = isDriver ? 'raw_driver' : 'raw_client';
            const fileName = isDriver ? '기사정산서_' + new Date().getTime() + '.png' : '업체정산서_' + new Date().getTime() + '.png';
            const targetEl = document.getElementById(targetId);
            let bodyHtml;
            if (isDriver) {{
                bodyHtml = '<table border="1" style="width:100%; border-collapse:collapse; font-size:14px; text-align:center;"><thead><tr style="background:#f2f2f2;"><th>기사명</th><th>업체명</th><th>입금일</th><th>오더일</th><th>노선</th><th>기사운임</th><th>지급상태</th></tr></thead><tbody>' + (targetEl.querySelector('tbody').innerHTML) + '</tbody></table>';
                printContent.innerHTML = `
                    <div style="padding:40px; background:white; font-family: 'Malgun Gothic', sans-serif;">
                        <h1 style="text-align:center; font-size:32px; border-bottom:3px solid #000; padding-bottom:15px; margin-bottom:20px;">기사 정산서</h1>
                        <div style="text-align:right; margin-bottom:10px;">출력일: ${{new Date().toLocaleDateString('ko-KR', {{ timeZone: 'Asia/Seoul' }})}}</div>
                        ${{bodyHtml}}
                        <div style="text-align:right; margin-top:40px; font-weight:bold; font-size:24px;">에스엠 로지텍 (인)</div>
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
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys="[]")
@app.route('/export_custom_settlement')
@login_required 
def export_custom_settlement():
    t = request.args.get('type', 'client'); s = request.args.get('start',''); e = request.args.get('end','')
    c = request.args.get('client',''); d = request.args.get('driver',''); st = request.args.get('status', '')
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ledger").fetchall(); conn.close()
    filtered_data = []
    for row in rows:
        r = dict(row)
        in_dt = r['in_dt']; out_dt = r['out_dt']; p_due = r['pay_due_dt']; pre = r['pre_post']
        o_dt = r['order_dt'] or ""; t_img = r['tax_img'] or ""; s_img = r['ship_img'] or ""
        m_st = "조건부미수금" if not pre and not in_dt and not p_due else ("수금완료" if in_dt else "미수")
        p_st = "지급완료" if out_dt else ("미지급" if in_dt and any('static' in p for p in t_img.split(',')) and any('static' in p for p in s_img.split(',')) else "조건부미지급")
        if s and e and not (s <= o_dt <= e): continue
        if c and c not in str(r['client_name']): continue
        if d and d not in str(r['d_name']): continue
        if st == 'misu_all' and in_dt: continue
        if st == 'misu_only' and m_st != '미수': continue
        if st == 'cond_misu' and m_st != '조건부미수금': continue
        if st == 'pay_all' and out_dt: continue
        if st == 'pay_only' and p_st != '미지급': continue
        if st == 'cond_pay' and p_st != '조건부미지급': continue
        if st == 'done_in' and not in_dt: continue
        if st == 'done_out' and not out_dt: continue
        filtered_data.append(r)
    df = pd.DataFrame(filtered_data)
    if df.empty: return "데이터가 없습니다."
    group_col = 'client_name' if t == 'client' else 'd_name'
    amt_col = 'fee' if t == 'client' else 'fee_out'
    df[amt_col] = pd.to_numeric(df[amt_col], errors='coerce').fillna(0)
    excel_list = []
    for name, group in df.groupby(group_col):
        for idx, row in group.iterrows():
            amt = int(row[amt_col]); vat = int(amt * 0.1)
            excel_list.append({'구분': name, '오더일': row['order_dt'], '노선': row['route'], '공급가액': amt, '부가세(10%)': vat, '합계': amt + vat})
        g_amt = group[amt_col].sum(); g_vat = int(g_amt * 0.1)
        excel_list.append({'구분': f'[{name}] 합계', '오더일': '-', '노선': '-', '공급가액': int(g_amt), '부가세(10%)': g_vat, '합계': int(g_amt + g_vat)})
        excel_list.append({})
    result_df = pd.DataFrame(excel_list)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w: result_df.to_excel(w, index=False)
    out.seek(0); return send_file(out, as_attachment=True, download_name=f"{t}_settlement.xlsx")

@app.route('/export_misu_info')
@login_required 
def export_misu_info():
    q_st = request.args.get('status', ''); q_name = request.args.get('name', '')
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
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
        export_data.append({'거래처명': row_dict['client_name'], '사업자번호': row_dict['biz_num'], '대표자': row_dict['biz_owner'], '메일': row_dict['mail'], '연락처': row_dict['c_phone'], '노선': row_dict['route'], '업체운임': row_dict['fee'], '오더일': row_dict['order_dt'], '결제예정일': row_dict['pay_due_dt']})
    df = pd.DataFrame(export_data)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w: df.to_excel(w, index=False)
    out.seek(0); return send_file(out, as_attachment=True, download_name="misu_client_info.xlsx")

@app.route('/export_tax_not_issued')
@login_required
def export_tax_not_issued():
    """정산관리 - 세금계산서 미발행 건 엑셀 다운로드. 컬럼: 사업자구분~메일주소, 오더일, 노선, 업체명, 업체운임 / 정렬: 업체명, 오더일, 노선, 업체운임"""
    q_name = request.args.get('name', '')
    q_start = request.args.get('start', ''); q_end = request.args.get('end', '')
    # 업체명 → 업체 마스터 정보 (clients)
    client_by_name = {str(c.get('업체명') or '').strip(): c for c in clients_db if (c.get('업체명') or '').strip()}
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ledger").fetchall(); conn.close()
    export_data = []
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
        cname = str(r.get('client_name') or '').strip()
        client = client_by_name.get(cname, {})
        # 사업자구분, 결제특이사항, 발행구분, 사업자등록번호, 대표자명, 사업자주소, 업태, 종목, 메일주소 (업체 마스터 우선, 없으면 장부값)
        export_data.append({
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
            '업체운임': r.get('fee', ''),
        })
    # 정렬: 업체명 → 오더일 → 노선 → 업체운임
    df = pd.DataFrame(export_data)
    if df.empty:
        cols = ['사업자구분', '결제특이사항', '발행구분', '사업자등록번호', '대표자명', '사업자주소', '업태', '종목', '메일주소', '오더일', '노선', '업체명', '업체운임']
        df = pd.DataFrame(columns=cols)
    else:
        df = df.sort_values(by=['업체명', '오더일', '노선', '업체운임'], ascending=[True, True, True, True], na_position='last')
        df = df[['사업자구분', '결제특이사항', '발행구분', '사업자등록번호', '대표자명', '사업자주소', '업태', '종목', '메일주소', '오더일', '노선', '업체명', '업체운임']]
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        df.to_excel(w, index=False)
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
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
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
    st = request.args.get('status', '')
    
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ledger").fetchall(); conn.close()
    
    # 기사관리에서 개인/고정="고정"인 차량번호 목록 (차량번호 기준)
    fixed_c_nums = {str(dr.get('차량번호', '')).strip() for dr in drivers_db if str(dr.get('개인/고정', '')).strip() == '고정'}
    fixed_c_nums.discard('')
    export_data = []

    for row in rows:
        r = dict(row)
        # 상태 계산 로직 (통계 함수와 동일하게 적용)
        in_dt = r.get('in_dt'); out_dt = r.get('out_dt')
        m_st = "수금완료" if in_dt else ("조건부미수" if not r.get('pre_post') and not r.get('pay_due_dt') else "미수")
        p_st = "지급완료" if out_dt else ("조건부미지급" if not in_dt else "미지급")
        c_num = str(r.get('c_num', '')).strip()
        d_type = "직영" if c_num in fixed_c_nums else "일반"

        # 필터링
        if s and e and not (s <= (r['order_dt'] or "") <= e): continue
        if c and c not in str(r['client_name']): continue
        if d and d not in str(r['d_name']): continue
        if st:
            if st in ["미수", "조건부미수", "수금완료"] and st != m_st: continue
            if st in ["미지급", "조건부미지급", "지급완료"] and st != p_st: continue
            if st == "고정" and c_num not in fixed_c_nums: continue
            if st in ["직영", "일반"] and st != d_type: continue

        export_data.append({
            '오더일': r['order_dt'], '업체명': r['client_name'], '노선': r['route'],
            '기사명': r['d_name'], '업체운임': r['fee'], '수금상태': m_st,
            '기사운임': r['fee_out'], '지급상태': p_st, '기사구분': d_type
        })
        
    df = pd.DataFrame(export_data)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name='통계데이터')
    out.seek(0)
    return send_file(out, as_attachment=True, download_name=f"SM_Logis_Stats_{now_kst().strftime('%y%m%d')}.xlsx")

@app.route('/upload_evidence/<int:ledger_id>', methods=['GET', 'POST'])
@login_required 
def upload_evidence(ledger_id):
    target_type = request.args.get('type', 'all')
    try:
        seq_val = int(request.args.get('seq', 1) or 1)
        target_seq = str(max(1, min(5, seq_val)))
    except (ValueError, TypeError):
        target_seq = '1'
    if request.method == 'POST':
        tax_file, ship_file = request.files.get('tax_file'), request.files.get('ship_file')
        conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
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

@app.route('/api/save_ledger', methods=['POST'])
@login_required 
def save_ledger_api():
    data = request.json or {}
    if not isinstance(data, dict):
        return jsonify({"status": "error", "message": "invalid request"}), 400
    conn = sqlite3.connect('ledger.db')
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
        sql = ", ".join([f"'{k}' = ?" for k in keys])
        vals = [data.get(k, '') for k in keys] + [target_id]
        cursor.execute(f"UPDATE ledger SET {sql} WHERE id = ?", vals)
    else:
        action_type = "신규등록"
        placeholders = ", ".join(['?'] * len(keys))
        cursor.execute(f"INSERT INTO ledger ({', '.join([f'[{k}]' for k in keys])}) VALUES ({placeholders})", 
                       [data.get(k, '') for k in keys])
        target_id = cursor.lastrowid

    details = f"업체:{data.get('client_name')}, 노선:{data.get('route')}, 업체운임:{data.get('fee')}, 기사운임:{data.get('fee_out')}"
    cursor.execute("INSERT INTO activity_logs (action, target_id, details) VALUES (?, ?, ?)",
                   (action_type, target_id, details))

    if data.get('d_name') and data.get('c_num'):
        d_vals = (
            data.get('d_phone',''), data.get('bank_acc',''), data.get('tax_biz_num',''),
            data.get('tax_biz_name',''), data.get('memo1',''), 
            data.get('d_bank_name',''), data.get('d_bank_owner',''), 
            data.get('d_name'), data.get('c_num')
        )
        cursor.execute("SELECT 1 FROM drivers WHERE 기사명 = ? AND 차량번호 = ?", (data.get('d_name'), data.get('c_num')))
        if cursor.fetchone():
            cursor.execute("UPDATE drivers SET 연락처=?, 계좌번호=?, 사업자번호=?, 사업자=?, 메모=?, 은행명=?, 예금주=? WHERE 기사명=? AND 차량번호=?", d_vals)
        else:
            cursor.execute("INSERT INTO drivers (연락처, 계좌번호, 사업자번호, 사업자, 메모, 은행명, 예금주, 기사명, 차량번호) VALUES (?,?,?,?,?,?,?,?,?)", d_vals)

    conn.commit()
    conn.close()
    load_db_to_mem()
    return jsonify({"status": "success"})

@app.route('/api/get_order_logs/<int:order_id>')
@login_required
def get_order_logs(order_id):
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
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
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
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
    per_page = 50
    start_dt = request.args.get('start', '')
    end_dt = request.args.get('end', '')
    
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    
    # 기본 쿼리
    query = "SELECT * FROM ledger"
    params = []
    
    # 날짜 필터링 조건 추가
    if start_dt and end_dt:
        query += " WHERE order_dt BETWEEN ? AND ?"
        params.extend([start_dt, end_dt])
        
    query += " ORDER BY id DESC"
    
    all_rows = conn.execute(query, params).fetchall()
    total_count = len(all_rows)
    total_pages = (total_count + per_page - 1) // per_page
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    page_rows = [dict(r) for r in all_rows[start_idx:end_idx]]
    conn.close()
    return jsonify({"data": page_rows, "total_pages": total_pages, "current_page": page})


@app.route('/api/get_ledger_row/<int:row_id>')
@login_required
def get_ledger_row(row_id):
    """단일 장부 행 조회 (정산관리 → 통합장부입력 연동용)"""
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM ledger WHERE id = ?", (row_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "not found"}), 404
    return jsonify(dict(row))


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
    conn = sqlite3.connect('ledger.db')
    cursor = conn.cursor()
    display_name = next((col['n'] for col in FULL_COLUMNS if col['k'] == key), key)
    cursor.execute(f"UPDATE ledger SET [{key}] = ? WHERE id = ?", (data.get('value'), row_id))
    log_details = f"[{display_name}] 항목이 '{data.get('value')}'(으)로 변경됨"
    cursor.execute("INSERT INTO activity_logs (action, target_id, details) VALUES (?, ?, ?)",
                   ("상태변경", row_id, log_details))
    conn.commit()
    conn.close()
    return jsonify({"status": "success"})

@app.route('/export_clients')
@login_required
def export_clients():
    """업체관리 - 비고, 사업자구분, 결제특이사항, 발행구분, 사업자등록번호, 대표자명, 사업자주소, 업태, 종목, 메일주소, 오더일, 노선, 업체운임 순 엑셀"""
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    # 업체명별 최신 오더 1건 (오더일, 노선, 업체운임)
    ledger_rows = conn.execute(
        "SELECT client_name, order_dt, route, fee FROM ledger WHERE client_name IS NOT NULL AND client_name != '' ORDER BY id DESC"
    ).fetchall()
    conn.close()
    latest_order = {}
    for r in ledger_rows:
        cname = (r[0] or '').strip()
        if cname and cname not in latest_order:
            latest_order[cname] = {'오더일': r[1] or '', '노선': r[2] or '', '업체운임': r[3] or ''}
    # 컬럼 순서: 비고, 사업자구분, 결제특이사항, 발행구분, 사업자등록번호, 대표자명, 사업자주소, 업태, 종목, 메일주소, 오더일, 노선, 업체운임
    export_cols = ['비고', '사업자구분', '결제특이사항', '발행구분', '사업자등록번호', '대표자명', '사업자주소', '업태', '종목', '메일주소', '오더일', '노선', '업체운임']
    export_data = []
    for c in clients_db:
        cname = (c.get('업체명') or '').strip()
        order_info = latest_order.get(cname, {'오더일': '', '노선': '', '업체운임': ''})
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
            '업체운임': order_info['업체운임'],
        }
        export_data.append(row)
    df = pd.DataFrame(export_data, columns=export_cols)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        df.to_excel(w, index=False)
    out.seek(0)
    return send_file(out, as_attachment=True, download_name="clients.xlsx")

@app.route('/manage_clients', methods=['GET', 'POST'])
@login_required 
def manage_clients():
    global clients_db
    if request.method == 'POST' and 'file' in request.files:
        file = request.files['file']
        if file.filename != '':
            try:
                if file.filename.lower().endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(file, engine='openpyxl')
                else:
                    df = pd.read_csv(io.StringIO(file.stream.read().decode("utf-8-sig")))
                df = df.fillna('').astype(str)
                conn = sqlite3.connect('ledger.db')
                df.to_sql('clients', conn, if_exists='replace', index=False)
                conn.commit(); conn.close(); load_db_to_mem()
            except Exception as e: return f"업로드 오류: {str(e)}"
    rows_html = "".join([f"<tr>{''.join([f'<td>{r.get(c, "")}</td>' for c in CLIENT_COLS])}</tr>" for r in clients_db])
    content = f"""<div class="section"><h2>업체 관리</h2>
    <div style="margin-bottom:15px;">
        <form method="post" enctype="multipart/form-data" style="display:inline;"><input type="file" name="file"><button type="submit" class="btn">업로드</button></form>
    </div>
    <div class="scroll-x"><table><thead><tr>{"".join([f"<th>{c}</th>" for c in CLIENT_COLS])}</tr></thead><tbody>{rows_html}</tbody></table></div></div>"""
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys="[]")
# --- [도착현황 라우트 및 API] ---
@app.route('/arrival')
@login_required
def arrival():
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM arrival_status ORDER BY order_idx ASC, id ASC").fetchall(); conn.close()
    items = [dict(r) for r in rows]
    items_json = json.dumps(items, ensure_ascii=False)

    content = f"""
    <div class="section">
        <h2>🚚 도착현황</h2>
        <div style="display:flex; flex-wrap:wrap; gap:12px; align-items:flex-end; margin-bottom:20px; padding:15px; background:#f8fafc; border-radius:8px; border:1px solid #e2e8f0;">
            <div>
                <label style="display:block; font-size:11px; color:#64748b; margin-bottom:4px;">도착 예정 시간</label>
                <input type="datetime-local" id="arrivalTargetTime" style="width:200px; padding:8px 12px; border:1px solid #cbd5e1; border-radius:6px;">
            </div>
            <div style="flex:1; min-width:200px;">
                <label style="display:block; font-size:11px; color:#64748b; margin-bottom:4px;">내용 (자유 입력)</label>
                <input type="text" id="arrivalContent" placeholder="예: 서울→부산 12톤 김기사" style="width:100%; padding:8px 12px; border:1px solid #cbd5e1; border-radius:6px;">
            </div>
            <button onclick="addArrivalItem()" class="btn-save" style="padding:8px 18px;">추가</button>
        </div>
        <div class="arrival-list" id="arrivalList"></div>
    </div>
    <style>
        .arrival-item {{ background:white; border:1px solid #e2e8f0; border-radius:8px; padding:14px 16px; margin-bottom:10px; display:flex; align-items:flex-start; gap:14px; box-shadow:0 1px 3px rgba(0,0,0,0.05); }}
        .arrival-item.expired {{ background:#fef2f2; border-color:#fecaca; }}
        .arrival-item .countdown {{ font-size:20px; font-weight:700; color:#1a2a6c; min-width:140px; flex-shrink:0; }}
        .arrival-item .countdown.warn {{ color:#dc2626; }}
        .arrival-item .countdown.done {{ color:#64748b; font-size:14px; }}
        .arrival-item .content-area {{ flex:1; word-break:break-all; line-height:1.5; }}
        .arrival-item .content-display {{ cursor:pointer; padding:4px 8px; margin:-4px -8px; border-radius:4px; }}
        .arrival-item .content-display:hover {{ background:#f1f5f9; }}
        .arrival-item .content-edit {{ width:100%; border:1px solid #e2e8f0; padding:6px 10px; border-radius:4px; font-size:13px; min-height:60px; resize:vertical; }}
        .arrival-item .meta {{ font-size:11px; color:#94a3b8; margin-top:6px; }}
        .arrival-item .del-btn {{ color:#ef4444; cursor:pointer; padding:4px 8px; font-size:12px; flex-shrink:0; }}
        .arrival-item .del-btn:hover {{ text-decoration:underline; }}
    </style>
    <script>
        let arrivalItems = {items_json};

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
                const contentEsc = content.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
                const id = item.id;
                return `<div class="arrival-item" data-id="${{id}}" id="arrival-row-${{id}}">
                    <div class="countdown" id="cd-${{id}}" data-target="${{targetTime}}"></div>
                    <div class="content-area">
                        <div class="content-display" id="content-display-${{id}}">${{contentEsc || '(내용 없음)'}}</div>
                        <textarea class="content-edit" id="content-edit-${{id}}" style="display:none;" onblur="saveArrivalContent(${{id}}, this.value)"></textarea>
                        <div class="meta" id="meta-${{id}}"></div>
                    </div>
                    <span class="del-btn" onclick="deleteArrivalItem(${{id}})">삭제</span>
                </div>`;
            }}).join('');

            arrivalItems.forEach(item => {{
                const displayEl = document.getElementById('content-display-' + item.id);
                const editEl = document.getElementById('content-edit-' + item.id);
                if (displayEl && editEl) {{
                    displayEl.onclick = () => {{ displayEl.style.display='none'; editEl.value = item.content || ''; editEl.style.display='block'; editEl.focus(); }};
                }}
            }});

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
                    const s = Math.floor((diff % 60000) / 1000);
                    el.textContent = (h > 0 ? h + '시간 ' : '') + m + '분 ' + s + '초';
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

        setInterval(function() {{ updateAllCountdowns(); reorderArrivalList(); }}, 1000);

        function addArrivalItem() {{
            const targetTime = document.getElementById('arrivalTargetTime').value;
            const content = document.getElementById('arrivalContent').value.trim();
            fetch('/api/arrival/add', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{ target_time: targetTime || null, content: content }})
            }}).then(r => r.json()).then(res => {{
                if (res.status === 'success') {{
                    arrivalItems.push({{ id: res.id, target_time: targetTime || null, content: content, order_idx: arrivalItems.length }});
                    document.getElementById('arrivalTargetTime').value = '';
                    document.getElementById('arrivalContent').value = '';
                    renderArrivalList();
                }}
            }});
        }}

        function saveArrivalContent(id, value) {{
            const displayEl = document.getElementById('content-display-' + id);
            const editEl = document.getElementById('content-edit-' + id);
            if (displayEl) displayEl.style.display = 'block';
            if (editEl) editEl.style.display = 'none';
            fetch('/api/arrival/update', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{ id: id, content: value }})
            }}).then(r => r.json()).then(res => {{
                if (res.status === 'success') {{
                    const item = arrivalItems.find(i => i.id == id);
                    if (item) item.content = value;
                    if (displayEl) displayEl.textContent = value || '(내용 없음)';
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
    conn = sqlite3.connect('ledger.db')
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(MAX(order_idx), -1) + 1 FROM arrival_status")
    next_idx = cursor.fetchone()[0]
    cursor.execute("INSERT INTO arrival_status (target_time, content, order_idx) VALUES (?, ?, ?)", (target_time, content, next_idx))
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
    content = d.get('content', '')
    target_time = d.get('target_time')
    conn = sqlite3.connect('ledger.db')
    if target_time is not None:
        conn.execute("UPDATE arrival_status SET content=?, target_time=? WHERE id=?", (content, target_time, nid))
    else:
        conn.execute("UPDATE arrival_status SET content=? WHERE id=?", (content, nid))
    conn.commit(); conn.close()
    return jsonify({"status": "success"})

@app.route('/api/arrival/delete/<int:id>', methods=['POST'])
@login_required
def arrival_delete(id):
    conn = sqlite3.connect('ledger.db'); conn.execute("DELETE FROM arrival_status WHERE id=?", (id,)); conn.commit(); conn.close()
    return jsonify({"status": "success"})

@app.route('/manage_drivers', methods=['GET', 'POST'])
@login_required 
def manage_drivers():
    global drivers_db
    err_msg = ""
    if request.method == 'POST' and 'file' in request.files:
        file = request.files['file']
        if file.filename != '':
            try:
                if file.filename.lower().endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(file, engine='openpyxl')
                else:
                    df = pd.read_csv(io.StringIO(file.stream.read().decode("utf-8-sig")))
                df = df.fillna('').astype(str)
                conn = sqlite3.connect('ledger.db')
                df.to_sql('drivers', conn, if_exists='replace', index=False)
                conn.commit(); conn.close(); load_db_to_mem()
            except Exception as e:
                err_msg = f"<p style='color:red; margin-bottom:15px;'>업로드 오류: {str(e)}<br>엑셀(.xlsx, .xls) 또는 CSV 파일만 업로드 가능합니다.</p>"
    
    # 출력 컬럼 정의 (은행명, 예금주 포함)
    DISPLAY_DRIVER_COLS = ["기사명", "차량번호", "연락처", "은행명", "계좌번호", "예금주", "사업자번호", "사업자", "개인/고정", "메모"]
    rows_html = "".join([f"<tr>{''.join([f'<td>{r.get(c, "")}</td>' for c in DISPLAY_DRIVER_COLS])}</tr>" for r in drivers_db])
    content = f"""<div class="section"><h2>🚚 기사 관리 (은행/계좌 정보)</h2>
    {err_msg}
    <form method="post" enctype="multipart/form-data" style="margin-bottom:15px;">
        <input type="file" name="file"> <button type="submit" class="btn-save">엑셀 업로드</button>
    </form>
    <div class="scroll-x"><table><thead><tr>{"".join([f"<th>{c}</th>" for c in DISPLAY_DRIVER_COLS])}</tr></thead><tbody>{rows_html}</tbody></table></div></div>"""
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys="[]")

# 배포 시 FLASK_DEBUG=0 또는 미설정, FLASK_SECRET_KEY·ADMIN_PW 반드시 설정
if __name__ == '__main__':
    app.run(debug=True, port=5000) # debug=True가 자동 반영의 핵심!