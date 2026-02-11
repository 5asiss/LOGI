from flask import Flask, render_template_string, request, jsonify
import pandas as pd
import io
import json
import sqlite3
import os
from datetime import datetime, timedelta

app = Flask(__name__)

# 이미지 업로드 폴더 설정
UPLOAD_FOLDER = 'static/evidences'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

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
    {"n": "증빙사진", "k": "img_upload", "t": "link"},
    {"n": "업체계산서사진", "k": "client_tax_img", "t": "text"}
]

DRIVER_COLS = ["기사명", "차량번호", "연락처", "계좌번호", "사업자번호", "사업자", "개인/고정", "메모"]
CLIENT_COLS = ["사업자구분", "업체명", "발행구분", "사업자등록번호", "대표자명", "사업자주소", "업태", "종목", "메일주소", "담당자", "연락처", "결제특이사항", "비고"]

def init_db():
    conn = sqlite3.connect('ledger.db')
    cursor = conn.cursor()
    cols_sql = ", ".join([f"{c['k']} TEXT" for c in FULL_COLUMNS])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, {cols_sql})")
    cursor.execute("PRAGMA table_info(ledger)")
    existing_cols = [info[1] for info in cursor.fetchall()]
    for col in ["tax_img", "ship_img", "client_tax_img"]:
        if col not in existing_cols:
            try: cursor.execute(f"ALTER TABLE ledger ADD COLUMN {col} TEXT")
            except: pass
    cursor.execute("CREATE TABLE IF NOT EXISTS drivers (id INTEGER PRIMARY KEY AUTOINCREMENT, " + ", ".join([f"'{c}' TEXT" for c in DRIVER_COLS]) + ")")
    cursor.execute("CREATE TABLE IF NOT EXISTS clients (id INTEGER PRIMARY KEY AUTOINCREMENT, " + ", ".join([f"'{c}' TEXT" for c in CLIENT_COLS]) + ")")
    conn.commit(); conn.close()

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
    <title>바구니삼촌 통합 정산 시스템 v10</title>
    <style>
        body { font-family: 'Malgun Gothic', sans-serif; margin: 10px; font-size: 11px; background: #f0f2f5; }
        .nav { background: #1a2a6c; padding: 10px; border-radius: 5px; margin-bottom: 15px; display: flex; gap: 15px; }
        .nav a { color: white; text-decoration: none; font-weight: bold; }
        .section { background: white; padding: 15px; border-radius: 5px; margin-bottom: 15px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        .scroll-x { overflow-x: auto; max-width: 100%; border: 1px solid #ccc; background: white; }
        table { border-collapse: collapse; width: 100%; white-space: nowrap; }
        th, td { border: 1px solid #dee2e6; padding: 4px; text-align: center; }
        th { background: #f8f9fa; position: sticky; top: 0; z-index: 5; }
        input[type="text"], input[type="number"], input[type="date"], input[type="datetime-local"] { width: 110px; border: 1px solid #ddd; padding: 3px; font-size: 11px; }
        input[type="checkbox"] { transform: scale(1.1); }
        .btn-save { background: #27ae60; color: white; padding: 10px 25px; border: none; border-radius: 3px; cursor: pointer; font-weight: bold; font-size: 13px; }
        .btn-edit { background: #f39c12; color: white; padding: 2px 5px; border: none; border-radius: 2px; cursor: pointer; }
        .btn-status { padding: 4px 8px; border: none; border-radius: 3px; cursor: pointer; font-weight: bold; color: white; font-size: 10px; }
        .bg-red { background: #e74c3c; }
        .bg-green { background: #2ecc71; }
        .bg-orange { background: #f39c12; }
        .bg-gray { background: #95a5a6; cursor: not-allowed; }
        .search-bar { padding: 8px; width: 300px; border: 2px solid #1a2a6c; border-radius: 4px; margin-bottom: 10px; }
        .draggable { cursor: move; }
        .draggable.dragging { opacity: 0.5; background: #e9ecef; }
        .search-results { position: absolute; background: white; border: 1px solid #ccc; z-index: 1000; max-height: 200px; overflow-y: auto; display: none; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }
        .search-item { padding: 8px; cursor: pointer; border-bottom: 1px solid #eee; }
        .quick-order-grid { display: grid; grid-template-columns: repeat(6, 1fr); gap: 10px; margin-bottom: 10px; }
        .quick-order-grid div { display: flex; flex-direction: column; }
        .quick-order-grid label { font-weight: bold; margin-bottom: 3px; color: #1a2a6c; }
        #imgModal { display:none; position:fixed; z-index:9999; left:0; top:0; width:100%; height:100%; background:rgba(0,0,0,0.8); text-align:center; }
        #imgModal img { max-width:90%; max-height:90%; margin-top:30px; border:3px solid white; }
        #imgModal .close { position:absolute; top:15px; right:35px; color:white; font-size:40px; cursor:pointer; }
        .memo-board { height: 140px; background: #dfe6e9; border: 2px dashed #b2bec3; position: relative; margin-bottom: 15px; border-radius: 5px; overflow: hidden; }
        .sticky-note { position: absolute; width: 160px; background: #fff9c4; border: 1px solid #fbc02d; padding: 8px; cursor: move; z-index: 100; box-shadow: 2px 2px 5px rgba(0,0,0,0.1); border-radius: 5px; }
        .multi-img-btns { display: flex; gap: 2px; justify-content: center; }
        .img-num-btn { width: 18px; height: 18px; font-size: 9px; padding: 0; cursor: pointer; border: 1px solid #ccc; background: white; border-radius: 2px; }
        .img-num-btn.active { background: #2ecc71; color: white; border-color: #27ae60; }
    </style>
</head>
<body>
    <div class="nav">
        <a href="/">통합장부입력</a>
        <a href="/settlement">정산관리</a>
        <a href="/manage_drivers">기사관리</a>
        <a href="/manage_clients">업체관리</a>
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

        window.viewImg = function(src) {
            if(!src || src.includes('❌') || src === '/' || src.includes('None')) return;
            const paths = src.split(',');
            document.getElementById('modalImg').src = '/' + paths[0].trim();
            document.getElementById('imgModal').style.display = 'block';
        };

        const getChosung = (str) => {
            const cho = ["ㄱ","ㄲ","ㄴ","ㄷ","ㄸ","ㄹ","ㅁ","ㅂ","ㅃ","ㅅ","ㅆ","ㅇ","ㅈ","ㅉ","ㅊ","ㅋ","ㅌ","ㅍ","ㅎ"];
            let res = "";
            for(let i=0; i<str.length; i++) {
                let code = str.charCodeAt(i) - 44032;
                if(code>-1 && code<11172) res += cho[Math.floor(code/588)];
                else res += str.charAt(i);
            }
            return res;
        };

        document.addEventListener('input', function(e) {
            if(e.target.classList.contains('driver-search') || e.target.classList.contains('client-search')) {
                const isDriver = e.target.classList.contains('driver-search');
                const val = e.target.value.toLowerCase();
                const db = isDriver ? drivers : clients;
                const popup = document.getElementById('search-popup');
                if(val.length < 1) { popup.style.display = 'none'; return; }
                const filtered = db.filter(item => {
                    const target = isDriver ? (item.기사명 + (item.차량번호||'')) : (item.업체명||'');
                    return target.toLowerCase().includes(val) || getChosung(target).includes(val);
                });
                if(filtered.length > 0) {
                    const rect = e.target.getBoundingClientRect();
                    popup.style.display = 'block'; popup.style.top = (rect.bottom + window.scrollY) + 'px'; popup.style.left = rect.left + 'px'; popup.style.width = rect.width + 'px';
                    popup.innerHTML = filtered.map(item => `<div class="search-item" onclick='fillData(${JSON.stringify(item)}, "${isDriver?'driver':'client'}", "${e.target.id}")'>${isDriver ? item.기사명+' ['+item.차량번호+']' : item.업체명}</div>`).join('');
                } else { popup.style.display = 'none'; }
            }
        });

        window.fillData = function(item, type, targetInputId) {
            const prefix = targetInputId.startsWith('q_') ? 'q_' : '';
            if(type === 'driver') {
                document.querySelector(`input[name="${prefix}d_name"]`).value = item.기사명 || '';
                document.querySelector(`input[name="${prefix}c_num"]`).value = item.차량번호 || '';
                if(!prefix) {
                    document.querySelector('input[name="d_phone"]').value = item.연락처 || '';
                    document.querySelector('input[name="bank_acc"]').value = item.계좌번호 || '';
                    document.querySelector('input[name="tax_biz_num"]').value = item.사업자번호 || '';
                    document.querySelector('input[name="tax_biz_name"]').value = item.사업자 || '';
                }
            } else {
                document.querySelector(`input[name="${prefix}client_name"]`).value = item.업체명 || '';
                if(!prefix) {
                    document.querySelector('input[name="c_phone"]').value = item.연락처 || '';
                    document.querySelector('input[name="biz_num"]').value = item.사업자등록번호 || '';
                    document.querySelector('input[name="biz_addr"]').value = item.사업자주소 || '';
                    document.querySelector('input[name="biz_owner"]').value = item.대표자명 || '';
                }
            }
            document.getElementById('search-popup').style.display = 'none';
        };

        function loadLedgerList() {
            const body = document.getElementById('ledgerBody');
            if (!body) return; 
            fetch('/api/get_ledger').then(r => r.json()).then(data => {
                lastLedgerData = data;
                renderTableRows(data);
            });
        }

        function saveLedger(formId) {
            const form = document.getElementById(formId);
            const formData = new FormData(form);
            const data = {};
            const isQuick = (formId === 'quickOrderForm');
            formData.forEach((v, k) => {
                const key = isQuick ? k.replace('q_', '') : k;
                const input = form.querySelector(`[name="${k}"]`);
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
                data.order_dt = data.order_dt || new Date().toISOString().split('T')[0];
                data.dispatch_dt = data.dispatch_dt || new Date().toISOString().slice(0,16);
            }
            if (currentEditId) data['id'] = currentEditId;
            fetch('/api/save_ledger', {
                method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data)
            }).then(r => r.json()).then(res => {
                if(res.status === 'success') {
                    currentEditId = null; 
                    form.reset();
                    loadLedgerList(); 
                    fetch('/api/load_db_mem').then(r => r.json()).then(db => {
                        drivers = db.drivers; clients = db.clients;
                    });
                }
            });
        }

        function renderTableRows(data) {
            const body = document.getElementById('ledgerBody');
            if (!body) return;
            body.innerHTML = data.map(item => `
                <tr class="draggable" draggable="true" data-id="${item.id}">
                    <td><button class="btn-edit" onclick="editEntry(${item.id})">수정</button></td>
                    ${columnKeys.map(key => {
                        let val = item[key] || '';
                        if(key.includes('img') && val.startsWith('static')) {
                            return `<td onclick="viewImg('${val}')" style="cursor:pointer; color:blue;">🖼️보기</td>`;
                        }
                        return `<td>${val}</td>`;
                    }).join('')}
                </tr>
            `).join('');
            initDraggable();
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
                const afterElement = getDragAfterElement(body, e.clientY);
                const dragging = document.querySelector('.dragging');
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
            fetch('/api/update_status', { 
                method: 'POST', 
                headers: {'Content-Type': 'application/json'}, 
                body: JSON.stringify({id: id, key: key, value: val}) 
            }).then(() => location.reload());
        };

        function addMemo() {
            const board = document.getElementById('memoBoard'); if(!board) return;
            const note = document.createElement('div'); note.className = 'sticky-note'; note.style.left = '50px'; note.style.top = '20px';
            note.innerHTML = `<div style="font-size:10px; font-weight:bold; border-bottom:1px solid #fbc02d; margin-bottom:5px;">퀵 메모 <span style="float:right; cursor:pointer;" onclick="this.parentElement.parentElement.remove()">×</span></div>
                              <input type="text" placeholder="기사명/차량번호" style="width:100%; border:none; background:transparent; font-size:10px; margin-bottom:3px; border-bottom:1px solid #eee;">
                              <input type="text" placeholder="도착지" style="width:100%; border:none; background:transparent; font-size:10px; margin-bottom:3px; border-bottom:1px solid #eee;">
                              <input type="text" placeholder="도착시간" style="width:100%; border:none; background:transparent; font-size:10px;">`;
            board.appendChild(note); dragElement(note);
        }

        function dragElement(elmnt) {
            let p1=0, p2=0, p3=0, p4=0;
            elmnt.onmousedown = (e) => { 
                if(e.target.tagName === 'INPUT') return;
                e.preventDefault(); p3=e.clientX; p4=e.clientY; 
                document.onmouseup=()=>document.onmousemove=null; 
                document.onmousemove=(e)=>{
                    e.preventDefault(); p1=p3-e.clientX; p2=p4-e.clientY; p3=e.clientX; p4=e.clientY; 
                    elmnt.style.top=(elmnt.offsetTop-p2)+"px"; elmnt.style.left=(elmnt.offsetLeft-p1)+"px";
                };
            };
        }

        window.onload = loadLedgerList;
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    col_keys_json = json.dumps([c['k'] for c in FULL_COLUMNS])
    content = f"""
    <div class="memo-board" id="memoBoard">
        <button onclick="addMemo()" style="margin:10px; cursor:pointer;">+ 퀵 메모 추가</button>
        <small style="color:#636e72;">메모를 마우스로 잡고 이동하세요 (입력란 외 클릭).</small>
    </div>
    <div class="section" style="background:#fff9c4; border:2px solid #fbc02d;">
        <h3>⚡ 빠른 오더 입력</h3>
        <form id="quickOrderForm">
            <div class="quick-order-grid">
                <div><label>업체명</label><input type="text" name="q_client_name" id="q_client_name" class="client-search" placeholder="초성검색..."></div>
                <div><label>노선</label><input type="text" name="q_route" placeholder="상차-하차"></div>
                <div><label>업체운임</label><input type="number" name="q_fee" placeholder="0"></div>
                <div><label>기사명</label><input type="text" name="q_d_name" id="q_d_name" class="driver-search" placeholder="이름입력"></div>
                <div><label>차량번호</label><input type="text" name="q_c_num" id="q_c_num" class="driver-search" placeholder="차량번호"></div>
                <div><label>기사운임</label><input type="number" name="q_fee_out" placeholder="0"></div>
            </div>
            <div style="text-align:right;"><button type="button" class="btn-save" style="background:#e67e22;" onclick="saveLedger('quickOrderForm')">장부 즉시 등록</button></div>
        </form>
    </div>
    <div class="section">
        <h3>1. 장부 상세 데이터 입력</h3>
        <form id="ledgerForm">
            <div class="scroll-x"><table><thead><tr><th>관리</th>{"".join([f"<th>{c['n']}</th>" for c in FULL_COLUMNS])}</tr></thead><tbody><tr><td>-</td>{"".join([f"<td><input type='{c.get('t', 'text')}' name='{c['k']}' class='{c.get('c', '')}'></td>" for c in FULL_COLUMNS])}</tr></tbody></table></div>
            <div style="text-align:right; margin-top:15px;"><button type="button" class="btn-save" onclick="saveLedger('ledgerForm')">상세 저장 및 추가 ↓</button></div>
        </form>
    </div>
    <div class="section">
        <h3>2. 장부 목록 (원천 데이터 저장소)</h3>
        <input type="text" id="ledgerSearch" class="search-bar" placeholder="실시간 목록 검색..." onkeyup="filterLedger()">
        <div class="scroll-x"><table id="resultTable"><thead><tr><th>관리</th>{"".join([f"<th>{c['n']}</th>" for c in FULL_COLUMNS])}</tr></thead><tbody id="ledgerBody"></tbody></table></div>
    </div>
    """
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys=col_keys_json)

@app.route('/settlement')
def settlement():
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    q_status = request.args.get('status', ''); q_name = request.args.get('name', '')
    rows = conn.execute("SELECT * FROM ledger ORDER BY dispatch_dt DESC").fetchall(); conn.close()
    table_rows = ""; today = datetime.now()

    for row in rows:
        has_in_dt = bool(row['in_dt']); has_prepost = bool(row['pre_post']); has_paydue = bool(row['pay_due_dt'])
        order_date = None
        try: order_date = datetime.strptime(row['order_dt'], '%Y-%m-%d')
        except: pass
        is_over_30 = (order_date and (today - order_date).days >= 30)
        misu_status = "ok"
        if not has_in_dt:
            if not has_prepost and not is_over_30 and not has_paydue: misu_status = "conditional"
            else: misu_status = "misu"
        
        has_out_dt = bool(row['out_dt'])
        if q_status == 'misu' and (has_in_dt or misu_status == "ok"): continue
        if q_status == 'pay' and has_out_dt: continue
        if q_name and q_name not in str(row['client_name']) and q_name not in str(row['d_name']): continue

        misu_btn = '<button class="btn-status bg-green">수금완료</button>' if has_in_dt else f'<button class="btn-status {"bg-orange" if misu_status=="conditional" else "bg-red"}" onclick="changeStatus({row["id"]}, \'in_dt\', \'{today.strftime("%Y-%m-%d")}\')">{"조건부미수" if misu_status=="conditional" else "미수"}</button>'
        tax_chk_val = row['tax_chk'] if row['tax_chk'] else "미발행"
        tax_issued_btn = f'<button class="btn-status {"bg-green" if tax_chk_val=="발행완료" else "bg-orange"}" onclick="changeStatus({row["id"]}, \'tax_chk\', \'발행완료\')">{tax_chk_val}</button>'
        pay_btn = '<button class="btn-status bg-green">지급완료</button>' if has_out_dt else f'<button class="btn-status bg-red" onclick="changeStatus({row["id"]}, \'out_dt\', \'{today.strftime("%Y-%m-%d")}\')">미지급</button>'
        
        def make_multi_btns(id, img_type, paths):
            path_list = [p.strip() for p in paths.split(',')] if paths else []
            btns = ""
            for i in range(1, 6):
                has_file = len(path_list) >= i and path_list[i-1] != ""
                active_class = "active" if has_file else ""
                click_fn = f"viewImg('{path_list[i-1]}')" if has_file else f"window.open('/upload_evidence/{id}?type={img_type}&seq={i}', '_blank')"
                btns += f'<button class="img-num-btn {active_class}" onclick="{click_fn}">{i}</button>'
            return f'<div class="multi-img-btns">{btns}</div>'

        driver_tax_btns = make_multi_btns(row['id'], 'tax', row['tax_img'])
        ship_btns = make_multi_btns(row['id'], 'ship', row['ship_img'])
        
        table_rows += f"""<tr>
            <td>{row['client_name']}</td>
            <td>{tax_issued_btn}</td>
            <td>{row['order_dt']}</td><td>{row['dispatch_dt']}</td><td>{row['route']}</td><td>{row['d_name']}</td><td>{row['c_num']}</td><td>{row['fee']}</td><td>{row['in_dt']}</td><td>{misu_btn}</td><td>{row['fee_out']}</td><td>{pay_btn}</td>
            <td><a href="/upload_evidence/{row['id']}?type=tax" target="_blank" style="color:blue;">[계산서링크]</a></td>
            <td>{driver_tax_btns}</td>
            <td><a href="/upload_evidence/{row['id']}?type=ship" target="_blank" style="color:green;">[운송장링크]</a></td>
            <td>{ship_btns}</td>
        </tr>"""

    content = f"""<div class="section"><h2>정산 관리</h2><form class="filter-box" method="get">상태: <select name="status"><option value="">전체</option><option value="misu">미수금</option><option value="pay">기사미지급</option></select> 이름: <input type="text" name="name" placeholder="거래처/기사명"> <button type="submit" class="btn">조회</button></form><div class="scroll-x"><table><thead><tr><th>거래처명</th><th>업체계산서</th><th>오더일</th><th>배차일</th><th>노선</th><th>기사명</th><th>차량번호</th><th>업체운임</th><th>입금일</th><th>수금상태</th><th>기사운임</th><th>지급상태</th><th>계산서전송</th><th>기사계산서(1~5)</th><th>운송장전송</th><th>운송장(1~5)</th></tr></thead><tbody>{table_rows}</tbody></table></div></div>"""
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys="[]")

@app.route('/upload_evidence/<int:ledger_id>', methods=['GET', 'POST'])
def upload_evidence(ledger_id):
    target_type = request.args.get('type', 'all') 
    target_seq = request.args.get('seq', '1')
    
    if request.method == 'POST':
        tax_file = request.files.get('tax_file')
        ship_file = request.files.get('ship_file')
        conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT tax_img, ship_img FROM ledger WHERE id = ?", (ledger_id,)).fetchone()
        
        def update_path_string(old_str, new_val, seq):
            plist = [p.strip() for p in old_str.split(',')] if old_str else [""] * 5
            while len(plist) < 5: plist.append("")
            plist[int(seq)-1] = new_val
            return ",".join(plist)

        if tax_file:
            path = os.path.join(UPLOAD_FOLDER, f"tax_{ledger_id}_{target_seq}_{tax_file.filename}")
            tax_file.save(path)
            new_tax = update_path_string(row['tax_img'], path, target_seq)
            conn.execute("UPDATE ledger SET tax_img = ? WHERE id = ?", (new_tax, ledger_id))
        if ship_file:
            path = os.path.join(UPLOAD_FOLDER, f"ship_{ledger_id}_{target_seq}_{ship_file.filename}")
            ship_file.save(path)
            new_ship = update_path_string(row['ship_img'], path, target_seq)
            conn.execute("UPDATE ledger SET ship_img = ? WHERE id = ?", (new_ship, ledger_id))
        
        conn.commit(); conn.close()
        return "<h3>업로드 완료되었습니다. 창을 닫아주세요.</h3>"
    
    title = "계산서" if target_type == "tax" else "운송장" if target_type == "ship" else "증빙"
    return f"""
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>body{{font-family:sans-serif; padding:20px; text-align:center;}} input{{margin-bottom:20px; width:100%; padding:10px;}} button{{width:100%; padding:15px; background:#007bff; color:white; border:none; border-radius:5px; font-weight:bold; font-size:16px;}}</style>
    <h3>바구니삼촌 {title} 업로드 (순번:{target_seq}번)</h3>
    <form id="uploadForm">
        {"<p style='text-align:left;'>계산서 사진: <input type='file' id='tax_input' accept='image/*' capture='camera'></p>" if target_type in ['tax', 'all'] else ""}
        {"<p style='text-align:left;'>운송장 사진: <input type='file' id='ship_input' accept='image/*' capture='camera'></p>" if target_type in ['ship', 'all'] else ""}
        <button type="button" onclick="processAndUpload()">최적화하여 전송하기</button>
    </form>
    <div id="status" style="margin-top:20px; color:blue; font-weight:bold;"></div>
    <script>
        async function processAndUpload() {{
            const status = document.getElementById('status');
            const taxInput = document.getElementById('tax_input');
            const shipInput = document.getElementById('ship_input');
            const formData = new FormData();
            status.innerText = "사진 압축 중...";
            const compress = (file) => new Promise((resolve) => {{
                const reader = new FileReader();
                reader.readAsDataURL(file);
                reader.onload = (e) => {{
                    const img = new Image();
                    img.src = e.target.result;
                    img.onload = () => {{
                        const canvas = document.createElement('canvas');
                        let width = img.width; let height = img.height;
                        const max = 1200;
                        if(width > height) {{ if(width > max) {{ height *= max/width; width = max; }} }}
                        else {{ if(height > max) {{ width *= max/height; height = max; }} }}
                        canvas.width = width; canvas.height = height;
                        const ctx = canvas.getContext('2d');
                        ctx.drawImage(img, 0, 0, width, height);
                        canvas.toBlob((blob) => resolve(blob), 'image/jpeg', 0.7);
                    }};
                }};
            }});
            if(taxInput && taxInput.files[0]) formData.append('tax_file', await compress(taxInput.files[0]), 'tax.jpg');
            if(shipInput && shipInput.files[0]) formData.append('ship_file', await compress(shipInput.files[0]), 'ship.jpg');
            status.innerText = "전송 중...";
            fetch(window.location.href, {{ method: 'POST', body: formData }}).then(r => r.text()).then(t => {{ document.body.innerHTML = t; }});
        }}
    </script>
    """

@app.route('/api/save_ledger', methods=['POST'])
def save_ledger_api():
    data = request.json
    conn = sqlite3.connect('ledger.db'); cursor = conn.cursor()
    keys = [c['k'] for c in FULL_COLUMNS]
    if 'id' in data:
        update_sql = ", ".join([f"{k} = ?" for k in keys])
        vals = [data.get(k, '') for k in keys] + [data['id']]
        cursor.execute(f"UPDATE ledger SET {update_sql} WHERE id = ?", vals)
    else:
        vals = [data.get(k, '') for k in keys]
        cursor.execute(f"INSERT INTO ledger ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", vals)
    if data.get('client_name'):
        cursor.execute("SELECT id FROM clients WHERE 업체명 = ?", (data['client_name'],))
        client_res = cursor.fetchone()
        client_data = (data.get('biz_num', ''), data.get('biz_owner', ''), data.get('biz_addr', ''), 
                       data.get('mail', ''), data.get('c_mgr_name', ''), data.get('c_phone', ''), 
                       data.get('pay_memo', ''), data.get('biz_type1', ''), data.get('biz_type2', ''), data['client_name'])
        if client_res: cursor.execute("UPDATE clients SET 사업자등록번호=?, 대표자명=?, 사업자주소=?, 메일주소=?, 담당자=?, 연락처=?, 결제특이사항=?, 종목=?, 업태=? WHERE 업체명=?", client_data)
        else: cursor.execute("INSERT INTO clients (사업자등록번호, 대표자명, 사업자주소, 메일주소, 담당자, 연락처, 결제특이사항, 종목, 업태, 업체명) VALUES (?,?,?,?,?,?,?,?,?,?)", client_data)
    if data.get('d_name') and data.get('c_num'):
        cursor.execute("SELECT id FROM drivers WHERE 기사명 = ? AND 차량번호 = ?", (data['d_name'], data['c_num']))
        driver_res = cursor.fetchone()
        driver_data = (data.get('d_phone', ''), data.get('bank_acc', ''), data.get('tax_biz_num', ''), 
                       data.get('tax_biz_name', ''), data.get('memo1', ''), data['d_name'], data['c_num'])
        if driver_res: cursor.execute("UPDATE drivers SET 연락처=?, 계좌번호=?, 사업자번호=?, 사업자=?, 메모=? WHERE 기사명=? AND 차량번호=?", driver_data)
        else: cursor.execute("INSERT INTO drivers (연락처, 계좌번호, 사업자번호, 사업자, 메모, 기사명, 차량번호) VALUES (?,?,?,?,?,?,?)", driver_data)
    conn.commit(); conn.close()
    load_db_to_mem()
    return jsonify({"status": "success"})

@app.route('/api/load_db_mem')
def api_load_db_mem():
    load_db_to_mem()
    return jsonify({"drivers": drivers_db, "clients": clients_db})

@app.route('/api/get_ledger')
def get_ledger():
    conn = sqlite3.connect('ledger.db'); df = pd.read_sql("SELECT * FROM ledger ORDER BY id DESC", conn); conn.close()
    return jsonify(df.to_dict('records'))

@app.route('/api/update_status', methods=['POST'])
def update_status():
    data = request.json; conn = sqlite3.connect('ledger.db')
    conn.execute(f"UPDATE ledger SET {data['key']} = ? WHERE id = ?", (data['value'], data['id']))
    conn.commit(); conn.close(); return jsonify({"status": "success"})

@app.route('/manage_drivers', methods=['GET', 'POST'])
def manage_drivers():
    global drivers_db
    if request.method == 'POST' and 'file' in request.files:
        file = request.files['file']
        if file.filename != '':
            df = pd.read_excel(file, engine='openpyxl') if file.filename.endswith(('.xlsx', '.xls')) else pd.read_csv(io.StringIO(file.stream.read().decode("utf-8-sig")))
            df = df.fillna('').astype(str); conn = sqlite3.connect('ledger.db'); df.to_sql('drivers', conn, if_exists='replace', index=False); conn.commit(); conn.close(); load_db_to_mem()
    rows_html = "".join([f"<tr>{''.join([f'<td>{r.get(c, "")}</td>' for c in DRIVER_COLS])}</tr>" for r in drivers_db])
    content = f"""<div class="section"><h2>기사 관리</h2><form method="post" enctype="multipart/form-data"><input type="file" name="file"><button type="submit" class="btn">업로드</button></form><div class="scroll-x"><table><thead><tr>{"".join([f"<th>{c}</th>" for c in DRIVER_COLS])}</tr></thead><tbody>{rows_html}</tbody></table></div></div>"""
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys="[]")

@app.route('/manage_clients', methods=['GET', 'POST'])
def manage_clients():
    global clients_db
    if request.method == 'POST' and 'file' in request.files:
        file = request.files['file']
        if file.filename != '':
            try:
                df = pd.read_excel(file, engine='openpyxl') if file.filename.endswith(('.xlsx', '.xls')) else pd.read_csv(io.StringIO(file.stream.read().decode("utf-8-sig")))
                df = df.fillna('').astype(str); conn = sqlite3.connect('ledger.db'); df.to_sql('clients', conn, if_exists='replace', index=False); conn.commit(); conn.close(); load_db_to_mem()
            except Exception as e: return f"업로드 오류: {str(e)}"
    rows_html = "".join([f"<tr>{''.join([f'<td>{r.get(c, "")}</td>' for c in CLIENT_COLS])}</tr>" for r in clients_db])
    content = f"""<div class="section"><h2>업체 관리</h2><form method="post" enctype="multipart/form-data"><input type="file" name="file"><button type="submit" class="btn">업로드</button></form><div class="scroll-x"><table><thead><tr>{"".join([f"<th>{c}</th>" for c in CLIENT_COLS])}</tr></thead><tbody>{rows_html}</tbody></table></div></div>"""
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys="[]")

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5000)