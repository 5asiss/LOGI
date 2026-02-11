from flask import Flask, render_template_string, request, jsonify, send_file
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
    <title>바구니삼촌 통합 정산 시스템</title>
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
        .btn-save { background: #27ae60; color: white; padding: 10px 25px; border: none; border-radius: 3px; cursor: pointer; font-weight: bold; font-size: 13px; }
        .btn-status { padding: 4px 8px; border: none; border-radius: 3px; cursor: pointer; font-weight: bold; color: white; font-size: 10px; }
        .bg-red { background: #e74c3c; } .bg-green { background: #2ecc71; } .bg-orange { background: #f39c12; } .bg-gray { background: #95a5a6; }
        .bg-blue { background: #3498db; }
        .search-bar { padding: 8px; width: 300px; border: 2px solid #1a2a6c; border-radius: 4px; margin-bottom: 10px; }
        .stat-card { flex: 1; border: 1px solid #ddd; padding: 12px; border-radius: 8px; text-align: center; background: #fff; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
        .stat-val { font-size: 14px; font-weight: bold; color: #1a2a6c; margin-top: 5px; line-height: 1.4; }
        .search-results { position: absolute; background: white; border: 1px solid #ccc; z-index: 1000; max-height: 200px; overflow-y: auto; display: none; }
        .search-item { padding: 8px; cursor: pointer; border-bottom: 1px solid #eee; }
        .quick-order-grid { display: grid; grid-template-columns: repeat(6, 1fr); gap: 10px; margin-bottom: 10px; }
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
        .link-btn { font-size: 9px; padding: 2px 4px; border: 1px solid #ccc; background: #f8f9fa; color: #333; text-decoration: none; border-radius: 2px; }
        .link-btn:hover { background: #e9ecef; }
        .link-btn.has-file { background: #e3f2fd; border-color: #2196f3; color: #1976d2; font-weight: bold; }
    </style>
</head>
<body>
    <div class="nav">
        <a href="/">통합장부입력</a>
        <a href="/settlement">정산관리</a>
        <a href="/statistics">통계분석</a>
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
            if(!src || src.includes('❌') || src === '/' || src.includes('None') || src == '') return;
            let paths = src.split(',').filter(p => p.trim().startsWith('static'));
            if(paths.length > 0) {
                document.getElementById('modalImg').src = '/' + paths[0].trim();
                document.getElementById('imgModal').style.display = 'block';
            }
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
                data.order_dt = data.order_dt || new Date().toISOString().split('T')[0];
                data.dispatch_dt = data.dispatch_dt || new Date().toISOString().slice(0,16);
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
                } else {
                    alert('저장 중 오류가 발생했습니다.');
                }
            }).catch(err => {
                console.error('Error:', err);
                alert('서버 통신 오류가 발생했습니다.');
            });
        }

        function loadLedgerList() {
            const body = document.getElementById('ledgerBody');
            if (!body) return; 
            fetch('/api/get_ledger').then(r => r.json()).then(data => {
                lastLedgerData = data;
                renderTableRows(data);
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
                        if(key === 'tax_img' || key === 'ship_img') {
                            let paths = val.split(',').map(p => p.trim());
                            let btns = '<div style="display:flex; gap:2px; justify-content:center;">';
                            for(let i=0; i<5; i++) {
                                let p = (paths[i] && paths[i].startsWith('static')) ? paths[i] : '';
                                if(p) btns += `<button class="img-num-btn active" onclick="viewImg('${p}')">${i+1}</button>`;
                                else btns += `<button class="img-num-btn" style="cursor:default; color:#ccc;">${i+1}</button>`;
                            }
                            btns += '</div>';
                            return `<td>${btns}</td>`;
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

        window.onload = loadLedgerList;
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    col_keys_json = json.dumps([c['k'] for c in FULL_COLUMNS])
    content = f"""
    <div class="memo-board" id="memoBoard"><button onclick="addMemo()" style="margin:10px;">+ 퀵 메모</button></div>
    <div class="section" style="background:#fff9c4; border:2px solid #fbc02d;">
        <h3>⚡ 빠른 오더 입력</h3>
        <form id="quickOrderForm">
            <div class="quick-order-grid">
                <div><label>업체명</label><input type="text" name="q_client_name" id="q_client_name" class="client-search"></div>
                <div><label>노선</label><input type="text" name="q_route"></div>
                <div><label>업체운임</label><input type="number" name="q_fee"></div>
                <div><label>기사명</label><input type="text" name="q_d_name" id="q_d_name" class="driver-search"></div>
                <div><label>차량번호</label><input type="text" name="q_c_num" id="q_c_num" class="driver-search"></div>
                <div><label>기사운임</label><input type="number" name="q_fee_out"></div>
            </div>
            <div style="text-align:right;"><button type="button" class="btn-save" style="background:#e67e22;" onclick="saveLedger('quickOrderForm')">장부 즉시 등록</button></div>
        </form>
    </div>
    <div class="section">
        <h3>1. 장부 상세 데이터 입력</h3>
        <form id="ledgerForm"><div class="scroll-x"><table><thead><tr><th>관리</th>{"".join([f"<th>{c['n']}</th>" for c in FULL_COLUMNS])}</tr></thead><tbody><tr><td>-</td>{"".join([f"<td><input type='{c.get('t', 'text')}' name='{c['k']}' class='{c.get('c', '')}'></td>" for c in FULL_COLUMNS])}</tr></tbody></table></div>
        <div style="text-align:right; margin-top:15px;"><button type="button" class="btn-save" onclick="saveLedger('ledgerForm')">상세 저장 및 추가 ↓</button></div></form>
    </div>
    <div class="section"><h3>2. 장부 목록 (원천 데이터)</h3><input type="text" id="ledgerSearch" class="search-bar" placeholder="실시간 검색..." onkeyup="filterLedger()">
    <div class="scroll-x"><table><thead><tr><th>관리</th>{"".join([f"<th>{c['n']}</th>" for c in FULL_COLUMNS])}</tr></thead><tbody id="ledgerBody"></tbody></table></div></div>
    """
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys=col_keys_json)

@app.route('/settlement')
def settlement():
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    q_status = request.args.get('status', ''); q_name = request.args.get('name', '')
    rows = conn.execute("SELECT * FROM ledger ORDER BY dispatch_dt DESC").fetchall(); conn.close()
    table_rows = ""; today = datetime.now()
    
    for row in rows:
        in_dt = row['in_dt']; out_dt = row['out_dt']; pay_due_dt = row['pay_due_dt']
        pre_post = row['pre_post']; dispatch_dt_str = row['dispatch_dt']
        tax_img = row['tax_img'] or ""; ship_img = row['ship_img'] or ""
        
        misu_status = "미수"; misu_color = "bg-red"
        if in_dt:
            misu_status = "수금완료"; misu_color = "bg-green"
        else:
            is_over_30 = False
            if dispatch_dt_str:
                try:
                    d_dt = datetime.fromisoformat(dispatch_dt_str.replace(' ', 'T'))
                    if today > d_dt + timedelta(days=30): is_over_30 = True
                except: pass
            is_due_passed = False
            if pay_due_dt:
                try:
                    p_due = datetime.strptime(pay_due_dt, "%Y-%m-%d")
                    if today.date() > p_due.date(): is_due_passed = True
                except: pass
            if not pre_post and not in_dt and not pay_due_dt:
                if is_over_30: misu_status = "미수"; misu_color = "bg-red"
                else: misu_status = "조건부미수금"; misu_color = "bg-blue"
            elif is_due_passed or pre_post:
                misu_status = "미수"; misu_color = "bg-red"

        pay_status = "미지급"; pay_color = "bg-red"
        if out_dt:
            pay_status = "지급완료"; pay_color = "bg-green"
        else:
            has_tax_img = any('static' in p for p in tax_img.split(','))
            has_ship_img = any('static' in p for p in ship_img.split(','))
            if in_dt and has_tax_img and has_ship_img:
                pay_status = "미지급"; pay_color = "bg-red"
            else:
                pay_status = "조건부미지급"; pay_color = "bg-blue"

        misu_btn = f'<button class="btn-status {misu_color}" onclick="changeStatus({row["id"]}, \'in_dt\', \'{today.strftime("%Y-%m-%d")}\')">{misu_status}</button>'
        tax_issued_btn = f'<button class="btn-status {"bg-green" if row["tax_chk"]=="발행완료" else "bg-orange"}" onclick="changeStatus({row["id"]}, \'tax_chk\', \'발행완료\')">{row["tax_chk"] if row["tax_chk"] else "미발행"}</button>'
        pay_btn = f'<button class="btn-status {pay_color}" onclick="changeStatus({row["id"]}, \'out_dt\', \'{today.strftime("%Y-%m-%d")}\')">{pay_status}</button>'
        
        def make_direct_links(ledger_id, img_type, raw_paths):
            paths = [p.strip() for p in (raw_paths or "").split(',')] if raw_paths else []
            links_html = '<div style="display:flex; gap:3px; justify-content:center;">'
            for i in range(1, 6):
                has_file = len(paths) >= i and paths[i-1].startswith('static')
                css_class = "link-btn has-file" if has_file else "link-btn"
                links_html += f'<a href="/upload_evidence/{ledger_id}?type={img_type}&seq={i}" target="_blank" class="{css_class}">{i}</a>'
            links_html += '</div>'
            return links_html

        table_rows += f"<tr><td>{row['client_name']}</td><td>{tax_issued_btn}</td><td>{row['order_dt']}</td><td>{row['route']}</td><td>{row['d_name']}</td><td>{row['c_num']}</td><td>{row['fee']}</td><td>{misu_btn}</td><td>{row['fee_out']}</td><td>{pay_btn}</td><td>{make_direct_links(row['id'], 'tax', row['tax_img'])}</td><td>{make_direct_links(row['id'], 'ship', row['ship_img'])}</td></tr>"
    
    content = f"""<div class="section"><h2>정산 관리 (필터검색)</h2>
    <form class="filter-box" method="get">
        필터: <select name="status">
            <option value="">전체상태</option>
            <option value="misu_all" {'selected' if q_status=='misu_all' else ''}>미수금 전체</option>
            <option value="misu_only" {'selected' if q_status=='misu_only' else ''}>미수</option>
            <option value="cond_misu" {'selected' if q_status=='cond_misu' else ''}>조건부미수</option>
            <option value="pay_all" {'selected' if q_status=='pay_all' else ''}>미지급 전체</option>
            <option value="pay_only" {'selected' if q_status=='pay_only' else ''}>미지급</option>
            <option value="cond_pay" {'selected' if q_status=='cond_pay' else ''}>조건부미지급</option>
            <option value="done_in" {'selected' if q_status=='done_in' else ''}>수금완료</option>
            <option value="done_out" {'selected' if q_status=='done_out' else ''}>지급완료</option>
        </select>
        <input type="text" name="name" value="{q_name}" placeholder="거래처/기사명">
        <button type="submit">조회</button>
    </form>
    <div style="margin-bottom:15px;">
        <a href="/export_misu_info?status={q_status}&name={q_name}" class="btn-status bg-red">미수금 거래처정보 엑셀</a>
        <a href="/export_pay_info?status={q_status}&name={q_name}" class="btn-status bg-orange">미지급 기사정보 엑셀</a>
    </div>
    <div class="scroll-x"><table><thead><tr><th>업체명</th><th>계산서</th><th>오더일</th><th>노선</th><th>기사명</th><th>차량번호</th><th>업체운임</th><th>수금상태</th><th>기사운임</th><th>지급상태</th><th>기사계산서(1~5)</th><th>운송장(1~5)</th></tr></thead><tbody>{table_rows}</tbody></table></div></div>"""
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys="[]")

@app.route('/statistics')
def statistics():
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    q_start = request.args.get('start', ''); q_end = request.args.get('end', '')
    q_client = request.args.get('client', '').strip(); q_driver = request.args.get('driver', '').strip()
    q_status = request.args.get('status', '')
    
    rows = conn.execute("SELECT * FROM ledger").fetchall(); conn.close()
    filtered_rows = []
    today = datetime.now()
    for row in rows:
        row_dict = dict(row)
        in_dt = row_dict['in_dt']; out_dt = row_dict['out_dt']; pay_due_dt = row_dict['pay_due_dt']
        pre_post = row_dict['pre_post']; dispatch_dt_str = row_dict['dispatch_dt']
        tax_img = row_dict['tax_img'] or ""; ship_img = row_dict['ship_img'] or ""
        order_dt = row_dict['order_dt'] or ""

        m_status = "조건부미수금" if not pre_post and not in_dt and not pay_due_dt else ("수금완료" if in_dt else "미수")
        if m_status == "조건부미수금":
            try:
                d_dt = datetime.fromisoformat(dispatch_dt_str.replace(' ', 'T'))
                if today > d_dt + timedelta(days=30): m_status = "미수"
            except: pass
        if not in_dt and pay_due_dt:
            try:
                p_due = datetime.strptime(pay_due_dt, "%Y-%m-%d")
                if today.date() > p_due.date(): m_status = "미수"
            except: pass

        p_status = "지급완료" if out_dt else "미지급"
        if not out_dt:
            has_tax = any('static' in p for p in tax_img.split(','))
            has_ship = any('static' in p for p in ship_img.split(','))
            if not (in_dt and has_tax and has_ship): p_status = "조건부미지급"

        if q_start and q_end and not (q_start <= order_dt <= q_end): continue
        if q_client and q_client not in str(row_dict['client_name']): continue
        if q_driver and q_driver not in str(row_dict['d_name']): continue
        if q_status == 'misu_all' and in_dt: continue
        if q_status == 'misu_only' and m_status != '미수': continue
        if q_status == 'cond_misu' and m_status != '조건부미수금': continue
        if q_status == 'pay_all' and out_dt: continue
        if q_status == 'pay_only' and p_status != '미지급': continue
        if q_status == 'cond_pay' and p_status != '조건부미지급': continue
        if q_status == 'done_in' and not in_dt: continue
        if q_status == 'done_out' and not out_dt: continue

        filtered_rows.append(row_dict)

    st = {'cnt': len(filtered_rows), 'fee': 0, 'fo': 0, 'prof': 0}
    df_f = pd.DataFrame(filtered_rows)
    profit_by_client_top = ""; profit_by_driver_top = ""
    full_settlement_client = ""; full_settlement_driver = ""
    
    if not df_f.empty:
        df_f['fee'] = pd.to_numeric(df_f['fee'], errors='coerce').fillna(0)
        df_f['fee_out'] = pd.to_numeric(df_f['fee_out'], errors='coerce').fillna(0)
        
        # 상위 5개 요약
        client_stats_top = df_f.groupby('client_name')['fee'].sum().sort_values(ascending=False).head(5)
        profit_by_client_top = "".join([f"<tr><td>{n}</td><td>{int(v):,}원</td></tr>" for n, v in client_stats_top.items()])
        driver_stats_top = df_f.groupby('d_name')['fee_out'].sum().sort_values(ascending=False).head(5)
        profit_by_driver_top = "".join([f"<tr><td>{n}</td><td>{int(v):,}원</td></tr>" for n, v in driver_stats_top.items()])
        
        # [수정 지점] 업체별 정산서 리스트 (부가세 10% 내역 포함)
        client_full = df_f.groupby('client_name').agg({'fee': 'sum', 'id': 'count'}).sort_values(by='fee', ascending=False)
        for n, v in client_full.iterrows():
            total_fee = int(v['fee'])
            vat = int(total_fee * 0.1)
            full_settlement_client += f"<tr><td>{n}</td><td>{int(v['id'])}건</td><td style='text-align:right;'>{total_fee:,}원</td><td style='text-align:right;'>{vat:,}원</td><td style='text-align:right; font-weight:bold;'>{total_fee+vat:,}원</td></tr>"
        
        # [수정 지점] 기사별 정산서 리스트 (부가세 10% 내역 포함)
        driver_full = df_f.groupby('d_name').agg({'fee_out': 'sum', 'id': 'count'}).sort_values(by='fee_out', ascending=False)
        for n, v in driver_full.iterrows():
            total_fo = int(v['fee_out'])
            vat = int(total_fo * 0.1)
            full_settlement_driver += f"<tr><td>{n}</td><td>{int(v['id'])}건</td><td style='text-align:right;'>{total_fo:,}원</td><td style='text-align:right;'>{vat:,}원</td><td style='text-align:right; font-weight:bold;'>{total_fo+vat:,}원</td></tr>"

    for r in filtered_rows:
        st['fee'] += int(r['fee'] or 0); st['fo'] += int(r['fee_out'] or 0)
    
    st['prof'] = st['fee'] - st['fo']
    # [수정 지점] 나의 수익 통계 부가세 계산 (매출부가세, 매입부가세, 최종수익부가세)
    fee_vat = int(st['fee'] * 0.1)
    fo_vat = int(st['fo'] * 0.1)
    prof_vat = fee_vat - fo_vat

    list_html = "".join([f"<tr><td>{r['client_name']}</td><td>{r['order_dt']}</td><td>{r['route']}</td><td>{r['d_name']}</td><td>{int(r['fee'] or 0):,}</td><td>{int(r['fee_out'] or 0):,}</td><td>{(int(r['fee'] or 0) - int(r['fee_out'] or 0)):,}</td><td>{r['in_dt'] or '미수'}</td><td>{r['out_dt'] or '미지급'}</td></tr>" for r in filtered_rows])

    content = f"""
    <div class="section">
        <h2>📊 효율적 경영 통계 (검색 필터)</h2>
        <form class="filter-box" method="get">
            기간: <input type="date" name="start" value="{q_start}"> ~ <input type="date" name="end" value="{q_end}">
            업체: <input type="text" name="client" value="{q_client}" placeholder="업체명">
            기사: <input type="text" name="driver" value="{q_driver}" placeholder="기사명">
            상태: <select name="status">
                <option value="">전체상태</option>
                <option value="misu_all" {'selected' if q_status=='misu_all' else ''}>미수금(전체)</option>
                <option value="misu_only" {'selected' if q_status=='misu_only' else ''}>미수</option>
                <option value="cond_misu" {'selected' if q_status=='cond_misu' else ''}>조건부미수</option>
                <option value="pay_all" {'selected' if q_status=='pay_all' else ''}>미지급(전체)</option>
                <option value="pay_only" {'selected' if q_status=='pay_only' else ''}>미지급</option>
                <option value="cond_pay" {'selected' if q_status=='cond_pay' else ''}>조건부미지급</option>
                <option value="done_in" {'selected' if q_status=='done_in' else ''}>수금완료</option>
                <option value="done_out" {'selected' if q_status=='done_out' else ''}>지급완료</option>
            </select>
            <button type="submit" class="btn">통계조회</button>
        </form>
        <div style="display:flex; gap:10px; margin-bottom:20px;">
            <div class="stat-card"><div class="stat-title">총 진행 건수</div><div class="stat-val">{st['cnt']}건</div></div>
            <div class="stat-card"><div class="stat-title">매출(업체운임 합계)</div><div class="stat-val">{st['fee']:,}원<br><small>(부가세: {fee_vat:,}원)</small><br>총합: {st['fee']+fee_vat:,}원</div></div>
            <div class="stat-card"><div class="stat-title">지출(기사운임 합계)</div><div class="stat-val">{st['fo']:,}원<br><small>(부가세: {fo_vat:,}원)</small><br>총합: {st['fo']+fo_vat:,}원</div></div>
            <div class="stat-card" style="background:#e3f2fd;"><div class="stat-title">나의 수수료 수익</div><div class="stat-val" style="color:blue;">{st['prof']:,}원<br><small>(수익부가세: {prof_vat:,}원)</small><br>실수익: {st['prof']+prof_vat:,}원</div></div>
        </div>
        <div style="margin-bottom:15px;">
            <a href="/export_stats?start={q_start}&end={q_end}&client={q_client}&driver={q_driver}&status={q_status}" class="btn-status bg-green">현재 검색 결과 엑셀 다운로드</a>
        </div>
        <div class="scroll-x"><table><thead><tr><th>업체명</th><th>오더일</th><th>노선</th><th>기사명</th><th>업체운임</th><th>기사운임</th><th>순수익</th><th>수금일</th><th>지급일</th></tr></thead><tbody>{list_html}</tbody></table></div>
        
        <hr style="margin:40px 0;">
        <div style="display:flex; gap:20px;">
            <div class="section" style="flex:1; background:#f8f9fa;">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                    <h3 style="margin:0; color:#2c3e50;">🧾 업체별 상세 정산서</h3>
                    <a href="/export_custom_settlement?type=client&start={q_start}&end={q_end}&client={q_client}&driver={q_driver}&status={q_status}" class="link-btn has-file">업체별 정산서 엑셀 다운</a>
                </div>
                <div style="max-height:400px; overflow-y:auto; background:white;">
                    <table style="width:100%;">
                        <thead style="position:sticky; top:0; background:#eee;"><tr><th>업체명</th><th>건수</th><th>공급가액</th><th>부가세</th><th>합계금액</th></tr></thead>
                        <tbody>{full_settlement_client}</tbody>
                    </table>
                </div>
            </div>
            <div class="section" style="flex:1; background:#f8f9fa;">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                    <h3 style="margin:0; color:#2c3e50;">💸 기사별 상세 정산서</h3>
                    <a href="/export_custom_settlement?type=driver&start={q_start}&end={q_end}&client={q_client}&driver={q_driver}&status={q_status}" class="link-btn has-file">기사별 정산서 엑셀 다운</a>
                </div>
                <div style="max-height:400px; overflow-y:auto; background:white;">
                    <table style="width:100%;">
                        <thead style="position:sticky; top:0; background:#eee;"><tr><th>기사명</th><th>건수</th><th>공급가액</th><th>부가세</th><th>합계금액</th></tr></thead>
                        <tbody>{full_settlement_driver}</tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
    """
    return render_template_string(BASE_HTML, content_body=content, drivers_json=json.dumps(drivers_db), clients_json=json.dumps(clients_db), col_keys="[]")

@app.route('/export_custom_settlement')
def export_custom_settlement():
    t = request.args.get('type', 'client'); s = request.args.get('start',''); e = request.args.get('end','')
    c = request.args.get('client',''); d = request.args.get('driver',''); st = request.args.get('status', '')
    
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ledger").fetchall(); conn.close()
    
    filtered_data = []
    today = datetime.now()
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
    
    # 엑셀 시트 생성용 리스트 가공
    excel_list = []
    for name, group in df.groupby(group_col):
        for idx, row in group.iterrows():
            amt = int(row[amt_col])
            vat = int(amt * 0.1)
            excel_list.append({
                '구분': name, '오더일': row['order_dt'], '노선': row['route'],
                '공급가액': amt, '부가세(10%)': vat, '합계': amt + vat
            })
        # 그룹별 합계행 추가
        g_amt = group[amt_col].sum()
        g_vat = int(g_amt * 0.1)
        excel_list.append({'구분': f'[{name}] 합계', '오더일': '-', '노선': '-', '공급가액': int(g_amt), '부가세(10%)': g_vat, '합계': int(g_amt + g_vat)})
        excel_list.append({}) # 빈줄 추가

    result_df = pd.DataFrame(excel_list)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w: result_df.to_excel(w, index=False)
    out.seek(0); return send_file(out, as_attachment=True, download_name=f"{t}_settlement.xlsx")

@app.route('/export_misu_info')
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
        export_data.append({
            '거래처명': row_dict['client_name'], '사업자번호': row_dict['biz_num'], '대표자': row_dict['biz_owner'],
            '메일': row_dict['mail'], '연락처': row_dict['c_phone'], '노선': row_dict['route'], 
            '업체운임': row_dict['fee'], '오더일': row_dict['order_dt'], '결제예정일': row_dict['pay_due_dt']
        })
    df = pd.DataFrame(export_data)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w: df.to_excel(w, index=False)
    out.seek(0); return send_file(out, as_attachment=True, download_name="misu_client_info.xlsx")

@app.route('/export_pay_info')
def export_pay_info():
    q_st = request.args.get('status', ''); q_name = request.args.get('name', '')
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ledger").fetchall(); conn.close()
    export_data = []
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
        export_data.append({
            '기사명': row_dict['d_name'], '차량번호': row_dict['c_num'], '연락처': row_dict['d_phone'],
            '은행계좌': row_dict['bank_acc'], '예금주': row_dict['tax_biz_name'], '노선': row_dict['route'],
            '기사운임': row_dict['fee_out'], '오더일': row_dict['order_dt'], '배차일': row_dict['dispatch_dt']
        })
    df = pd.DataFrame(export_data)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w: df.to_excel(w, index=False)
    out.seek(0); return send_file(out, as_attachment=True, download_name="pay_driver_info.xlsx")

@app.route('/export_stats')
def export_stats():
    s = request.args.get('start',''); e = request.args.get('end','')
    c = request.args.get('client',''); d = request.args.get('driver',''); st = request.args.get('status', '')
    conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM ledger").fetchall(); conn.close()
    data = []
    for row in rows:
        r = dict(row)
        o_dt = r['order_dt'] or ""
        if s and e and not (s <= o_dt <= e): continue
        if c and c not in str(r['client_name']): continue
        if d and d not in str(r['d_name']): continue
        data.append(r)
    df = pd.DataFrame(data)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w: df.to_excel(w, index=False)
    out.seek(0); return send_file(out, as_attachment=True, download_name="filtered_stats.xlsx")

@app.route('/upload_evidence/<int:ledger_id>', methods=['GET', 'POST'])
def upload_evidence(ledger_id):
    target_type = request.args.get('type', 'all'); target_seq = request.args.get('seq', '1')
    if request.method == 'POST':
        tax_file, ship_file = request.files.get('tax_file'), request.files.get('ship_file')
        conn = sqlite3.connect('ledger.db'); conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT tax_img, ship_img FROM ledger WHERE id = ?", (ledger_id,)).fetchone()
        def update_p(old, new, seq):
            plist = [p.strip() for p in old.split(',')] if old else [""] * 5
            while len(plist) < 5: plist.append("")
            plist[int(seq)-1] = new
            return ",".join(plist)
        if tax_file:
            path = os.path.join(UPLOAD_FOLDER, f"tax_{ledger_id}_{target_seq}_{tax_file.filename}")
            tax_file.save(path); conn.execute("UPDATE ledger SET tax_img = ? WHERE id = ?", (update_p(row['tax_img'] or "", path, target_seq), ledger_id))
        if ship_file:
            path = os.path.join(UPLOAD_FOLDER, f"ship_{ledger_id}_{target_seq}_{ship_file.filename}")
            ship_file.save(path); conn.execute("UPDATE ledger SET ship_img = ? WHERE id = ?", (update_p(row['ship_img'] or "", path, target_seq), ledger_id))
        conn.commit(); conn.close(); return "<h3>업로드 완료</h3><script>setTimeout(()=>location.reload(), 1000);</script>"
    seq_btns_html = ""
    for i in range(1, 6):
        is_active = "active" if str(i) == target_seq else ""
        btn_url = f"/upload_evidence/{ledger_id}?type={target_type}&seq={i}"
        seq_btns_html += f'<button class="seq-btn {is_active}" onclick="location.href=\'{btn_url}\'">{i}번</button>'
    return f"""<meta name="viewport" content="width=device-width,initial-scale=1.0"><style>body{{padding:20px; text-align:center; font-family:sans-serif;}} .seq-btns{{display:flex; gap:10px; justify-content:center; margin-bottom:20px;}} .seq-btn{{padding:10px 15px; border:1px solid #ccc; background:white; cursor:pointer;}} .seq-btn.active{{background:#007bff; color:white; border-color:#007bff; font-weight:bold;}} button[type="button"]{{width:100%; padding:15px; background:#28a745; color:white; border:none; border-radius:5px; font-weight:bold; cursor:pointer; margin-top:10px;}}</style><h3>증빙 업로드 - { "기사계산서" if target_type=="tax" else "운송장" }</h3><div class="seq-btns">{seq_btns_html}</div><p>현재 선택된 슬롯: <b>{target_seq}번</b></p><form id="uploadForm">파일 선택: <input type='file' id='file_input' accept='image/*' style='margin-bottom:10px;'><button type="button" onclick="processAndUpload()">전송하기</button></form><div id="status"></div><script>async function processAndUpload(){{const s=document.getElementById('status'); const fileInput = document.getElementById('file_input'); if(!fileInput.files[0]) {{ alert("파일을 선택해주세요."); return; }} s.innerText="압축 및 전송중..."; const compress=(f)=>new Promise((r)=>{{const reader=new FileReader(); reader.readAsDataURL(f); reader.onload=(e)=>{{const img=new Image(); img.src=e.target.result; img.onload=()=>{{const cvs=document.createElement('canvas'); let w=img.width,h=img.height; if(w>1200){{h*=1200/w;w=1200}} cvs.width=w;cvs.height=h; cvs.getContext('2d').drawImage(img,0,0,w,h); cvs.toBlob((b)=>r(b),'image/jpeg',0.7)}}}}}}); const fd=new FormData(); const type = "{target_type}"; const fileBlob = await compress(fileInput.files[0]); fd.append(type === 'tax' ? 'tax_file' : 'ship_file', fileBlob, 'upload.jpg'); fetch(location.href,{{method:'POST',body:fd}}).then(r=>r.text()).then(t=>{{document.body.innerHTML=t; if(window.opener) window.opener.location.reload(); }});}}</script>"""

@app.route('/api/save_ledger', methods=['POST'])
def save_ledger_api():
    data = request.json; conn = sqlite3.connect('ledger.db'); cursor = conn.cursor()
    keys = [c['k'] for c in FULL_COLUMNS]
    if 'id' in data:
        sql = ", ".join([f"{k} = ?" for k in keys]); vals = [data.get(k, '') for k in keys] + [data['id']]; cursor.execute(f"UPDATE ledger SET {sql} WHERE id = ?", vals)
    else:
        cursor.execute(f"INSERT INTO ledger ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", [data.get(k, '') for k in keys])
    if data.get('client_name'):
        cursor.execute("SELECT rowid FROM clients WHERE 업체명 = ?", (data.get('client_name'),))
        res = cursor.fetchone()
        c_d = (data.get('biz_num',''),data.get('biz_owner',''),data.get('biz_addr',''),data.get('mail',''),data.get('c_mgr_name',''),data.get('c_phone',''),data.get('pay_memo',''),data.get('biz_type1',''),data.get('biz_type2',''),data.get('client_name'))
        if res: cursor.execute("UPDATE clients SET 사업자등록번호=?,대표자명=?,사업자주소=?,메일주소=?,담당자=?,연락처=?,결제특이사항=?,종목=?,업태=? WHERE 업체명=?", c_d)
        else: cursor.execute("INSERT INTO clients (사업자등록번호,대표자명,사업자주소,메일주소,담당자,연락처,결제특이사항,종목,업태,업체명) VALUES (?,?,?,?,?,?,?,?,?,?)", c_d)
    if data.get('d_name') and data.get('c_num'):
        cursor.execute("SELECT rowid FROM drivers WHERE 기사명 = ? AND 차량번호 = ?", (data.get('d_name'), data.get('c_num')))
        res = cursor.fetchone()
        d_d = (data.get('d_phone',''),data.get('bank_acc',''),data.get('tax_biz_num',''),data.get('tax_biz_name',''),data.get('memo1',''),data.get('d_name'),data.get('c_num'))
        if res: cursor.execute("UPDATE drivers SET 연락처=?,계좌번호=?,사업자번호=?,사업자=?,메모=? WHERE 기사명=? AND 차량번호=?", d_d)
        else: cursor.execute("INSERT INTO drivers (연락처,계좌번호,사업자번호,사업자,메모,기사명,차량번호) VALUES (?,?,?,?,?,?,?)", d_d)
    conn.commit(); conn.close(); load_db_to_mem(); return jsonify({"status": "success"})

@app.route('/api/load_db_mem')
def api_load_db_mem(): load_db_to_mem(); return jsonify({"drivers": drivers_db, "clients": clients_db})

@app.route('/api/get_ledger')
def get_ledger():
    conn = sqlite3.connect('ledger.db'); df = pd.read_sql("SELECT * FROM ledger ORDER BY id DESC", conn); conn.close()
    return jsonify(df.to_dict('records'))

@app.route('/api/update_status', methods=['POST'])
def update_status():
    data = request.json; conn = sqlite3.connect('ledger.db'); conn.execute(f"UPDATE ledger SET {data['key']} = ? WHERE id = ?", (data['value'], data['id'])); conn.commit(); conn.close(); return jsonify({"status": "success"})

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