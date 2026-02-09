import os
import pandas as pd
from datetime import datetime, timedelta
from flask import Flask, render_template_string, request, jsonify, redirect, url_for, send_from_directory, session
from flask_sqlalchemy import SQLAlchemy
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = "sm_logitechs_ultimate_integrated_v62_final"
db = SQLAlchemy()

# 1. 환경 설정
basedir = os.path.abspath(os.path.dirname(__file__))
db_path = os.path.join(basedir, "instance", "logi_v2026_final.db")
upload_folder = os.path.join(basedir, "uploads")

if not os.path.exists(os.path.join(basedir, "instance")): os.makedirs(os.path.join(basedir, "instance"))
if not os.path.exists(upload_folder): os.makedirs(upload_folder)

app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{db_path}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['UPLOAD_FOLDER'] = upload_folder
db.init_app(app)

# --------------------------------------------------------------------------------
# 2. 데이터베이스 모델 (필드 완벽 보존)
# --------------------------------------------------------------------------------

class MasterClient(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    biz_type = db.Column(db.String(50))      # 사업자구분
    company = db.Column(db.String(100))      # 업체명
    issue_type = db.Column(db.String(50))    # 발행구분
    biz_num = db.Column(db.String(50))       # 사업자등록번호
    biz_name = db.Column(db.String(100))     # 사업자명
    owner_name = db.Column(db.String(50))    # 대표자명
    address = db.Column(db.String(500))      # 사업자주소
    biz_status = db.Column(db.String(100))   # 업태/업종
    biz_item = db.Column(db.String(100))     # 종목
    email = db.Column(db.String(100))        # 메일주소
    manager = db.Column(db.String(50))       # 담당자
    phone = db.Column(db.String(50))         # 연락처
    payment_memo = db.Column(db.Text)        # 결제특이사항

class MasterDriver(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50))          # 기사명
    car_num = db.Column(db.String(50))       # 차량번호
    phone = db.Column(db.String(50))         # 연락처
    account = db.Column(db.String(100))      # 계좌번호
    biz_num = db.Column(db.String(50))       # 사업자번호
    biz_name = db.Column(db.String(100))     # 사업자(상호)
    is_fixed = db.Column(db.String(50))      # 개인/고정
    memo = db.Column(db.Text)                # 메모

class TransportOrder(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_name = db.Column(db.String(100)); client_phone = db.Column(db.String(50))
    biz_num = db.Column(db.String(50)); email = db.Column(db.String(100)); address = db.Column(db.String(500))
    biz_status = db.Column(db.String(100))
    load_loc = db.Column(db.String(200)); unload_loc = db.Column(db.String(200))
    receiver_name = db.Column(db.String(50)); receiver_phone = db.Column(db.String(50))
    channel = db.Column(db.String(50)); order_date = db.Column(db.String(50)) 
    start_dt = db.Column(db.String(50)); end_dt = db.Column(db.String(50))    
    pay_method = db.Column(db.String(50)); pay_type = db.Column(db.String(50)); pay_date = db.Column(db.String(50))
    shipper_fare = db.Column(db.Integer, default=0); driver_fare = db.Column(db.Integer, default=0)
    unpaid_status = db.Column(db.String(20), default="정상"); unpaid_amount = db.Column(db.Integer, default=0)
    extra_memo = db.Column(db.Text); order_tax_img = db.Column(db.String(200)) 
    
    driver_name = db.Column(db.String(50)); car_num = db.Column(db.String(50)); driver_phone = db.Column(db.String(50))
    driver_account = db.Column(db.String(100)); driver_biz_num = db.Column(db.String(50)); driver_biz_name = db.Column(db.String(100))
    payout_check = db.Column(db.String(20), default="미배차")
    order_memo = db.Column(db.Text); driver_waybill_img = db.Column(db.String(200)); driver_tax_img = db.Column(db.String(200))
    actual_load_time = db.Column(db.String(50)); actual_unload_time = db.Column(db.String(50))
    estimated_arrival_time = db.Column(db.String(50))
    
    payment_method_order = db.Column(db.String(50))
    order_due_date = db.Column(db.String(50))
    driver_payout_method = db.Column(db.String(50))
    driver_payout_date = db.Column(db.String(50))
    is_receipt_ok = db.Column(db.Boolean, default=False)

class DeliveryLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    order_id = db.Column(db.Integer, db.ForeignKey('transport_order.id'))
    status = db.Column(db.String(50)); message = db.Column(db.Text)
    created_at = db.Column(db.String(50), default=lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

# --------------------------------------------------------------------------------
# 3. 비즈니스 로직 및 API
# --------------------------------------------------------------------------------

def save_file(file):
    if file and file.filename:
        filename = secure_filename(f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{file.filename}")
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        return filename
    return ""

def add_log(order_id, status, message):
    log = DeliveryLog(order_id=order_id, status=status, message=message)
    db.session.add(log)

@app.route('/api/autocomplete/<type>')
def autocomplete(type):
    q = request.args.get('term', '')
    if type == 'client':
        res = MasterClient.query.filter((MasterClient.company.contains(q)) | (MasterClient.phone.contains(q)) | (MasterClient.biz_num.contains(q))).all()
        return jsonify([{"label": f"{c.company} ({c.biz_num})", "value": c.company, "phone": c.phone, "biz": c.biz_num, "email": c.email, "addr": c.address, "status": c.biz_status} for c in res])
    else:
        res = MasterDriver.query.filter((MasterDriver.name.contains(q)) | (MasterDriver.car_num.contains(q)) | (MasterDriver.phone.contains(q))).all()
        return jsonify([{"label": f"{d.name} ({d.phone})", "value": d.name, "phone": d.phone, "car": d.car_num, "acc": d.account, "biz_num": d.biz_num, "biz_name": d.biz_name, "is_fixed": d.is_fixed} for d in res])

@app.route('/api/order_logs/<int:id>')
def get_order_logs(id):
    logs = DeliveryLog.query.filter_by(order_id=id).order_by(DeliveryLog.id.desc()).all()
    order = TransportOrder.query.get(id)
    return jsonify({"logs": [{"time": l.created_at, "status": l.status, "msg": l.message} for l in logs]})

@app.route('/upload/client', methods=['POST'])
def upload_client_excel():
    file = request.files.get('file')
    if not file: return redirect(url_for('index', tab='client'))
    try:
        df = pd.read_excel(file, dtype=str).fillna("")
        for _, r in df.iterrows():
            if not r.get('업체명'): continue
            db.session.add(MasterClient(
                biz_type=r.get('사업자구분',''), company=r.get('업체명',''), issue_type=r.get('발행구분',''),
                biz_num=str(r.get('사업자등록번호','')), biz_name=r.get('사업자명',''), owner_name=r.get('대표자명',''),
                address=r.get('사업자주소',''), biz_status=r.get('업태',''), biz_item=r.get('종목',''),
                email=r.get('메일주소',''), manager=r.get('담당자',''), phone=r.get('연락처',''), payment_memo=r.get('결제특이사항','')
            ))
        db.session.commit()
    except Exception: db.session.rollback()
    return redirect(url_for('index', tab='client'))

@app.route('/upload/driver', methods=['POST'])
def upload_driver_excel():
    file = request.files.get('file')
    if not file: return redirect(url_for('index', tab='driver'))
    try:
        df = pd.read_excel(file, dtype=str).fillna("")
        for _, r in df.iterrows():
            if not r.get('기사명'): continue
            db.session.add(MasterDriver(
                name=r.get('기사명'), car_num=r.get('차량번호'), phone=r.get('연락처'),
                account=r.get('계좌번호'), biz_num=r.get('사업자번호'), biz_name=r.get('사업자'),
                is_fixed=r.get('개인/고정'), memo=r.get('메모')
            ))
        db.session.commit()
    except Exception: db.session.rollback()
    return redirect(url_for('index', tab='driver'))

@app.route('/api/edit/client/<int:id>', methods=['POST'])
def edit_client(id):
    c = MasterClient.query.get_or_404(id)
    for field in ['biz_type','company','biz_num','phone','address','email']:
        setattr(c, field, request.form.get(field))
    db.session.commit(); return redirect(url_for('index', tab='client'))

@app.route('/api/edit/driver/<int:id>', methods=['POST'])
def edit_driver(id):
    d = MasterDriver.query.get_or_404(id)
    for field in ['name','car_num','phone','account']:
        setattr(d, field, request.form.get(field))
    db.session.commit(); return redirect(url_for('index', tab='driver'))

@app.route('/order/add', methods=['POST'])
def add_order():
    c_name = request.form.get('client_name')
    new_o = TransportOrder(
        client_name=c_name, client_phone=request.form.get('client_phone'), biz_num=request.form.get('biz_num'),
        load_loc=request.form.get('load_loc'), unload_loc=request.form.get('unload_loc'),
        order_date=request.form.get('order_date') or datetime.now().strftime('%Y-%m-%d %H:%M'),
        shipper_fare=int(request.form.get('shipper_fare', 0) or 0), driver_fare=int(request.form.get('driver_fare', 0) or 0),
        payout_check="미배차"
    )
    db.session.add(new_o); db.session.flush(); add_log(new_o.id, "신규등록", "오더 생성"); db.session.commit()
    return redirect(url_for('index', tab='order'))

@app.route('/order/dispatch/<int:id>', methods=['POST'])
def update_dispatch(id):
    o = TransportOrder.query.get_or_404(id)
    fields = ['load_loc','unload_loc','driver_name','car_num','driver_phone','driver_account','driver_biz_num','driver_biz_name','order_memo']
    for f in fields: setattr(o, f, request.form.get(f))
    o.shipper_fare = int(request.form.get('shipper_fare', 0) or 0)
    o.driver_fare = int(request.form.get('driver_fare', 0) or 0)
    o.payout_check = request.form.get('payout_check', '배차완료')
    add_log(o.id, o.payout_check, "배차 정보 업데이트"); db.session.commit()
    return redirect(url_for('index', tab='dispatch'))

@app.route('/order/complete/<int:id>', methods=['POST'])
def complete_order(id):
    o = TransportOrder.query.get_or_404(id)
    o.driver_waybill_img = save_file(request.files.get('driver_waybill_img'))
    if o.driver_waybill_img: o.is_receipt_ok = True
    o.driver_tax_img = save_file(request.files.get('driver_tax_img'))
    o.payout_check = "배송완료"
    add_log(o.id, "배송완료", "증빙 업로드 완료"); db.session.commit()
    return redirect(url_for('index', tab='dispatch'))

@app.route('/api/order_settle/<int:id>', methods=['POST'])
def order_settle(id):
    o = TransportOrder.query.get_or_404(id)
    action = request.form.get('action')
    if action == 'shipper_pay': o.unpaid_status = "입금완료"; add_log(o.id, "입금확인", "화주 입금 완료")
    elif action == 'driver_payout': o.payout_check = "지급완료"; add_log(o.id, "지급완료", "기사료 지급 완료")
    db.session.commit(); return redirect(url_for('index', tab=request.form.get('current_tab', 'order')))

@app.route('/delete/<type>/<int:id>')
def delete_item(type, id):
    target = db.session.get(TransportOrder, id) if type in ['order','dispatch'] else db.session.get(MasterClient, id) if type=='client' else db.session.get(MasterDriver, id)
    if target: db.session.delete(target); db.session.commit()
    return redirect(url_for('index', tab=type))

@app.route('/uploads/<filename>')
def uploaded_file(filename): return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

# --------------------------------------------------------------------------------
# 4. 기사용 페이지 (Direct Link 포함)
# --------------------------------------------------------------------------------

@app.route('/work', methods=['GET', 'POST'])
def driver_work():
    name = request.args.get('name', '').strip()
    driver = MasterDriver.query.filter_by(name=name).first()
    if not driver: return render_template_string(DRIVER_LOGIN_HTML)
    tasks = TransportOrder.query.filter_by(car_num=driver.car_num).order_by(TransportOrder.id.desc()).all()
    return render_template_string(DRIVER_WORK_HTML, driver=driver, tasks=tasks)

@app.route('/work/direct/<int:order_id>')
def driver_direct_work(order_id):
    o = TransportOrder.query.get_or_404(order_id)
    if not o.driver_name: return "배차 정보 없음", 404
    return render_template_string(DRIVER_DIRECT_WORK_HTML, t=o)

@app.route('/api/work_update/<int:id>', methods=['POST'])
def work_update(id):
    o = TransportOrder.query.get_or_404(id)
    o.actual_load_time = request.form.get('actual_load_time')
    o.actual_unload_time = request.form.get('actual_unload_time')
    wb = save_file(request.files.get('driver_waybill_img'))
    tx = save_file(request.files.get('driver_tax_img'))
    if wb: o.driver_waybill_img = wb; o.is_receipt_ok = True
    if tx: o.driver_tax_img = tx
    if o.actual_unload_time and (wb or tx): o.payout_check = "배송완료"
    db.session.commit(); return redirect(url_for('driver_work', name=o.driver_name))

# --------------------------------------------------------------------------------
# 5. 메인 화면 로직 (현황판 데이터 매핑 추가)
# --------------------------------------------------------------------------------

@app.route('/')
@app.route('/tab/<tab>')
def index(tab='dashboard'):
    query = TransportOrder.query.order_by(TransportOrder.id.desc()).all()
    stats = {'today': 0, 'total_ar': 0, 'total_ap': 0}
    today = datetime.now().strftime('%Y-%m-%d')
    
    for o in query:
        # 현황판용 가로 데이터 구성
        o.dashboard_row = {
            "오더일": o.order_date[:10] if o.order_date else "",
            "노선": f"{o.load_loc} ➔ {o.unload_loc}",
            "기사명": o.driver_name or "-",
            "차량번호": o.car_num or "-",
            "연락처": o.driver_phone or "-",
            "업체명": o.client_name or "-",
            "사업자번호": o.biz_num or "-",
            "운임": f"{o.shipper_fare:,}",
            "공급가액": f"{int(o.shipper_fare/1.1):,}",
            "부가세": f"{int(o.shipper_fare - (o.shipper_fare/1.1)):,}",
            "지급일": o.driver_payout_date or "-",
            "기사운임": f"{o.driver_fare:,}",
            "순수입": f"{(o.shipper_fare - o.driver_fare):,}"
        }
        if o.unpaid_status != '입금완료' and o.payout_check in ['배송완료', '지급완료']: stats['total_ar'] += o.shipper_fare
        if o.payout_check == '배송완료': stats['total_ap'] += o.driver_fare
        if o.order_date and o.order_date[:10] == today: stats['today'] += o.shipper_fare

    clients = MasterClient.query.all(); drivers = MasterDriver.query.all()
    return render_template_string(ADMIN_HTML, tab=tab, orders=query, clients=clients, drivers=drivers, stats=stats, now_time=datetime.now().strftime('%Y-%m-%dT%H:%M'))

# --------------------------------------------------------------------------------
# 통합 HTML (v62 디자인 + 현황판 + 클릭 해결)
# --------------------------------------------------------------------------------

ADMIN_HTML = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>주식회사 에스엠로지텍 TMS v62.0</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script src="https://code.jquery.com/ui/1.12.1/jquery-ui.js"></script>
    <link rel="stylesheet" href="https://code.jquery.com/ui/1.12.1/themes/base/jquery-ui.css">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    <style>
        @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css');
        body { font-family: 'Pretendard', sans-serif; background-color: #f1f5f9; color: #1e293b; }
        .glass-card { background: white; border-radius: 1.5rem; border: 1px solid #e2e8f0; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05); }
        .tab-active { background: #2563eb; color: white !important; font-weight: 800; box-shadow: 0 10px 15px -3px rgba(37, 99, 235, 0.2); }
        .status-badge { padding: 4px 10px; border-radius: 9999px; font-weight: 800; font-size: 10px; }
        .modal { display: none; position: fixed; inset: 0; background: rgba(15, 23, 42, 0.7); backdrop-filter: blur(4px); z-index: 100; align-items: center; justify-content: center; }
        th { background: #f8fafc; color: #64748b; font-size: 11px; font-weight: 800; padding: 15px; border-bottom: 2px solid #e2e8f0; text-align: center; }
        td { padding: 15px; border-bottom: 1px solid #f1f5f9; font-size: 13px; font-weight: 600; text-align: center; }
        .form-input { width: 100%; border: 1px solid #e2e8f0; border-radius: 0.75rem; padding: 0.75rem; font-weight: 700; font-size: 13px; outline: none; }
        .form-label { font-size: 11px; font-weight: 800; color: #64748b; margin-bottom: 0.25rem; display: block; margin-left: 0.25rem; }
    </style>
</head>
<body class="p-4 md:p-8">
    <div class="max-w-[1850px] mx-auto space-y-8">
        <header class="flex flex-col md:flex-row md:items-center justify-between gap-6 bg-white p-6 rounded-[2rem] shadow-sm border border-white">
            <h1 class="text-3xl font-[900] tracking-tighter text-slate-900 flex items-center gap-3">
                <span class="text-blue-600 uppercase italic">SM LOGITECHS</span> <span class="bg-slate-800 text-white px-3 py-1 rounded-2xl text-xl not-italic">TMS v62</span>
            </h1>
            <nav class="flex gap-1 p-1 bg-slate-100 rounded-2xl overflow-x-auto">
                <a href="/tab/dashboard" class="whitespace-nowrap px-6 py-3 rounded-xl font-bold text-slate-500 hover:bg-white {{ 'tab-active' if tab == 'dashboard' }}">📊 현황판보기</a>
                {% for t in ['order','dispatch','client','driver','revenue','unpaid'] %}
                <a href="/tab/{{t}}" class="whitespace-nowrap px-6 py-3 rounded-xl font-bold text-slate-500 hover:bg-white {{ 'tab-active' if tab == t }}">
                    {{ {'order':'오더관리','dispatch':'배차관리','client':'거래처관리','driver':'기사관리','revenue':'매출정산','unpaid':'미수금'}[t] }}
                </a>
                {% endfor %}
            </nav>
        </header>

        <div class="grid grid-cols-2 lg:grid-cols-4 gap-6">
            <div class="glass-card p-6 border-l-[6px] border-blue-600">
                <p class="text-[11px] font-black text-slate-400 uppercase tracking-widest">오늘 매출</p>
                <h2 class="text-2xl font-black mt-1">{{ "{:,}".format(stats['today']) }}원</h2>
            </div>
            <div class="glass-card p-6 border-l-[6px] border-red-500">
                <p class="text-[11px] font-black text-slate-400 uppercase tracking-widest">미수금(AR)</p>
                <h2 class="text-2xl font-black mt-1">{{ "{:,}".format(stats['total_ar']) }}원</h2>
            </div>
            <div class="glass-card p-6 border-l-[6px] border-orange-500">
                <p class="text-[11px] font-black text-slate-400 uppercase tracking-widest">미지급(AP)</p>
                <h2 class="text-2xl font-black mt-1">{{ "{:,}".format(stats['total_ap']) }}원</h2>
            </div>
            <div class="glass-card p-6 border-l-[6px] border-slate-800">
                <p class="text-[11px] font-black text-slate-400 uppercase tracking-widest">총 오더 건수</p>
                <h2 class="text-2xl font-black mt-1">{{ orders|length }}건</h2>
            </div>
        </div>

        {% if tab == 'dashboard' %}
        <section class="glass-card overflow-hidden border-t-8 border-blue-600 shadow-2xl">
            <div class="p-4 bg-blue-50 border-b flex justify-between items-center"><h3 class="font-black text-blue-900 uppercase">Unified Logistics Dashboard (Horizontal Scroll)</h3></div>
            <div class="overflow-auto max-h-[600px]">
                <table class="w-full text-[11px] whitespace-nowrap border-collapse">
                    <thead class="sticky top-0 z-10">
                        <tr class="bg-slate-800 text-white">
                            <th class="p-4">오더일</th><th class="p-4">노선</th><th class="p-4">기사명</th><th class="p-4">차량번호</th><th class="p-4">연락처</th>
                            <th class="p-4">업체명</th><th class="p-4">사업자번호</th><th class="p-4 text-blue-300">운임</th><th class="p-4">공급가액</th>
                            <th class="p-4">부가세</th><th class="p-4 text-orange-300">지급일</th><th class="p-4 text-orange-300">기사운임</th><th class="p-4 text-emerald-400">순수입</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for o in orders %}
                        <tr class="hover:bg-blue-50 transition-colors border-b">
                            <td class="p-4 font-bold">{{ o.dashboard_row['오더일'] }}</td>
                            <td class="p-4 font-black text-slate-800">{{ o.dashboard_row['노선'] }}</td>
                            <td class="p-4 text-blue-600 font-bold">{{ o.dashboard_row['기사명'] }}</td>
                            <td class="p-4">{{ o.dashboard_row['차량번호'] }}</td>
                            <td class="p-4 text-slate-400">{{ o.dashboard_row['연락처'] }}</td>
                            <td class="p-4 font-black">{{ o.dashboard_row['업체명'] }}</td>
                            <td class="p-4">{{ o.dashboard_row['사업자번호'] }}</td>
                            <td class="p-4 text-right font-black text-blue-600">{{ o.dashboard_row['운임'] }}원</td>
                            <td class="p-4 text-right text-slate-400">{{ o.dashboard_row['공급가액'] }}</td>
                            <td class="p-4 text-right text-slate-400">{{ o.dashboard_row['부가세'] }}</td>
                            <td class="p-4 text-orange-600 font-bold">{{ o.dashboard_row['지급일'] }}</td>
                            <td class="p-4 text-right font-black text-orange-600">{{ o.dashboard_row['기사운임'] }}원</td>
                            <td class="p-4 text-right font-black text-emerald-600 bg-emerald-50">{{ o.dashboard_row['순수입'] }}원</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </section>
        {% endif %}

        {% if tab == 'order' %}
        <section class="glass-card p-8 border-t-8 border-slate-800 shadow-xl">
            <h3 class="text-lg font-black mb-6 italic underline decoration-blue-500">📑 발주 정보 통합 등록</h3>
            <form action="/order/add" method="POST" class="grid grid-cols-1 md:grid-cols-4 lg:grid-cols-6 gap-4">
                <div class="md:col-span-2"><label class="form-label text-blue-600">거래처명(검색)</label><input type="text" id="c_search" name="client_name" class="form-input bg-blue-50/50" placeholder="업체명 검색"></div>
                <div><label class="form-label text-blue-600">사업자번호</label><input type="text" id="c_biz" name="biz_num" class="form-input bg-blue-50/50"></div>
                <div class="md:col-span-2"><label class="form-label font-black">🚛 노선 정보 (상차 ➔ 하차)</label><div class="flex items-center gap-2 mt-1"><input type="text" name="load_loc" class="flex-1 form-input bg-emerald-50" placeholder="상차지"><i class="fa-solid fa-arrow-right text-slate-300"></i><input type="text" name="unload_loc" class="flex-1 form-input bg-orange-50" placeholder="하차지"></div></div>
                <div><label class="form-label">오더접수일시</label><input type="datetime-local" name="order_date" value="{{ now_time }}" class="form-input text-xs"></div>
                <div><label class="form-label text-blue-600 font-black">화주운임</label><input type="number" name="shipper_fare" class="form-input border-blue-200 font-black text-blue-700"></div>
                <div><label class="form-label text-orange-600 font-black">기사운임</label><input type="number" name="driver_fare" class="form-input border-orange-200 font-black text-orange-700"></div>
                <div class="md:col-span-2 flex items-end"><button type="submit" class="w-full bg-blue-600 text-white py-4 rounded-2xl font-black shadow-lg hover:-translate-y-1 transition-all">오더 최종 확정</button></div>
                <input type="hidden" name="client_phone" id="c_phone"><input type="hidden" name="email" id="c_email"><input type="hidden" name="address" id="c_addr"><input type="hidden" name="biz_status" id="c_status">
            </form>
        </section>

        <section class="glass-card overflow-hidden">
            <div class="overflow-x-auto">
                <table class="w-full">
                    <thead><tr><th>상태</th><th>거래처/기사</th><th>노선</th><th class="text-right">화주운임</th><th class="text-right">기사운임</th><th>관리</th></tr></thead>
                    <tbody>
                        {% for o in orders %}
                        <tr class="hover:bg-slate-50 transition-colors cursor-pointer group">
                            <td onclick="openLogModal({{o.id}})"><span class="status-badge {{ 'bg-blue-600 text-white' if o.payout_check == '미배차' else 'bg-orange-500 text-white' if o.payout_check == '기사상차' else 'bg-slate-800 text-white' if o.payout_check == '배송완료' else 'bg-emerald-600 text-white' if o.payout_check == '지급완료' else 'bg-slate-200 text-slate-600' }}">{{ o.payout_check }}</span></td>
                            <td onclick="openLogModal({{o.id}})" class="text-left"><div class="font-black text-slate-900">{{ o.client_name }}</div><div class="text-[11px] font-bold text-blue-600">{{ o.driver_name or '-' }}</div></td>
                            <td onclick="openLogModal({{o.id}})" class="font-bold text-slate-500 text-[12px]">{{ o.load_loc }} ➔ {{ o.unload_loc }}</td>
                            <td onclick="openLogModal({{o.id}})" class="text-right font-black text-blue-600">{{ "{:,}".format(o.shipper_fare) }}원</td>
                            <td onclick="openLogModal({{o.id}})" class="text-right font-black text-orange-600">{{ "{:,}".format(o.driver_fare) }}원</td>
                            <td onclick="event.stopPropagation()"><a href="/delete/order/{{o.id}}" class="text-slate-300 hover:text-red-600 transition-colors" onclick="return confirm('삭제?')"><i class="fa-solid fa-trash"></i></a></td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </section>
        {% endif %}

        {% if tab == 'dispatch' %}
        <section class="glass-card overflow-hidden border-t-8 border-orange-500 shadow-2xl">
            <div class="p-6 bg-orange-50 border-b flex justify-between items-center"><h3 class="font-black text-orange-900 italic uppercase underline decoration-orange-300">Dispatch Live View (Direct Link Enable)</h3></div>
            <table class="w-full text-xs">
                <thead><tr><th>상태</th><th>화주</th><th>기사명/차번</th><th>노선</th><th class="text-right">기사운임</th><th>작업 및 링크</th></tr></thead>
                <tbody>
                    {% for o in orders if o.payout_check in ['미배차','배차완료','기사상차','배송완료'] %}
                    <tr class="hover:bg-orange-50/50 transition-all cursor-pointer">
                        <td onclick="openLogModal({{o.id}})"><span class="status-badge {{ 'bg-orange-600 text-white' if o.payout_check == '기사상차' else 'bg-blue-600 text-white' if o.payout_check == '미배차' else 'bg-slate-800 text-white' }}">{{ o.payout_check }}</span></td>
                        <td onclick="openLogModal({{o.id}})" class="font-black">{{ o.client_name }}</td>
                        <td onclick="openLogModal({{o.id}})">
                            <div class="font-black text-blue-600">{{ o.driver_name or '미배정' }}</div>
                            <div class="text-[10px] text-slate-400">{{ o.car_num }}</div>
                        </td>
                        <td onclick="openLogModal({{o.id}})" class="font-bold text-slate-700">{{ o.load_loc }} ➔ {{ o.unload_loc }}</td>
                        <td onclick="openLogModal({{o.id}})" class="text-right font-black text-orange-600">{{ "{:,}".format(o.driver_fare) }}원</td>
                        <td class="flex gap-2 justify-center" onclick="event.stopPropagation()">
                            <button onclick="openDispatchModal({{o.id}}, '{{o.client_name}}', {{o.shipper_fare}}, {{o.driver_fare}}, '{{o.load_loc}}', '{{o.unload_loc}}', '{{o.driver_name or ''}}', '{{o.car_num or ''}}', '{{o.driver_phone or ''}}', '{{o.driver_account or ''}}', '{{o.payout_check}}', '', '', '{{o.driver_biz_num}}', '{{o.driver_biz_name}}', '{{o.order_memo}}')" 
                                    class="bg-blue-600 text-white px-4 py-2 rounded-xl text-[11px] font-black shadow-lg">수정</button>
                            <button onclick="copyWorkLink({{o.id}})" class="bg-emerald-100 text-emerald-700 px-3 py-2 rounded-xl text-[10px] font-black hover:bg-emerald-200 transition-all">
                                <i class="fa-solid fa-link"></i> 링크복사
                            </button>
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </section>
        {% endif %}

        {% if tab == 'client' or tab == 'driver' %}
        <section class="space-y-6">
            <div class="glass-card p-6 border-t-8 border-indigo-600 bg-indigo-50/20">
                <h3 class="font-black mb-4"><i class="fa-solid fa-file-excel mr-2 text-indigo-600"></i> {{ tab|upper }} 엑셀 대량 업로드</h3>
                <form action="/upload/{{ tab }}" method="POST" enctype="multipart/form-data" class="flex gap-4">
                    <input type="file" name="file" class="bg-white p-3 rounded-xl flex-1 font-bold border-2 border-dashed border-indigo-200">
                    <button class="bg-indigo-600 text-white px-10 rounded-xl font-black shadow-lg">업로드 실행</button>
                </form>
            </div>
            <div class="glass-card overflow-hidden">
                <table class="w-full text-xs">
                    {% if tab == 'client' %}
                    <thead><tr><th>업체명</th><th>사업자번호</th><th>대표자</th><th>연락처</th><th>주소</th><th>관리</th></tr></thead>
                    <tbody>
                        {% for c in clients %}
                        <tr class="hover:bg-indigo-50/50">
                            <td class="font-black text-indigo-600">{{ c.company }}</td><td>{{ c.biz_num }}</td><td>{{ c.owner_name }}</td><td>{{ c.phone }}</td><td class="max-w-[200px] truncate">{{ c.address }}</td>
                            <td><button onclick="openClientEditModal({{c.id}},'','{{c.company}}','','{{c.biz_num}}','','','{{c.address}}','','','{{c.email}}','','{{c.phone}}','')" class="text-blue-600 font-bold underline">수정</button></td>
                        </tr>
                        {% endfor %}
                    </tbody>
                    {% else %}
                    <thead><tr><th>기사명</th><th>차량번호</th><th>연락처</th><th>계좌정보</th><th>관리</th></tr></thead>
                    <tbody>
                        {% for d in drivers %}
                        <tr class="hover:bg-slate-50">
                            <td class="font-black text-blue-700">{{ d.name }}</td><td class="font-bold">{{ d.car_num }}</td><td>{{ d.phone }}</td><td>{{ d.account }}</td>
                            <td><button onclick="openDriverEditModal({{d.id}},'{{d.name}}','{{d.car_num}}','{{d.phone}}','{{d.account}}','','','','')" class="text-blue-600 font-bold underline">수정</button></td>
                        </tr>
                        {% endfor %}
                    </tbody>
                    {% endif %}
                </table>
            </div>
        </section>
        {% endif %}
    </div>

    <div id="logModal" class="modal">
        <div class="bg-white max-w-2xl w-full p-8 rounded-[2.5rem] shadow-2xl mx-4">
            <h3 class="text-2xl font-black mb-6 italic text-slate-800 uppercase">Order Detailed Log</h3>
            <div id="log_content" class="max-h-[300px] overflow-y-auto space-y-3 mb-6 pr-2 border-y py-4 border-slate-100 text-xs"></div>
            <div class="grid grid-cols-2 gap-4">
                <form action="" id="shipperPayForm" method="POST" class="bg-blue-50 p-6 rounded-3xl border border-blue-100">
                    <input type="hidden" name="action" value="shipper_pay"><input type="hidden" name="current_tab" value="{{tab}}">
                    <button class="w-full bg-blue-600 text-white py-4 rounded-2xl font-black shadow-lg">입금 확인 완료</button>
                </form>
                <form action="" id="driverPayForm" method="POST" class="bg-orange-50 p-6 rounded-3xl border border-orange-100">
                    <input type="hidden" name="action" value="driver_payout"><input type="hidden" name="current_tab" value="{{tab}}">
                    <button class="w-full bg-orange-600 text-white py-4 rounded-2xl font-black shadow-lg">지급 처리 완료</button>
                </form>
            </div>
            <button onclick="closeModal()" class="w-full mt-6 py-2 text-slate-400 font-bold">창 닫기</button>
        </div>
    </div>

    <div id="dispatchModal" class="modal">
        <div class="bg-white max-w-2xl w-full p-8 rounded-[2.5rem] shadow-2xl mx-4 border-t-8 border-orange-600 overflow-y-auto max-h-[90vh]">
            <h3 class="text-xl font-black mb-6 uppercase text-orange-600 text-center italic">배차 및 운송정보 수정</h3>
            <form action="" id="dispatchForm" method="POST" class="grid grid-cols-2 gap-4">
                <input type="hidden" name="payout_check" id="d_status_input" value="배차완료">
                <div class="col-span-2 space-y-2 bg-slate-50 p-4 rounded-2xl border">
                    <label class="form-label text-orange-600 font-black">🚛 노선 정보 수정</label>
                    <div class="flex items-center gap-2"><input type="text" id="d_load" name="load_loc" class="flex-1 form-input border-emerald-200" placeholder="상차"><i class="fa-solid fa-arrow-right text-slate-300"></i><input type="text" id="d_unload" name="unload_loc" class="flex-1 form-input border-orange-200" placeholder="하차"></div>
                </div>
                <div class="col-span-1"><label class="form-label">기사명(검색)</label><input type="text" id="d_search" name="driver_name" class="form-input bg-blue-50"></div>
                <div class="col-span-1"><label class="form-label">차량번호</label><input type="text" id="d_car" name="car_num" class="form-input bg-blue-50"></div>
                <div class="col-span-1"><label class="form-label text-blue-600 font-black">화주운임</label><input type="number" id="d_sfare" name="shipper_fare" class="form-input border-blue-200 font-black text-blue-700"></div>
                <div class="col-span-1"><label class="form-label text-orange-600 font-black">기사운임</label><input type="number" id="d_dfare" name="driver_fare" class="form-input border-orange-200 font-black text-orange-700"></div>
                <div class="col-span-2 flex gap-3 mt-4">
                    <button type="submit" onclick="setStatus('배차완료')" class="flex-1 bg-blue-600 text-white py-4 rounded-2xl font-black shadow-lg">배차정보 저장</button>
                    <button type="button" onclick="closeModal()" class="flex-1 bg-slate-200 py-4 rounded-2xl font-black">닫기</button>
                </div>
            </form>
            <div id="complete_section" class="mt-8 pt-8 border-t-2 border-dashed border-slate-100 hidden text-center">
                <h4 class="text-sm font-black text-emerald-600 mb-4 uppercase">Evidence Submission</h4>
                <form id="completeForm" action="" method="POST" enctype="multipart/form-data" class="grid grid-cols-2 gap-4">
                    <input type="file" name="driver_waybill_img" class="text-[10px]"><input type="file" name="driver_tax_img" class="text-[10px]">
                    <button type="submit" class="col-span-2 bg-emerald-600 text-white py-4 rounded-2xl font-black shadow-xl hover:bg-emerald-700 transition-colors">최종 배송 완료 및 제출</button>
                </form>
            </div>
        </div>
    </div>

    <div id="clientEditModal" class="modal"><div class="bg-white max-w-4xl w-full p-8 rounded-[2.5rem] border-t-8 border-indigo-600"><h3 class="text-xl font-black mb-6 italic">거래처 정보 수정</h3><form id="clientEditForm" method="POST" class="grid grid-cols-2 md:grid-cols-4 gap-4"><input type="text" name="company" id="e_c_company" class="form-input" placeholder="업체명"><input type="text" name="biz_num" id="e_c_biz_num" class="form-input" placeholder="사업자번호"><input type="text" name="phone" id="e_c_phone" class="form-input" placeholder="연락처"><input type="text" name="address" id="e_c_address" class="form-input col-span-2" placeholder="주소"><input type="text" name="email" id="e_c_email" class="form-input" placeholder="이메일"><div class="col-span-4 flex gap-2 mt-4"><button type="submit" class="flex-1 bg-indigo-600 text-white py-4 rounded-xl font-black">업데이트</button><button type="button" onclick="closeModal()" class="flex-1 bg-slate-200 py-4 rounded-xl font-black">취소</button></div></form></div></div>
    <div id="driverEditModal" class="modal"><div class="bg-white max-w-2xl w-full p-8 rounded-[2.5rem] border-t-8 border-slate-900"><h3 class="text-xl font-black mb-6 italic">기사 정보 수정</h3><form id="driverEditForm" method="POST" class="grid grid-cols-2 gap-4"><input type="text" name="name" id="e_d_name" class="form-input" placeholder="기사명"><input type="text" name="car_num" id="e_d_car" class="form-input" placeholder="차량번호"><input type="text" name="phone" id="e_d_phone" class="form-input" placeholder="연락처"><input type="text" name="account" id="e_d_account" class="form-input" placeholder="계좌번호"><div class="col-span-2 flex gap-2 mt-4"><button type="submit" class="flex-1 bg-slate-900 text-white py-4 rounded-xl font-black">수정 완료</button><button type="button" onclick="closeModal()" class="flex-1 bg-slate-200 py-4 rounded-xl font-black">취소</button></div></form></div></div>

    <script>
        $(document).ready(function() {
            $("#c_search").autocomplete({
                source: "/api/autocomplete/client",
                select: function(e, ui) {
                    $("#c_search").val(ui.item.value); $("#c_biz").val(ui.item.biz);
                    $("#c_phone").val(ui.item.phone); $("#c_email").val(ui.item.email);
                    $("#c_addr").val(ui.item.addr); $("#c_status").val(ui.item.status); return false;
                }
            });
            $("#d_search").autocomplete({
                source: "/api/autocomplete/driver",
                select: function(e, ui) {
                    $("#d_search").val(ui.item.value); $("#d_car").val(ui.item.car);
                }
            });
        });

        function setStatus(st) { $("#d_status_input").val(st); }
        function copyWorkLink(id) {
            const link = window.location.origin + "/work/direct/" + id;
            const t = document.createElement("input"); t.value = link; document.body.appendChild(t); t.select(); document.execCommand("copy"); document.body.removeChild(t);
            alert("기사 전용 링크 복사됨: " + link);
        }
        function openLogModal(id) {
            $("#shipperPayForm").attr("action", "/api/order_settle/"+id);
            $("#driverPayForm").attr("action", "/api/order_settle/"+id);
            $("#logModal").css("display","flex").fadeIn(100); 
            fetch('/api/order_logs/'+id).then(r=>r.json()).then(data=>{
                let html = data.logs.map(l=>`<div class='p-3 bg-slate-50 rounded-2xl mb-2'><p class='text-[10px] text-slate-400'>${l.time}</p><p class='font-black text-xs text-slate-700'>[${l.status}] ${l.msg}</p></div>`).join('');
                $("#log_content").html(html || "기록 없음");
            });
        }
        function openDispatchModal(id, client, s_fare, d_fare, load, unload, d_name, car, phone, acc, status, p_method, p_date, biz_num, biz_name, memo) {
            $("#dispatchForm").attr("action", "/order/dispatch/"+id);
            $("#completeForm").attr("action", "/order/complete/"+id);
            $("#d_sfare").val(s_fare); $("#d_dfare").val(d_fare);
            $("#d_load").val(load); $("#d_unload").val(unload);
            $("#d_search").val(d_name); $("#d_car").val(car);
            if(status === '배차완료' || status === '기사상차') { $("#complete_section").show(); } else { $("#complete_section").hide(); }
            $("#dispatchModal").css("display","flex").fadeIn(100);
        }
        function openClientEditModal(id, bt, co, it, bn, bnm, onm, ad, bs, bi, em, mg, ph, pm) {
            $("#clientEditForm").attr("action", "/api/edit/client/" + id);
            $("#e_c_company").val(co); $("#e_c_biz_num").val(bn); $("#e_c_phone").val(ph); $("#e_c_address").val(ad); $("#e_c_email").val(em);
            $("#clientEditModal").css("display", "flex");
        }
        function openDriverEditModal(id, n, c, p, a, b, bn, f, m) {
            $("#driverEditForm").attr("action", "/api/edit/driver/" + id);
            $("#e_d_name").val(n); $("#e_d_car").val(c); $("#e_d_phone").val(p); $("#e_d_account").val(a);
            $("#driverEditModal").css("display", "flex");
        }
        function closeModal() { $(".modal").hide(); }
    </script>
</body>
</html>
"""

DRIVER_LOGIN_HTML = """
<!DOCTYPE html>
<html lang="ko">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>SM DRIVER LOGIN</title><script src="https://cdn.tailwindcss.com"></script></head>
<body class="bg-slate-900 flex items-center justify-center min-h-screen p-6"><div class="w-full max-w-sm bg-slate-800 p-8 rounded-[3rem] border border-slate-700 text-center shadow-2xl">
    <h1 class="text-3xl font-black text-blue-500 mb-8 italic uppercase tracking-widest underline decoration-white">SM DRIVER LOGIN</h1>
    <form action="/work" method="GET" class="space-y-4">
        <input type="text" name="name" placeholder="기사님 성함" class="w-full p-5 rounded-2xl bg-slate-900 text-white font-black text-center" required>
        <button class="w-full bg-blue-600 text-white py-5 rounded-2xl font-black text-xl shadow-xl active:scale-95 transition-all">업무 리스트 확인</button>
    </form>
</div></body>
</html>
"""

DRIVER_WORK_HTML = """
<!DOCTYPE html>
<html lang="ko">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>배송 업무 - {{ driver.name }}</title><script src="https://cdn.tailwindcss.com"></script></head>
<body class="bg-slate-50 pb-20">
    <div class="bg-slate-900 text-white p-6 rounded-b-[2.5rem] shadow-xl sticky top-0 z-50 flex justify-between items-center">
        <h1 class="text-xl font-black">{{ driver.name }} <span class="text-slate-400 text-xs font-light">[{{ driver.car_num }}]</span></h1>
    </div>
    <div class="p-4 space-y-6">
        {% for t in tasks %}
        <div class="bg-white p-6 rounded-[2.5rem] shadow-lg border border-slate-100">
            <div class="flex justify-between items-center mb-2"><span class="px-3 py-1 bg-blue-100 text-blue-700 rounded-full text-[10px] font-black">{{ t.payout_check }}</span><span class="text-[10px] text-slate-300 font-bold">{{ t.order_date }}</span></div>
            <h3 class="text-xl font-black mb-4">{{ t.load_loc }} ➔ {{ t.unload_loc }}</h3>
            <p class="text-xs text-slate-400 mb-4 font-bold">{{ t.client_name }}</p>
            <form action="/api/work_update/{{ t.id }}" method="POST" enctype="multipart/form-data" class="space-y-4">
                <div class="grid grid-cols-2 gap-3">
                    <div><label class="text-[10px] font-black text-slate-400 uppercase text-center block">픽업 시간</label><input type="time" name="actual_load_time" value="{{ t.actual_load_time }}" class="w-full p-4 bg-slate-50 rounded-2xl font-black border-none text-center"></div>
                    <div><label class="text-[10px] font-black text-slate-400 uppercase text-center block">완료 시간</label><input type="time" name="actual_unload_time" value="{{ t.actual_unload_time }}" class="w-full p-4 bg-slate-50 rounded-2xl font-black border-none text-center"></div>
                </div>
                <div class="grid grid-cols-2 gap-3">
                    <label class="bg-blue-50 p-4 rounded-2xl text-center cursor-pointer active:bg-blue-100"><span class="text-[11px] font-black text-blue-600 block">📸 인수증</span><input type="file" name="driver_waybill_img" class="hidden" accept="image/*" capture="camera"></label>
                    <label class="bg-orange-50 p-4 rounded-2xl text-center cursor-pointer active:bg-orange-100"><span class="text-[11px] font-black text-orange-600 block">📸 계산서</span><input type="file" name="driver_tax_img" class="hidden" accept="image/*" capture="camera"></label>
                </div>
                <button class="w-full bg-slate-900 text-white py-5 rounded-[2rem] font-black shadow-lg active:scale-95 transition-all">업무 완료 업데이트</button>
            </form>
        </div>
        {% endfor %}
    </div>
</body>
</html>
"""

DRIVER_DIRECT_WORK_HTML = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>SM로지텍 증빙제출</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
</head>
<body class="bg-slate-950 text-white p-4">
    <div class="max-w-md mx-auto py-6">
        <div class="text-center mb-6">
            <h1 class="text-2xl font-black text-blue-500 italic tracking-tighter">SM LOGITECHS</h1>
            <p class="text-slate-500 text-xs mt-1 font-bold italic text-white underline decoration-blue-500">배송 증빙 다이렉트 업로드</p>
        </div>
        <div class="bg-slate-900 rounded-[2.5rem] p-6 shadow-2xl border border-slate-800">
            <div class="mb-8 bg-slate-950/50 p-5 rounded-3xl border border-slate-800">
                <p class="text-[10px] text-blue-400 font-black mb-1">운송 요약</p>
                <h2 class="text-xl font-black mb-2">{{ t.load_loc }} ➔ {{ t.unload_loc }}</h2>
                <div class="flex justify-between text-xs font-bold text-slate-400">
                    <span>{{ t.client_name }}</span>
                    <span class="text-blue-500 font-black">{{ t.driver_name }} 기사님</span>
                </div>
            </div>
            <form action="/api/work_update/{{ t.id }}" method="POST" enctype="multipart/form-data" class="space-y-6">
                <div class="grid grid-cols-2 gap-4">
                    <div class="space-y-2">
                        <label class="text-[10px] font-black text-slate-500 ml-2 italic uppercase">상차 시간</label>
                        <input type="time" name="actual_load_time" value="{{ t.actual_load_time or '' }}" class="w-full p-4 bg-slate-950 rounded-2xl text-white font-black text-center border border-slate-800 focus:border-blue-500 outline-none">
                    </div>
                    <div class="space-y-2">
                        <label class="text-[10px] font-black text-slate-500 ml-2 italic uppercase">하차 시간</label>
                        <input type="time" name="actual_unload_time" value="{{ t.actual_unload_time or '' }}" class="w-full p-4 bg-slate-950 rounded-2xl text-white font-black text-center border border-slate-800 focus:border-blue-500 outline-none">
                    </div>
                </div>
                <div class="space-y-4">
                    <label class="block cursor-pointer">
                        <div class="flex items-center justify-between p-5 bg-slate-950 rounded-3xl border-2 border-dashed {{ 'border-emerald-500 bg-emerald-500/5' if t.driver_waybill_img else 'border-slate-800' }}">
                            <div class="flex items-center gap-3"><i class="fa-solid fa-camera {{ 'text-emerald-500' if t.driver_waybill_img else 'text-slate-600' }}"></i><span class="font-black text-sm">인수증(운송장)</span></div>
                            <span class="text-[10px] font-black text-blue-500">{{ '업로드 완료' if t.driver_waybill_img else '사진 촬영' }}</span>
                        </div>
                        <input type="file" name="driver_waybill_img" class="hidden" accept="image/*" capture="camera">
                    </label>
                    <label class="block cursor-pointer">
                        <div class="flex items-center justify-between p-5 bg-slate-950 rounded-3xl border-2 border-dashed {{ 'border-orange-500 bg-orange-500/5' if t.driver_tax_img else 'border-slate-800' }}">
                            <div class="flex items-center gap-3"><i class="fa-solid fa-file-invoice {{ 'text-orange-500' if t.driver_tax_img else 'text-slate-600' }}"></i><span class="font-black text-sm">기사 계산서</span></div>
                            <span class="text-[10px] font-black text-blue-500">{{ '업로드 완료' if t.driver_tax_img else '사진 촬영' }}</span>
                        </div>
                        <input type="file" name="driver_tax_img" class="hidden" accept="image/*" capture="camera">
                    </label>
                </div>
                <button type="submit" class="w-full bg-blue-600 text-white py-5 rounded-[2rem] font-black text-lg shadow-xl shadow-blue-900/20 active:scale-95 transition-all">업무 기록 업데이트</button>
            </form>
        </div>
    </div>
    <script>
        document.querySelectorAll('input[type="file"]').forEach(input => {
            input.addEventListener('change', function() {
                if(this.files.length > 0) {
                    const labelDiv = this.previousElementSibling;
                    labelDiv.classList.add('border-blue-500', 'bg-blue-500/10');
                    labelDiv.querySelector('span:last-child').innerText = "사진 찍기 완료";
                }
            });
        });
    </script>
</body>
</html>
"""

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(host='0.0.0.0', port=8000, debug=True)