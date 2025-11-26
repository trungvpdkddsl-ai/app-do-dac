import streamlit as st
import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta
import time
import hashlib
import re
import requests
import threading
import calendar
import base64 # Hỗ trợ xem PDF

# --- 1. CẤU HÌNH ---
TELEGRAM_TOKEN = "8514665869:AAHUfTHgNlEEK_Yz6yYjZa-1iR645Cgr190"
TELEGRAM_CHAT_ID = "-5046493421"

DB_FILE = "quan_ly_do_dac.db"
BASE_UPLOAD_FOLDER = "uploads"
if not os.path.exists(BASE_UPLOAD_FOLDER):
    os.makedirs(BASE_UPLOAD_FOLDER)

ROLES = ["Quản lý", "Nhân viên", "Chưa cấp quyền"]

STAGES_ORDER = [
    "1. Tạo mới", "2. Đo đạc", "3. Làm hồ sơ", "4. Ký hồ sơ", 
    "5. Lấy hồ sơ", "6. Nộp hồ sơ", "7. Hoàn thành"
]

WORKFLOW_DEFAULT = {
    "1. Tạo mới": "2. Đo đạc", "2. Đo đạc": "3. Làm hồ sơ", "3. Làm hồ sơ": "4. Ký hồ sơ",
    "4. Ký hồ sơ": "5. Lấy hồ sơ", "5. Lấy hồ sơ": "6. Nộp hồ sơ", "6. Nộp hồ sơ": "7. Hoàn thành",
    "7. Hoàn thành": None
}

# --- 2. VISUAL ---
def render_progress_bar(current_stage, status):
    try: idx = STAGES_ORDER.index(current_stage)
    except: idx = 0
    color = "#dc3545" if status in ["Tạm dừng", "Kết thúc sớm"] else "#ffc107"
    st.markdown(f"""<style>.step-container {{display: flex; justify-content: space-between; margin-bottom: 15px;}} .step-item {{flex: 1; text-align: center; position: relative;}} .step-item:not(:last-child)::after {{content: ''; position: absolute; top: 15px; left: 50%; width: 100%; height: 2px; background: #e0e0e0; z-index: -1;}} .step-circle {{width: 30px; height: 30px; margin: 0 auto 5px; border-radius: 50%; line-height: 30px; color: white; font-weight: bold; font-size: 12px;}} .done {{background: #28a745;}} .active {{background: {color}; color: black;}} .pending {{background: #e9ecef; color: #999;}}</style>""", unsafe_allow_html=True)
    h = '<div class="step-container">'
    for i, s in enumerate(STAGES_ORDER):
        cls = "done" if i < idx else "active" if i == idx else "pending"
        ico = "✓" if i < idx else str(i+1)
        if i == idx and status == "Tạm dừng": ico = "⛔"
        if i == idx and status == "Kết thúc sớm": ico = "⏹️"
        h += f'<div class="step-item"><div class="step-circle {cls}">{ico}</div><div style="font-size:11px">{s.split(". ")[1]}</div></div>'
    st.markdown(h+'</div>', unsafe_allow_html=True)

# --- 3. DATABASE ---
def get_connection(): return sqlite3.connect(DB_FILE, check_same_thread=False, timeout=10)
def make_hash(p): return hashlib.sha256(str.encode(p)).hexdigest()
def sanitize(n): return re.sub(r'[\\/*?:"<>|]', "", str(n)).strip()
def generate_code(jid, start, name):
    try: d = pd.to_datetime(start).strftime('%d%m%y')
    except: d = datetime.now().strftime('%d%m%y')
    return f"{d}-{int(jid):03d} {sanitize(name)}"

def get_folder_path(jid):
    conn = get_connection(); c = conn.cursor()
    c.execute("SELECT id, start_time, customer_name, customer_phone, address FROM jobs WHERE id=?", (jid,))
    row = c.fetchone(); conn.close()
    if row:
        f = generate_code(row[0], row[1], row[2])
        p = os.path.join(BASE_UPLOAD_FOLDER, f)
        if not os.path.exists(p): os.makedirs(p)
        return p
    return BASE_UPLOAD_FOLDER

def init_db():
    conn = get_connection(); c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, fullname TEXT, role TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, customer_name TEXT, customer_phone TEXT, address TEXT, current_stage TEXT, status TEXT, assigned_to TEXT, start_time TIMESTAMP, deadline TIMESTAMP, last_updated TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER, stage TEXT, action_by TEXT, note TEXT, file_path TEXT, timestamp TIMESTAMP)''')
    try: 
        c.execute("ALTER TABLE jobs ADD COLUMN is_survey_only INTEGER DEFAULT 0")
        c.execute("ALTER TABLE jobs ADD COLUMN deposit INTEGER DEFAULT 0")
        c.execute("ALTER TABLE jobs ADD COLUMN survey_fee INTEGER DEFAULT 0")
        c.execute("ALTER TABLE jobs ADD COLUMN is_paid INTEGER DEFAULT 0")
    except: pass
    conn.commit(); conn.close()

# --- TELEGRAM ---
def send_telegram_msg(msg):
    if not TELEGRAM_TOKEN: return
    def run(): 
        try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"})
        except: pass
    threading.Thread(target=run).start()

# --- LOGIC ---
def create_user(u, p, n, r="Chưa cấp quyền"):
    conn = get_connection()
    try: conn.execute("INSERT INTO users VALUES (?, ?, ?, ?)", (u, make_hash(p), n, r)); conn.commit(); return True
    except: return False
    finally: conn.close()

def login_user(u, p):
    conn = get_connection(); d = conn.execute("SELECT * FROM users WHERE username=? AND password=?", (u, make_hash(p))).fetchall(); conn.close(); return d

def get_active_users():
    conn = get_connection(); d = conn.execute("SELECT username, fullname FROM users WHERE role!='Chưa cấp quyền'").fetchall(); conn.close()
    return [f"{u[0]} - {u[1]}" for u in d]

def get_all_users():
    conn = get_connection(); df = pd.read_sql_query("SELECT username, fullname, role FROM users", conn); conn.close(); return df

def update_user_role(u, r):
    conn = get_connection(); conn.execute("UPDATE users SET role=? WHERE username=?", (r, u)); conn.commit(); conn.close()

def add_job(n, p, a, f, u, asn, d, is_survey, deposit, fee):
    conn = get_connection(); c = conn.cursor(); now = datetime.now()
    dl = now + timedelta(days=d); asn_c = asn.split(" - ")[0] if asn else None
    sv_flag = 1 if is_survey else 0; dep_flag = 1 if deposit else 0
    c.execute("INSERT INTO jobs (customer_name, customer_phone, address, current_stage, status, assigned_to, start_time, deadline, last_updated, is_survey_only, deposit, survey_fee, is_paid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0)", (n, p, a, "1. Tạo mới", "Đang xử lý", asn_c, now, dl, now, sv_flag, dep_flag, fee))
    jid = c.lastrowid; conn.commit(); conn.close()
    code = generate_code(jid, now, n)
    type_msg = "(CHỈ ĐO ĐẠC)" if is_survey else ""
    money_msg = "✅ Đã tạm ứng" if deposit else "❌ Chưa tạm ứng"
    send_telegram_msg(f"🚀 <b>MỚI #{jid} {type_msg}</b>\n📂 <b>{code}</b>\n📍 {a}\n👉 Giao: {asn_c}\n💰 {money_msg}")
    save_log(jid, "1. Tạo mới", u, f"Khởi tạo {type_msg} (Hạn {d} ngày)", f)

def update_stage(jid, stg, nt, f, u, asn, d, is_survey, deposit, fee, is_paid):
    conn = get_connection(); c = conn.cursor(); now = datetime.now()
    nxt = "7. Hoàn thành" if is_survey == 1 and stg == "3. Làm hồ sơ" else WORKFLOW_DEFAULT.get(stg)
    asn_c = asn.split(" - ")[0] if asn else None
    
    c.execute("SELECT start_time, customer_name FROM jobs WHERE id=?", (jid,))
    row = c.fetchone(); code = generate_code(jid, row[0], row[1]) if row else f"#{jid}"

    if nxt:
        dl = now + timedelta(days=d)
        c.execute("UPDATE jobs SET current_stage=?, status=?, assigned_to=?, deadline=?, last_updated=?, deposit=?, survey_fee=?, is_paid=? WHERE id=?", 
                  (nxt, "Hoàn thành" if nxt=="7. Hoàn thành" else "Đang xử lý", asn_c, dl, now, 1 if deposit else 0, fee, 1 if is_paid else 0, jid))
        conn.commit()
        send_telegram_msg(f"✅ <b>CẬP NHẬT</b>\n📂 <b>{code}</b>\n{stg} ➡ <b>{nxt}</b>\n👤 {u}")
        save_log(jid, stg, u, f"{nt} (Chuyển: {asn_c})", f)
    conn.close()

def update_finance_only(jid, deposit, fee, is_paid, u):
    conn = get_connection(); c = conn.cursor()
    c.execute("UPDATE jobs SET deposit=?, survey_fee=?, is_paid=? WHERE id=?", (1 if deposit else 0, fee, 1 if is_paid else 0, jid))
    conn.commit()
    c.execute("SELECT start_time, customer_name FROM jobs WHERE id=?", (jid,)); row = c.fetchone(); conn.close()
    code = generate_code(jid, row[0], row[1]) if row else f"#{jid}"
    send_telegram_msg(f"💰 <b>TÀI CHÍNH</b>\n📂 <b>{code}</b>\n👤 {u}\nPhí: {fee:,} VNĐ | Thu đủ: {'✅' if is_paid else '❌'}")

def pause_job(jid, rs, u):
    conn = get_connection(); c = conn.cursor()
    c.execute("UPDATE jobs SET status='Tạm dừng' WHERE id=?", (jid,)); conn.commit()
    c.execute("SELECT start_time, customer_name FROM jobs WHERE id=?", (jid,)); row = c.fetchone(); conn.close()
    code = generate_code(jid, row[0], row[1]) if row else f"#{jid}"
    send_telegram_msg(f"⛔ <b>PAUSE</b>\n📂 <b>{code}</b>\n👤 Bởi: {u}\n📝 Lý do: {rs}")
    save_log(jid, "Tạm dừng", u, f"Lý do: {rs}", None)

def resume_job(jid, u):
    conn = get_connection(); c = conn.cursor()
    c.execute("UPDATE jobs SET status='Đang xử lý' WHERE id=?", (jid,)); conn.commit()
    c.execute("SELECT start_time, customer_name FROM jobs WHERE id=?", (jid,)); row = c.fetchone(); conn.close()
    code = generate_code(jid, row[0], row[1]) if row else f"#{jid}"
    send_telegram_msg(f"▶️ <b>KHÔI PHỤC</b>\n📂 <b>{code}</b>\n👤 Bởi: {u}")
    save_log(jid, "Khôi phục", u, "Tiếp tục", None)

def terminate_job(jid, rs, u):
    conn = get_connection(); c = conn.cursor()
    c.execute("UPDATE jobs SET status='Kết thúc sớm' WHERE id=?", (jid,)); conn.commit()
    c.execute("SELECT start_time, customer_name FROM jobs WHERE id=?", (jid,)); row = c.fetchone(); conn.close()
    code = generate_code(jid, row[0], row[1]) if row else f"#{jid}"
    send_telegram_msg(f"⏹️ <b>KẾT THÚC SỚM</b>\n📂 <b>{code}</b>\n👤 Bởi: {u}\n📝 Lý do: {rs}")
    save_log(jid, "Kết thúc sớm", u, f"Lý do: {rs}", None)

def save_log(jid, stg, u, nt, f=None):
    fp = ""
    if f:
        path = get_folder_path(jid); fname = f"{datetime.now().strftime('%H%M%S')}_{sanitize(f.name)}"
        fp = os.path.join(path, fname)
        with open(fp, "wb") as file: file.write(f.getbuffer())
    conn = get_connection()
    conn.execute("INSERT INTO logs (job_id, stage, action_by, note, file_path, timestamp) VALUES (?,?,?,?,?,?)", (jid, stg, u, nt, fp, datetime.now()))
    conn.commit(); conn.close()

def get_my_jobs(u, r):
    conn = get_connection()
    q = f"SELECT * FROM jobs WHERE assigned_to='{u}' AND status NOT IN ('Hoàn thành', 'Kết thúc sớm')" if r != "Quản lý" else "SELECT * FROM jobs WHERE status NOT IN ('Hoàn thành', 'Kết thúc sớm')"
    df = pd.read_sql_query(q + " ORDER BY deadline ASC", conn); conn.close(); return df

def search_jobs(q, s_date=None, e_date=None):
    conn = get_connection(); query = "SELECT * FROM jobs WHERE 1=1"; params = []
    if s_date and e_date:
        query += " AND start_time BETWEEN ? AND ?"
        params.extend([datetime.combine(s_date, datetime.min.time()), datetime.combine(e_date, datetime.max.time())])
    df = pd.read_sql_query(query + " ORDER BY id DESC", conn, params=params); conn.close()
    if df.empty: return df
    if q:
        df['full'] = df.apply(lambda x: generate_code(x['id'], x['start_time'], x['customer_name']), axis=1)
        return df[df.apply(lambda r: q.lower() in str(r).lower(), axis=1)]
    return df

def get_logs(jid):
    conn = get_connection(); return pd.read_sql_query("SELECT * FROM logs WHERE job_id=? ORDER BY timestamp DESC", conn, params=(jid,))

def get_stats(s, e):
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM jobs WHERE start_time BETWEEN ? AND ?", conn, params=(datetime.combine(s, datetime.min.time()), datetime.combine(e, datetime.max.time())))
    conn.close(); return df

def get_unpaid_jobs():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM jobs WHERE is_paid = 0 OR is_paid IS NULL", conn)
    conn.close(); return df

# --- 4. UI ---
st.set_page_config(page_title="Hệ thống Đo Đạc", layout="wide", page_icon="🏗️")
init_db()

if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False

if not st.session_state['logged_in']:
    st.title("🔐 Đăng nhập")
    c1, c2 = st.columns(2)
    with c1:
        u = st.text_input("User"); p = st.text_input("Pass", type='password')
        if st.button("Login"):
            d = login_user(u, p)
            if d: st.session_state['logged_in']=True; st.session_state['user']=d[0][0]; st.session_state['role']=d[0][3]; st.rerun()
            else: st.error("Sai thông tin!")
    with c2:
        nu = st.text_input("Mới"); np = st.text_input("Mật khẩu mới", type='password'); nn = st.text_input("Họ tên")
        if st.button("Đăng ký"):
            if create_user(nu, np, nn): st.success("OK!"); else: st.error("Trùng!")
else:
    user = st.session_state['user']; role = st.session_state['role']
    st.sidebar.title(f"👤 {user}"); st.sidebar.info(f"{role}")
    if st.sidebar.button("Đăng xuất"): st.session_state['logged_in']=False; st.rerun()
    
    # --- SIDEBAR THÔNG BÁO ---
    try: df_all = pd.read_sql_query("SELECT * FROM jobs", get_connection())
    except: df_all = pd.DataFrame()

    if not df_all.empty:
        st.sidebar.markdown("---"); st.sidebar.subheader("🔔 Cảnh báo hạn"); now = datetime.now()
        if role != "Quản lý": my_alert_df = df_all[(df_all['assigned_to'].astype(str) == user) & (~df_all['status'].isin(['Hoàn thành', 'Kết thúc sớm']))]
        else: my_alert_df = df_all[~df_all['status'].isin(['Hoàn thành', 'Kết thúc sớm'])]
        
        if not my_alert_df.empty:
            my_alert_df['dl_dt'] = pd.to_datetime(my_alert_df['deadline'])
            over = my_alert_df[my_alert_df['dl_dt'] < now]
            soon = my_alert_df[(my_alert_df['dl_dt'] >= now) & (my_alert_df['dl_dt'] <= now + timedelta(days=1))]
            if not over.empty: st.sidebar.error(f"🔴 {len(over)} HS Quá hạn"); st.sidebar.dataframe(over[['customer_name']], hide_index=True)
            if not soon.empty: st.sidebar.warning(f"🟡 {len(soon)} HS Sắp đến"); st.sidebar.dataframe(soon[['customer_name']], hide_index=True)

    menu = ["🏠 Việc Cần Làm", "🔍 Tra Cứu", "📝 Tạo Hồ Sơ", "📊 Báo Cáo"]
    if role == "Quản lý": menu.insert(1, "💰 Công Nợ"); menu.append("👥 Nhân Sự")
    sel = st.sidebar.radio("Menu", menu)

    if sel == "🏠 Việc Cần Làm":
        st.title("📋 Tiến trình hồ sơ")
        df = get_my_jobs(user, role)
        if df.empty: st.info("Trống!")
        else:
            now = datetime.now()
            over = df[pd.to_datetime(df['deadline']) < now]; soon = df[(pd.to_datetime(df['deadline']) >= now) & (pd.to_datetime(df['deadline']) <= now + timedelta(days=1))]
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("🔴 Quá Hạn", len(over), border=True); k2.metric("🟡 Gấp", len(soon), border=True); k3.metric("🟢 Tổng", len(df), border=True)
            st.divider()

            for i, j in df.iterrows():
                code = generate_code(j['id'], j['start_time'], j['customer_name'])
                icon = "⛔" if j['status']=='Tạm dừng' else "⏹️" if j['status']=='Kết thúc sớm' else ("🔴" if pd.to_datetime(j['deadline']) < now else "🟢")
                
                with st.expander(f"{icon} {code} | {j['current_stage']}"):
                    render_progress_bar(j['current_stage'], j['status'])
                    
                    # --- CHIA 2 TAB ---
                    tab_info, tab_files = st.tabs(["📝 Xử lý & Tài chính", "📂 File đính kèm"])
                    
                    # TAB 2: FILE
                    with tab_files:
                        st.markdown("### 📂 Kho dữ liệu")
                        logs = get_logs(j['id'])
                        files_found = False
                        for x, l in logs.iterrows():
                            if l['file_path'] and os.path.exists(l['file_path']):
                                files_found = True
                                fn = os.path.basename(l['file_path'])
                                c_f1, c_f2 = st.columns([3, 1])
                                c_f1.info(f"📎 {fn} (từ bước {l['stage']})")
                                with open(l['file_path'], "rb") as f: c_f2.download_button("⬇️ Tải", f, file_name=fn, key=f"dl_f_{l['id']}")
                                
                                if fn.lower().endswith(('.png', '.jpg', '.jpeg')):
                                    st.image(l['file_path'], width=300)
                                elif fn.lower().endswith('.pdf'):
                                    with open(l['file_path'], "rb") as f:
                                        base64_pdf = base64.b64encode(f.read()).decode('utf-8')
                                    st.markdown(f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="100%" height="400" type="application/pdf"></iframe>', unsafe_allow_html=True)
                                st.divider()
                        if not files_found: st.caption("Chưa có file nào được tải lên.")

                    # TAB 1: THÔNG TIN
                    with tab_info:
                        st.subheader(f"👤 {j['customer_name']}")
                        if j.get('is_survey_only') == 1: st.warning("🛠️ CHỈ ĐO ĐẠC")
                        
                        c1, c2 = st.columns([1.5, 1])
                        with c1:
                            st.write(f"📞 {j['customer_phone']} | 📍 {j['address']}"); st.write(f"⏰ Hạn: {j['deadline']}")
                            st.markdown("#### 💰 Tài Chính")
                            with st.form(f"money_{j['id']}"):
                                dep_ok = st.checkbox("Đã thu tạm ứng?", value=(j.get('deposit')==1))
                                if not dep_ok: st.caption("🔴 Chưa thu tạm ứng")
                                fee = st.number_input("Phí (VNĐ)", value=j.get('survey_fee') or 0, step=100000)
                                paid_ok = st.checkbox("Đã thu đủ tiền?", value=(j.get('is_paid')==1))
                                if st.form_submit_button("💾 Lưu"): update_finance_only(j['id'], dep_ok, fee, paid_ok, user); st.success("Lưu!"); st.rerun()
                            
                            st.markdown("#### 📜 Nhật ký")
                            with st.container(border=True):
                                logs = get_logs(j['id'])
                                for x, l in logs.iterrows():
                                    st.text(f"{pd.to_datetime(l['timestamp']).strftime('%d/%m %H:%M')} | {l['action_by']}: {l['note']}")

                        with c2:
                            if j['status'] in ['Tạm dừng', 'Kết thúc sớm']:
                                st.error(f"{j['status'].upper()}")
                                if j['status']=='Tạm dừng':
                                    if st.button("▶️ Tiếp tục", key=f"r{j['id']}"): resume_job(j['id'], user); st.rerun()
                            else:
                                with st.form(f"f{j['id']}"):
                                    nt = st.text_area("Note"); fl = st.file_uploader("Up File")
                                    cur = j['current_stage']; nxt = "7. Hoàn thành" if j.get('is_survey_only')==1 and cur=="3. Làm hồ sơ" else WORKFLOW_DEFAULT.get(cur)
                                    asn=None; d=1
                                    if nxt and nxt!="7. Hoàn thành": st.write(f"Sang: **{nxt}**"); asn = st.selectbox("Giao", get_active_users()); d = st.number_input("Hạn", value=2)
                                    else: st.info("Kết thúc")
                                    if st.form_submit_button("✅ Chuyển"): 
                                        dep = 1 if j.get('deposit')==1 else 0; fee = j.get('survey_fee') or 0; pdone = 1 if j.get('is_paid')==1 else 0
                                        update_stage(j['id'], cur, nt, fl, user, asn, d, j.get('is_survey_only'), dep, fee, pdone); st.success("Xong!"); time.sleep(0.5); st.rerun()
                                
                                c_s1, c_s2 = st.columns(2)
                                if c_s1.button("⏸️ Dừng", key=f"p{j['id']}"): st.session_state[f'pm_{j['id']}']=True
                                if c_s2.button("⏹️ Kết thúc", key=f"t{j['id']}"): st.session_state[f'tm_{j['id']}']=True
                                
                                if st.session_state.get(f'pm_{j['id']}', False):
                                    rs = st.text_input("Lý do:", key=f"rs{j['id']}")
                                    if st.button("OK", key=f"okp{j['id']}"): pause_job(j['id'], rs, user); st.rerun()
                                if st.session_state.get(f'tm_{j['id']}', False):
                                    rst = st.text_input("Lý do:", key=f"rst{j['id']}")
                                    if st.button("OK", key=f"okt{j['id']}"): terminate_job(j['id'], rst, user); st.rerun()

    # --- CÁC TAB KHÁC GIỮ NGUYÊN ---
    elif sel == "💰 Công Nợ":
        st.title("💰 Công Nợ")
        try:
            df = get_unpaid_jobs(); st.metric("Chưa thu tiền", len(df))
            if not df.empty:
                df['Mã'] = df.apply(lambda x: generate_code(x['id'], x['start_time'], x['customer_name']), axis=1)
                st.dataframe(df[['Mã', 'customer_phone', 'survey_fee', 'deposit']], use_container_width=True)
            else: st.success("Sạch nợ!")
        except: pass

    elif sel == "📝 Tạo Hồ Sơ":
        st.title("Tạo Mới")
        with st.form("new"):
            c1, c2 = st.columns(2); n = c1.text_input("Tên"); p = c2.text_input("SĐT"); a = st.text_input("Đ/c"); f = st.file_uploader("File")
            st.divider(); c_o, c_a = st.columns(2); is_sv = c_o.checkbox("🛠️ CHỈ ĐO ĐẠC"); 
            st.markdown("---"); st.write("💰 **Phí:**"); c_m1, c_m2 = st.columns(2); dep_ok = c_m1.checkbox("Đã tạm ứng?"); fee_val = c_m2.number_input("Phí:", value=0, step=100000)
            asn = st.selectbox("Giao:", get_active_users()); d = st.number_input("Hạn", value=1)
            if st.form_submit_button("Tạo"):
                if n and asn: add_job(n, p, a, f, user, asn, d, is_sv, dep_ok, fee_val); st.success("OK!"); st.rerun()
                else: st.error("Thiếu tin")

    elif sel == "🔍 Tra Cứu":
        st.title("Tra Cứu"); c_s1, c_s2 = st.columns([1, 2])
        with c_s1: s_d = st.date_input("Từ", datetime.now()-timedelta(30)); e_d = st.date_input("Đến", datetime.now())
        with c_s2: q = st.text_input("Từ khóa:")
        if st.button("🔍 Tìm"):
            df = search_jobs(q, s_d, e_d); st.write(f"Thấy: {len(df)}")
            for i, j in df.iterrows():
                with st.expander(f"{j['full']} ({j['status']})"):
                    render_progress_bar(j['current_stage'], j['status']); st.subheader(f"👤 {j['customer_name']}"); st.write(f"Người làm: {j['assigned_to']}")
                    st.markdown("**File:**"); logs = get_logs(j['id'])
                    for x, l in logs.iterrows():
                        if l['file_path'] and os.path.exists(l['file_path']):
                            fn = os.path.basename(l['file_path']); c1, c2 = st.columns([3, 1])
                            c1.text(f"{l['timestamp']} | {fn}"); 
                            with open(l['file_path'], "rb") as f: c2.download_button("⬇️", f, file_name=fn, key=f"sd{l['id']}")

    elif sel == "📊 Báo Cáo":
        st.title("Thống Kê"); c1, c2 = st.columns(2); ft = c1.radio("Xem:", ["Tháng/Năm", "Ngày"], horizontal=True)
        if ft == "Tháng/Năm": m = c2.selectbox("Tháng", range(1, 13), index=datetime.now().month-1); y = c2.number_input("Năm", value=datetime.now().year); s = datetime(y, m, 1); e = datetime(y, m, calendar.monthrange(y, m)[1])
        else: s = c2.date_input("Từ", datetime.now()-timedelta(30)); e = c2.date_input("Đến", datetime.now())
        df = get_stats(s, e)
        if not df.empty:
            tot = len(df); fin = len(df[df['status']=='Hoàn thành']); proc = tot - fin; now = datetime.now(); df['late'] = df.apply(lambda x: x['status']!='Hoàn thành' and pd.to_datetime(x['deadline'])<now, axis=1); late = len(df[df['late']==True])
            k1, k2, k3, k4 = st.columns(4); k1.metric("Tổng", tot); k2.metric("Đúng hạn", proc-late); k3.metric("Quá hạn", late); k4.metric("Xong", fin)
            st.bar_chart(df['current_stage'].value_counts()); st.dataframe(df[['customer_name', 'current_stage', 'assigned_to']], use_container_width=True)
        else: st.warning("Không có dữ liệu.")

    elif sel == "👥 Nhân Sự":
        if role == "Quản lý":
            st.title("Phân Quyền"); df = get_all_users()
            for i, u in df.iterrows():
                c1, c2 = st.columns([3, 2]); c1.write(f"**{u['username']}** ({u['fullname']})")
                if u['username']!=user:
                    idx = ROLES.index(u['role']) if u['role'] in ROLES else 2
                    nr = c2.selectbox("Quyền", ROLES, index=idx, key=u['username'])
                    if nr!=u['role']: update_user_role(u['username'], nr); st.toast("Lưu!"); st.rerun()
        else: st.error("Cấm!")
