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

# --- 1. CẤU HÌNH ---
# Điền Token Telegram của bạn vào đây (Nếu không dùng thì để trống)
TELEGRAM_TOKEN = "8514665869:AAHUfTHgNlEEK_Yz6yYjZa-1iR645Cgr190"
TELEGRAM_CHAT_ID = "#-5046493421"

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
    active_color = "#dc3545" if status == "Tạm dừng" else "#ffc107"
    st.markdown(f"""<style>.step-container {{display: flex; justify-content: space-between; margin-bottom: 15px;}} .step-item {{flex: 1; text-align: center; position: relative;}} .step-item:not(:last-child)::after {{content: ''; position: absolute; top: 15px; left: 50%; width: 100%; height: 2px; background: #e0e0e0; z-index: -1;}} .step-circle {{width: 30px; height: 30px; margin: 0 auto 5px; border-radius: 50%; line-height: 30px; color: white; font-weight: bold; font-size: 12px;}} .done {{background: #28a745;}} .active {{background: {active_color}; color: black;}} .pending {{background: #e9ecef; color: #999;}}</style>""", unsafe_allow_html=True)
    h = '<div class="step-container">'
    for i, s in enumerate(STAGES_ORDER):
        cls = "done" if i < idx else "active" if i == idx else "pending"
        ico = "✓" if i < idx else str(i+1)
        if i == idx and status == "Tạm dừng": ico = "⛔"
        h += f'<div class="step-item"><div class="step-circle {cls}">{ico}</div><div style="font-size:11px">{s.split(". ")[1]}</div></div>'
    st.markdown(h+'</div>', unsafe_allow_html=True)

# --- 3. DATABASE ---
def get_connection(): return sqlite3.connect(DB_FILE, check_same_thread=False, timeout=10)
def make_hash(p): return hashlib.sha256(str.encode(p)).hexdigest()
def sanitize(n): return re.sub(r'[\\/*?:"<>|]', "", str(n)).strip()
def generate_code(jid, start, name, phone, addr):
    try: d = pd.to_datetime(start).strftime('%d%m%y')
    except: d = datetime.now().strftime('%d%m%y')
    return f"{d}-{int(jid):03d} {sanitize(name)} {sanitize(phone)} {sanitize(addr)}"

def get_folder_path(jid):
    conn = get_connection(); c = conn.cursor()
    c.execute("SELECT id, start_time, customer_name, customer_phone, address FROM jobs WHERE id=?", (jid,))
    row = c.fetchone(); conn.close()
    if row:
        f = generate_code(row[0], row[1], row[2], row[3], row[4])
        p = os.path.join(BASE_UPLOAD_FOLDER, f)
        if not os.path.exists(p): os.makedirs(p)
        return p
    return BASE_UPLOAD_FOLDER

def init_db():
    conn = get_connection(); c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, fullname TEXT, role TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, customer_name TEXT, customer_phone TEXT, address TEXT, current_stage TEXT, status TEXT, assigned_to TEXT, start_time TIMESTAMP, deadline TIMESTAMP, last_updated TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER, stage TEXT, action_by TEXT, note TEXT, file_path TEXT, timestamp TIMESTAMP)''')
    # Migration
    try: c.execute("ALTER TABLE jobs ADD COLUMN is_survey_only INTEGER DEFAULT 0")
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

def add_job(n, p, a, f, u, asn, d, is_survey):
    conn = get_connection(); c = conn.cursor(); now = datetime.now()
    dl = now + timedelta(days=d); asn_c = asn.split(" - ")[0] if asn else None
    sv_flag = 1 if is_survey else 0
    c.execute("INSERT INTO jobs (customer_name, customer_phone, address, current_stage, status, assigned_to, start_time, deadline, last_updated, is_survey_only) VALUES (?,?,?,?,?,?,?,?,?,?)", (n, p, a, "1. Tạo mới", "Đang xử lý", asn_c, now, dl, now, sv_flag))
    jid = c.lastrowid; conn.commit(); conn.close()
    type_msg = "(CHỈ ĐO ĐẠC)" if is_survey else ""
    send_telegram_msg(f"🚀 <b>MỚI #{jid} {type_msg}</b>\n👤 {n}\n👉 Giao: {asn_c}")
    save_log(jid, "1. Tạo mới", u, f"Khởi tạo {type_msg} (Hạn {d} ngày)", f)

def update_stage(jid, stg, nt, f, u, asn, d):
    conn = get_connection(); c = conn.cursor()
    c.execute("SELECT is_survey_only FROM jobs WHERE id=?", (jid,))
    row = c.fetchone(); is_survey = row[0] if row else 0
    if is_survey == 1 and stg == "3. Làm hồ sơ": nxt = "7. Hoàn thành"
    else: nxt = WORKFLOW_DEFAULT.get(stg)
    asn_c = asn.split(" - ")[0] if asn else None
    if nxt:
        now = datetime.now(); dl = now + timedelta(days=d)
        conn.execute("UPDATE jobs SET current_stage=?, status=?, assigned_to=?, deadline=?, last_updated=? WHERE id=?", (nxt, "Hoàn thành" if nxt=="7. Hoàn thành" else "Đang xử lý", asn_c, dl, now, jid))
        conn.commit(); send_telegram_msg(f"✅ <b>UPDATE #{jid}</b>\n{stg}->{nxt}\n👤 {u}"); save_log(jid, stg, u, f"{nt} (Chuyển: {asn_c})", f)
    conn.close()

def pause_job(jid, rs, u):
    conn = get_connection(); conn.execute("UPDATE jobs SET status='Tạm dừng' WHERE id=?", (jid,)); conn.commit(); conn.close()
    send_telegram_msg(f"⛔ <b>PAUSE #{jid}</b>\nLý do: {rs}")
    save_log(jid, "Tạm dừng", u, f"Lý do: {rs}", None)

def resume_job(jid, u):
    conn = get_connection(); conn.execute("UPDATE jobs SET status='Đang xử lý' WHERE id=?", (jid,)); conn.commit(); conn.close()
    save_log(jid, "Khôi phục", u, "Tiếp tục", None)

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
    q = f"SELECT * FROM jobs WHERE assigned_to='{u}' AND status!='Hoàn thành'" if r != "Quản lý" else "SELECT * FROM jobs WHERE status!='Hoàn thành'"
    df = pd.read_sql_query(q + " ORDER BY deadline ASC", conn); conn.close(); return df

def search_jobs(q, s_date=None, e_date=None):
    conn = get_connection(); query = "SELECT * FROM jobs WHERE 1=1"; params = []
    if s_date and e_date:
        query += " AND start_time BETWEEN ? AND ?"
        params.extend([datetime.combine(s_date, datetime.min.time()), datetime.combine(e_date, datetime.max.time())])
    df = pd.read_sql_query(query + " ORDER BY id DESC", conn, params=params); conn.close()
    if df.empty: return df
    if q:
        df['full'] = df.apply(lambda x: generate_code(x['id'], x['start_time'], x['customer_name'], x['customer_phone'], x['address']), axis=1)
        return df[df.apply(lambda r: q.lower() in str(r).lower(), axis=1)]
    return df

def get_logs(jid):
    conn = get_connection(); return pd.read_sql_query("SELECT * FROM logs WHERE job_id=? ORDER BY timestamp DESC", conn, params=(jid,))

def get_stats(s, e):
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM jobs WHERE start_time BETWEEN ? AND ?", conn, params=(datetime.combine(s, datetime.min.time()), datetime.combine(e, datetime.max.time())))
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
    
    menu = ["🏠 Việc Cần Làm", "🔍 Tra Cứu", "📝 Tạo Hồ Sơ", "📊 Báo Cáo"]
    if role == "Quản lý": menu.append("👥 Nhân Sự")
    sel = st.sidebar.radio("Menu", menu)

    if sel == "🏠 Việc Cần Làm":
        st.title("📋 Danh sách công việc")
        df = get_my_jobs(user, role)
        if df.empty: st.info("Tuyệt vời! Bạn không có việc tồn đọng.")
        else:
            now = datetime.now()
            over = df[pd.to_datetime(df['deadline']) < now]
            soon = df[(pd.to_datetime(df['deadline']) >= now) & (pd.to_datetime(df['deadline']) <= now + timedelta(days=1))]
            ontrack = df[pd.to_datetime(df['deadline']) > now + timedelta(days=1)]
            
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("🔴 Quá Hạn", len(over), border=True); k2.metric("🟡 Sắp Đến", len(soon), border=True)
            k3.metric("🟢 Trong Hạn", len(ontrack), border=True); k4.metric("Tổng Cộng", len(df), border=True)
            st.divider()
            
            f_opt = st.radio("Lọc:", ["Tất cả", "🔴 Quá hạn", "🟡 Sắp đến", "🟢 Trong hạn"], horizontal=True)
            if "Quá hạn" in f_opt: df_show = over
            elif "Sắp đến" in f_opt: df_show = soon
            elif "Trong hạn" in f_opt: df_show = ontrack
            else: df_show = df

            for i, j in df_show.iterrows():
                code = generate_code(j['id'], j['start_time'], j['customer_name'], j['customer_phone'], j['address'])
                dl = pd.to_datetime(j['deadline'])
                icon = "⛔" if j['status']=='Tạm dừng' else ("🔴" if dl < now else "🟡" if dl <= now+timedelta(days=1) else "🟢")
                txt = f"{j['current_stage']} (TẠM DỪNG)" if j['status']=='Tạm dừng' else j['current_stage']
                
                with st.expander(f"{icon} {code} | {txt}"):
                    render_progress_bar(j['current_stage'], j['status'])
                    st.subheader(f"👤 Chủ hồ sơ: {j['customer_name']}")
                    if j['is_survey_only'] == 1: st.warning("🛠️ Hồ sơ này CHỈ ĐO ĐẠC")

                    c1, c2 = st.columns([1.5, 1])
                    with c1:
                        st.write(f"📞 **{j['customer_phone']}** | 📍 {j['address']}"); st.write(f"⏰ Hạn: **{dl.strftime('%d/%m %H:%M')}**")
                        st.markdown("---"); st.markdown("**📜 Lịch sử & File:**")
                        logs = get_logs(j['id'])
                        for x, l in logs.iterrows():
                            ts = pd.to_datetime(l['timestamp']).strftime('%d/%m %H:%M'); st.text(f"{ts} | {l['action_by']}: {l['note']}")
                            if l['file_path'] and os.path.exists(l['file_path']):
                                fn = os.path.basename(l['file_path']); cf1, cf2 = st.columns([3, 1])
                                cf1.markdown(f"📎 **{fn}**"); 
                                with open(l['file_path'], "rb") as f: cf2.download_button("⬇️ Tải", f, file_name=fn, key=f"d{l['id']}")
                                if fn.lower().endswith(('.png', '.jpg')): st.image(l['file_path'], width=150)
                            st.markdown("---")
                    with c2:
                        if j['status']=='Tạm dừng':
                            st.error("ĐANG TẠM DỪNG"); 
                            if st.button("▶️ Tiếp tục", key=f"r{j['id']}"): resume_job(j['id'], user); st.rerun()
                        else:
                            with st.form(f"f{j['id']}"):
                                nt = st.text_area("Ghi chú"); fl = st.file_uploader("Kết quả")
                                cur = j['current_stage']; nxt = "7. Hoàn thành" if j['is_survey_only']==1 and cur=="3. Làm hồ sơ" else WORKFLOW_DEFAULT.get(cur)
                                asn = None; d = 1
                                if nxt and nxt!="7. Hoàn thành": st.write(f"Sang: **{nxt}**"); asn = st.selectbox("Giao", get_active_users()); d = st.number_input("Hạn", value=2)
                                else: st.info(f"Bước tiếp: **{nxt}** (Kết thúc)")
                                if st.form_submit_button("✅ Chuyển"): update_stage(j['id'], cur, nt, fl, user, asn, d); st.success("Xong!"); time.sleep(0.5); st.rerun()
                            st.write(""); 
                            if st.button("⏸️ Dừng", key=f"p{j['id']}"): st.session_state[f'pm_{j['id']}']=True
                            if st.session_state.get(f'pm_{j['id']}', False):
                                rs = st.text_input("Lý do:", key=f"rs{j['id']}")
                                c_p1, c_p2 = st.columns(2)
                                if c_p1.button("OK", key=f"ok{j['id']}"): pause_job(j['id'], rs, user); st.rerun()
                                if c_p2.button("Hủy", key=f"no{j['id']}"): st.session_state[f'pm_{j['id']}']=False; st.rerun()

    elif sel == "🔍 Tra Cứu":
        st.title("Tra Cứu Hồ Sơ")
        c_s1, c_s2 = st.columns([1, 2])
        with c_s1: s_d = st.date_input("Từ", datetime.now()-timedelta(30)); e_d = st.date_input("Đến", datetime.now())
        with c_s2: q = st.text_input("Từ khóa:")
        if st.button("🔍 Tìm"):
            df = search_jobs(q, s_d, e_d); st.write(f"Tìm thấy: **{len(df)}**")
            for i, j in df.iterrows():
                with st.expander(f"{j['full']} ({j['status']})"):
                    render_progress_bar(j['current_stage'], j['status']); st.subheader(f"👤 {j['customer_name']}"); st.write(f"Người làm: {j['assigned_to']}")
                    st.markdown("**Lịch sử:**"); logs = get_logs(j['id'])
                    for x, l in logs.iterrows():
                        st.text(f"{l['timestamp']} | {l['action_by']}: {l['note']}")
                        if l['file_path'] and os.path.exists(l['file_path']):
                            fn = os.path.basename(l['file_path'])
                            with open(l['file_path'], "rb") as f: st.download_button(f"⬇️ {fn}", f, file_name=fn, key=f"s{l['id']}")

    elif sel == "📝 Tạo Hồ Sơ":
        st.title("Tạo Hồ Sơ Mới")
        with st.form("new"):
            c1, c2 = st.columns(2); n = c1.text_input("Tên"); p = c2.text_input("SĐT"); a = st.text_input("Đ/c"); f = st.file_uploader("File")
            st.divider(); c_o, c_a = st.columns(2)
            is_sv = c_o.checkbox("🛠️ Hồ sơ CHỈ ĐO ĐẠC"); asn = c_a.selectbox("Giao cho:", get_active_users()); d = st.number_input("Hạn", value=1)
            if st.form_submit_button("🚀 Tạo"):
                if n and asn: add_job(n, p, a, f, user, asn, d, is_sv); st.success("OK!"); time.sleep(1)
                else: st.error("Thiếu tin!")

    elif sel == "📊 Báo Cáo":
        st.title("Thống Kê")
        c1, c2 = st.columns(2); ft = c1.radio("Xem:", ["Tháng/Năm", "Ngày"], horizontal=True)
        if ft == "Tháng/Năm":
            m = c2.selectbox("Tháng", range(1, 13), index=datetime.now().month-1); y = c2.number_input("Năm", value=datetime.now().year)
            s = datetime(y, m, 1); e = datetime(y, m, calendar.monthrange(y, m)[1])
        else: s = c2.date_input("Từ", datetime.now()-timedelta(30)); e = c2.date_input("Đến", datetime.now())
        
        df = get_stats(s, e)
        if not df.empty:
            tot = len(df); fin = len(df[df['status']=='Hoàn thành']); proc = tot - fin
            now = datetime.now(); df['late'] = df.apply(lambda x: x['status']!='Hoàn thành' and pd.to_datetime(x['deadline'])<now, axis=1)
            late = len(df[df['late']==True])
            
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Tổng", tot, border=True); k2.metric("Đúng hạn", proc-late, border=True)
            k3.metric("Quá hạn", late, border=True, delta_color="inverse"); k4.metric("Xong", fin, f"{fin/tot*100:.1f}%", border=True)
            
            st.divider(); c_ch1, c_ch2 = st.columns([2, 1])
            sc = df['current_stage'].value_counts().reset_index(); sc.columns=['Giai đoạn', 'SL']; c_ch1.bar_chart(sc, x='Giai đoạn', y='SL', color="#0068c9")
            dt = pd.DataFrame({'SL': [proc-late, late, fin]}, index=['Đúng hạn', 'Quá hạn', 'Xong']); dt['%'] = (dt['SL']/tot*100).round(1)
            c_ch2.dataframe(dt, use_container_width=True)
            
            st.markdown("### Chi tiết")
            df['Mã'] = df.apply(lambda x: generate_code(x['id'], x['start_time'], x['customer_name'], '', ''), axis=1)
            df['TT'] = df.apply(lambda x: '🔴 Trễ' if x['late'] else ('✅ Xong' if x['status']=='Hoàn thành' else '🟢 Ổn'), axis=1)
            st.dataframe(df[['Mã', 'customer_name', 'current_stage', 'assigned_to', 'deadline', 'TT']], use_container_width=True)
        else: st.warning("Không có dữ liệu.")

    elif sel == "👥 Nhân Sự":
        if role == "Quản lý":
            st.title("Quản Lý Nhân Sự")
            df = get_all_users()
            for i, u in df.iterrows():
                c1, c2 = st.columns([3, 2]); c1.write(f"**{u['username']}** ({u['fullname']})")
                if u['username']!=user:
                    idx = ROLES.index(u['role']) if u['role'] in ROLES else 2
                    nr = c2.selectbox("Quyền", ROLES, index=idx, key=u['username'])
                    if nr!=u['role']: update_user_role(u['username'], nr); st.toast("Đã lưu!"); st.rerun()
        else: st.error("Cấm truy cập!")
