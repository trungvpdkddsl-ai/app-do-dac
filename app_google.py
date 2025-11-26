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
import base64

# --- 1. CẤU HÌNH ---
TELEGRAM_TOKEN = "8514665869:AAHUfTHgNlEEK_Yz6yYjZa-1iR645Cgr190"
TELEGRAM_CHAT_ID = "-5046493421"

DB_FILE = "quan_ly_do_dac.db"
BASE_UPLOAD_FOLDER = "uploads"
if not os.path.exists(BASE_UPLOAD_FOLDER):
    os.makedirs(BASE_UPLOAD_FOLDER)

ROLES = ["Quản lý", "Nhân viên", "Chưa cấp quyền"]
STAGES_ORDER = ["1. Tạo mới", "2. Đo đạc", "3. Làm hồ sơ", "4. Ký hồ sơ", "5. Lấy hồ sơ", "6. Nộp hồ sơ", "7. Hoàn thành"]
WORKFLOW_DEFAULT = {"1. Tạo mới": "2. Đo đạc", "2. Đo đạc": "3. Làm hồ sơ", "3. Làm hồ sơ": "4. Ký hồ sơ", "4. Ký hồ sơ": "5. Lấy hồ sơ", "5. Lấy hồ sơ": "6. Nộp hồ sơ", "6. Nộp hồ sơ": "7. Hoàn thành", "7. Hoàn thành": None}

# --- 2. HÀM HỖ TRỢ ---
def get_connection(): return sqlite3.connect(DB_FILE, check_same_thread=False, timeout=30)
def make_hash(p): return hashlib.sha256(str.encode(p)).hexdigest()
def sanitize(n): return re.sub(r'[\\/*?:"<>|]', "", str(n)).strip()
def generate_code(jid, start, name):
    try: d = pd.to_datetime(start).strftime('%d%m%y')
    except: d = datetime.now().strftime('%d%m%y')
    return f"{d}-{int(jid):03d} {sanitize(name)}"

def get_folder_path(jid):
    conn = get_connection(); c = conn.cursor()
    c.execute("SELECT id, start_time, customer_name FROM jobs WHERE id=?", (jid,))
    row = c.fetchone(); conn.close()
    if row:
        f = generate_code(row[0], row[1], row[2])
        p = os.path.join(BASE_UPLOAD_FOLDER, f)
        if not os.path.exists(p): os.makedirs(p)
        return p
    return BASE_UPLOAD_FOLDER

def save_uploaded_file(uploaded_file, job_id):
    if uploaded_file is None: return ""
    try:
        folder = get_folder_path(job_id)
        file_path = os.path.join(folder, uploaded_file.name)
        with open(file_path, "wb") as f: f.write(uploaded_file.getbuffer())
        return file_path
    except: return ""

def init_db():
    conn = get_connection(); c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, fullname TEXT, role TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, customer_name TEXT, customer_phone TEXT, address TEXT, current_stage TEXT, status TEXT, assigned_to TEXT, start_time TIMESTAMP, deadline TIMESTAMP, last_updated TIMESTAMP, is_survey_only INTEGER DEFAULT 0, deposit INTEGER DEFAULT 0, survey_fee INTEGER DEFAULT 0, is_paid INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER, stage TEXT, action_by TEXT, note TEXT, file_path TEXT, timestamp TIMESTAMP)''')
    try: c.execute("ALTER TABLE jobs ADD COLUMN is_survey_only INTEGER DEFAULT 0")
    except: pass
    conn.commit(); conn.close()

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

def save_log_entry(jid, stg, u, nt, fp):
    conn = get_connection()
    conn.execute("INSERT INTO logs (job_id, stage, action_by, note, file_path, timestamp) VALUES (?,?,?,?,?,?)", (jid, stg, u, nt, fp, datetime.now()))
    conn.commit(); conn.close()

def add_job(n, p, a, f, u, asn_list, d, is_survey, deposit, fee):
    conn = get_connection(); c = conn.cursor(); now = datetime.now(); dl = now + timedelta(days=d)
    
    # Xử lý danh sách người được giao (List -> String)
    asn_str = ", ".join([x.split(" - ")[0] for x in asn_list]) if asn_list else ""
    
    c.execute("INSERT INTO jobs (customer_name, customer_phone, address, current_stage, status, assigned_to, start_time, deadline, last_updated, is_survey_only, deposit, survey_fee, is_paid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0)", (n, p, a, "1. Tạo mới", "Đang xử lý", asn_str, now, dl, now, 1 if is_survey else 0, 1 if deposit else 0, fee))
    jid = c.lastrowid; conn.commit(); conn.close()
    
    fp = save_uploaded_file(f, jid)
    code = generate_code(jid, now, n)
    type_msg = "(CHỈ ĐO ĐẠC)" if is_survey else ""
    money_msg = "✅ Đã tạm ứng" if deposit else "❌ Chưa tạm ứng"
    
    send_telegram_msg(f"🚀 <b>MỚI #{jid} {type_msg}</b>\n📂 <b>{code}</b>\n📍 {a}\n👉 Giao: {asn_str}\n💰 {money_msg}")
    save_log_entry(jid, "1. Tạo mới", u, f"Khởi tạo (Hạn {d} ngày)", fp)

def update_stage(jid, stg, nt, f, u, asn_list, d, is_survey, deposit, fee, is_paid, customer_name):
    conn = get_connection()
    nxt = "7. Hoàn thành" if is_survey == 1 and stg == "3. Làm hồ sơ" else WORKFLOW_DEFAULT.get(stg)
    
    # Xử lý danh sách người được giao
    asn_str = ", ".join([x.split(" - ")[0] for x in asn_list]) if asn_list else ""
    
    if nxt:
        now = datetime.now(); dl = now + timedelta(days=d)
        conn.execute("UPDATE jobs SET current_stage=?, status=?, assigned_to=?, deadline=?, last_updated=?, deposit=?, survey_fee=?, is_paid=? WHERE id=?", 
                  (nxt, "Hoàn thành" if nxt=="7. Hoàn thành" else "Đang xử lý", asn_str, dl, now, 1 if deposit else 0, fee, 1 if is_paid else 0, jid))
        conn.commit(); conn.close()
        
        fp = save_uploaded_file(f, jid)
        conn2 = get_connection(); c2 = conn2.cursor()
        c2.execute("SELECT start_time FROM jobs WHERE id=?", (jid,)); row = c2.fetchone(); conn2.close()
        start_t = row[0] if row else datetime.now()
        code = generate_code(jid, start_t, customer_name)
        
        send_telegram_msg(f"✅ <b>CẬP NHẬT</b>\n📂 <b>{code}</b>\n{stg} ➡ <b>{nxt}</b>\n👤 {u}")
        save_log_entry(jid, stg, u, f"{nt} (Chuyển: {asn_str})", fp)

def update_finance_only(jid, deposit, fee, is_paid):
    conn = get_connection()
    conn.execute("UPDATE jobs SET deposit=?, survey_fee=?, is_paid=? WHERE id=?", (1 if deposit else 0, fee, 1 if is_paid else 0, jid))
    conn.commit(); conn.close()

def pause_job(jid, rs, u):
    conn = get_connection(); conn.execute("UPDATE jobs SET status='Tạm dừng' WHERE id=?", (jid,)); conn.commit(); conn.close()
    save_log_entry(jid, "Tạm dừng", u, f"Lý do: {rs}", "")

def resume_job(jid, u):
    conn = get_connection(); conn.execute("UPDATE jobs SET status='Đang xử lý' WHERE id=?", (jid,)); conn.commit(); conn.close()
    save_log_entry(jid, "Khôi phục", u, "Tiếp tục", "")

def terminate_job(jid, rs, u):
    conn = get_connection(); conn.execute("UPDATE jobs SET status='Kết thúc sớm' WHERE id=?", (jid,)); conn.commit(); conn.close()
    save_log_entry(jid, "Kết thúc sớm", u, f"Lý do: {rs}", "")

def get_my_jobs(u, r):
    conn = get_connection()
    # Lấy tất cả các công việc chưa hoàn thành
    query = "SELECT * FROM jobs WHERE status NOT IN ('Hoàn thành', 'Kết thúc sớm') ORDER BY deadline ASC"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty: return df
    
    # LỌC MỚI: Kiểm tra xem tên user có nằm trong chuỗi "user1, user2" không
    # (Kể cả Quản lý cũng chỉ thấy việc được giao cho mình ở Tab này)
    
    # Hàm lọc custom
    def is_assigned_to_me(assigned_str):
        if not assigned_str: return False
        # Tách chuỗi "admin, hung" thành list ["admin", "hung"] rồi check
        assignees = [x.strip() for x in str(assigned_str).split(",")]
        return u in assignees

    # Áp dụng lọc
    return df[df['assigned_to'].apply(is_assigned_to_me)]

def get_all_jobs_for_admin():
    # Hàm riêng cho Admin xem hết ở các Tab khác (Báo cáo, Tra cứu)
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM jobs ORDER BY deadline ASC", conn)
    conn.close()
    return df

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
    conn = get_connection(); df = pd.read_sql_query("SELECT * FROM jobs WHERE is_paid = 0 OR is_paid IS NULL", conn); conn.close(); return df

# --- VISUAL ---
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

# --- UI ---
st.set_page_config(page_title="Hệ thống Đo Đạc", layout="wide", page_icon="🏗️")
init_db()

if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False

if not st.session_state['logged_in']:
    st.title("🔐 Đăng nhập V16.0")
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
    try: df_all = get_all_jobs_for_admin()
    except: df_all = pd.DataFrame()

    if not df_all.empty:
        st.sidebar.markdown("---"); st.sidebar.subheader("🔔 Cảnh báo hạn"); now = datetime.now()
        # Lọc thông báo cho user hiện tại (giống get_my_jobs)
        my_alert_df = df_all[df_all['assigned_to'].apply(lambda x: user in str(x).split(", ")) & (~df_all['status'].isin(['Hoàn thành', 'Kết thúc sớm']))]
        
        if not my_alert_df.empty:
            my_alert_df['dl_dt'] = pd.to_datetime(my_alert_df['deadline'])
            over = my_alert_df[my_alert_df['dl_dt'] < now]
            soon = my_alert_df[(my_alert_df['dl_dt'] >= now) & (my_alert_df['dl_dt'] <= now + timedelta(days=1))]
            if not over.empty: st.sidebar.error(f"🔴 {len(over)} HS Quá hạn"); st.sidebar.dataframe(over[['customer_name']], hide_index=True)
            if not soon.empty: st.sidebar.warning(f"🟡 {len(soon)} HS Sắp đến"); st.sidebar.dataframe(soon[['customer_name']], hide_index=True)

    menu = ["🏠 Việc Của Tôi", "🔍 Tra Cứu", "📝 Tạo Hồ Sơ", "📊 Báo Cáo"]
    if role == "Quản lý": menu.insert(1, "💰 Công Nợ"); menu.append("👥 Nhân Sự")
    sel = st.sidebar.radio("Menu", menu)

    if sel == "🏠 Việc Của Tôi":
        st.title("📋 Tiến trình hồ sơ (Đa nhiệm)")
        df = get_my_jobs(user, role)
        if df.empty: st.info("Tuyệt vời! Bạn không có việc tồn đọng.")
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
                    
                    t1, t2, t3, t4 = st.tabs(["ℹ️ Thông tin & File", "⚙️ Xử lý Hồ sơ", "💰 Tài Chính", "📜 Nhật ký"])
                    
                    with t1:
                        st.subheader(f"👤 {j['customer_name']}")
                        if j.get('is_survey_only') == 1: st.warning("🛠️ CHỈ ĐO ĐẠC")
                        c1, c2 = st.columns(2)
                        c1.write(f"📞 **{j['customer_phone']}**"); c2.write(f"📍 {j['address']}")
                        c1.write(f"⏰ Hạn: **{j['deadline']}**"); c2.write(f"Trạng thái: {j['status']}")
                        # Hiển thị người đang cùng làm (nếu có nhiều người)
                        st.info(f"👥 Nhóm xử lý: {j['assigned_to']}") 
                        st.markdown("---")
                        st.markdown("**📂 File đính kèm:**")
                        logs = get_logs(j['id'])
                        files_found = False
                        for x, l in logs.iterrows():
                            if l['file_path'] and os.path.exists(l['file_path']):
                                files_found = True
                                fn = os.path.basename(l['file_path'])
                                with open(l['file_path'], "rb") as f:
                                    st.download_button(f"⬇️ {fn}", f, file_name=fn, key=f"dl_{j['id']}_{x}")
                        if not files_found: st.caption("Chưa có file.")

                    with t2:
                        if j['status'] in ['Tạm dừng', 'Kết thúc sớm']:
                            st.error(f"HỒ SƠ ĐANG: {j['status'].upper()}")
                            if j['status'] == 'Tạm dừng' and st.button("▶️ Tiếp tục", key=f"r{j['id']}"): resume_job(j['id'], user); st.rerun()
                        else:
                            with st.form(f"f{j['id']}"):
                                nt = st.text_area("Ghi chú"); fl = st.file_uploader("Upload File")
                                cur = j['current_stage']; nxt = "7. Hoàn thành" if j.get('is_survey_only')==1 and cur=="3. Làm hồ sơ" else WORKFLOW_DEFAULT.get(cur)
                                asn_list=[]; d=1
                                if nxt and nxt!="7. Hoàn thành": 
                                    st.write(f"Chuyển sang: **{nxt}**")
                                    # SỬA ĐỔI: CHO PHÉP CHỌN NHIỀU NGƯỜI
                                    asn_list = st.multiselect("Giao cho (Chọn nhiều):", get_active_users())
                                    d = st.number_input("Hạn", value=2)
                                else: st.info("Kết thúc")
                                
                                if st.form_submit_button("✅ Chuyển bước"): 
                                    if not asn_list and nxt!="7. Hoàn thành":
                                        st.error("Vui lòng chọn người nhận việc!")
                                    else:
                                        dep = 1 if j.get('deposit')==1 else 0; fee = j.get('survey_fee') or 0; pdone = 1 if j.get('is_paid')==1 else 0
                                        update_stage(j['id'], cur, nt, fl, user, asn_list, d, j.get('is_survey_only'), dep, fee, pdone, j['customer_name']); st.success("Xong!"); time.sleep(0.5); st.rerun()
                            
                            c_stop1, c_stop2 = st.columns(2)
                            if c_stop1.button("⏸️ Dừng", key=f"p{j['id']}"): st.session_state[f'pm_{j['id']}'] = True
                            if c_stop2.button("⏹️ Kết thúc", key=f"t{j['id']}"): st.session_state[f'tm_{j['id']}'] = True
                            
                            if st.session_state.get(f'pm_{j['id']}', False):
                                rs = st.text_input("Lý do dừng:", key=f"rs{j['id']}")
                                if st.button("OK Dừng", key=f"okp{j['id']}"): pause_job(j['id'], rs, user); st.rerun()
                            if st.session_state.get(f'tm_{j['id']}', False):
                                rst = st.text_input("Lý do kết thúc:", key=f"rst{j['id']}")
                                if st.button("OK Kết thúc", key=f"okt{j['id']}"): terminate_job(j['id'], rst, user); st.rerun()

                    with t3:
                        st.markdown("#### 💰 Quản lý thu chi")
                        with st.form(f"money_{j['id']}"):
                            dep_ok = st.checkbox("Đã thu tạm ứng?", value=(j.get('deposit')==1))
                            fee = st.number_input("Phí đo đạc", value=j.get('survey_fee') or 0, step=100000)
                            paid_ok = st.checkbox("Đã thu đủ tiền?", value=(j.get('is_paid')==1))
                            if st.form_submit_button("💾 Lưu Tài Chính"): update_finance_only(j['id'], dep_ok, fee, paid_ok); st.success("Lưu!"); st.rerun()

                    with t4:
                        st.markdown("#### 📜 Nhật ký xử lý")
                        logs = get_logs(j['id'])
                        for x, l in logs.iterrows():
                            st.text(f"{pd.to_datetime(l['timestamp']).strftime('%d/%m %H:%M')} | {l['action_by']}: {l['note']}")
                            if l['file_path']: st.caption(f"📎 {os.path.basename(l['file_path'])}")

    # --- CÁC TAB KHÁC GIỮ NGUYÊN ---
    elif sel == "💰 Công Nợ":
        st.title("💰 Quản Lý Công Nợ")
        try:
            df = get_all_jobs_for_admin() # Dùng hàm lấy hết
            if not df.empty:
                unpaid = df[df['is_paid'].apply(lambda x: int(x) if pd.notna(x) else 0) == 0]
                st.metric("Tổng hồ sơ chưa thu tiền", len(unpaid))
                if not unpaid.empty:
                    unpaid['Mã'] = unpaid.apply(lambda x: generate_code(x['id'], x['start_time'], x['customer_name']), axis=1)
                    st.dataframe(
                        unpaid[['Mã', 'customer_phone', 'survey_fee', 'deposit', 'assigned_to']],
                        column_config={
                            "Mã": "Hồ sơ", "customer_phone": "SĐT",
                            "survey_fee": st.column_config.NumberColumn("Phí (VNĐ)", format="%d"),
                            "deposit": st.column_config.CheckboxColumn("Đã cọc?"),
                            "assigned_to": "Người đang giữ"
                        }, use_container_width=True
                    )
                else: st.success("Sạch nợ!")
        except: pass

    elif sel == "📝 Tạo Hồ Sơ":
        st.title("Tạo Hồ Sơ")
        with st.form("new"):
            c1, c2 = st.columns(2); n = c1.text_input("Tên"); p = c2.text_input("SĐT"); a = st.text_input("Đ/c"); f = st.file_uploader("File")
            st.divider(); c_o, c_a = st.columns(2); is_sv = c_o.checkbox("🛠️ CHỈ ĐO ĐẠC"); st.markdown("---"); st.write("💰 **Phí:**"); c_m1, c_m2 = st.columns(2); dep_ok = c_m1.checkbox("Đã tạm ứng?"); fee_val = c_m2.number_input("Phí:", value=0, step=100000)
            
            # SỬA ĐỔI: CHỌN NHIỀU NGƯỜI LÚC TẠO
            asn_list = st.multiselect("Giao cho (Chọn nhiều):", get_active_users())
            
            d = st.number_input("Hạn", value=1)
            if st.form_submit_button("Tạo"):
                if n and asn_list: add_job(n, p, a, f, user, asn_list, d, is_sv, dep_ok, fee_val); st.success("OK!"); st.rerun()
                else: st.error("Thiếu tin")

    elif sel == "🔍 Tra Cứu":
        st.title("Tra Cứu"); q = st.text_input("Tìm kiếm")
        if q:
            df = get_all_jobs_for_admin(); res = df[df.apply(lambda r: q.lower() in str(r).lower(), axis=1)]; st.dataframe(res)

    elif sel == "📊 Báo Cáo":
        st.title("Thống Kê"); df = get_all_jobs_for_admin()
        if not df.empty: st.bar_chart(df['current_stage'].value_counts()); st.dataframe(df)
            
    elif sel == "👥 Nhân Sự":
        if role == "Quản lý":
            st.title("Phân Quyền"); df = get_all_users()
            for i, u in df.iterrows():
                c1, c2 = st.columns([2, 2]); c1.write(f"**{u['username']}** ({u['fullname']})")
                if u['username']!=user:
                    idx = ROLES.index(u['role']) if u['role'] in ROLES else 2
                    nr = c2.selectbox("Quyền", ROLES, index=idx, key=u['username'])
                    if nr!=u['role']: update_user_role(u['username'], nr); st.toast("Lưu!"); st.rerun()
        else: st.error("Cấm truy cập!")
