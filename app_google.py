import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import time
import requests
import threading
import hashlib
import re
import calendar
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# --- 1. CẤU HÌNH ---
TELEGRAM_TOKEN = ""
TELEGRAM_CHAT_ID = ""

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
ROLES = ["Quản lý", "Nhân viên", "Chưa cấp quyền"]
STAGES_ORDER = ["1. Tạo mới", "2. Đo đạc", "3. Làm hồ sơ", "4. Ký hồ sơ", "5. Lấy hồ sơ", "6. Nộp hồ sơ", "7. Hoàn thành"]
WORKFLOW_DEFAULT = {"1. Tạo mới": "2. Đo đạc", "2. Đo đạc": "3. Làm hồ sơ", "3. Làm hồ sơ": "4. Ký hồ sơ", "4. Ký hồ sơ": "5. Lấy hồ sơ", "5. Lấy hồ sơ": "6. Nộp hồ sơ", "6. Nộp hồ sơ": "7. Hoàn thành", "7. Hoàn thành": None}

# --- 2. VISUAL (THANH TIẾN ĐỘ & MÀU SẮC) ---
def render_progress_bar(current_stage, status):
    try: idx = STAGES_ORDER.index(current_stage)
    except: idx = 0
    active_color = "#dc3545" if status == "Tạm dừng" else "#ffc107"
    
    st.markdown(f"""
    <style>
        .step-container {{display: flex; justify-content: space-between; margin-bottom: 15px;}}
        .step-item {{flex: 1; text-align: center; position: relative;}}
        .step-item:not(:last-child)::after {{content: ''; position: absolute; top: 15px; left: 50%; width: 100%; height: 2px; background: #e0e0e0; z-index: -1;}}
        .step-circle {{width: 30px; height: 30px; margin: 0 auto 5px; border-radius: 50%; line-height: 30px; color: white; font-weight: bold; font-size: 12px;}}
        .done {{background: #28a745;}} .active {{background: {active_color}; color: black; box-shadow: 0 0 0 3px rgba(0,0,0,0.1);}} .pending {{background: #e9ecef; color: #999;}}
    </style>""", unsafe_allow_html=True)
    
    h = '<div class="step-container">'
    for i, s in enumerate(STAGES_ORDER):
        cls = "done" if i < idx else "active" if i == idx else "pending"
        ico = "✓" if i < idx else str(i+1)
        if i == idx and status == "Tạm dừng": ico = "⛔"
        h += f'<div class="step-item"><div class="step-circle {cls}">{ico}</div><div style="font-size:11px">{s.split(". ")[1]}</div></div>'
    st.markdown(h+'</div>', unsafe_allow_html=True)

# --- 3. KẾT NỐI GOOGLE ---
def get_gcp_creds(): return Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)

def get_sheet(sheet_name="DB_DODAC"):
    creds = get_gcp_creds(); client = gspread.authorize(creds)
    try: return client.open(sheet_name).sheet1
    except: st.error(f"Không tìm thấy Sheet '{sheet_name}'"); return None

def get_users_sheet():
    creds = get_gcp_creds(); client = gspread.authorize(creds)
    try: 
        sh = client.open("DB_DODAC")
        try: return sh.worksheet("USERS")
        except: ws = sh.add_worksheet(title="USERS", rows="100", cols="5"); ws.append_row(["username", "password", "fullname", "role", "email"]); return ws
    except: return None

def upload_to_drive(file_obj, folder_name):
    if not file_obj: return ""
    try:
        creds = get_gcp_creds(); service = build('drive', 'v3', credentials=creds)
        q = "mimeType='application/vnd.google-apps.folder' and name='APP_DATA'"
        res = service.files().list(q=q, fields="files(id)").execute()
        if not res.get('files'): return ""
        pid = res['files'][0]['id']
        q_sub = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and '{pid}' in parents"
        res_sub = service.files().list(q=q_sub, fields="files(id)").execute()
        fid = res_sub['files'][0]['id'] if res_sub.get('files') else service.files().create(body={'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [pid]}, fields='id').execute().get('id')
        media = MediaIoBaseUpload(file_obj, mimetype=file_obj.type)
        f = service.files().create(body={'name': file_obj.name, 'parents': [fid]}, media_body=media, fields='webViewLink').execute()
        return f.get('webViewLink')
    except: return ""

# --- 4. LOGIC ---
def make_hash(p): return hashlib.sha256(str.encode(p)).hexdigest()
def send_telegram_msg(msg):
    if not TELEGRAM_TOKEN: return
    def run(): 
        try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"})
        except: pass
    threading.Thread(target=run).start()

def login_user(u, p):
    sh = get_users_sheet(); 
    if not sh: return None
    try: 
        cell = sh.find(u)
        if cell:
            row = sh.row_values(cell.row)
            if row[1] == make_hash(p): return row
    except: pass
    return None

def create_user(u, p, n, e):
    sh = get_users_sheet()
    if not sh: return False
    try: 
        if sh.find(u): return False
        sh.append_row([u, make_hash(p), n, "Chưa cấp quyền", e]); return True
    except: return False

def get_all_users(): sh = get_users_sheet(); return pd.DataFrame(sh.get_all_records())
def update_user_role(u, r): sh = get_users_sheet(); c = sh.find(u); sh.update_cell(c.row, 4, r)
def get_active_users_list(): df = get_all_users(); return df[df['role']!='Chưa cấp quyền'].apply(lambda x: f"{x['username']} - {x['fullname']}", axis=1).tolist() if not df.empty else []

def get_all_jobs_df():
    sh = get_sheet(); data = sh.get_all_records(); df = pd.DataFrame(data)
    if not df.empty: df['id'] = df['id'].astype(int)
    return df

def add_job(n, p, a, f, u, asn, d, is_survey):
    sh = get_sheet(); now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dl = (datetime.now()+timedelta(days=d)).strftime("%Y-%m-%d %H:%M:%S")
    jid = int(time.time())
    link = upload_to_drive(f, f"{jid}_{n}")
    log = f"[{now}] {u}: Khởi tạo | File: {link}"
    asn_clean = asn.split(" - ")[0] if asn else ""
    sv_flag = 1 if is_survey else 0
    # Cột: id, start, name, phone, addr, stage, status, assign, deadline, file, logs, is_survey
    sh.append_row([jid, now, n, p, a, "1. Tạo mới", "Đang xử lý", asn_clean, dl, link, log, sv_flag])
    type_msg = "(CHỈ ĐO ĐẠC)" if is_survey else ""
    send_telegram_msg(f"🚀 <b>MỚI #{jid} {type_msg}</b>\n👤 {n}\n👉 {asn_clean}")

def update_stage(jid, stg, nt, f, u, asn, d, is_survey):
    sh = get_sheet(); cell = sh.find(str(jid)); r = cell.row
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); lnk = ""
    if f: cn = sh.cell(r, 3).value; lnk = upload_to_drive(f, f"{jid}_{cn}")
    
    nxt = "7. Hoàn thành" if is_survey==1 and stg=="3. Làm hồ sơ" else WORKFLOW_DEFAULT.get(stg)
    if nxt:
        sh.update_cell(r, 6, nxt)
        if asn: sh.update_cell(r, 8, asn.split(" - ")[0])
        sh.update_cell(r, 9, (datetime.now()+timedelta(days=d)).strftime("%Y-%m-%d %H:%M:%S"))
        olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{now}] {u}: {stg}->{nxt} | Note: {nt} | File: {lnk}")
        if nxt=="7. Hoàn thành": sh.update_cell(r, 7, "Hoàn thành")
        send_telegram_msg(f"✅ <b>UPDATE #{jid}</b>\n{stg}->{nxt}\n👤 {u}")

def pause_job(jid, rs, u):
    sh = get_sheet(); r = sh.find(str(jid)).row
    sh.update_cell(r, 7, "Tạm dừng")
    olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{datetime.now()}] {u}: TẠM DỪNG: {rs}")
    send_telegram_msg(f"⛔ <b>PAUSE #{jid}</b>\n{rs}")

def resume_job(jid, u):
    sh = get_sheet(); r = sh.find(str(jid)).row
    sh.update_cell(r, 7, "Đang xử lý")
    olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{datetime.now()}] {u}: KHÔI PHỤC")

def generate_code(jid, start, name):
    try: d = datetime.strptime(start, "%Y-%m-%d %H:%M:%S").strftime('%d%m%y')
    except: d = datetime.now().strftime('%d%m%y')
    return f"{d}-{int(jid)} {name}"

# --- 5. UI MAIN ---
st.set_page_config(page_title="Đo Đạc Cloud V8", page_icon="☁️", layout="wide")

if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False

if not st.session_state['logged_in']:
    st.title("🔐 Cloud V8.0")
    c1, c2 = st.columns(2)
    with c1:
        u = st.text_input("User"); p = st.text_input("Pass", type='password')
        if st.button("Đăng nhập"):
            d = login_user(u, p)
            if d: st.session_state['logged_in']=True; st.session_state['user']=d[0]; st.session_state['role']=d[3]; st.rerun()
            else: st.error("Sai!")
    with c2:
        nu = st.text_input("User Mới"); np = st.text_input("Pass Mới", type='password'); nn = st.text_input("Họ Tên"); ne = st.text_input("Email")
        if st.button("Đăng Ký"):
            if create_user(nu, np, nn, ne): st.success("OK! Chờ duyệt.")
            else: st.error("Trùng!")
else:
    user = st.session_state['user']; role = st.session_state['role']
    st.sidebar.title(f"👤 {user}"); st.sidebar.info(f"{role}")
    if st.sidebar.button("Logout"): st.session_state['logged_in']=False; st.rerun()
    
    menu = ["🏠 Việc Của Tôi", "🔍 Tra Cứu", "📝 Tạo Mới", "📊 Báo Cáo"]
    if role == "Quản lý": menu.append("👥 Nhân Sự")
    sel = st.sidebar.radio("Menu", menu)

    if sel == "🏠 Việc Của Tôi":
        st.title("📋 Dashboard Công Việc")
        try:
            df = get_all_jobs_df()
            if df.empty: st.info("Trống!")
            else:
                my_df = df if role == "Quản lý" else df[(df['assigned_to'].astype(str) == user) & (df['status'] != 'Hoàn thành')]
                if my_df.empty: st.info("Hết việc!")
                else:
                    # KPI Cards
                    now = datetime.now()
                    my_df['dl_dt'] = pd.to_datetime(my_df['deadline'])
                    over = my_df[my_df['dl_dt'] < now]
                    soon = my_df[(my_df['dl_dt'] >= now) & (my_df['dl_dt'] <= now + timedelta(days=1))]
                    
                    k1, k2, k3, k4 = st.columns(4)
                    k1.metric("🔴 Quá Hạn", len(over), border=True)
                    k2.metric("🟡 Gấp", len(soon), border=True)
                    k3.metric("🟢 Trong Hạn", len(my_df) - len(over) - len(soon), border=True)
                    k4.metric("Tổng Cộng", len(my_df), border=True)
                    st.divider()

                    for i, j in my_df.iterrows():
                        code = generate_code(j['id'], j['start_time'], j['customer_name'])
                        icon = "⛔" if j['status']=='Tạm dừng' else ("🔴" if j['dl_dt'] < now else "🟡" if j['dl_dt'] <= now+timedelta(days=1) else "🟢")
                        txt = f"{j['current_stage']} (TẠM DỪNG)" if j['status']=='Tạm dừng' else j['current_stage']
                        
                        with st.expander(f"{icon} {code} | {txt}"):
                            render_progress_bar(j['current_stage'], j['status'])
                            st.subheader(f"👤 {j['customer_name']}")
                            if j.get('is_survey_only') == 1: st.warning("🛠️ CHỈ ĐO ĐẠC")
                            
                            c1, c2 = st.columns([1.5, 1])
                            with c1:
                                st.write(f"📞 {j['customer_phone']} | 📍 {j['address']}")
                                st.write(f"⏰ Hạn: **{j['deadline']}**")
                                st.info(f"📜 **Logs:**\n{j['logs']}")
                                if j['file_link']: st.markdown(f"[📂 Mở file đính kèm]({j['file_link']})")
                            with c2:
                                if j['status']=='Tạm dừng':
                                    if st.button("▶️ Tiếp tục", key=f"r{j['id']}"): resume_job(j['id'], user); st.rerun()
                                else:
                                    with st.form(f"f{j['id']}"):
                                        nt = st.text_area("Note"); fl = st.file_uploader("KQ")
                                        cur = j['current_stage']; is_sv = j.get('is_survey_only', 0)
                                        nxt = "7. Hoàn thành" if is_sv==1 and cur=="3. Làm hồ sơ" else WORKFLOW_DEFAULT.get(cur)
                                        
                                        if nxt and nxt!="7. Hoàn thành":
                                            st.write(f"Sang: **{nxt}**"); asn = st.selectbox("Giao", get_active_users_list())
                                            d = st.number_input("Hạn", value=2)
                                        else:
                                            st.info("Kết thúc quy trình")
                                            asn = ""; d = 0
                                            
                                        if st.form_submit_button("✅ Chuyển"):
                                            update_stage(j['id'], cur, nt, fl, user, asn, d, is_sv)
                                            st.success("Done!"); time.sleep(1); st.rerun()
                                    if st.button("⏸️ Dừng", key=f"p{j['id']}"):
                                        st.session_state[f'pm_{j['id']}'] = True
                                    
                                    if st.session_state.get(f'pm_{j['id']}', False):
                                        rs = st.text_input("Lý do:", key=f"rs{j['id']}")
                                        if st.button("OK", key=f"ok{j['id']}"): pause_job(j['id'], rs, user); st.rerun()

        except Exception as e: st.error(f"Lỗi: {e}")

    elif sel == "📝 Tạo Mới":
        st.title("Tạo Hồ Sơ")
        with st.form("new"):
            c1, c2 = st.columns(2); n = c1.text_input("Tên"); p = c2.text_input("SĐT"); a = st.text_input("Đ/c"); f = st.file_uploader("File")
            st.divider()
            c_o, c_a = st.columns(2)
            is_sv = c_o.checkbox("🛠️ CHỈ ĐO ĐẠC")
            asn = c_a.selectbox("Giao cho", get_active_users_list()); d = st.number_input("Hạn", value=1)
            if st.form_submit_button("Tạo"):
                if n and asn: add_job(n, p, a, f, user, asn, d, is_sv); st.success("OK!")
                else: st.error("Thiếu tin")

    elif sel == "🔍 Tra Cứu":
        st.title("Tra Cứu"); q = st.text_input("Tìm kiếm")
        if q:
            df = get_all_jobs_df()
            res = df[df.apply(lambda r: q.lower() in str(r).lower(), axis=1)]
            st.dataframe(res)

    elif sel == "📊 Báo Cáo":
        st.title("Thống Kê")
        df = get_all_jobs_df()
        if not df.empty:
            st.bar_chart(df['current_stage'].value_counts())
            st.dataframe(df)
            
    elif sel == "👥 Nhân Sự":
        if role == "Quản lý":
            st.title("Phân Quyền")
            df = get_all_users()
            for i, u in df.iterrows():
                c1, c2 = st.columns([2, 2]); c1.write(f"**{u['username']}** ({u['fullname']})")
                if u['username']!=user:
                    idx = ROLES.index(u['role']) if u['role'] in ROLES else 2
                    nr = c2.selectbox("Role", ROLES, index=idx, key=u['username'])
                    if nr!=u['role']: update_user_role(u['username'], nr); st.toast("Lưu!"); st.rerun()
        else: st.error("Cấm!")
