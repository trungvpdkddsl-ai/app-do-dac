import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import time
import requests
import threading
import hashlib
import re
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

# --- 2. KẾT NỐI GOOGLE ---
def get_gcp_creds():
    return Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)

def get_sheet(sheet_name="DB_DODAC"):
    creds = get_gcp_creds()
    client = gspread.authorize(creds)
    try:
        return client.open(sheet_name).sheet1
    except:
        st.error(f"Không tìm thấy Sheet '{sheet_name}'")
        return None

def get_users_sheet():
    creds = get_gcp_creds()
    client = gspread.authorize(creds)
    try:
        sh = client.open("DB_DODAC")
        try:
            return sh.worksheet("USERS")
        except:
            ws = sh.add_worksheet(title="USERS", rows="100", cols="5")
            ws.append_row(["username", "password", "fullname", "role"])
            return ws
    except:
        return None

def upload_to_drive(file_obj, folder_name):
    if not file_obj: return ""
    try:
        creds = get_gcp_creds()
        service = build('drive', 'v3', credentials=creds)
        q = "mimeType='application/vnd.google-apps.folder' and name='APP_DATA'"
        res = service.files().list(q=q, fields="files(id)").execute()
        if not res.get('files'): return "Err: No APP_DATA folder"
        pid = res['files'][0]['id']
        q_sub = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and '{pid}' in parents"
        res_sub = service.files().list(q=q_sub, fields="files(id)").execute()
        
        if res_sub.get('files'):
            fid = res_sub['files'][0]['id']
        else:
            fid = service.files().create(body={'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [pid]}, fields='id').execute().get('id')
            
        media = MediaIoBaseUpload(file_obj, mimetype=file_obj.type)
        f = service.files().create(body={'name': file_obj.name, 'parents': [fid]}, media_body=media, fields='webViewLink').execute()
        return f.get('webViewLink')
    except:
        return ""

# --- 3. LOGIC USER ---
def make_hash(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def login_user(u, p):
    sh = get_users_sheet()
    if not sh: return None
    try:
        cell = sh.find(u)
        if cell:
            row = sh.row_values(cell.row) # [user, pass, name, role]
            if row[1] == make_hash(p): return row
    except: pass
    return None

def create_user(u, p, n):
    sh = get_users_sheet()
    if not sh: return False
    try:
        if sh.find(u): return False
        sh.append_row([u, make_hash(p), n, "Chưa cấp quyền"])
        return True
    except: return False

def get_all_users():
    sh = get_users_sheet()
    return pd.DataFrame(sh.get_all_records())

def update_user_role(u, r):
    sh = get_users_sheet()
    try:
        cell = sh.find(u)
        if cell: sh.update_cell(cell.row, 4, r)
    except: pass

def get_active_users_list():
    df = get_all_users()
    if df.empty: return []
    return df[df['role'] != 'Chưa cấp quyền'].apply(lambda x: f"{x['username']} - {x['fullname']}", axis=1).tolist()

# --- 4. LOGIC APP ---
def send_telegram_msg(msg):
    if not TELEGRAM_TOKEN: return
    def run():
        try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"})
        except: pass
    threading.Thread(target=run).start()

def get_all_jobs_df():
    sh = get_sheet()
    data = sh.get_all_records()
    df = pd.DataFrame(data)
    if not df.empty: df['id'] = df['id'].astype(int)
    return df

def add_job(n, p, a, f, u, asn, d):
    sh = get_sheet()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dl = (datetime.now()+timedelta(days=d)).strftime("%Y-%m-%d %H:%M:%S")
    jid = int(time.time())
    link = upload_to_drive(f, f"{jid}_{n}")
    log = f"[{now}] {u}: Khởi tạo | File: {link}"
    asn_clean = asn.split(" - ")[0] if asn else ""
    sh.append_row([jid, now, n, p, a, "1. Tạo mới", "Đang xử lý", asn_clean, dl, link, log])
    send_telegram_msg(f"🚀 <b>MỚI #{jid}</b>\n👤 {n}\n👉 Giao: {asn_clean}")

def update_stage(jid, stg, nt, f, u, asn, d):
    sh = get_sheet()
    cell = sh.find(str(jid))
    if cell:
        r = cell.row
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lnk = ""
        if f:
            c_name = sh.cell(r, 3).value
            lnk = upload_to_drive(f, f"{jid}_{c_name}")
        
        nxt = WORKFLOW_DEFAULT.get(stg)
        if nxt:
            sh.update_cell(r, 6, nxt)
            if asn: sh.update_cell(r, 8, asn.split(" - ")[0])
            ndl = (datetime.now()+timedelta(days=d)).strftime("%Y-%m-%d %H:%M:%S")
            sh.update_cell(r, 9, ndl)
            
            olog = sh.cell(r, 11).value
            nlog = f"\n[{now}] {u}: {stg}->{nxt} | Note: {nt} | File: {lnk}"
            sh.update_cell(r, 11, olog+nlog)
            
            if nxt=="7. Hoàn thành": sh.update_cell(r, 7, "Hoàn thành")
            send_telegram_msg(f"✅ <b>UPDATE #{jid}</b>\n{stg}->{nxt}\n👤 {u}")

def pause_job(jid, rs, u):
    sh = get_sheet()
    cell = sh.find(str(jid))
    if cell:
        r = cell.row
        sh.update_cell(r, 7, "Tạm dừng")
        olog = sh.cell(r, 11).value
        nlog = f"\n[{datetime.now()}] {u}: TẠM DỪNG | Lý do: {rs}"
        sh.update_cell(r, 11, olog+nlog)
        send_telegram_msg(f"⛔ <b>PAUSE #{jid}</b>\nLý do: {rs}")

def resume_job(jid, u):
    sh = get_sheet()
    cell = sh.find(str(jid))
    if cell:
        r = cell.row
        sh.update_cell(r, 7, "Đang xử lý")
        olog = sh.cell(r, 11).value
        nlog = f"\n[{datetime.now()}] {u}: KHÔI PHỤC"
        sh.update_cell(r, 11, olog+nlog)

# --- 5. VISUAL ---
def render_progress_bar(current_stage, status):
    try: idx = STAGES_ORDER.index(current_stage)
    except: idx = 0
    color = "#dc3545" if status == "Tạm dừng" else "#ffc107"
    
    st.markdown(f"""<style>
        .step-container {{display: flex; justify-content: space-between; margin-bottom: 15px;}}
        .step-item {{flex: 1; text-align: center; position: relative;}}
        .step-item:not(:last-child)::after {{content: ''; position: absolute; top: 15px; left: 50%; width: 100%; height: 2px; background: #e0e0e0; z-index: -1;}}
        .step-circle {{width: 30px; height: 30px; margin: 0 auto 5px; border-radius: 50%; line-height: 30px; color: white; font-weight: bold; font-size: 12px;}}
        .done {{background: #28a745;}} .active {{background: {color}; color: black;}} .pending {{background: #e9ecef; color: #999;}}
    </style>""", unsafe_allow_html=True)
    
    h = '<div class="step-container">'
    for i, s in enumerate(STAGES_ORDER):
        cls = "done" if i < idx else "active" if i == idx else "pending"
        ico = "✓" if i < idx else str(i+1)
        if i == idx and status == "Tạm dừng": ico = "⛔"
        h += f'<div class="step-item"><div class="step-circle {cls}">{ico}</div><div style="font-size:11px">{s.split(". ")[1]}</div></div>'
    st.markdown(h+'</div>', unsafe_allow_html=True)

def generate_code(jid, start, name):
    try: d = datetime.strptime(start, "%Y-%m-%d %H:%M:%S").strftime('%d%m%y')
    except: d = datetime.now().strftime('%d%m%y')
    return f"{d}-{int(jid):03d} {name}"

# --- 6. UI MAIN ---
st.set_page_config(page_title="Đo Đạc Cloud Pro", page_icon="☁️", layout="wide")

if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False

if not st.session_state['logged_in']:
    st.title("🔐 Đăng nhập Hệ thống Cloud")
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Đăng nhập")
        u = st.text_input("User")
        p = st.text_input("Pass", type='password')
        if st.button("Login"):
            d = login_user(u, p)
            if d: 
                st.session_state['logged_in']=True
                st.session_state['user']=d[0]
                st.session_state['role']=d[3]
                st.rerun()
            else: 
                st.error("Sai thông tin hoặc chưa có tài khoản!")
    with c2:
        st.subheader("Đăng ký")
        nu = st.text_input("User mới")
        np = st.text_input("Pass mới", type='password')
        nn = st.text_input("Họ Tên")
        if st.button("Đăng ký"): 
            if create_user(nu, np, nn): 
                st.success("OK! Chờ duyệt.")
            else: 
                st.error("Trùng tên!")
else:
    user = st.session_state['user']
    role = st.session_state['role']
    st.sidebar.title(f"👤 {user}")
    st.sidebar.info(f"{role}")
    if st.sidebar.button("Đăng xuất"):
        st.session_state['logged_in']=False
        st.rerun()
    
    menu = ["🏠 Việc Của Tôi", "🔍 Tra Cứu", "📝 Tạo Mới", "📊 Báo Cáo"]
    if role == "Quản lý": menu.append("👥 Nhân Sự")
    sel = st.sidebar.radio("Menu", menu)

    if sel == "🏠 Việc Của Tôi":
        st.title("📋 Việc Cần Làm")
        try:
            df = get_all_jobs_df()
            if df.empty:
                st.info("Trống!")
            else:
                if role != "Quản lý":
                    my_df = df[(df['assigned_to'].astype(str) == user) & (df['status'] != 'Hoàn thành')]
                else:
                    my_df = df[df['status'] != 'Hoàn thành']
                
                if my_df.empty:
                    st.info("Hết việc!")
                else:
                    st.metric("Tổng số hồ sơ", len(my_df))
                    for i, j in my_df.iterrows():
                        code = generate_code(j['id'], j['start_time'], j['customer_name'])
                        icon = "⛔" if j['status']=='Tạm dừng' else "🟢"
                        with st.expander(f"{icon} {code} | {j['current_stage']}"):
                            render_progress_bar(j['current_stage'], j['status'])
                            c1, c2 = st.columns([1.5, 1])
                            with c1:
                                st.write(f"📞 {j['customer_phone']} | 📍 {j['address']}")
                                st.info(f"📜 **Logs:**\n{j['logs']}")
                                if j['file_link']: st.markdown(f"[📂 File đính kèm]({j['file_link']})")
                            with c2:
                                if j['status']=='Tạm dừng':
                                    if st.button("▶️ Tiếp tục", key=f"r{j['id']}"):
                                        resume_job(j['id'], user)
                                        st.rerun()
                                else:
                                    with st.form(f"f{j['id']}"):
                                        nt = st.text_area("Note")
                                        fl = st.file_uploader("KQ")
                                        cur = j['current_stage']
                                        nxt = WORKFLOW_DEFAULT.get(cur)
                                        asn = st.selectbox("Giao", get_active_users_list())
                                        d = st.number_input("Hạn", value=1)
                                        if nxt and nxt!="7. Hoàn thành": st.write(f"Sang: {nxt}")
                                        if st.form_submit_button("✅ Chuyển"):
                                            update_stage(j['id'], cur, nt, fl, user, asn, d)
                                            st.success("Done!")
                                            time.sleep(1)
                                            st.rerun()
                                    if st.button("⏸️ Dừng", key=f"p{j['id']}"):
                                        pause_job(j['id'], "Tạm dừng", user)
                                        st.rerun()
        except Exception as e: st.error(f"Lỗi tải: {e}")

    elif sel == "📝 Tạo Mới":
        st.title("Tạo Hồ Sơ")
        with st.form("new"):
            n = st.text_input("Tên")
            p = st.text_input("SĐT")
            a = st.text_input("Đ/c")
            f = st.file_uploader("File")
            asn = st.selectbox("Giao cho", get_active_users_list())
            d = st.number_input("Hạn", value=1)
            if st.form_submit_button("Tạo"):
                if n and asn:
                    add_job(n, p, a, f, user, asn, d)
                    st.success("OK!")
                else: st.error("Thiếu thông tin")

    elif sel == "👥 Nhân Sự":
        st.title("Phân Quyền")
        df = get_all_users()
        for i, u in df.iterrows():
            c1, c2 = st.columns([2, 2])
            c1.write(f"**{u['username']}** ({u['fullname']})")
            if u['username']!=user:
                idx = ROLES.index(u['role']) if u['role'] in ROLES else 2
                nr = c2.selectbox("Role", ROLES, index=idx, key=u['username'])
                if nr!=u['role']:
                    update_user_role(u['username'], nr)
                    st.toast("Lưu!")
                    st.rerun()
    
    elif sel == "🔍 Tra Cứu":
        st.title("Tra Cứu")
        q = st.text_input("Tìm kiếm")
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
