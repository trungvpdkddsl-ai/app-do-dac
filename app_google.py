import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import time
import requests
import threading
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# --- 1. CẤU HÌNH ---
# Telegram (Điền của bạn vào)
TELEGRAM_TOKEN = ""
TELEGRAM_CHAT_ID = ""

# Google Cloud Scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

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

# --- 2. KẾT NỐI GOOGLE (BACKEND MỚI) ---
def get_gcp_creds():
    return Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)

def get_sheet():
    creds = get_gcp_creds()
    client = gspread.authorize(creds)
    return client.open("DB_DODAC").sheet1

def upload_to_drive(file_obj, folder_name):
    if not file_obj: return ""
    try:
        creds = get_gcp_creds()
        service = build('drive', 'v3', credentials=creds)
        
        # Tìm thư mục gốc APP_DATA
        q = "mimeType='application/vnd.google-apps.folder' and name='APP_DATA'"
        res = service.files().list(q=q, fields="files(id)").execute()
        if not res.get('files'): return "Error: Chưa tạo folder APP_DATA"
        parent_id = res['files'][0]['id']
        
        # Tạo/Tìm folder con
        q_sub = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and '{parent_id}' in parents"
        res_sub = service.files().list(q=q_sub, fields="files(id)").execute()
        if not res_sub.get('files'):
            meta = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
            folder_id = service.files().create(body=meta, fields='id').execute().get('id')
        else:
            folder_id = res_sub['files'][0]['id']
            
        # Upload file
        meta_file = {'name': file_obj.name, 'parents': [folder_id]}
        media = MediaIoBaseUpload(file_obj, mimetype=file_obj.type)
        file = service.files().create(body=meta_file, media_body=media, fields='webViewLink').execute()
        return file.get('webViewLink')
    except Exception as e:
        return f"Lỗi upload: {str(e)}"

# --- 3. CÁC HÀM LOGIC (CHUYỂN TỪ SQL SANG SHEET) ---
def send_telegram_msg(message):
    if not TELEGRAM_TOKEN: return
    def run():
        try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"})
        except: pass
    threading.Thread(target=run).start()

def get_all_jobs_df():
    sheet = get_sheet()
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    # Chuyển đổi kiểu dữ liệu cho đúng chuẩn V7.2
    if not df.empty:
        df['id'] = df['id'].astype(int)
    return df

def add_job(name, phone, addr, file_obj, user, assign, days):
    sheet = get_sheet()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    deadline = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    job_id = int(time.time()) # Dùng timestamp làm ID
    
    file_link = upload_to_drive(file_obj, f"{job_id}_{name}")
    log = f"[{now}] {user}: Khởi tạo | File: {file_link}"
    
    # Thứ tự cột phải khớp với Bước 1: id, start_time, customer_name, customer_phone, address, current_stage, status, assigned_to, deadline, file_link, logs
    row = [job_id, now, name, phone, addr, "1. Tạo mới", "Đang xử lý", assign.split(" - ")[0], deadline, file_link, log]
    sheet.append_row(row)
    
    send_telegram_msg(f"🚀 <b>MỚI #{job_id}</b>\n👤 {name}\n📍 {addr}\n👉 Giao: {assign}")

def update_stage(job_id, current_stage, note, file_obj, user, assign, days):
    sheet = get_sheet()
    cell = sheet.find(str(job_id))
    if cell:
        r = cell.row
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Upload file mới (nếu có)
        new_link = ""
        if file_obj:
            c_name = sheet.cell(r, 3).value 
            new_link = upload_to_drive(file_obj, f"{job_id}_{c_name}")
            
        # Update Logic
        next_stg = WORKFLOW_DEFAULT.get(current_stage)
        if next_stg:
            sheet.update_cell(r, 6, next_stg) # Stage
            if assign: sheet.update_cell(r, 8, assign.split(" - ")[0]) # Assign
            new_dl = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_cell(r, 9, new_dl) # Deadline
            
            # Update Log
            old_log = sheet.cell(r, 11).value
            log_entry = f"\n[{now}] {user}: {current_stage}->{next_stg} | Note: {note} | File: {new_link}"
            sheet.update_cell(r, 11, old_log + log_entry)
            
            if next_stg == "7. Hoàn thành": sheet.update_cell(r, 7, "Hoàn thành")
            
            send_telegram_msg(f"✅ <b>UPDATE #{job_id}</b>\n{current_stage} -> {next_stg}\n👤 {user}")

# --- 4. HÀM VISUAL (THANH TIẾN ĐỘ) ---
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
    
    html = '<div class="step-container">'
    for i, s in enumerate(STAGES_ORDER):
        cls = "done" if i < idx else "active" if i == idx else "pending"
        icon = "✓" if i < idx else str(i+1)
        if i == idx and status == "Tạm dừng": icon = "⛔"
        html += f'<div class="step-item"><div class="step-circle {cls}">{icon}</div><div style="font-size:11px">{s.split(". ")[1]}</div></div>'
    st.markdown(html + '</div>', unsafe_allow_html=True)

# --- 5. GIAO DIỆN CHÍNH ---
st.set_page_config(page_title="Đo Đạc Cloud", page_icon="☁️", layout="wide")

# Giả lập đăng nhập đơn giản cho Cloud (Vì không có DB user)
if 'user' not in st.session_state:
    st.title("☁️ Đăng nhập Hệ thống Cloud")
    u = st.text_input("Tên nhân viên")
    if st.button("Vào làm việc"):
        if u:
            st.session_state['user'] = u
            st.rerun()
else:
    # Sidebar
    user = st.session_state['user']
    st.sidebar.title(f"👤 {user}")
    if st.sidebar.button("Đăng xuất"): 
        del st.session_state['user']
        st.rerun()
    
    menu = st.sidebar.radio("Menu", ["🏠 Việc Của Tôi", "📝 Tạo Mới", "📊 Báo Cáo"])

    # --- TAB VIỆC CỦA TÔI ---
    if menu == "🏠 Việc Của Tôi":
        st.title("📋 Danh sách hồ sơ")
        try:
            df = get_all_jobs_df()
            if df.empty:
                st.info("Chưa có hồ sơ nào.")
            else:
                # Lọc việc của user (đơn giản hóa)
                # Trên cloud tạm thời hiển thị hết để test, sau này lọc sau
                my_df = df[df['status'] != 'Hoàn thành']
                
                # Metrics
                total = len(my_df)
                st.metric("Tổng hồ sơ đang chạy", total)
                
                for i, j in my_df.iterrows():
                    with st.expander(f"📂 {j['customer_name']} | {j['current_stage']}"):
                        render_progress_bar(j['current_stage'], j['status'])
                        
                        c1, c2 = st.columns([1.5, 1])
                        with c1:
                            st.write(f"📞 {j['customer_phone']} | 📍 {j['address']}")
                            st.write(f"👤 Người làm: **{j['assigned_to']}**")
                            st.write(f"⏰ Hạn: {j['deadline']}")
                            
                            st.info("📜 **Lịch sử & File:**")
                            # Xử lý hiển thị log từ text (vì Google Sheet lưu log dạng text dài)
                            st.text(j['logs'])
                            if j['file_link']:
                                st.markdown(f"[📂 Mở file đính kèm trên Drive]({j['file_link']})")

                        with c2:
                            st.write("👉 **Xử lý**")
                            with st.form(f"act_{j['id']}"):
                                nt = st.text_area("Ghi chú")
                                fl = st.file_uploader("File KQ")
                                cur = j['current_stage']; nxt = WORKFLOW_DEFAULT.get(cur)
                                asn = st.text_input("Người tiếp (Tên)", value=user)
                                day = st.number_input("Hạn (ngày)", value=1)
                                
                                if nxt and nxt != "7. Hoàn thành":
                                    st.write(f"Chuyển sang: **{nxt}**")
                                
                                if st.form_submit_button("✅ Chuyển bước"):
                                    update_stage(j['id'], cur, nt, fl, user, asn, day)
                                    st.success("Đã chuyển!")
                                    time.sleep(1); st.rerun()

        except Exception as e:
            st.error(f"Lỗi tải dữ liệu: {e}")

    # --- TAB TẠO MỚI ---
    elif menu == "📝 Tạo Mới":
        st.title("Tạo hồ sơ mới")
        with st.form("new"):
            c1, c2 = st.columns(2)
            n = c1.text_input("Tên khách")
            p = c2.text_input("SĐT")
            a = st.text_input("Địa chỉ")
            f = st.file_uploader("File gốc")
            asn = st.text_input("Giao cho ai?", value=user)
            d = st.number_input("Hạn (ngày)", value=1)
            
            if st.form_submit_button("🚀 Tạo hồ sơ"):
                add_job(n, p, a, f, user, asn, d)
                st.success("Đã tạo xong!")

    # --- TAB BÁO CÁO ---
    elif menu == "📊 Báo Cáo":
        st.title("Thống kê")
        try:
            df = get_all_jobs_df()
            if not df.empty:
                st.bar_chart(df['current_stage'].value_counts())
                st.dataframe(df[['id', 'customer_name', 'current_stage', 'assigned_to']])
        except:
            st.warning("Chưa có dữ liệu")
