import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import time
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io

# --- CẤU HÌNH ---
# (Những thông tin này sẽ lấy từ Secret của Streamlit Cloud)
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# --- KẾT NỐI GOOGLE ---
def get_gcp_creds():
    # Lấy thông tin từ st.secrets
    creds_dict = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return creds

def get_db_connection():
    creds = get_gcp_creds()
    client = gspread.authorize(creds)
    # Mở file Google Sheet theo tên
    try:
        sheet = client.open("DB_DODAC").sheet1
    except:
        st.error("Không tìm thấy file Google Sheet tên 'DB_DODAC'. Hãy tạo và share cho robot!")
        return None
    return sheet

def upload_to_drive(file_obj, folder_name):
    creds = get_gcp_creds()
    service = build('drive', 'v3', credentials=creds)
    
    # 1. Tìm ID thư mục gốc APP_DATA
    query = "mimeType='application/vnd.google-apps.folder' and name='APP_DATA'"
    results = service.files().list(q=query, fields="files(id)").execute()
    items = results.get('files', [])
    if not items: return None # Chưa tạo thư mục APP_DATA
    parent_id = items[0]['id']
    
    # 2. Tạo thư mục con cho hồ sơ (Nếu chưa có)
    query_sub = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and '{parent_id}' in parents"
    res_sub = service.files().list(q=query_sub, fields="files(id)").execute()
    sub_items = res_sub.get('files', [])
    
    if not sub_items:
        file_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
        folder = service.files().create(body=file_metadata, fields='id').execute()
        folder_id = folder.get('id')
    else:
        folder_id = sub_items[0]['id']
        
    # 3. Upload file
    file_metadata = {'name': file_obj.name, 'parents': [folder_id]}
    media = MediaIoBaseUpload(file_obj, mimetype=file_obj.type)
    file = service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
    return file.get('webViewLink')

# --- CÁC HÀM XỬ LÝ DATA (Thay thế SQL) ---
def init_db():
    sheet = get_db_connection()
    if sheet:
        # Kiểm tra xem đã có tiêu đề chưa, chưa thì tạo
        if not sheet.row_values(1):
            headers = ["id", "created_at", "customer_name", "phone", "address", "stage", "status", "assigned_to", "deadline", "logs_json"]
            sheet.append_row(headers)

def get_all_jobs():
    sheet = get_db_connection()
    data = sheet.get_all_records()
    return pd.DataFrame(data)

def add_job_google(name, phone, addr, file_obj, user, assign, days):
    sheet = get_db_connection()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    deadline = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    
    # Tạo ID giả lập (Timestamp)
    job_id = int(time.time())
    
    # Upload file nếu có
    file_link = ""
    folder_name = f"{job_id}_{name}"
    if file_obj:
        file_link = upload_to_drive(file_obj, folder_name)
    
    # Log đầu tiên
    log = f"[{now}] {user}: Khởi tạo | File: {file_link}"
    
    row = [job_id, now, name, phone, addr, "1. Tạo mới", "Đang xử lý", assign.split("-")[0], deadline, log]
    sheet.append_row(row)

def update_stage_google(job_id, stage, note, file_obj, user, assign, days):
    sheet = get_db_connection()
    # Tìm dòng có job_id
    cell = sheet.find(str(job_id))
    if cell:
        row_idx = cell.row
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Upload file mới
        file_link = ""
        if file_obj:
            # Lấy tên khách để tìm folder (Hơi thủ công)
            val = sheet.row_values(row_idx)
            c_name = val[2] # Cột tên
            file_link = upload_to_drive(file_obj, f"{job_id}_{c_name}")

        # Cập nhật dữ liệu
        # Cột F(6): Stage, G(7): Status, H(8): Assign, I(9): Deadline, J(10): Logs
        
        # Logic workflow
        WORKFLOW = {
            "1. Tạo mới": "2. Đo đạc", "2. Đo đạc": "3. Làm hồ sơ",
            "3. Làm hồ sơ": "4. Ký hồ sơ", "4. Ký hồ sơ": "5. Lấy hồ sơ",
            "5. Lấy hồ sơ": "6. Nộp hồ sơ", "6. Nộp hồ sơ": "7. Hoàn thành",
            "7. Hoàn thành": None
        }
        next_stg = WORKFLOW.get(stage)
        
        if next_stg:
            # Update Stage
            sheet.update_cell(row_idx, 6, next_stg)
            # Update Assign
            if assign: sheet.update_cell(row_idx, 8, assign.split("-")[0])
            # Update Deadline
            new_dl = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_cell(row_idx, 9, new_dl)
            
            # Append Log (Cộng dồn vào cột Logs cũ)
            old_log = sheet.cell(row_idx, 10).value
            new_log_entry = f"\n[{now}] {user}: {stage}->{next_stg} | Note: {note} | File: {file_link}"
            sheet.update_cell(row_idx, 10, old_log + new_log_entry)
            
            if next_stg == "7. Hoàn thành":
                sheet.update_cell(row_idx, 7, "Hoàn thành")

# --- GIAO DIỆN (RÚT GỌN CHO PHÙ HỢP GOOGLE) ---
st.set_page_config(page_title="Đo Đạc Cloud", page_icon="☁️")

if 'user' not in st.session_state:
    st.title("☁️ Đăng nhập Cloud")
    u = st.text_input("User")
    if st.button("Vào"):
        st.session_state['user'] = u
        st.rerun()
else:
    st.sidebar.write(f"User: {st.session_state['user']}")
    menu = st.sidebar.radio("Menu", ["Việc của tôi", "Tạo mới"])
    
    if menu == "Tạo mới":
        st.title("Tạo hồ sơ (Lưu Google Sheets)")
        with st.form("new"):
            n = st.text_input("Tên")
            p = st.text_input("SĐT")
            a = st.text_input("Địa chỉ")
            f = st.file_uploader("File")
            asn = st.text_input("Giao cho (Tên)")
            d = st.number_input("Hạn", value=1)
            if st.form_submit_button("Tạo"):
                add_job_google(n, p, a, f, st.session_state['user'], asn, d)
                st.success("Đã lưu lên Cloud!")
                
    elif menu == "Việc của tôi":
        st.title("Danh sách hồ sơ")
        try:
            df = get_all_jobs()
            # Lọc việc của mình
            my_df = df[df['assigned_to'].astype(str) == st.session_state['user']]
            
            for i, row in my_df.iterrows():
                with st.expander(f"{row['customer_name']} - {row['stage']}"):
                    st.write(f"Logs: {row['logs_json']}")
                    with st.form(f"act_{row['id']}"):
                        nt = st.text_area("Note")
                        fl = st.file_uploader("KQ")
                        asn = st.text_input("Người tiếp")
                        if st.form_submit_button("Chuyển"):
                            update_stage_google(row['id'], row['stage'], nt, fl, st.session_state['user'], asn, 1)
                            st.success("Done")
                            st.rerun()
        except Exception as e:
            st.error(f"Lỗi kết nối: {e}")