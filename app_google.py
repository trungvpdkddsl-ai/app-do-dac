# app_google_optimized.py
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import time
import requests
import threading
import hashlib
import re
import gspread
import base64
import calendar
import io
import json
from google.oauth2.service_account import Credentials
from streamlit_tags import st_tags
import plotly.express as px
import plotly.graph_objects as go
from collections import defaultdict

# ==================== CẤU HÌNH HỆ THỐNG NÂNG CAO ====================
st.set_page_config(
    page_title="Hệ Thống Quản Lý Đo Đạc V4-Pro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 🔒 Biến môi trường (nên dùng st.secrets)
TELEGRAM_TOKEN = st.secrets.get("TELEGRAM_TOKEN", "8514665869:AAHUfTHgNlEEK_Yz6yYjZa-1iR645Cgr190")
TELEGRAM_CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", "-5055192262")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# 📊 URL Services
APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbyEMEGyS_sVCA4eyVRFXxnOuGqMnJOKOIqZqKxi4HpYBcpr7U72WUXCoKLm20BQomVC/exec"
DRIVE_FOLDER_ID = "1SrARuA1rgKLZmoObGor-GkNx33F6zNQy"

# 👥 Roles & Stages
ROLES = ["Quản lý", "Trưởng nhóm", "Nhân viên", "Thực tập", "Chưa cấp quyền"]
STAGES_ORDER = [
    "1. Tạo mới", "2. Đo đạc", "3. Hoàn thiện trích đo", 
    "4. Làm hồ sơ", "5. Ký hồ sơ", "6. Lấy hồ sơ", 
    "7. Nộp hồ sơ", "8. Hoàn thành"
]

PROCEDURES_LIST = [
    "Cấp lần đầu", "Cấp đổi", "Chuyển quyền", 
    "Tách thửa", "Thừa kế", "Cung cấp thông tin", "Đính chính"
]

# 🔄 Workflow Definitions
WORKFLOW_FULL = {
    "1. Tạo mới": "2. Đo đạc", 
    "2. Đo đạc": "3. Hoàn thiện trích đo", 
    "3. Hoàn thiện trích đo": "4. Làm hồ sơ",
    "4. Làm hồ sơ": "5. Ký hồ sơ", 
    "5. Ký hồ sơ": "6. Lấy hồ sơ", 
    "6. Lấy hồ sơ": "7. Nộp hồ sơ", 
    "7. Nộp hồ sơ": "8. Hoàn thành", 
    "8. Hoàn thành": None
}

WORKFLOW_SHORT = {
    "1. Tạo mới": "4. Làm hồ sơ", 
    "4. Làm hồ sơ": "5. Ký hồ sơ", 
    "5. Ký hồ sơ": "6. Lấy hồ sơ", 
    "6. Lấy hồ sơ": "7. Nộp hồ sơ", 
    "7. Nộp hồ sơ": "8. Hoàn thành", 
    "8. Hoàn thành": None
}

# ⏱️ SLA Configuration (giờ)
STAGE_SLA_HOURS = {
    "1. Tạo mới": 0, 
    "2. Đo đạc": 24, 
    "3. Hoàn thiện trích đo": 24, 
    "4. Làm hồ sơ": 24, 
    "5. Ký hồ sơ": 72, 
    "6. Lấy hồ sơ": 24, 
    "7. Nộp hồ sơ": 360
}

# 💰 Price Configuration
PROCEDURE_PRICES = {
    "Cấp lần đầu": 1500000,
    "Cấp đổi": 1500000,
    "Chuyển quyền": 1500000,
    "Tách thửa": 2000000,
    "Thừa kế": 1500000,
    "Cung cấp thông tin": 800000,
    "Đính chính": 1000000
}

# ==================== HÀM TIỆN ÍCH NÂNG CAO ====================
@st.cache_data(ttl=300)
def get_gcp_creds():
    """Lấy credentials từ Streamlit secrets"""
    return Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], 
        scopes=SCOPES
    )

@st.cache_resource
def get_gspread_client():
    """Tạo client gspread cached"""
    creds = get_gcp_creds()
    return gspread.authorize(creds)

def safe_int(value, default=0):
    """Chuyển đổi an toàn sang số nguyên"""
    if pd.isna(value) or value == "":
        return default
    try:
        # Xử lý nhiều định dạng số
        if isinstance(value, (int, float)):
            return int(value)
        clean = str(value).replace(",", "").replace(".", "").strip()
        return int(clean) if clean else default
    except:
        return default

def safe_float(value, default=0.0):
    """Chuyển đổi an toàn sang số thực"""
    try:
        return float(str(value).replace(",", ""))
    except:
        return default

def format_currency(value):
    """Định dạng tiền tệ"""
    try:
        return f"{safe_int(value):,} đ"
    except:
        return "0 đ"

def get_proc_abbr(proc_name):
    """Lấy viết tắt của thủ tục"""
    mapping = {
        "Cấp lần đầu": "CLD", "Cấp đổi": "CD", "Chuyển quyền": "CQ", 
        "Tách thửa": "TT", "Thừa kế": "TK", 
        "Cung cấp thông tin": "CCTT", "Đính chính": "DC"
    }
    return mapping.get(proc_name, "K")

def extract_proc_from_log(log_text):
    """Trích xuất tên thủ tục từ log"""
    patterns = [
        r'Khởi tạo \((.*?)\)',
        r'Thủ tục: (.*?)\n',
        r'Procedure: (.*?)[\s|]'
    ]
    for pattern in patterns:
        match = re.search(pattern, str(log_text))
        if match:
            return match.group(1)
    return "Không xác định"

def get_next_stage_dynamic(current_stage, proc_name):
    """Xác định bước tiếp theo theo workflow"""
    if proc_name in ["Cung cấp thông tin", "Đính chính"]:
        return WORKFLOW_SHORT.get(current_stage)
    return WORKFLOW_FULL.get(current_stage)

def calculate_working_hours(start_dt, end_dt=None):
    """Tính giờ làm việc (trừ T7, CN)"""
    if end_dt is None:
        end_dt = datetime.now()
    
    working_hours = 0
    current = start_dt
    
    while current < end_dt:
        # Chỉ tính giờ làm việc trong ngày (8h-17h)
        if current.weekday() < 5:  # Thứ 2-6
            hour_start = max(current.hour, 8)
            hour_end = min(current.hour + 1, 17)
            if hour_start < hour_end:
                working_hours += 1
        current += timedelta(hours=1)
    
    return working_hours

def calculate_deadline(start_date, hours_to_add):
    """Tính deadline chỉ tính giờ làm việc"""
    if hours_to_add == 0:
        return None
    
    current_date = start_date
    added_hours = 0
    
    while added_hours < hours_to_add:
        current_date += timedelta(hours=1)
        # Chỉ tính giờ làm việc (8h-17h, thứ 2-6)
        if current_date.weekday() < 5 and 8 <= current_date.hour < 17:
            added_hours += 1
    
    return current_date

def generate_unique_name(jid, start_time, name, phone, addr, proc_name):
    """Tạo tên file duy nhất"""
    try:
        jid_str = str(jid)
        seq = jid_str[-2:] if len(jid_str) >= 2 else "01"
        
        if isinstance(start_time, str):
            d_obj = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        else:
            d_obj = start_time
            
        date_str = d_obj.strftime('%d%m%y')
    except:
        date_str = datetime.now().strftime('%d%m%y')
        seq = "01"
    
    abbr = get_proc_abbr(proc_name)
    clean_phone = str(phone).replace("'", "").replace(" ", "")[-9:]  # Lấy 9 số cuối
    clean_name = name.strip()[:30]  # Giới hạn độ dài
    
    return f"{date_str}-{seq}{'-' + abbr if abbr else ''} {clean_name} {clean_phone}"

def extract_files_from_log(log_text):
    """Trích xuất file từ log"""
    pattern = r"File:\s*(.*?)\s*-\s*(https?://[^\s]+)"
    matches = re.findall(pattern, str(log_text))
    
    if not matches:
        # Tìm link trực tiếp
        raw_links = re.findall(r'(https?://drive\.google\.com/[^\s]+)', str(log_text))
        return [("File đính kèm", l) for l in raw_links]
    
    return matches

def get_drive_id(link):
    """Trích xuất ID từ Google Drive link"""
    patterns = [
        r'/d/([a-zA-Z0-9_-]+)',
        r'id=([a-zA-Z0-9_-]+)',
        r'folders/([a-zA-Z0-9_-]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, str(link))
        if match:
            return match.group(1)
    return None

# ==================== QUẢN LÝ TRẠNG THÁI & HIỂN THỊ ====================
def get_status_badge(status, deadline=None, logs=""):
    """Tạo badge trạng thái với màu sắc"""
    now = datetime.now()
    
    status_config = {
        "Đang xử lý": {"color": "#28a745", "bg": "#e6fffa", "text": "🟢 Đang thực hiện", "icon": "🟢"},
        "Tạm dừng": {"color": "#6c757d", "bg": "#f8f9fa", "text": "⏸️ Tạm dừng", "icon": "⏸️"},
        "Hoàn thành": {"color": "#004085", "bg": "#cce5ff", "text": "✅ Hoàn thành", "icon": "✅"},
        "Đã xóa": {"color": "#343a40", "bg": "#e2e6ea", "text": "🗑️ Đã xóa", "icon": "🗑️"},
        "Kết thúc sớm": {"color": "#343a40", "bg": "#e2e6ea", "text": "⏹️ Kết thúc", "icon": "⏹️"}
    }
    
    config = status_config.get(status, status_config["Đang xử lý"])
    
    # Kiểm tra quá hạn
    if status == "Đang xử lý" and deadline:
        try:
            dl_dt = pd.to_datetime(deadline)
            if now > dl_dt:
                config = {"color": "#dc3545", "bg": "#ffe6e6", "text": "🔴 Quá hạn", "icon": "🔴"}
            elif now <= dl_dt <= now + timedelta(hours=24):
                config = {"color": "#fd7e14", "bg": "#fff3cd", "text": "⚠️ Sắp đến hạn", "icon": "⚠️"}
        except:
            pass
    
    # Kiểm tra tạm dừng do chưa thanh toán
    if status == "Tạm dừng" and "Hoàn thành - Chưa thanh toán" in str(logs):
        config = {"color": "#fd7e14", "bg": "#fff3cd", "text": "💰 Chưa thanh toán", "icon": "💰"}
    
    return config

def render_status_badge_html(row):
    """Render badge HTML cho table"""
    config = get_status_badge(row['status'], row['deadline'], row.get('logs', ''))
    
    return f"""
    <span style='
        background-color: {config['bg']}; 
        color: {config['color']}; 
        padding: 4px 10px; 
        border-radius: 12px; 
        font-weight: bold; 
        font-size: 12px; 
        border: 1px solid {config['color']};
        white-space: nowrap;
        display: inline-flex;
        align-items: center;
        gap: 4px;
    '>
        {config['icon']} {config['text']}
    </span>
    """

# ==================== GOOGLE SHEETS OPERATIONS ====================
@st.cache_data(ttl=60)
def get_sheet_data(sheet_name="DB_DODAC", worksheet=None):
    """Lấy dữ liệu từ Google Sheets với caching"""
    try:
        client = get_gspread_client()
        spreadsheet = client.open(sheet_name)
        
        if worksheet:
            ws = spreadsheet.worksheet(worksheet)
        else:
            ws = spreadsheet.sheet1
            
        data = ws.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame()
        
    except Exception as e:
        st.error(f"Lỗi kết nối Google Sheets: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=120)
def get_all_jobs_df():
    """Lấy toàn bộ dữ liệu công việc"""
    df = get_sheet_data("DB_DODAC")
    
    if not df.empty:
        # Đảm bảo cột cần thiết
        required_columns = ['id', 'start_time', 'customer_name', 'customer_phone', 
                           'address', 'current_stage', 'status', 'assigned_to', 
                           'deadline', 'file_link', 'logs']
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = ""
        
        # Chuyển đổi kiểu dữ liệu
        df['id'] = df['id'].apply(safe_int)
        df['start_dt'] = pd.to_datetime(df['start_time'], errors='coerce')
        df['deadline_dt'] = pd.to_datetime(df['deadline'], errors='coerce')
        
        # Cột tài chính
        financial_cols = ['deposit', 'survey_fee', 'is_paid', 'is_survey_only']
        for col in financial_cols:
            if col not in df.columns:
                df[col] = 0
            df[col] = df[col].apply(safe_int)
        
        # Thêm cột thông tin bổ sung
        df['proc_name'] = df['logs'].apply(extract_proc_from_log)
        df['duration'] = (datetime.now() - df['start_dt']).dt.days
        df['is_overdue'] = df.apply(
            lambda x: x['status'] == 'Đang xử lý' and pd.notna(x['deadline_dt']) and datetime.now() > x['deadline_dt'],
            axis=1
        )
    
    return df

@st.cache_data(ttl=300)
def get_users_df():
    """Lấy dữ liệu người dùng"""
    df = get_sheet_data("DB_DODAC", "USERS")
    if df.empty:
        # Tạo cấu trúc mặc định
        df = pd.DataFrame(columns=['username', 'password', 'fullname', 'role', 'email', 'phone', 'active'])
    return df

@st.cache_data(ttl=300)
def get_audit_logs_df():
    """Lấy logs audit"""
    df = get_sheet_data("DB_DODAC", "AUDIT_LOGS")
    if df.empty:
        df = pd.DataFrame(columns=['Timestamp', 'User', 'Action', 'Details', 'IP_Address'])
    return df

def update_sheet_cell(sheet_name, cell_range, values):
    """Cập nhật ô trong Google Sheets"""
    try:
        client = get_gspread_client()
        spreadsheet = client.open(sheet_name)
        ws = spreadsheet.sheet1
        ws.update(cell_range, values)
        return True
    except Exception as e:
        st.error(f"Lỗi cập nhật: {e}")
        return False

# ==================== FILE MANAGEMENT ====================
def upload_file_to_drive(file_obj, folder_name):
    """Upload file lên Google Drive"""
    if not file_obj:
        return None, None
    
    try:
        file_content = file_obj.read()
        file_base64 = base64.b64encode(file_content).decode('utf-8')
        
        payload = {
            "action": "upload",
            "filename": file_obj.name,
            "mime_type": file_obj.type,
            "file_base64": file_base64,
            "folder_id": DRIVE_FOLDER_ID,
            "sub_folder_name": folder_name
        }
        
        response = requests.post(APPS_SCRIPT_URL, json=payload, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("status") == "success":
                return result.get("link"), file_obj.name
            else:
                st.error(f"Lỗi từ script: {result.get('message')}")
        else:
            st.error(f"Lỗi kết nối: {response.status_code}")
            
    except Exception as e:
        st.error(f"Lỗi upload: {str(e)}")
    
    return None, None

def delete_file_from_drive(file_id):
    """Xóa file từ Google Drive"""
    try:
        payload = {"action": "delete", "file_id": file_id}
        response = requests.post(APPS_SCRIPT_URL, json=payload, timeout=10)
        return response.status_code == 200
    except:
        return False

# ==================== AUTHENTICATION & SECURITY ====================
def hash_password(password):
    """Hash mật khẩu với salt"""
    salt = "DODAC_SYSTEM_2024"
    return hashlib.sha256(f"{salt}{password}".encode()).hexdigest()

def authenticate_user(username, password):
    """Xác thực người dùng"""
    users_df = get_users_df()
    
    if users_df.empty:
        return None
    
    user_row = users_df[users_df['username'] == username]
    
    if user_row.empty:
        return None
    
    user_data = user_row.iloc[0]
    
    # Kiểm tra active
    if 'active' in user_data and str(user_data['active']).lower() == 'false':
        return None
    
    # Kiểm tra password
    if user_data['password'] == hash_password(password):
        return {
            'username': user_data['username'],
            'fullname': user_data.get('fullname', username),
            'role': user_data.get('role', 'Nhân viên'),
            'email': user_data.get('email', ''),
            'phone': user_data.get('phone', '')
        }
    
    return None

def register_user(username, password, fullname, email="", phone=""):
    """Đăng ký người dùng mới"""
    users_df = get_users_df()
    
    # Kiểm tra username tồn tại
    if username in users_df['username'].values:
        return False, "Username đã tồn tại"
    
    # Validate username
    if not re.match(r'^[a-zA-Z0-9_]{3,20}$', username):
        return False, "Username chỉ cho phép chữ, số và gạch dưới (3-20 ký tự)"
    
    # Tạo user mới
    new_user = {
        'username': username,
        'password': hash_password(password),
        'fullname': fullname,
        'role': 'Chưa cấp quyền',
        'email': email,
        'phone': phone,
        'active': True,
        'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    try:
        # Thêm vào Google Sheets
        client = get_gspread_client()
        spreadsheet = client.open("DB_DODAC")
        
        try:
            ws = spreadsheet.worksheet("USERS")
        except:
            ws = spreadsheet.add_worksheet(title="USERS", rows="1000", cols="10")
            ws.append_row(['username', 'password', 'fullname', 'role', 'email', 'phone', 'active', 'created_at'])
        
        ws.append_row(list(new_user.values()))
        
        # Clear cache
        get_users_df.clear()
        
        return True, "Đăng ký thành công, chờ duyệt"
    except Exception as e:
        return False, f"Lỗi hệ thống: {str(e)}"

# ==================== NOTIFICATION SYSTEM ====================
def send_telegram_notification(message, parse_mode="HTML"):
    """Gửi thông báo Telegram"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    
    def send():
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            data = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True
            }
            requests.post(url, data=data, timeout=10)
        except:
            pass
    
    threading.Thread(target=send, daemon=True).start()

def send_user_notification(user_id, message, notification_type="info"):
    """Gửi thông báo cho người dùng cụ thể"""
    # TODO: Triển khai hệ thống thông báo nội bộ
    pass

# ==================== AUDIT LOGGING ====================
def log_audit_action(user, action, details, ip_address=""):
    """Ghi log hành động"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = [timestamp, user, action, details, ip_address]
        
        client = get_gspread_client()
        spreadsheet = client.open("DB_DODAC")
        
        try:
            ws = spreadsheet.worksheet("AUDIT_LOGS")
        except:
            ws = spreadsheet.add_worksheet(title="AUDIT_LOGS", rows="10000", cols="5")
            ws.append_row(['Timestamp', 'User', 'Action', 'Details', 'IP_Address'])
        
        ws.append_row(log_entry)
        get_audit_logs_df.clear()
        
    except Exception as e:
        print(f"Lỗi ghi audit log: {e}")

# ==================== JOB MANAGEMENT FUNCTIONS ====================
def create_new_job(customer_info, procedure, files, assigned_to, created_by):
    """Tạo hồ sơ mới"""
    try:
        # Lấy ID tiếp theo
        df = get_all_jobs_df()
        today = datetime.now().date()
        
        if df.empty:
            seq = 1
        else:
            today_jobs = df[df['start_dt'].dt.date == today]
            if today_jobs.empty:
                seq = 1
            else:
                last_id = today_jobs['id'].max()
                seq = int(str(last_id)[-2:]) + 1 if len(str(last_id)) >= 2 else 1
        
        job_id = int(f"{today.strftime('%y%m%d')}{seq:02d}")
        
        # Tạo thông tin cơ bản
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        
        # Tạo tên duy nhất
        unique_name = generate_unique_name(
            job_id, now_str, 
            customer_info['name'],
            customer_info['phone'],
            customer_info['address'],
            procedure
        )
        
        # Upload files nếu có
        file_links = []
        if files:
            for file in files:
                link, filename = upload_file_to_drive(file, unique_name)
                if link:
                    file_links.append(f"File: {filename} - {link}")
        
        # Tính deadline mặc định
        deadline = calculate_deadline(now, 24 * 30)  # 30 ngày làm việc
        
        # Tạo log entry
        file_log = " | ".join(file_links) if file_links else ""
        assign_log = f" -> Giao: {assigned_to}" if assigned_to else ""
        initial_log = f"[{now_str}] {created_by}: Khởi tạo ({procedure}){assign_log} {file_log}"
        
        # Tạo dữ liệu cho Google Sheets
        job_data = [
            job_id,                    # ID
            now_str,                   # start_time
            customer_info['name'],     # customer_name
            f"'{customer_info['phone']}",  # customer_phone
            customer_info['address'],  # address
            "1. Tạo mới",              # current_stage
            "Đang xử lý",              # status
            assigned_to.split(' - ')[0] if assigned_to else "",  # assigned_to
            deadline.strftime("%Y-%m-%d %H:%M:%S") if deadline else "",  # deadline
            file_links[0].split(' - ')[1] if file_links else "",  # file_link
            initial_log,               # logs
            0,  # deposit
            PROCEDURE_PRICES.get(procedure, 1500000),  # survey_fee
            0,  # is_paid
            0   # is_survey_only
        ]
        
        # Lưu vào Google Sheets
        client = get_gspread_client()
        spreadsheet = client.open("DB_DODAC")
        ws = spreadsheet.sheet1
        ws.append_row(job_data)
        
        # Clear cache
        get_all_jobs_df.clear()
        
        # Ghi log audit
        log_audit_action(
            created_by, 
            "CREATE_JOB", 
            f"ID: {job_id}, Tên: {customer_info['name']}, Thủ tục: {procedure}"
        )
        
        # Gửi thông báo Telegram
        telegram_msg = f"""
🚀 <b>HỒ SƠ MỚI #{seq:02d}</b>
📂 <b>{unique_name}</b>
📋 Thủ tục: {procedure}
👤 Khách hàng: {customer_info['name']}
📞 Điện thoại: {customer_info['phone']}
📍 Địa chỉ: {customer_info['address'][:50]}...
👷 Người phụ trách: {assigned_to.split(' - ')[0] if assigned_to else 'Chưa giao'}
📎 Files: {len(files)} file đính kèm
        """
        send_telegram_notification(telegram_msg)
        
        return job_id, unique_name
        
    except Exception as e:
        st.error(f"Lỗi tạo hồ sơ: {str(e)}")
        return None, None

def update_job_stage(job_id, current_stage, note, files, updated_by, assigned_to=None, 
                    financial_info=None, result_date=None):
    """Cập nhật trạng thái công việc"""
    try:
        df = get_all_jobs_df()
        job_row = df[df['id'] == job_id]
        
        if job_row.empty:
            st.error("Không tìm thấy hồ sơ")
            return False
        
        job_data = job_row.iloc[0]
        proc_name = extract_proc_from_log(job_data['logs'])
        
        # Xác định bước tiếp theo
        next_stage = get_next_stage_dynamic(current_stage, proc_name)
        
        # Xử lý trường hợp đặc biệt
        if note in ["Đã nhận kết quả đúng hạn.", "Đã nhận kết quả sớm.", "Hoàn thành (Đã TT)"]:
            next_stage = "8. Hoàn thành"
        
        if not next_stage:
            next_stage = "8. Hoàn thành"
        
        # Upload files mới
        file_logs = []
        if files:
            unique_name = generate_unique_name(
                job_id, job_data['start_time'],
                job_data['customer_name'],
                job_data['customer_phone'],
                job_data['address'],
                proc_name
            )
            
            for file in files:
                link, filename = upload_file_to_drive(file, unique_name)
                if link:
                    file_logs.append(f"File: {filename} - {link}")
        
        # Cập nhật Google Sheets
        client = get_gspread_client()
        spreadsheet = client.open("DB_DODAC")
        ws = spreadsheet.sheet1
        
        # Tìm row index
        cell = ws.find(str(job_id))
        if not cell:
            st.error("Không tìm thấy hồ sơ trong hệ thống")
            return False
        
        row_idx = cell.row
        
        # Cập nhật thông tin cơ bản
        updates = {
            6: next_stage,  # current_stage
            8: assigned_to.split(' - ')[0] if assigned_to else job_data['assigned_to']  # assigned_to
        }
        
        # Cập nhật deadline nếu có
        if result_date:
            deadline_str = result_date.strftime("%Y-%m-%d %H:%M:%S")
            updates[9] = deadline_str
        elif next_stage != "8. Hoàn thành":
            hours_to_add = STAGE_SLA_HOURS.get(next_stage, 24)
            new_deadline = calculate_deadline(datetime.now(), hours_to_add)
            if new_deadline:
                updates[9] = new_deadline.strftime("%Y-%m-%d %H:%M:%S")
        
        # Cập nhật tài chính nếu có
        if financial_info:
            updates[13] = 1 if financial_info.get('deposit_ok', False) else 0
            updates[14] = safe_int(financial_info.get('fee_amount', 0))
            updates[15] = 1 if financial_info.get('is_paid', False) else 0
        
        # Cập nhật status nếu hoàn thành
        if next_stage == "8. Hoàn thành":
            updates[7] = "Hoàn thành"
        
        # Áp dụng updates
        for col, value in updates.items():
            ws.update_cell(row_idx, col, value)
        
        # Cập nhật logs
        current_log = ws.cell(row_idx, 11).value or ""
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        assign_log = f" -> Giao: {assigned_to}" if assigned_to else ""
        file_log = " | ".join(file_logs) if file_logs else ""
        
        new_log = f"\n[{now_str}] {updated_by}: {current_stage}->{next_stage}{assign_log} | Note: {note} {file_log}"
        ws.update_cell(row_idx, 11, current_log + new_log)
        
        # Clear cache
        get_all_jobs_df.clear()
        
        # Ghi log audit
        log_audit_action(
            updated_by,
            "UPDATE_STAGE",
            f"ID: {job_id}, {current_stage} -> {next_stage}"
        )
        
        # Gửi thông báo
        unique_name = generate_unique_name(
            job_id, job_data['start_time'],
            job_data['customer_name'],
            job_data['customer_phone'],
            job_data['address'],
            proc_name
        )
        
        telegram_msg = f"""
✅ <b>CẬP NHẬT TRẠNG THÁI</b>
📂 <b>{unique_name}</b>
📈 {current_stage} → <b>{next_stage}</b>
👤 Bởi: {updated_by}
📝 Ghi chú: {note[:50]}{'...' if len(note) > 50 else ''}
        """
        
        if assigned_to:
            telegram_msg += f"\n👷 Giao cho: {assigned_to.split(' - ')[0]}"
        
        send_telegram_notification(telegram_msg)
        
        return True
        
    except Exception as e:
        st.error(f"Lỗi cập nhật: {str(e)}")
        return False

# ==================== UI COMPONENTS ====================
def render_custom_css():
    """Inject custom CSS styles"""
    st.markdown("""
    <style>
        /* Main container */
        .main {
            padding: 1rem 2rem;
        }
        
        /* Cards */
        .metric-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 12px;
            padding: 1.5rem;
            color: white;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        
        .info-card {
            background: white;
            border-radius: 12px;
            padding: 1.5rem;
            border: 1px solid #e0e0e0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        
        /* Buttons */
        .stButton > button {
            border-radius: 8px;
            font-weight: 500;
            transition: all 0.3s ease;
        }
        
        .stButton > button:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }
        
        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
        }
        
        .stTabs [data-baseweb="tab"] {
            border-radius: 8px 8px 0 0;
            padding: 10px 20px;
            background-color: #f8f9fa;
        }
        
        .stTabs [aria-selected="true"] {
            background-color: #007bff;
            color: white;
        }
        
        /* Dataframe */
        .dataframe {
            font-size: 14px;
        }
        
        /* Status badges */
        .status-badge {
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            display: inline-flex;
            align-items: center;
            gap: 4px;
        }
        
        /* Custom scrollbar */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        
        ::-webkit-scrollbar-track {
            background: #f1f1f1;
            border-radius: 4px;
        }
        
        ::-webkit-scrollbar-thumb {
            background: #888;
            border-radius: 4px;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: #555;
        }
        
        /* Form elements */
        .stTextInput > div > div > input {
            border-radius: 8px;
        }
        
        .stSelectbox > div > div {
            border-radius: 8px;
        }
        
        /* Sidebar */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
        }
        
        [data-testid="stSidebar"] .sidebar-content {
            color: white;
        }
        
        /* Hide Streamlit branding */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        
        /* Custom headers */
        .custom-header {
            background: linear-gradient(90deg, #007bff, #6610f2);
            color: white;
            padding: 1.5rem;
            border-radius: 12px;
            margin-bottom: 2rem;
        }
        
        /* Loading animation */
        @keyframes pulse {
            0% { opacity: 1; }
            50% { opacity: 0.5; }
            100% { opacity: 1; }
        }
        
        .pulse {
            animation: pulse 2s infinite;
        }
    </style>
    """, unsafe_allow_html=True)

def render_sidebar_menu(user_role):
    """Render sidebar menu với phân quyền"""
    with st.sidebar:
        st.markdown(f"""
        <div class="sidebar-content">
            <div style="text-align: center; padding: 1rem 0;">
                <h2 style="color: white;">📊 DODAC PRO</h2>
                <p style="color: rgba(255,255,255,0.8); font-size: 14px;">
                    Hệ thống quản lý đo đạc
                </p>
            </div>
            <hr style="border-color: rgba(255,255,255,0.2);">
        </div>
        """, unsafe_allow_html=True)
        
        # Hiển thị thông tin người dùng
        col1, col2 = st.columns([1, 3])
        with col1:
            st.markdown("👤")
        with col2:
            st.markdown(f"**{st.session_state.get('fullname', 'User')}**")
            st.caption(f"{user_role}")
        
        st.divider()
        
        # Menu chính
        menu_options = [
            ("🏠", "Tổng quan", ["Quản lý", "Trưởng nhóm", "Nhân viên", "Thực tập"]),
            ("📋", "Hồ sơ của tôi", ["Quản lý", "Trưởng nhóm", "Nhân viên", "Thực tập"]),
            ("➕", "Tạo hồ sơ", ["Quản lý", "Trưởng nhóm", "Nhân viên"]),
            ("📅", "Lịch biểu", ["Quản lý", "Trưởng nhóm", "Nhân viên", "Thực tập"]),
            ("📊", "Báo cáo", ["Quản lý", "Trưởng nhóm"]),
            ("💰", "Tài chính", ["Quản lý", "Trưởng nhóm"]),
            ("🗃️", "Lưu trữ", ["Quản lý", "Trưởng nhóm", "Nhân viên"]),
            ("👥", "Nhân sự", ["Quản lý"]),
            ("📈", "Phân tích", ["Quản lý"]),
            ("⚙️", "Cài đặt", ["Quản lý"]),
            ("🛡️", "Nhật ký", ["Quản lý"]),
            ("🗑️", "Thùng rác", ["Quản lý"])
        ]
        
        selected = st.session_state.get("selected_menu", "Tổng quan")
        
        for icon, label, allowed_roles in menu_options:
            if user_role in allowed_roles:
                if st.button(f"{icon} {label}", 
                           use_container_width=True,
                           type="primary" if selected == label else "secondary"):
                    st.session_state["selected_menu"] = label
                    st.rerun()
        
        st.divider()
        
        # Thống kê nhanh
        if user_role in ["Quản lý", "Trưởng nhóm"]:
            df = get_all_jobs_df()
            if not df.empty:
                active_jobs = df[df['status'] == 'Đang xử lý']
                urgent_jobs = active_jobs[active_jobs['is_overdue']]
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Đang xử lý", len(active_jobs))
                with col2:
                    st.metric("Quá hạn", len(urgent_jobs), delta_color="inverse")
        
        st.divider()
        
        # Đăng xuất
        if st.button("🚪 Đăng xuất", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

def render_dashboard():
    """Dashboard tổng quan"""
    st.markdown('<div class="custom-header"><h2>📊 Dashboard Tổng Quan</h2></div>', unsafe_allow_html=True)
    
    df = get_all_jobs_df()
    if df.empty:
        st.info("Chưa có dữ liệu trong hệ thống")
        return
    
    # ========== KPI METRICS ==========
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_jobs = len(df)
        active_jobs = len(df[df['status'] == 'Đang xử lý'])
        st.metric("Tổng hồ sơ", total_jobs, f"{active_jobs} đang xử lý")
    
    with col2:
        overdue_jobs = len(df[df['is_overdue']])
        st.metric("Hồ sơ quá hạn", overdue_jobs, delta_color="inverse")
    
    with col3:
        total_revenue = df['survey_fee'].sum()
        paid_revenue = df[df['is_paid'] == 1]['survey_fee'].sum()
        st.metric("Doanh thu", f"{total_revenue:,.0f} đ", f"{paid_revenue:,.0f} đ đã thu")
    
    with col4:
        completion_rate = len(df[df['status'] == 'Hoàn thành']) / total_jobs * 100 if total_jobs > 0 else 0
        st.metric("Tỷ lệ hoàn thành", f"{completion_rate:.1f}%")
    
    st.divider()
    
    # ========== CHARTS ==========
    tab1, tab2, tab3 = st.tabs(["📈 Xu hướng", "👥 Phân bổ", "⏱️ Hiệu suất"])
    
    with tab1:
        # Biểu đồ xu hướng theo tháng
        df['month'] = df['start_dt'].dt.strftime('%Y-%m')
        monthly_stats = df.groupby('month').agg({
            'id': 'count',
            'survey_fee': 'sum'
        }).reset_index()
        
        if not monthly_stats.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig1 = px.line(monthly_stats, x='month', y='id',
                              title='Số hồ sơ theo tháng',
                              markers=True)
                fig1.update_traces(line_color='#007bff')
                st.plotly_chart(fig1, use_container_width=True)
            
            with col2:
                fig2 = px.bar(monthly_stats, x='month', y='survey_fee',
                             title='Doanh thu theo tháng',
                             color_discrete_sequence=['#28a745'])
                st.plotly_chart(fig2, use_container_width=True)
    
    with tab2:
        # Phân bổ theo nhân viên và thủ tục
        col1, col2 = st.columns(2)
        
        with col1:
            user_dist = df['assigned_to'].value_counts().head(10)
            if not user_dist.empty:
                fig = px.pie(values=user_dist.values, names=user_dist.index,
                           title='Phân bổ theo nhân viên')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            proc_dist = df['proc_name'].value_counts()
            if not proc_dist.empty:
                fig = px.bar(x=proc_dist.index, y=proc_dist.values,
                           title='Phân bổ theo thủ tục',
                           color_discrete_sequence=['#fd7e14'])
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        # Hiệu suất xử lý
        active_df = df[df['status'] == 'Đang xử lý'].copy()
        if not active_df.empty:
            active_df['processing_days'] = (datetime.now() - active_df['start_dt']).dt.days
            
            col1, col2 = st.columns(2)
            
            with col1:
                stage_times = active_df.groupby('current_stage')['processing_days'].mean().sort_values()
                fig = px.bar(x=stage_times.index, y=stage_times.values,
                           title='Thời gian xử lý trung bình theo giai đoạn (ngày)')
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Top 10 hồ sơ lâu nhất
                longest_jobs = active_df.nlargest(10, 'processing_days')[['id', 'customer_name', 'processing_days', 'assigned_to']]
                st.dataframe(
                    longest_jobs.rename(columns={
                        'id': 'Mã HS',
                        'customer_name': 'Khách hàng',
                        'processing_days': 'Số ngày',
                        'assigned_to': 'Người phụ trách'
                    }),
                    use_container_width=True
                )

def render_job_list():
    """Hiển thị danh sách hồ sơ"""
    st.markdown('<div class="custom-header"><h2>📋 Quản lý hồ sơ</h2></div>', unsafe_allow_html=True)
    
    df = get_all_jobs_df()
    if df.empty:
        st.info("Chưa có hồ sơ nào trong hệ thống")
        return
    
    # ========== FILTERS ==========
    with st.expander("🔍 Bộ lọc nâng cao", expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            status_filter = st.multiselect(
                "Trạng thái",
                options=df['status'].unique(),
                default=['Đang xử lý']
            )
        
        with col2:
            stage_filter = st.multiselect(
                "Giai đoạn",
                options=STAGES_ORDER,
                default=[]
            )
        
        with col3:
            user_filter = st.multiselect(
                "Người phụ trách",
                options=sorted(df['assigned_to'].dropna().unique()),
                default=[]
            )
        
        with col4:
            proc_filter = st.multiselect(
                "Thủ tục",
                options=sorted(df['proc_name'].unique()),
                default=[]
            )
        
        col5, col6 = st.columns(2)
        
        with col5:
            date_range = st.date_input(
                "Khoảng thời gian",
                value=(datetime.now() - timedelta(days=30), datetime.now()),
                key="date_filter"
            )
        
        with col6:
            search_text = st.text_input("Tìm kiếm (tên, SĐT, địa chỉ)")
    
    # Áp dụng filters
    filtered_df = df.copy()
    
    if status_filter:
        filtered_df = filtered_df[filtered_df['status'].isin(status_filter)]
    
    if stage_filter:
        filtered_df = filtered_df[filtered_df['current_stage'].isin(stage_filter)]
    
    if user_filter:
        filtered_df = filtered_df[filtered_df['assigned_to'].isin(user_filter)]
    
    if proc_filter:
        filtered_df = filtered_df[filtered_df['proc_name'].isin(proc_filter)]
    
    if len(date_range) == 2:
        filtered_df = filtered_df[
            (filtered_df['start_dt'].dt.date >= date_range[0]) &
            (filtered_df['start_dt'].dt.date <= date_range[1])
        ]
    
    if search_text:
        search_lower = search_text.lower()
        filtered_df = filtered_df[
            filtered_df['customer_name'].str.lower().str.contains(search_lower) |
            filtered_df['customer_phone'].str.lower().str.contains(search_lower) |
            filtered_df['address'].str.lower().str.contains(search_lower)
        ]
    
    # ========== DISPLAY ==========
    st.info(f"📊 Hiển thị {len(filtered_df)}/{len(df)} hồ sơ")
    
    # Quick actions
    if st.session_state.get('role') in ['Quản lý', 'Trưởng nhóm']:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📤 Xuất Excel", use_container_width=True):
                # Xuất file Excel
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    filtered_df.to_excel(writer, index=False, sheet_name='Hồ sơ')
                st.download_button(
                    label="⬇️ Tải xuống",
                    data=output.getvalue(),
                    file_name=f"hồ_sơ_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
        with col2:
            if st.button("📧 Gửi thông báo", use_container_width=True):
                st.session_state['show_notification'] = True
        
        with col3:
            if st.button("🔄 Làm mới dữ liệu", use_container_width=True):
                get_all_jobs_df.clear()
                st.rerun()
    
    # Data display
    display_cols = [
        'id', 'customer_name', 'customer_phone', 'address',
        'proc_name', 'current_stage', 'assigned_to', 'status',
        'start_dt', 'deadline_dt', 'survey_fee', 'is_paid'
    ]
    
    display_df = filtered_df[display_cols].copy()
    display_df['start_dt'] = display_df['start_dt'].dt.strftime('%d/%m/%Y')
    display_df['deadline_dt'] = display_df['deadline_dt'].dt.strftime('%d/%m/%Y %H:%M')
    display_df['survey_fee'] = display_df['survey_fee'].apply(format_currency)
    display_df['is_paid'] = display_df['is_paid'].apply(lambda x: '✅' if x == 1 else '❌')
    
    # Tạo cột badge
    display_df['_status_badge'] = filtered_df.apply(render_status_badge_html, axis=1)
    
    # Hiển thị dưới dạng HTML table cho đẹp
    st.markdown("""
    <style>
        .dataframe-table {
            width: 100%;
            border-collapse: collapse;
        }
        .dataframe-table th {
            background-color: #f8f9fa;
            padding: 12px;
            text-align: left;
            border-bottom: 2px solid #dee2e6;
            font-weight: 600;
        }
        .dataframe-table td {
            padding: 10px;
            border-bottom: 1px solid #dee2e6;
        }
        .dataframe-table tr:hover {
            background-color: #f8f9fa;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Hiển thị table
    st.dataframe(
        display_df.rename(columns={
            'id': 'Mã HS',
            'customer_name': 'Khách hàng',
            'customer_phone': 'SĐT',
            'address': 'Địa chỉ',
            'proc_name': 'Thủ tục',
            'current_stage': 'Giai đoạn',
            'assigned_to': 'Người PT',
            '_status_badge': 'Trạng thái',
            'start_dt': 'Ngày tạo',
            'deadline_dt': 'Hạn xử lý',
            'survey_fee': 'Phí',
            'is_paid': 'Đã TT'
        }),
        use_container_width=True,
        height=600
    )
    
    # ========== JOB DETAIL VIEW ==========
    if 'selected_job_id' in st.session_state:
        st.divider()
        render_job_detail(st.session_state['selected_job_id'])

def render_job_detail(job_id):
    """Hiển thị chi tiết hồ sơ"""
    df = get_all_jobs_df()
    job = df[df['id'] == job_id]
    
    if job.empty:
        st.error("Không tìm thấy hồ sơ")
        return
    
    job_data = job.iloc[0]
    
    with st.container(border=True):
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader(f"Hồ sơ #{job_id}")
            st.markdown(f"**Khách hàng:** {job_data['customer_name']}")
            st.markdown(f"**SĐT:** {job_data['customer_phone']}")
            st.markdown(f"**Địa chỉ:** {job_data['address']}")
            
            proc_name = extract_proc_from_log(job_data['logs'])
            st.markdown(f"**Thủ tục:** {proc_name}")
            
            # Hiển thị timeline
            stages = STAGES_ORDER
            current_idx = stages.index(job_data['current_stage']) if job_data['current_stage'] in stages else -1
            
            timeline_html = "<div style='display: flex; justify-content: space-between; margin: 20px 0;'>"
            for i, stage in enumerate(stages):
                if i <= current_idx:
                    timeline_html += f"""
                    <div style='text-align: center; flex: 1;'>
                        <div style='background-color: #28a745; color: white; width: 30px; height: 30px; 
                                     border-radius: 50%; display: flex; align-items: center; justify-content: center;
                                     margin: 0 auto 5px;'>
                            {i+1}
                        </div>
                        <div style='font-size: 12px;'>{stage.split('. ')[1] if '. ' in stage else stage}</div>
                    </div>
                    """
                else:
                    timeline_html += f"""
                    <div style='text-align: center; flex: 1; opacity: 0.5;'>
                        <div style='background-color: #e9ecef; color: #6c757d; width: 30px; height: 30px; 
                                     border-radius: 50%; display: flex; align-items: center; justify-content: center;
                                     margin: 0 auto 5px;'>
                            {i+1}
                        </div>
                        <div style='font-size: 12px;'>{stage.split('. ')[1] if '. ' in stage else stage}</div>
                    </div>
                    """
            timeline_html += "</div>"
            
            st.markdown(timeline_html, unsafe_allow_html=True)
        
        with col2:
            config = get_status_badge(job_data['status'], job_data['deadline'], job_data['logs'])
            st.markdown(f"""
            <div style='
                background-color: {config['bg']}; 
                color: {config['color']}; 
                padding: 15px; 
                border-radius: 10px; 
                border: 2px solid {config['color']};
                text-align: center;
                margin-bottom: 20px;
            '>
                <div style='font-size: 24px; margin-bottom: 5px;'>{config['icon']}</div>
                <div style='font-weight: bold; font-size: 16px;'>{config['text']}</div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown(f"**Người phụ trách:** {job_data['assigned_to']}")
            st.markdown(f"**Ngày tạo:** {job_data['start_dt'].strftime('%d/%m/%Y %H:%M')}")
            
            if pd.notna(job_data['deadline_dt']):
                deadline_str = job_data['deadline_dt'].strftime('%d/%m/%Y %H:%M')
                days_left = (job_data['deadline_dt'] - datetime.now()).days
                
                if days_left < 0:
                    st.error(f"**Hạn xử lý:** {deadline_str} (Quá hạn {abs(days_left)} ngày)")
                elif days_left <= 3:
                    st.warning(f"**Hạn xử lý:** {deadline_str} (Còn {days_left} ngày)")
                else:
                    st.info(f"**Hạn xử lý:** {deadline_str} (Còn {days_left} ngày)")
        
        # Tabs chi tiết
        tab1, tab2, tab3, tab4 = st.tabs(["📁 Files", "💰 Tài chính", "📝 Xử lý", "📜 Lịch sử"])
        
        with tab1:
            # Hiển thị files
            file_list = extract_files_from_log(job_data['logs'])
            if file_list:
                for fname, link in file_list:
                    col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
                    with col1:
                        st.markdown(f"📄 **{fname}**")
                    with col2:
                        st.link_button("👁️ Xem", link)
                    with col3:
                        file_id = get_drive_id(link)
                        if file_id:
                            download_link = f"https://drive.google.com/uc?export=download&id={file_id}"
                            st.link_button("⬇️ Tải", download_link)
                    with col4:
                        if st.session_state.get('role') == 'Quản lý':
                            if st.button("🗑️", key=f"del_{link}"):
                                if delete_file_from_drive(file_id):
                                    st.success("Đã xóa file")
                                    st.rerun()
            else:
                st.info("Chưa có file nào")
            
            # Upload file mới
            with st.expander("➕ Thêm file mới"):
                new_files = st.file_uploader("Chọn file", accept_multiple_files=True)
                if st.button("Upload"):
                    if new_files:
                        # TODO: Implement upload logic
                        st.success(f"Đã thêm {len(new_files)} file")
        
        with tab2:
            # Thông tin tài chính
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Phí đo đạc", format_currency(job_data['survey_fee']))
                st.metric("Đặt cọc", "✅" if job_data['deposit'] == 1 else "❌")
            
            with col2:
                st.metric("Đã thanh toán", "✅" if job_data['is_paid'] == 1 else "❌")
                
                if st.session_state.get('role') in ['Quản lý', 'Trưởng nhóm']:
                    with st.form("update_finance"):
                        new_fee = st.number_input("Cập nhật phí", value=safe_int(job_data['survey_fee']))
                        is_paid = st.checkbox("Đã thanh toán", value=job_data['is_paid'] == 1)
                        
                        if st.form_submit_button("💾 Lưu"):
                            # TODO: Implement update logic
                            st.success("Đã cập nhật")
        
        with tab3:
            # Xử lý hồ sơ
            current_stage = job_data['current_stage']
            next_stage = get_next_stage_dynamic(current_stage, proc_name)
            
            if current_stage == "8. Hoàn thành":
                st.success("✅ Hồ sơ đã hoàn thành")
            else:
                with st.form(f"process_{job_id}"):
                    st.markdown(f"**Giai đoạn hiện tại:** {current_stage}")
                    st.markdown(f"**Chuyển đến:** {next_stage if next_stage else 'Hoàn thành'}")
                    
                    note = st.text_area("Ghi chú xử lý", height=100)
                    
                    new_files = st.file_uploader("File đính kèm", accept_multiple_files=True, key=f"files_{job_id}")
                    
                    assigned_to = st.selectbox(
                        "Giao cho",
                        options=get_active_users_list(),
                        index=0
                    )
                    
                    if st.form_submit_button("✅ Chuyển giai đoạn", type="primary"):
                        success = update_job_stage(
                            job_id, current_stage, note, new_files,
                            st.session_state.get('username'), assigned_to
                        )
                        if success:
                            st.success("Đã cập nhật!")
                            st.rerun()
        
        with tab4:
            # Hiển thị logs
            logs = job_data['logs']
            if logs:
                # Parse logs để hiển thị đẹp
                log_entries = re.findall(r'\[(.*?)\]\s*(.*?):\s*(.*?)(?=\n\[|$)', str(logs), re.DOTALL)
                
                for timestamp, user, action in log_entries:
                    with st.container(border=True):
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            st.markdown(f"**{timestamp}**")
                            st.caption(f"👤 {user}")
                        with col2:
                            st.markdown(action.strip())
            else:
                st.info("Chưa có nhật ký")

def render_create_job():
    """Giao diện tạo hồ sơ mới"""
    st.markdown('<div class="custom-header"><h2>➕ Tạo hồ sơ mới</h2></div>', unsafe_allow_html=True)
    
    with st.form("create_job_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            customer_name = st.text_input("Tên khách hàng *", max_chars=100)
            customer_phone = st.text_input("Số điện thoại *", max_chars=15)
            
        with col2:
            customer_address = st.text_area("Địa chỉ *", height=80, max_chars=200)
            procedure = st.selectbox("Thủ tục *", PROCEDURES_LIST)
        
        st.divider()
        
        col3, col4 = st.columns(2)
        
        with col3:
            assigned_to = st.selectbox(
                "Giao cho *",
                options=get_active_users_list(),
                help="Chọn người phụ trách chính"
            )
            
            # Tự động tính phí dựa trên thủ tục
            estimated_fee = PROCEDURE_PRICES.get(procedure, 1500000)
            st.info(f"💰 Phí ước tính: {format_currency(estimated_fee)}")
            
            is_urgent = st.checkbox("🔴 Ưu tiên cao (xử lý nhanh)")
        
        with col4:
            uploaded_files = st.file_uploader(
                "Tài liệu đính kèm",
                accept_multiple_files=True,
                help="Có thể upload nhiều file cùng lúc"
            )
            
            if uploaded_files:
                st.success(f"📎 Đã chọn {len(uploaded_files)} file")
                for file in uploaded_files:
                    st.caption(f"- {file.name} ({file.size // 1024} KB)")
        
        st.divider()
        
        # Thông tin bổ sung
        with st.expander("ℹ️ Thông tin bổ sung (không bắt buộc)"):
            col5, col6 = st.columns(2)
            with col5:
                customer_email = st.text_input("Email khách hàng")
                customer_id = st.text_input("CMND/CCCD")
            with col6:
                notes = st.text_area("Ghi chú thêm", height=60)
        
        submitted = st.form_submit_button("🚀 Tạo hồ sơ", type="primary")
        
        if submitted:
            # Validate
            if not customer_name or not customer_phone or not customer_address or not assigned_to:
                st.error("Vui lòng điền đầy đủ các trường bắt buộc (*)")
                return
            
            # Validate phone number
            if not re.match(r'^[0-9+\-\s]{10,15}$', customer_phone):
                st.warning("Số điện thoại có thể không hợp lệ")
            
            # Tạo hồ sơ
            customer_info = {
                'name': customer_name.strip(),
                'phone': customer_phone.strip(),
                'address': customer_address.strip(),
                'email': customer_email.strip() if customer_email else "",
                'id_number': customer_id.strip() if customer_id else ""
            }
            
            with st.spinner("Đang tạo hồ sơ..."):
                job_id, unique_name = create_new_job(
                    customer_info,
                    procedure,
                    uploaded_files,
                    assigned_to,
                    st.session_state.get('username')
                )
                
                if job_id:
                    st.success(f"✅ Đã tạo hồ sơ #{job_id} thành công!")
                    
                    # Hiển thị thông tin vừa tạo
                    col1, col2 = st.columns(2)
                    with col1:
                        st.info(f"**Mã hồ sơ:** {job_id}")
                        st.info(f"**Tên file:** {unique_name}")
                    with col2:
                        st.info(f"**Người phụ trách:** {assigned_to}")
                        st.info(f"**Thủ tục:** {procedure}")
                    
                    # Tự động chuyển đến trang chi tiết
                    if st.button("📋 Xem chi tiết hồ sơ"):
                        st.session_state['selected_job_id'] = job_id
                        st.session_state['selected_menu'] = "Hồ sơ của tôi"
                        st.rerun()
                    
                    # Xóa form
                    st.session_state['create_form_clear'] = True

def render_calendar():
    """Giao diện lịch biểu"""
    st.markdown('<div class="custom-header"><h2>📅 Lịch biểu công việc</h2></div>', unsafe_allow_html=True)
    
    df = get_all_jobs_df()
    if df.empty:
        st.info("Chưa có dữ liệu")
        return
    
    # Chọn tháng/năm
    col1, col2, col3 = st.columns(3)
    with col1:
        view_mode = st.selectbox("Chế độ xem", ["Tháng", "Tuần", "Ngày"])
    
    now = datetime.now()
    
    if view_mode == "Tháng":
        with col2:
            selected_month = st.selectbox("Tháng", range(1, 13), index=now.month - 1)
        with col3:
            selected_year = st.selectbox("Năm", range(2020, 2031), index=now.year - 2020)
        
        # Hiển thị calendar
        render_monthly_calendar(selected_year, selected_month, df)
    
    elif view_mode == "Tuần":
        # TODO: Implement weekly view
        st.info("Chế độ xem tuần đang phát triển")
    
    else:  # Ngày
        selected_date = st.date_input("Chọn ngày", now.date())
        render_daily_view(selected_date, df)

def render_monthly_calendar(year, month, df):
    """Hiển thị lịch tháng"""
    # Lấy calendar
    cal = calendar.monthcalendar(year, month)
    
    # Lọc công việc trong tháng
    df_month = df[
        (df['start_dt'].dt.year == year) & 
        (df['start_dt'].dt.month == month)
    ].copy()
    
    # Hiển thị header
    days = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"]
    cols = st.columns(7)
    for i, day in enumerate(days):
        cols[i].markdown(f"**{day}**", unsafe_allow_html=True)
    
    # Hiển thị từng tuần
    for week in cal:
        cols = st.columns(7)
        for i, day in enumerate(week):
            with cols[i]:
                if day != 0:
                    current_date = date(year, month, day)
                    
                    # Hiển thị ngày
                    is_today = current_date == datetime.now().date()
                    day_style = "background-color: #007bff; color: white; border-radius: 50%; padding: 5px; text-align: center;" if is_today else ""
                    st.markdown(f"<div style='{day_style} text-align: center; font-weight: bold;'>{day}</div>", unsafe_allow_html=True)
                    
                    # Lấy công việc trong ngày
                    day_jobs_start = df_month[df_month['start_dt'].dt.date == current_date]
                    day_jobs_deadline = df_month[df_month['deadline_dt'].dt.date == current_date]
                    
                    # Hiển thị công việc bắt đầu
                    if not day_jobs_start.empty:
                        with st.expander(f"📌 Nhận ({len(day_jobs_start)})", expanded=False):
                            for _, job in day_jobs_start.iterrows():
                                st.caption(f"#{job['id']} - {job['customer_name'][:15]}...")
                    
                    # Hiển thị công việc đến hạn
                    if not day_jobs_deadline.empty:
                        urgent_jobs = day_jobs_deadline[day_jobs_deadline['is_overdue']]
                        if not urgent_jobs.empty:
                            st.error(f"⚠️ {len(urgent_jobs)} quá hạn")
                        else:
                            st.info(f"📅 {len(day_jobs_deadline)} đến hạn")

def render_financial_dashboard():
    """Dashboard tài chính"""
    st.markdown('<div class="custom-header"><h2>💰 Quản lý tài chính</h2></div>', unsafe_allow_html=True)
    
    df = get_all_jobs_df()
    if df.empty:
        st.info("Chưa có dữ liệu")
        return
    
    # ========== TỔNG QUAN TÀI CHÍNH ==========
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_revenue = df['survey_fee'].sum()
        st.metric("Tổng doanh thu", format_currency(total_revenue))
    
    with col2:
        collected_revenue = df[df['is_paid'] == 1]['survey_fee'].sum()
        st.metric("Đã thu", format_currency(collected_revenue))
    
    with col3:
        pending_revenue = df[df['is_paid'] == 0]['survey_fee'].sum()
        st.metric("Chưa thu", format_currency(pending_revenue))
    
    with col4:
        collection_rate = collected_revenue / total_revenue * 100 if total_revenue > 0 else 0
        st.metric("Tỷ lệ thu", f"{collection_rate:.1f}%")
    
    st.divider()
    
    # ========== CHI TIẾT CÔNG NỢ ==========
    st.subheader("📋 Chi tiết công nợ")
    
    debt_df = df[df['is_paid'] == 0].copy()
    
    if not debt_df.empty:
        # Nhóm theo người phụ trách
        debt_by_user = debt_df.groupby('assigned_to').agg({
            'id': 'count',
            'survey_fee': 'sum'
        }).sort_values('survey_fee', ascending=False)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.dataframe(
                debt_by_user.rename(columns={
                    'id': 'Số hồ sơ',
                    'survey_fee': 'Tổng nợ'
                }),
                use_container_width=True
            )
        
        with col2:
            fig = px.bar(
                debt_by_user.reset_index(),
                x='assigned_to',
                y='survey_fee',
                title='Công nợ theo nhân viên',
                color_discrete_sequence=['#dc3545']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Bảng chi tiết công nợ
        st.subheader("📝 Danh sách hồ sơ chưa thanh toán")
        
        detail_cols = ['id', 'customer_name', 'customer_phone', 'assigned_to', 
                      'current_stage', 'survey_fee', 'start_dt']
        
        detail_df = debt_df[detail_cols].copy()
        detail_df['start_dt'] = detail_df['start_dt'].dt.strftime('%d/%m/%Y')
        detail_df['survey_fee'] = detail_df['survey_fee'].apply(format_currency)
        
        st.dataframe(
            detail_df.rename(columns={
                'id': 'Mã HS',
                'customer_name': 'Khách hàng',
                'customer_phone': 'SĐT',
                'assigned_to': 'Người PT',
                'current_stage': 'Giai đoạn',
                'survey_fee': 'Số tiền',
                'start_dt': 'Ngày tạo'
            }),
            use_container_width=True,
            height=400
        )
        
        # Export công nợ
        if st.button("📤 Xuất báo cáo công nợ"):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                debt_df.to_excel(writer, index=False, sheet_name='Công nợ')
            st.download_button(
                label="⬇️ Tải file Excel",
                data=output.getvalue(),
                file_name=f"cong_no_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    else:
        st.success("🎉 Không có công nợ nào trong hệ thống!")
    
    st.divider()
    
    # ========== THỐNG KÊ THEO THỦ TỤC ==========
    st.subheader("📊 Doanh thu theo thủ tục")
    
    revenue_by_proc = df.groupby('proc_name').agg({
        'id': 'count',
        'survey_fee': 'sum',
        'is_paid': lambda x: (x == 1).sum()
    }).sort_values('survey_fee', ascending=False)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.dataframe(
            revenue_by_proc.rename(columns={
                'id': 'Số hồ sơ',
                'survey_fee': 'Tổng doanh thu',
                'is_paid': 'Đã thu'
            }),
            use_container_width=True
        )
    
    with col2:
        fig = px.pie(
            revenue_by_proc.reset_index(),
            values='survey_fee',
            names='proc_name',
            title='Tỷ trọng doanh thu'
        )
        st.plotly_chart(fig, use_container_width=True)

def render_user_management():
    """Quản lý người dùng (chỉ Quản lý)"""
    if st.session_state.get('role') != 'Quản lý':
        st.error("⛔ Bạn không có quyền truy cập trang này")
        return
    
    st.markdown('<div class="custom-header"><h2>👥 Quản lý nhân sự</h2></div>', unsafe_allow_html=True)
    
    users_df = get_users_df()
    
    tab1, tab2, tab3 = st.tabs(["📋 Danh sách", "➕ Thêm mới", "📊 Thống kê"])
    
    with tab1:
        if not users_df.empty:
            # Filter active/inactive
            show_inactive = st.checkbox("Hiển thị tài khoản không hoạt động")
            filtered_users = users_df.copy()
            
            if not show_inactive and 'active' in filtered_users.columns:
                filtered_users = filtered_users[filtered_users['active'].astype(str).str.lower() == 'true']
            
            # Hiển thị danh sách
            for _, user in filtered_users.iterrows():
                with st.container(border=True):
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                    
                    with col1:
                        st.markdown(f"**{user.get('fullname', '')}**")
                        st.caption(f"👤 {user['username']}")
                        if user.get('email'):
                            st.caption(f"📧 {user['email']}")
                    
                    with col2:
                        # Role selector
                        current_role = user.get('role', 'Nhân viên')
                        new_role = st.selectbox(
                            "Vai trò",
                            ROLES,
                            index=ROLES.index(current_role) if current_role in ROLES else 0,
                            key=f"role_{user['username']}",
                            label_visibility="collapsed"
                        )
                        
                        if new_role != current_role:
                            # TODO: Update role
                            st.rerun()
                    
                    with col3:
                        # Active status
                        is_active = str(user.get('active', 'true')).lower() == 'true'
                        active_status = st.checkbox(
                            "Hoạt động",
                            value=is_active,
                            key=f"active_{user['username']}"
                        )
                        
                        if active_status != is_active:
                            # TODO: Update active status
                            st.rerun()
                    
                    with col4:
                        # Delete button (không cho xóa chính mình)
                        if user['username'] != st.session_state.get('username'):
                            if st.button("🗑️", key=f"delete_{user['username']}"):
                                # TODO: Delete user
                                st.warning(f"Xóa user {user['username']}?")
        
        else:
            st.info("Chưa có người dùng nào trong hệ thống")
    
    with tab2:
        with st.form("add_user_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                new_username = st.text_input("Username *", max_chars=20)
                new_password = st.text_input("Mật khẩu *", type="password")
                confirm_password = st.text_input("Xác nhận mật khẩu *", type="password")
            
            with col2:
                new_fullname = st.text_input("Họ tên *", max_chars=50)
                new_email = st.text_input("Email")
                new_phone = st.text_input("Số điện thoại")
                new_role = st.selectbox("Vai trò", ROLES, index=2)
            
            submitted = st.form_submit_button("➕ Thêm người dùng", type="primary")
            
            if submitted:
                # Validate
                if not new_username or not new_password or not new_fullname:
                    st.error("Vui lòng điền đầy đủ các trường bắt buộc (*)")
                    return
                
                if new_password != confirm_password:
                    st.error("Mật khẩu xác nhận không khớp")
                    return
                
                if new_username in users_df['username'].values:
                    st.error("Username đã tồn tại")
                    return
                
                # Thêm user
                success, message = register_user(
                    new_username, new_password, new_fullname,
                    new_email, new_phone
                )
                
                if success:
                    st.success(message)
                    get_users_df.clear()
                    st.rerun()
                else:
                    st.error(message)
    
    with tab3:
        if not users_df.empty:
            # Thống kê người dùng
            col1, col2, col3 = st.columns(3)
            
            with col1:
                total_users = len(users_df)
                active_users = len(users_df[users_df['active'].astype(str).str.lower() == 'true'])
                st.metric("Tổng người dùng", total_users, f"{active_users} đang hoạt động")
            
            with col2:
                role_dist = users_df['role'].value_counts()
                st.metric("Quản lý", role_dist.get('Quản lý', 0))
            
            with col3:
                st.metric("Nhân viên", role_dist.get('Nhân viên', 0))
            
            # Phân bổ vai trò
            fig = px.pie(
                users_df, 
                names='role',
                title='Phân bổ vai trò'
            )
            st.plotly_chart(fig, use_container_width=True)

def render_settings():
    """Trang cài đặt hệ thống"""
    if st.session_state.get('role') != 'Quản lý':
        st.error("⛔ Bạn không có quyền truy cập trang này")
        return
    
    st.markdown('<div class="custom-header"><h2>⚙️ Cài đặt hệ thống</h2></div>', unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["🔄 Workflow", "💰 Giá cả", "📊 Cấu hình"])
    
    with tab1:
        st.subheader("Cấu hình quy trình xử lý")
        
        # Hiển thị và chỉnh sửa SLA
        sla_df = pd.DataFrame([
            {"Giai đoạn": stage, "SLA (giờ)": hours}
            for stage, hours in STAGE_SLA_HOURS.items()
        ])
        
        edited_sla = st.data_editor(
            sla_df,
            num_rows="fixed",
            use_container_width=True
        )
        
        if st.button("💾 Lưu cấu hình SLA"):
            # TODO: Save to Google Sheets
            st.success("Đã lưu cấu hình SLA")
    
    with tab2:
        st.subheader("Cấu hình giá dịch vụ")
        
        # Hiển thị và chỉnh sửa giá
        prices_df = pd.DataFrame([
            {"Thủ tục": proc, "Giá (VNĐ)": price}
            for proc, price in PROCEDURE_PRICES.items()
        ])
        
        edited_prices = st.data_editor(
            prices_df,
            num_rows="fixed",
            column_config={
                "Giá (VNĐ)": st.column_config.NumberColumn(
                    format="%d ₫"
                )
            },
            use_container_width=True
        )
        
        if st.button("💾 Lưu cấu hình giá"):
            # TODO: Save to Google Sheets
            st.success("Đã lưu cấu hình giá")
    
    with tab3:
        st.subheader("Cấu hình hệ thống")
        
        # Telegram settings
        with st.expander("🤖 Cấu hình Telegram", expanded=True):
            telegram_token = st.text_input("Telegram Bot Token", value=TELEGRAM_TOKEN, type="password")
            telegram_chat = st.text_input("Telegram Chat ID", value=TELEGRAM_CHAT_ID)
            
            if st.button("Kiểm tra kết nối Telegram"):
                st.info("Chức năng đang phát triển")
        
        # Google Drive settings
        with st.expander("☁️ Cấu hình Google Drive"):
            drive_folder = st.text_input("Drive Folder ID", value=DRIVE_FOLDER_ID)
            apps_script_url = st.text_input("Apps Script URL", value=APPS_SCRIPT_URL)
        
        # System backup
        with st.expander("💾 Sao lưu & Khôi phục"):
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔄 Sao lưu dữ liệu", use_container_width=True):
                    with st.spinner("Đang sao lưu..."):
                        # TODO: Backup implementation
                        time.sleep(2)
                        st.success("Đã sao lưu thành công!")
            
            with col2:
                backup_file = st.file_uploader("Chọn file sao lưu", type=['json', 'xlsx'])
                if backup_file and st.button("🔄 Khôi phục dữ liệu", type="secondary", use_container_width=True):
                    st.warning("⚠️ Cảnh báo: Hành động này sẽ ghi đè dữ liệu hiện tại!")
                    
                    confirm = st.checkbox("Tôi hiểu và đồng ý")
                    if confirm and st.button("Xác nhận khôi phục", type="primary"):
                        with st.spinner("Đang khôi phục..."):
                            # TODO: Restore implementation
                            time.sleep(2)
                            st.success("Đã khôi phục thành công!")

# ==================== MAIN APPLICATION ====================
def main():
    # Khởi tạo session state
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    
    if 'selected_menu' not in st.session_state:
        st.session_state.selected_menu = "Tổng quan"
    
    # Inject CSS
    render_custom_css()
    
    # Kiểm tra đăng nhập
    if not st.session_state.logged_in:
        render_login_page()
    else:
        # Hiển thị ứng dụng chính
        render_main_app()

def render_login_page():
    """Trang đăng nhập"""
    st.markdown("""
    <div style='text-align: center; padding: 3rem 0;'>
        <h1 style='color: #007bff;'>📊 HỆ THỐNG QUẢN LÝ ĐO ĐẠC</h1>
        <p style='color: #6c757d; font-size: 1.1rem;'>
            Công cụ quản lý hồ sơ đo đạc chuyên nghiệp
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.container(border=True):
            st.subheader("🔐 Đăng nhập hệ thống")
            
            tab1, tab2 = st.tabs(["Đăng nhập", "Đăng ký"])
            
            with tab1:
                username = st.text_input("Tên đăng nhập", key="login_username")
                password = st.text_input("Mật khẩu", type="password", key="login_password")
                
                col_btn1, col_btn2 = st.columns(2)
                with col_btn1:
                    if st.button("🚪 Đăng nhập", type="primary", use_container_width=True):
                        if not username or not password:
                            st.error("Vui lòng nhập đầy đủ thông tin")
                            return
                        
                        user_data = authenticate_user(username, password)
                        
                        if user_data:
                            st.session_state.logged_in = True
                            st.session_state.username = user_data['username']
                            st.session_state.fullname = user_data['fullname']
                            st.session_state.role = user_data['role']
                            st.session_state.email = user_data['email']
                            st.session_state.phone = user_data['phone']
                            
                            # Ghi log đăng nhập
                            log_audit_action(
                                username,
                                "LOGIN",
                                f"Đăng nhập thành công từ IP: {st.experimental_user.ip_address if hasattr(st.experimental_user, 'ip_address') else 'Unknown'}"
                            )
                            
                            st.success(f"👋 Chào mừng {user_data['fullname']}!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Tên đăng nhập hoặc mật khẩu không đúng")
                
                with col_btn2:
                    if st.button("🔄 Quên mật khẩu", use_container_width=True):
                        st.info("Vui lòng liên hệ quản trị viên để đặt lại mật khẩu")
            
            with tab2:
                new_username = st.text_input("Tên đăng nhập mới", key="reg_username")
                new_password = st.text_input("Mật khẩu mới", type="password", key="reg_password")
                confirm_password = st.text_input("Xác nhận mật khẩu", type="password", key="reg_confirm")
                new_fullname = st.text_input("Họ tên đầy đủ", key="reg_fullname")
                new_email = st.text_input("Email", key="reg_email")
                
                if st.button("📝 Đăng ký tài khoản", type="primary", use_container_width=True):
                    if not new_username or not new_password or not new_fullname:
                        st.error("Vui lòng nhập đầy đủ thông tin bắt buộc")
                        return
                    
                    if new_password != confirm_password:
                        st.error("Mật khẩu xác nhận không khớp")
                        return
                    
                    success, message = register_user(
                        new_username, new_password, new_fullname, new_email
                    )
                    
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
        
        # Footer
        st.markdown("""
        <div style='text-align: center; margin-top: 3rem; color: #6c757d; font-size: 0.9rem;'>
            <hr>
            <p>© 2024 Hệ thống Quản lý Đo đạc. Phiên bản 4.0</p>
            <p>Liên hệ hỗ trợ: support@dodac.com | Hotline: 1900 1234</p>
        </div>
        """, unsafe_allow_html=True)

def render_main_app():
    """Ứng dụng chính sau khi đăng nhập"""
    # Sidebar
    render_sidebar_menu(st.session_state.get('role', 'Nhân viên'))
    
    # Main content
    selected_menu = st.session_state.get('selected_menu', 'Tổng quan')
    
    if selected_menu == 'Tổng quan':
        render_dashboard()
    
    elif selected_menu == 'Hồ sơ của tôi':
        render_job_list()
    
    elif selected_menu == 'Tạo hồ sơ':
        render_create_job()
    
    elif selected_menu == 'Lịch biểu':
        render_calendar()
    
    elif selected_menu == 'Báo cáo':
        render_dashboard()  # Tạm thời dùng dashboard
    
    elif selected_menu == 'Tài chính':
        render_financial_dashboard()
    
    elif selected_menu == 'Lưu trữ':
        st.info("Chức năng đang phát triển")
    
    elif selected_menu == 'Nhân sự':
        render_user_management()
    
    elif selected_menu == 'Phân tích':
        st.info("Chức năng đang phát triển")
    
    elif selected_menu == 'Cài đặt':
        render_settings()
    
    elif selected_menu == 'Nhật ký':
        # Hiển thị audit logs
        logs_df = get_audit_logs_df()
        if not logs_df.empty:
            st.dataframe(logs_df, use_container_width=True)
        else:
            st.info("Chưa có nhật ký nào")
    
    elif selected_menu == 'Thùng rác':
        df = get_all_jobs_df()
        deleted_jobs = df[df['status'] == 'Đã xóa']
        
        if not deleted_jobs.empty:
            st.dataframe(deleted_jobs, use_container_width=True)
        else:
            st.success("Thùng rác trống")

# ==================== SCHEDULER & BACKGROUND TASKS ====================
def background_scheduler():
    """Chạy các task nền"""
    while True:
        try:
            now = datetime.now()
            
            # Kiểm tra vào 8h và 13h hàng ngày
            if (now.hour == 8 or now.hour == 13) and now.minute < 5:
                send_daily_notifications()
            
            # Kiểm tra mỗi phút
            check_overdue_jobs()
            
            time.sleep(60)  # Chạy mỗi phút
            
        except Exception as e:
            print(f"Lỗi scheduler: {e}")
            time.sleep(300)

def send_daily_notifications():
    """Gửi thông báo hàng ngày"""
    try:
        df = get_all_jobs_df()
        if df.empty:
            return
        
        # Lọc hồ sơ đang xử lý
        active_df = df[df['status'] == 'Đang xử lý']
        
        # Hồ sơ sắp đến hạn (24h)
        soon_df = active_df[
            (active_df['deadline_dt'] > datetime.now()) & 
            (active_df['deadline_dt'] <= datetime.now() + timedelta(hours=24))
        ]
        
        if not soon_df.empty:
            message = f"⏰ **CẢNH BÁO HẠN XỬ LÝ ({len(soon_df)} hồ sơ)**\n\n"
            
            for _, job in soon_df.iterrows():
                hours_left = int((job['deadline_dt'] - datetime.now()).total_seconds() / 3600)
                proc_name = extract_proc_from_log(job['logs'])
                unique_name = generate_unique_name(
                    job['id'], job['start_time'],
                    job['customer_name'], job['customer_phone'],
                    job['address'], proc_name
                )
                
                message += f"🔸 {unique_name} - Còn {hours_left} giờ - {job['assigned_to']}\n"
            
            send_telegram_notification(message)
            
    except Exception as e:
        print(f"Lỗi gửi thông báo: {e}")

def check_overdue_jobs():
    """Kiểm tra hồ sơ quá hạn"""
    try:
        df = get_all_jobs_df()
        if df.empty:
            return
        
        overdue_df = df[df['is_overdue']]
        
        # Gửi cảnh báo cho quản lý nếu có hồ sơ quá hạn > 3 ngày
        critical_overdue = overdue_df[
            (datetime.now() - overdue_df['deadline_dt']).dt.days > 3
        ]
        
        if not critical_overdue.empty and datetime.now().hour == 9:
            message = f"🚨 **CẢNH BÁO QUÁ HẠN NGHIÊM TRỌNG ({len(critical_overdue)} hồ sơ)**\n\n"
            
            for _, job in critical_overdue.iterrows():
                days_overdue = (datetime.now() - job['deadline_dt']).days
                message += f"🔴 {job['customer_name']} - Quá hạn {days_overdue} ngày - {job['assigned_to']}\n"
            
            send_telegram_notification(message)
            
    except Exception as e:
        print(f"Lỗi kiểm tra quá hạn: {e}")

# ==================== RUN APPLICATION ====================
if __name__ == "__main__":
    # Khởi chạy scheduler trong thread riêng
    if 'scheduler_started' not in st.session_state:
        scheduler_thread = threading.Thread(target=background_scheduler, daemon=True)
        scheduler_thread.start()
        st.session_state.scheduler_started = True
    
    # Chạy ứng dụng chính
    main()
