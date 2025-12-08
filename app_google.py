# app_google_no_plotly.py
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
    clean_phone = str(phone).replace("'", "").replace(" ", "")[-9:]
    clean_name = name.strip()[:30]
    
    return f"{date_str}-{seq}{'-' + abbr if abbr else ''} {clean_name} {clean_phone}"

def extract_files_from_log(log_text):
    """Trích xuất file từ log"""
    pattern = r"File:\s*(.*?)\s*-\s*(https?://[^\s]+)"
    matches = re.findall(pattern, str(log_text))
    
    if not matches:
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
    
    if status == "Đang xử lý" and deadline:
        try:
            dl_dt = pd.to_datetime(deadline)
            if now > dl_dt:
                config = {"color": "#dc3545", "bg": "#ffe6e6", "text": "🔴 Quá hạn", "icon": "🔴"}
            elif now <= dl_dt <= now + timedelta(hours=24):
                config = {"color": "#fd7e14", "bg": "#fff3cd", "text": "⚠️ Sắp đến hạn", "icon": "⚠️"}
        except:
            pass
    
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
        required_columns = ['id', 'start_time', 'customer_name', 'customer_phone', 
                           'address', 'current_stage', 'status', 'assigned_to', 
                           'deadline', 'file_link', 'logs']
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = ""
        
        df['id'] = df['id'].apply(safe_int)
        df['start_dt'] = pd.to_datetime(df['start_time'], errors='coerce')
        df['deadline_dt'] = pd.to_datetime(df['deadline'], errors='coerce')
        
        financial_cols = ['deposit', 'survey_fee', 'is_paid', 'is_survey_only']
        for col in financial_cols:
            if col not in df.columns:
                df[col] = 0
            df[col] = df[col].apply(safe_int)
        
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
        df = pd.DataFrame(columns=['username', 'password', 'fullname', 'role', 'email', 'phone', 'active'])
    return df

@st.cache_data(ttl=300)
def get_audit_logs_df():
    """Lấy logs audit"""
    df = get_sheet_data("DB_DODAC", "AUDIT_LOGS")
    if df.empty:
        df = pd.DataFrame(columns=['Timestamp', 'User', 'Action', 'Details', 'IP_Address'])
    return df

# ==================== AUTHENTICATION ====================
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
    
    if 'active' in user_data and str(user_data['active']).lower() == 'false':
        return None
    
    if user_data['password'] == hash_password(password):
        return {
            'username': user_data['username'],
            'fullname': user_data.get('fullname', username),
            'role': user_data.get('role', 'Nhân viên'),
            'email': user_data.get('email', ''),
            'phone': user_data.get('phone', '')
        }
    
    return None

def get_active_users_list():
    """Lấy danh sách người dùng đang hoạt động"""
    users_df = get_users_df()
    if users_df.empty:
        return []
    
    active_users = users_df[
        (users_df['role'] != 'Chưa cấp quyền') & 
        (users_df['active'].astype(str).str.lower() == 'true')
    ]
    
    return active_users.apply(
        lambda x: f"{x['username']} - {x['fullname']}", 
        axis=1
    ).tolist()

# ==================== UI COMPONENTS ====================
def render_custom_css():
    """Inject custom CSS styles"""
    st.markdown("""
    <style>
        .main {
            padding: 1rem 2rem;
        }
        
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
        
        .stButton > button {
            border-radius: 8px;
            font-weight: 500;
            transition: all 0.3s ease;
        }
        
        .stButton > button:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }
        
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
        
        .status-badge {
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            display: inline-flex;
            align-items: center;
            gap: 4px;
        }
        
        .custom-header {
            background: linear-gradient(90deg, #007bff, #6610f2);
            color: white;
            padding: 1.5rem;
            border-radius: 12px;
            margin-bottom: 2rem;
        }
        
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

def render_sidebar_menu(user_role):
    """Render sidebar menu với phân quyền"""
    with st.sidebar:
        st.markdown(f"""
        <div style="text-align: center; padding: 1rem 0;">
            <h2 style="color: #333;">📊 DODAC PRO</h2>
            <p style="color: #666; font-size: 14px;">
                Hệ thống quản lý đo đạc
            </p>
        </div>
        <hr>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns([1, 3])
        with col1:
            st.markdown("👤")
        with col2:
            st.markdown(f"**{st.session_state.get('fullname', 'User')}**")
            st.caption(f"{user_role}")
        
        st.divider()
        
        menu_options = [
            ("🏠", "Tổng quan", ["Quản lý", "Trưởng nhóm", "Nhân viên", "Thực tập"]),
            ("📋", "Hồ sơ của tôi", ["Quản lý", "Trưởng nhóm", "Nhân viên", "Thực tập"]),
            ("➕", "Tạo hồ sơ", ["Quản lý", "Trưởng nhóm", "Nhân viên"]),
            ("📅", "Lịch biểu", ["Quản lý", "Trưởng nhóm", "Nhân viên", "Thực tập"]),
            ("📊", "Báo cáo", ["Quản lý", "Trưởng nhóm"]),
            ("💰", "Tài chính", ["Quản lý", "Trưởng nhóm"]),
            ("🗃️", "Lưu trữ", ["Quản lý", "Trưởng nhóm", "Nhân viên"]),
            ("👥", "Nhân sự", ["Quản lý"]),
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
    
    tab1, tab2, tab3 = st.tabs(["📈 Xu hướng", "👥 Phân bổ", "⏱️ Hiệu suất"])
    
    with tab1:
        df['month'] = df['start_dt'].dt.strftime('%Y-%m')
        monthly_stats = df.groupby('month').agg({
            'id': 'count',
            'survey_fee': 'sum'
        }).reset_index()
        
        if not monthly_stats.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📈 Số hồ sơ theo tháng")
                st.line_chart(monthly_stats.set_index('month')['id'], color='#007bff')
            
            with col2:
                st.subheader("💰 Doanh thu theo tháng")
                st.bar_chart(monthly_stats.set_index('month')['survey_fee'], color='#28a745')
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            user_dist = df['assigned_to'].value_counts().head(10)
            if not user_dist.empty:
                st.subheader("👥 Phân bổ theo nhân viên")
                st.dataframe(user_dist, use_container_width=True)
        
        with col2:
            proc_dist = df['proc_name'].value_counts()
            if not proc_dist.empty:
                st.subheader("📋 Phân bổ theo thủ tục")
                st.bar_chart(proc_dist, color='#fd7e14')
    
    with tab3:
        active_df = df[df['status'] == 'Đang xử lý'].copy()
        if not active_df.empty:
            active_df['processing_days'] = (datetime.now() - active_df['start_dt']).dt.days
            
            col1, col2 = st.columns(2)
            
            with col1:
                stage_times = active_df.groupby('current_stage')['processing_days'].mean().sort_values()
                st.subheader("⏱️ Thời gian xử lý trung bình")
                st.bar_chart(stage_times, color='#6f42c1')
            
            with col2:
                longest_jobs = active_df.nlargest(10, 'processing_days')[['id', 'customer_name', 'processing_days', 'assigned_to']]
                st.subheader("⏳ Top 10 hồ sơ lâu nhất")
                st.dataframe(
                    longest_jobs.rename(columns={
                        'id': 'Mã HS',
                        'customer_name': 'Khách hàng',
                        'processing_days': 'Số ngày',
                        'assigned_to': 'Người phụ trách'
                    }),
                    use_container_width=True,
                    height=300
                )

def render_job_list():
    """Hiển thị danh sách hồ sơ"""
    st.markdown('<div class="custom-header"><h2>📋 Quản lý hồ sơ</h2></div>', unsafe_allow_html=True)
    
    df = get_all_jobs_df()
    if df.empty:
        st.info("Chưa có hồ sơ nào trong hệ thống")
        return
    
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
                value=(datetime.now() - timedelta(days=30), datetime.now())
            )
        
        with col6:
            search_text = st.text_input("Tìm kiếm (tên, SĐT, địa chỉ)")
    
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
    
    st.info(f"📊 Hiển thị {len(filtered_df)}/{len(df)} hồ sơ")
    
    if st.session_state.get('role') in ['Quản lý', 'Trưởng nhóm']:
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📤 Xuất Excel", use_container_width=True):
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
            if st.button("🔄 Làm mới dữ liệu", use_container_width=True):
                get_all_jobs_df.clear()
                st.rerun()
    
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
    
    display_df['_status_badge'] = filtered_df.apply(render_status_badge_html, axis=1)
    
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
        
        with st.expander("ℹ️ Thông tin bổ sung (không bắt buộc)"):
            col5, col6 = st.columns(2)
            with col5:
                customer_email = st.text_input("Email khách hàng")
                customer_id = st.text_input("CMND/CCCD")
            with col6:
                notes = st.text_area("Ghi chú thêm", height=60)
        
        submitted = st.form_submit_button("🚀 Tạo hồ sơ", type="primary")
        
        if submitted:
            if not customer_name or not customer_phone or not customer_address or not assigned_to:
                st.error("Vui lòng điền đầy đủ các trường bắt buộc (*)")
                return
            
            if not re.match(r'^[0-9+\-\s]{10,15}$', customer_phone):
                st.warning("Số điện thoại có thể không hợp lệ")
            
            # TODO: Implement create job logic
            st.success("Chức năng tạo hồ sơ đang được phát triển...")

def render_calendar():
    """Giao diện lịch biểu"""
    st.markdown('<div class="custom-header"><h2>📅 Lịch biểu công việc</h2></div>', unsafe_allow_html=True)
    
    df = get_all_jobs_df()
    if df.empty:
        st.info("Chưa có dữ liệu")
        return
    
    view_mode = st.selectbox("Chế độ xem", ["Tháng", "Tuần", "Ngày"])
    
    now = datetime.now()
    
    if view_mode == "Tháng":
        col1, col2 = st.columns(2)
        with col1:
            selected_month = st.selectbox("Tháng", range(1, 13), index=now.month - 1, label_visibility="collapsed")
        with col2:
            selected_year = st.selectbox("Năm", range(2020, 2031), index=now.year - 2020, label_visibility="collapsed")
        
        render_monthly_calendar(selected_year, selected_month, df)
    
    elif view_mode == "Tuần":
        st.info("Chế độ xem tuần đang phát triển")
    
    else:
        selected_date = st.date_input("Chọn ngày", now.date(), label_visibility="collapsed")
        render_daily_view(selected_date, df)

def render_monthly_calendar(year, month, df):
    """Hiển thị lịch tháng"""
    cal = calendar.monthcalendar(year, month)
    
    df_month = df[
        (df['start_dt'].dt.year == year) & 
        (df['start_dt'].dt.month == month)
    ].copy()
    
    days = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"]
    cols = st.columns(7)
    for i, day in enumerate(days):
        cols[i].markdown(f"**{day}**", unsafe_allow_html=True)
    
    for week in cal:
        cols = st.columns(7)
        for i, day in enumerate(week):
            with cols[i]:
                if day != 0:
                    current_date = date(year, month, day)
                    
                    is_today = current_date == datetime.now().date()
                    day_style = "background-color: #007bff; color: white; border-radius: 50%; padding: 5px; text-align: center;" if is_today else ""
                    st.markdown(f"<div style='{day_style} text-align: center; font-weight: bold;'>{day}</div>", unsafe_allow_html=True)
                    
                    day_jobs_start = df_month[df_month['start_dt'].dt.date == current_date]
                    day_jobs_deadline = df_month[df_month['deadline_dt'].dt.date == current_date]
                    
                    if not day_jobs_start.empty:
                        with st.expander(f"📌 Nhận ({len(day_jobs_start)})", expanded=False):
                            for _, job in day_jobs_start.iterrows():
                                st.caption(f"#{job['id']} - {job['customer_name'][:15]}...")
                    
                    if not day_jobs_deadline.empty:
                        urgent_jobs = day_jobs_deadline[day_jobs_deadline['is_overdue']]
                        if not urgent_jobs.empty:
                            st.error(f"⚠️ {len(urgent_jobs)} quá hạn")
                        else:
                            st.info(f"📅 {len(day_jobs_deadline)} đến hạn")

def render_daily_view(selected_date, df):
    """Hiển thị view ngày"""
    day_jobs = df[
        (df['start_dt'].dt.date == selected_date) | 
        (df['deadline_dt'].dt.date == selected_date)
    ]
    
    if not day_jobs.empty:
        st.subheader(f"Công việc ngày {selected_date.strftime('%d/%m/%Y')}")
        
        starts = day_jobs[day_jobs['start_dt'].dt.date == selected_date]
        deadlines = day_jobs[day_jobs['deadline_dt'].dt.date == selected_date]
        
        if not starts.empty:
            st.write("#### 📌 Hồ sơ nhận mới:")
            for _, job in starts.iterrows():
                with st.container(border=True):
                    st.write(f"**{job['customer_name']}** - #{job['id']}")
                    st.caption(f"Thủ tục: {extract_proc_from_log(job['logs'])}")
                    st.caption(f"Người phụ trách: {job['assigned_to']}")
        
        if not deadlines.empty:
            st.write("#### 📅 Hồ sơ đến hạn:")
            for _, job in deadlines.iterrows():
                with st.container(border=True):
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.write(f"**{job['customer_name']}** - #{job['id']}")
                        st.caption(f"Giai đoạn: {job['current_stage']}")
                    with col2:
                        if job['is_overdue']:
                            st.error("🔴 QUÁ HẠN")
                        else:
                            st.info("🟢 Đúng hạn")
    else:
        st.info(f"Không có công việc nào cho ngày {selected_date.strftime('%d/%m/%Y')}")

def render_financial_dashboard():
    """Dashboard tài chính"""
    st.markdown('<div class="custom-header"><h2>💰 Quản lý tài chính</h2></div>', unsafe_allow_html=True)
    
    df = get_all_jobs_df()
    if df.empty:
        st.info("Chưa có dữ liệu")
        return
    
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
    
    st.subheader("📋 Chi tiết công nợ")
    
    debt_df = df[df['is_paid'] == 0].copy()
    
    if not debt_df.empty:
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
            st.subheader("📊 Công nợ theo nhân viên")
            st.bar_chart(debt_by_user['survey_fee'], color='#dc3545')
        
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
        st.subheader("📈 Tỷ trọng doanh thu")
        st.bar_chart(revenue_by_proc['survey_fee'], color='#28a745')

# ==================== MAIN APPLICATION ====================
def main():
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    
    if 'selected_menu' not in st.session_state:
        st.session_state.selected_menu = "Tổng quan"
    
    render_custom_css()
    
    if not st.session_state.logged_in:
        render_login_page()
    else:
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
                    
                    # TODO: Implement registration logic
                    st.info("Chức năng đăng ký đang được phát triển...")
        
        st.markdown("""
        <div style='text-align: center; margin-top: 3rem; color: #6c757d; font-size: 0.9rem;'>
            <hr>
            <p>© 2024 Hệ thống Quản lý Đo đạc. Phiên bản 4.0</p>
            <p>Liên hệ hỗ trợ: support@dodac.com | Hotline: 1900 1234</p>
        </div>
        """, unsafe_allow_html=True)

def render_main_app():
    """Ứng dụng chính sau khi đăng nhập"""
    render_sidebar_menu(st.session_state.get('role', 'Nhân viên'))
    
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
        render_dashboard()
    
    elif selected_menu == 'Tài chính':
        render_financial_dashboard()
    
    elif selected_menu == 'Lưu trữ':
        st.info("Chức năng đang phát triển")
    
    elif selected_menu == 'Nhân sự':
        if st.session_state.get('role') != 'Quản lý':
            st.error("⛔ Bạn không có quyền truy cập trang này")
        else:
            st.info("Chức năng quản lý nhân sự đang phát triển")
    
    elif selected_menu == 'Cài đặt':
        if st.session_state.get('role') != 'Quản lý':
            st.error("⛔ Bạn không có quyền truy cập trang này")
        else:
            st.info("Chức năng cài đặt đang phát triển")
    
    elif selected_menu == 'Nhật ký':
        if st.session_state.get('role') != 'Quản lý':
            st.error("⛔ Bạn không có quyền truy cập trang này")
        else:
            logs_df = get_audit_logs_df()
            if not logs_df.empty:
                st.dataframe(logs_df, use_container_width=True)
            else:
                st.info("Chưa có nhật ký nào")
    
    elif selected_menu == 'Thùng rác':
        if st.session_state.get('role') != 'Quản lý':
            st.error("⛔ Bạn không có quyền truy cập trang này")
        else:
            df = get_all_jobs_df()
            deleted_jobs = df[df['status'] == 'Đã xóa']
            
            if not deleted_jobs.empty:
                st.dataframe(deleted_jobs, use_container_width=True)
            else:
                st.success("Thùng rác trống")

# ==================== RUN APPLICATION ====================
if __name__ == "__main__":
    main()
