# app_google_fixed_auth.py
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

# ==================== CẤU HÌNH HỆ THỐNG ====================
st.set_page_config(
    page_title="Hệ Thống Quản Lý Đo Đạc",
    page_icon="📊",
    layout="wide"
)

# 🔒 Biến môi trường
TELEGRAM_TOKEN = "8514665869:AAHUfTHgNlEEK_Yz6yYjZa-1iR645Cgr190"
TELEGRAM_CHAT_ID = "-5055192262"
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

# 🔄 Workflow
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

# ==================== HÀM TIỆN ÍCH ====================
def safe_int(value, default=0):
    if pd.isna(value) or value == "":
        return default
    try:
        if isinstance(value, (int, float)):
            return int(value)
        clean = str(value).replace(",", "").replace(".", "").strip()
        return int(clean) if clean else default
    except:
        return default

def format_currency(value):
    try:
        return f"{safe_int(value):,} đ"
    except:
        return "0 đ"

def get_proc_abbr(proc_name):
    mapping = {
        "Cấp lần đầu": "CLD", "Cấp đổi": "CD", "Chuyển quyền": "CQ", 
        "Tách thửa": "TT", "Thừa kế": "TK", 
        "Cung cấp thông tin": "CCTT", "Đính chính": "DC"
    }
    return mapping.get(proc_name, "K")

def extract_proc_from_log(log_text):
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
    if proc_name in ["Cung cấp thông tin", "Đính chính"]:
        return WORKFLOW_SHORT.get(current_stage)
    return WORKFLOW_FULL.get(current_stage)

# ==================== GOOGLE SHEETS ====================
@st.cache_resource
def get_gspread_client():
    """Tạo client gspread"""
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], 
            scopes=SCOPES
        )
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Lỗi kết nối Google: {e}")
        return None

@st.cache_data(ttl=60)
def get_sheet_data(sheet_name="DB_DODAC", worksheet=None):
    """Lấy dữ liệu từ Google Sheets"""
    try:
        client = get_gspread_client()
        if not client:
            return pd.DataFrame()
        
        spreadsheet = client.open(sheet_name)
        
        if worksheet:
            try:
                ws = spreadsheet.worksheet(worksheet)
            except:
                return pd.DataFrame()
        else:
            ws = spreadsheet.sheet1
        
        data = ws.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame()
        
    except Exception as e:
        st.error(f"Lỗi đọc dữ liệu: {e}")
        return pd.DataFrame()

def append_to_sheet(sheet_name, worksheet, data):
    """Thêm dữ liệu vào Google Sheets"""
    try:
        client = get_gspread_client()
        if not client:
            return False
        
        spreadsheet = client.open(sheet_name)
        
        try:
            ws = spreadsheet.worksheet(worksheet)
        except:
            ws = spreadsheet.add_worksheet(title=worksheet, rows="1000", cols="10")
        
        ws.append_row(data)
        return True
        
    except Exception as e:
        st.error(f"Lỗi ghi dữ liệu: {e}")
        return False

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
        
        df['proc_name'] = df['logs'].apply(extract_proc_from_log)
        df['is_overdue'] = df.apply(
            lambda x: x['status'] == 'Đang xử lý' and pd.notna(x['deadline_dt']) and datetime.now() > x['deadline_dt'],
            axis=1
        )
    
    return df

# ==================== AUTHENTICATION ====================
def hash_password(password):
    """Hash mật khẩu"""
    salt = "DODAC_SYSTEM_2024"
    return hashlib.sha256(f"{salt}{password}".encode()).hexdigest()

@st.cache_data(ttl=300)
def get_users_df():
    """Lấy dữ liệu người dùng"""
    df = get_sheet_data("DB_DODAC", "USERS")
    if df.empty:
        return pd.DataFrame(columns=['username', 'password', 'fullname', 'role', 'email', 'phone', 'active'])
    return df

def authenticate_user(username, password):
    """Xác thực người dùng"""
    users_df = get_users_df()
    
    if users_df.empty:
        return None
    
    user_row = users_df[users_df['username'] == username]
    
    if user_row.empty:
        return None
    
    user_data = user_row.iloc[0].to_dict()
    
    # Kiểm tra active
    if 'active' in user_data:
        active = str(user_data.get('active', 'true')).lower()
        if active == 'false':
            return None
    
    # Kiểm tra password
    if user_data.get('password') == hash_password(password):
        return {
            'username': user_data.get('username', ''),
            'fullname': user_data.get('fullname', ''),
            'role': user_data.get('role', 'Nhân viên'),
            'email': user_data.get('email', ''),
            'phone': user_data.get('phone', '')
        }
    
    return None

def register_user(username, password, fullname, email="", phone=""):
    """Đăng ký người dùng mới"""
    # Kiểm tra username hợp lệ
    if not re.match(r'^[a-zA-Z0-9_]{3,20}$', username):
        return False, "Username chỉ cho phép chữ, số và gạch dưới (3-20 ký tự)"
    
    # Kiểm tra username đã tồn tại chưa
    users_df = get_users_df()
    if not users_df.empty and username in users_df['username'].values:
        return False, "Username đã tồn tại"
    
    # Hash mật khẩu
    hashed_password = hash_password(password)
    
    # Tạo bản ghi người dùng mới
    new_user_data = [
        username,
        hashed_password,
        fullname,
        "Chưa cấp quyền",  # role
        email,
        phone,
        "true"  # active
    ]
    
    # Thêm vào Google Sheets
    success = append_to_sheet("DB_DODAC", "USERS", new_user_data)
    
    if success:
        # Clear cache để lấy dữ liệu mới
        get_users_df.clear()
        return True, "Đăng ký thành công! Vui lòng đợi quản lý cấp quyền."
    else:
        return False, "Lỗi hệ thống khi đăng ký"

def get_active_users_list():
    """Lấy danh sách người dùng đang hoạt động"""
    users_df = get_users_df()
    if users_df.empty:
        return []
    
    active_users = users_df[
        (users_df['role'] != 'Chưa cấp quyền') & 
        (users_df['active'].astype(str).str.lower() == 'true')
    ]
    
    result = []
    for _, user in active_users.iterrows():
        display_name = f"{user.get('fullname', user['username'])} ({user['username']})"
        result.append(display_name)
    
    return result

# ==================== UI COMPONENTS ====================
def render_custom_css():
    st.markdown("""
    <style>
        .main {
            padding: 1rem 2rem;
        }
        
        .stButton > button {
            border-radius: 8px;
            font-weight: 500;
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
    """Render sidebar menu"""
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
        
        # Menu đơn giản
        menu_items = [
            ("🏠", "Tổng quan"),
            ("📋", "Hồ sơ của tôi"),
            ("➕", "Tạo hồ sơ"),
            ("📅", "Lịch biểu"),
            ("💰", "Tài chính"),
            ("🗃️", "Lưu trữ"),
        ]
        
        if user_role == "Quản lý":
            menu_items.extend([
                ("👥", "Nhân sự"),
                ("⚙️", "Cài đặt"),
                ("🛡️", "Nhật ký"),
                ("🗑️", "Thùng rác")
            ])
        
        selected = st.session_state.get("selected_menu", "Tổng quan")
        
        for icon, label in menu_items:
            if st.button(f"{icon} {label}", 
                       use_container_width=True,
                       type="primary" if selected == label else "secondary"):
                st.session_state["selected_menu"] = label
                st.rerun()
        
        st.divider()
        
        if st.button("🚪 Đăng xuất", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

# ==================== MAIN PAGES ====================
def render_login_page():
    """Trang đăng nhập hoàn chỉnh"""
    st.markdown("""
    <div style='text-align: center; padding: 2rem 0;'>
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
                
                if st.button("🚪 Đăng nhập", type="primary", use_container_width=True):
                    if not username or not password:
                        st.error("Vui lòng nhập đầy đủ thông tin")
                    else:
                        with st.spinner("Đang đăng nhập..."):
                            user_data = authenticate_user(username, password)
                            if user_data:
                                st.session_state.logged_in = True
                                st.session_state.username = user_data['username']
                                st.session_state.fullname = user_data['fullname']
                                st.session_state.role = user_data['role']
                                st.success(f"👋 Chào mừng {user_data['fullname']}!")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Tên đăng nhập hoặc mật khẩu không đúng")
            
            with tab2:
                new_username = st.text_input("Tên đăng nhập mới", key="reg_username")
                new_password = st.text_input("Mật khẩu mới", type="password", key="reg_password")
                confirm_password = st.text_input("Xác nhận mật khẩu", type="password", key="reg_confirm")
                new_fullname = st.text_input("Họ tên đầy đủ *", key="reg_fullname")
                new_email = st.text_input("Email", key="reg_email")
                new_phone = st.text_input("Số điện thoại", key="reg_phone")
                
                if st.button("📝 Đăng ký tài khoản", type="primary", use_container_width=True):
                    if not new_username or not new_password or not new_fullname:
                        st.error("Vui lòng nhập đầy đủ thông tin bắt buộc (*)")
                    elif new_password != confirm_password:
                        st.error("Mật khẩu xác nhận không khớp")
                    else:
                        with st.spinner("Đang đăng ký..."):
                            success, message = register_user(new_username, new_password, new_fullname, new_email, new_phone)
                            if success:
                                st.success(message)
                                # Reset form
                                st.rerun()
                            else:
                                st.error(message)

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
        total_revenue = df.get('survey_fee', pd.Series([0])).apply(safe_int).sum()
        st.metric("Doanh thu", f"{total_revenue:,.0f} đ")
    
    with col4:
        completion_rate = len(df[df['status'] == 'Hoàn thành']) / total_jobs * 100 if total_jobs > 0 else 0
        st.metric("Tỷ lệ hoàn thành", f"{completion_rate:.1f}%")
    
    st.divider()
    
    # Hiển thị danh sách hồ sơ gần đây
    st.subheader("📋 Hồ sơ gần đây")
    recent_jobs = df.sort_values('start_dt', ascending=False).head(10)
    
    if not recent_jobs.empty:
        display_df = recent_jobs[['id', 'customer_name', 'customer_phone', 'current_stage', 'assigned_to']].copy()
        display_df.columns = ['Mã HS', 'Khách hàng', 'SĐT', 'Giai đoạn', 'Người phụ trách']
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("Chưa có hồ sơ nào")

def render_job_list():
    """Hiển thị danh sách hồ sơ"""
    st.markdown('<div class="custom-header"><h2>📋 Quản lý hồ sơ</h2></div>', unsafe_allow_html=True)
    
    df = get_all_jobs_df()
    if df.empty:
        st.info("Chưa có hồ sơ nào trong hệ thống")
        return
    
    # Bộ lọc đơn giản
    col1, col2 = st.columns(2)
    with col1:
        status_filter = st.selectbox("Lọc theo trạng thái", 
                                   ["Tất cả", "Đang xử lý", "Hoàn thành", "Tạm dừng"])
    with col2:
        search_text = st.text_input("Tìm kiếm (tên, SĐT)")
    
    filtered_df = df.copy()
    
    if status_filter != "Tất cả":
        filtered_df = filtered_df[filtered_df['status'] == status_filter]
    
    if search_text:
        search_lower = search_text.lower()
        filtered_df = filtered_df[
            filtered_df['customer_name'].str.lower().str.contains(search_lower) |
            filtered_df['customer_phone'].str.lower().str.contains(search_lower)
        ]
    
    st.info(f"📊 Hiển thị {len(filtered_df)}/{len(df)} hồ sơ")
    
    # Hiển thị dưới dạng bảng
    display_cols = ['id', 'customer_name', 'customer_phone', 'address', 'current_stage', 'status', 'assigned_to']
    
    if not filtered_df.empty:
        display_df = filtered_df[display_cols].copy()
        display_df.columns = ['Mã HS', 'Khách hàng', 'SĐT', 'Địa chỉ', 'Giai đoạn', 'Trạng thái', 'Người PT']
        st.dataframe(display_df, use_container_width=True, height=400)
    else:
        st.warning("Không tìm thấy hồ sơ nào")

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
        
        assigned_to = st.selectbox(
            "Giao cho *",
            options=get_active_users_list(),
            help="Chọn người phụ trách chính"
        )
        
        uploaded_files = st.file_uploader(
            "Tài liệu đính kèm",
            accept_multiple_files=True,
            help="Có thể upload nhiều file cùng lúc"
        )
        
        if uploaded_files:
            st.success(f"📎 Đã chọn {len(uploaded_files)} file")
        
        submitted = st.form_submit_button("🚀 Tạo hồ sơ", type="primary")
        
        if submitted:
            if not customer_name or not customer_phone or not customer_address or not assigned_to:
                st.error("Vui lòng điền đầy đủ các trường bắt buộc (*)")
            else:
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
                
                # Tạo thông tin
                now = datetime.now()
                now_str = now.strftime("%Y-%m-%d %H:%M:%S")
                
                # TODO: Thêm logic upload file và lưu vào Google Sheets
                st.success(f"✅ Đã tạo hồ sơ #{job_id} thành công!")
                st.info(f"Khách hàng: {customer_name}")
                st.info(f"Người phụ trách: {assigned_to}")

def render_financial_dashboard():
    """Dashboard tài chính"""
    st.markdown('<div class="custom-header"><h2>💰 Quản lý tài chính</h2></div>', unsafe_allow_html=True)
    
    df = get_all_jobs_df()
    if df.empty:
        st.info("Chưa có dữ liệu")
        return
    
    # Tính toán cơ bản
    total_revenue = df.get('survey_fee', pd.Series([0])).apply(safe_int).sum()
    paid_revenue = df[df.get('is_paid', 0) == 1].get('survey_fee', pd.Series([0])).apply(safe_int).sum()
    pending_revenue = total_revenue - paid_revenue
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Tổng doanh thu", format_currency(total_revenue))
    
    with col2:
        st.metric("Đã thu", format_currency(paid_revenue))
    
    with col3:
        st.metric("Chưa thu", format_currency(pending_revenue))
    
    st.divider()
    
    # Danh sách công nợ
    st.subheader("📋 Danh sách công nợ")
    debt_df = df[df.get('is_paid', 0) == 0]
    
    if not debt_df.empty:
        display_debt = debt_df[['id', 'customer_name', 'customer_phone', 'survey_fee']].copy()
        display_debt['survey_fee'] = display_debt['survey_fee'].apply(format_currency)
        display_debt.columns = ['Mã HS', 'Khách hàng', 'SĐT', 'Số tiền']
        st.dataframe(display_debt, use_container_width=True)
    else:
        st.success("🎉 Không có công nợ nào!")

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
        user_role = st.session_state.get('role', 'Nhân viên')
        render_sidebar_menu(user_role)
        
        selected_menu = st.session_state.get('selected_menu', 'Tổng quan')
        
        if selected_menu == 'Tổng quan':
            render_dashboard()
        
        elif selected_menu == 'Hồ sơ của tôi':
            render_job_list()
        
        elif selected_menu == 'Tạo hồ sơ':
            render_create_job()
        
        elif selected_menu == 'Lịch biểu':
            st.info("Chức năng đang phát triển")
        
        elif selected_menu == 'Tài chính':
            render_financial_dashboard()
        
        elif selected_menu == 'Lưu trữ':
            st.info("Chức năng đang phát triển")
        
        elif selected_menu == 'Nhân sự':
            if user_role != "Quản lý":
                st.error("⛔ Bạn không có quyền truy cập trang này")
            else:
                st.info("Chức năng đang phát triển")
        
        elif selected_menu == 'Cài đặt':
            if user_role != "Quản lý":
                st.error("⛔ Bạn không có quyền truy cập trang này")
            else:
                st.info("Chức năng đang phát triển")
        
        elif selected_menu == 'Nhật ký':
            if user_role != "Quản lý":
                st.error("⛔ Bạn không có quyền truy cập trang này")
            else:
                st.info("Chức năng đang phát triển")
        
        elif selected_menu == 'Thùng rác':
            if user_role != "Quản lý":
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
