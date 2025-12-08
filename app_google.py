import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
import time
import requests
import threading
import hashlib
import re
import gspread
import base64
import io
import calendar
from google.oauth2.service_account import Credentials

# --- 1. CẤU HÌNH & CONSTANTS ---
st.set_page_config(
    page_title="Đo Đạc Cloud V4-Ultimate", 
    page_icon="📡", 
    layout="wide",
    initial_sidebar_state="expanded"
)

TELEGRAM_TOKEN = "8514665869:AAHUfTHgNlEEK_Yz6yYjZa-1iR645Cgr190"
TELEGRAM_CHAT_ID = "-5055192262"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbyEMEGyS_sVCA4eyVRFXxnOuGqMnJOKOIqZqKxi4HpYBcpr7U72WUXCoKLm20BQomVC/exec"
DRIVE_FOLDER_ID = "1SrARuA1rgKLZmoObGor-GkNx33F6zNQy"

ROLES = ["Quản lý", "Nhân viên", "Chưa cấp quyền"]
STAGES_ORDER = ["1. Tạo mới", "2. Đo đạc", "3. Hoàn thiện trích đo", "4. Làm hồ sơ", "5. Ký hồ sơ", "6. Lấy hồ sơ", "7. Nộp hồ sơ", "8. Hoàn thành"]
PROCEDURES_LIST = ["Cấp lần đầu", "Cấp đổi", "Chuyển quyền", "Tách thửa", "Thừa kế", "Cung cấp thông tin", "Đính chính"]
WORKFLOW_FULL = {
    "1. Tạo mới": "2. Đo đạc", "2. Đo đạc": "3. Hoàn thiện trích đo", "3. Hoàn thiện trích đo": "4. Làm hồ sơ",
    "4. Làm hồ sơ": "5. Ký hồ sơ", "5. Ký hồ sơ": "6. Lấy hồ sơ", "6. Lấy hồ sơ": "7. Nộp hồ sơ", 
    "7. Nộp hồ sơ": "8. Hoàn thành", "8. Hoàn thành": None
}
# SLA (Giờ)
STAGE_SLA_HOURS = {"1. Tạo mới": 0, "2. Đo đạc": 24, "3. Hoàn thiện trích đo": 24, "4. Làm hồ sơ": 24, "5. Ký hồ sơ": 72, "6. Lấy hồ sơ": 24, "7. Nộp hồ sơ": 360}

# --- 2. CSS & UI STYLING ---
def inject_custom_css():
    st.markdown("""
    <style>
        .block-container { padding-top: 2rem; }
        .stButton>button { border-radius: 8px; font-weight: 500; }
        .job-card { border: 1px solid #e0e0e0; border-radius: 10px; padding: 15px; margin-bottom: 15px; background-color: white; box-shadow: 0 2px 5px rgba(0,0,0,0.05); }
        .job-header { font-weight: bold; color: #1e3a8a; font-size: 1.1rem; }
        .badge { padding: 4px 8px; border-radius: 12px; font-size: 0.75rem; font-weight: bold; }
        .badge-danger { background-color: #fee2e2; color: #991b1b; }
        .badge-warning { background-color: #fef3c7; color: #92400e; }
        .badge-success { background-color: #d1fae5; color: #065f46; }
        .metric-card { background-color: #f8fafc; padding: 15px; border-radius: 8px; border-left: 4px solid #3b82f6; }
    </style>
    """, unsafe_allow_html=True)

# --- 3. DATABASE & CACHING LAYER ---
@st.cache_resource
def get_gspread_client():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    return gspread.authorize(creds)

def get_worksheet(name):
    try:
        client = get_gspread_client()
        return client.open("DB_DODAC").worksheet(name)
    except Exception as e:
        st.error(f"Lỗi kết nối DB [{name}]: {e}")
        return None

# --- CACHED DATA FETCHING ---
@st.cache_data(ttl=300)
def fetch_all_data():
    """Lấy toàn bộ dữ liệu Jobs, Users, Comments một lần để tối ưu hiệu suất"""
    try:
        client = get_gspread_client()
        sh = client.open("DB_DODAC")
        
        # Jobs
        ws_jobs = sh.sheet1
        df_jobs = pd.DataFrame(ws_jobs.get_all_records())
        
        # Users
        try: ws_users = sh.worksheet("USERS")
        except: ws_users = sh.add_worksheet("USERS", 100, 5)
        df_users = pd.DataFrame(ws_users.get_all_records())
        
        # Comments
        try: ws_comments = sh.worksheet("COMMENTS")
        except: ws_comments = sh.add_worksheet("COMMENTS", 1000, 5) # Create if not exists
        df_comments = pd.DataFrame(ws_comments.get_all_records())
        
        # Pre-process Jobs
        if not df_jobs.empty:
            df_jobs['id'] = df_jobs['id'].astype(str)
            df_jobs['start_dt'] = pd.to_datetime(df_jobs['start_time'], errors='coerce')
            df_jobs['deadline_dt'] = pd.to_datetime(df_jobs['deadline'], errors='coerce')
            # Safe int conversions
            for col in ['survey_fee', 'deposit', 'is_paid']:
                 if col in df_jobs.columns: 
                     df_jobs[col] = pd.to_numeric(df_jobs[col].astype(str).str.replace(r'[,.]', '', regex=True), errors='coerce').fillna(0).astype(int)

        return df_jobs, df_users, df_comments
    except Exception as e:
        st.error(f"Lỗi tải dữ liệu: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def clear_cache():
    fetch_all_data.clear()

# --- 4. BUSINESS LOGIC & HELPERS ---
def safe_str(val): return str(val) if pd.notna(val) else ""
def make_hash(p): return hashlib.sha256(str.encode(p)).hexdigest()

def send_telegram_msg(msg):
    if not TELEGRAM_TOKEN: return
    def _run(): 
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"})
    threading.Thread(target=_run).start()

def log_audit(user, action, details):
    def _log():
        try: get_worksheet("AUDIT_LOGS").append_row([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user, action, details])
        except: pass
    threading.Thread(target=_log).start()

# --- UPLOAD ---
def upload_file_script(file_obj, sub_folder):
    if not file_obj: return None, None
    try:
        content = file_obj.read()
        b64 = base64.b64encode(content).decode('utf-8')
        res = requests.post(APPS_SCRIPT_URL, json={
            "filename": file_obj.name, "mime_type": file_obj.type, 
            "file_base64": b64, "folder_id": DRIVE_FOLDER_ID, "sub_folder_name": sub_folder
        }).json()
        if res.get("status") == "success": return res.get("link"), file_obj.name
    except Exception as e: st.error(f"Upload lỗi: {e}")
    return None, None

# --- DATABASE WRITES (Actions) ---
def action_add_job(n, p, a, proc, files, u, asn):
    ws = get_worksheet("Sheet1")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = datetime.now().strftime('%y%m%d')
    
    # Generate ID logic
    ids = ws.col_values(1)
    today_ids = [i for i in ids if i.startswith(prefix)]
    seq = int(today_ids[-1][-2:]) + 1 if today_ids else 1
    jid = f"{prefix}{seq:02}"
    
    # File handling
    log_files, link_main = "", ""
    unique_name = f"{jid}-{proc} {n}"
    if files:
        for f in files:
            l, fn = upload_file_script(f, unique_name)
            if l: log_files += f" | File: {fn} - {l}"; link_main = l

    dl = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
    log = f"[{now_str}] {u}: Khởi tạo ({proc}) -> {asn.split(' - ')[0]}{log_files}"
    
    ws.append_row([jid, now_str, n, f"'{p}", a, "1. Tạo mới", "Đang xử lý", asn.split(' - ')[0], dl, link_main, log, 0, 0, 0, 0])
    clear_cache()
    send_telegram_msg(f"🆕 <b>MỚI #{jid}</b>\n👤 {n}\n📌 {proc}\n👉 {asn}")
    st.toast("✅ Đã tạo hồ sơ mới!", icon="🎉")

def action_update_stage(jid, current_stg, note, files, u, asn, fee, is_paid, is_finish=False):
    ws = get_worksheet("Sheet1")
    cell = ws.find(str(jid))
    if not cell: return
    r = cell.row
    
    next_stg = "8. Hoàn thành" if is_finish else (WORKFLOW_FULL.get(current_stg) or "8. Hoàn thành")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Files
    log_files = ""
    if files:
        for f in files:
            l, fn = upload_file_script(f, f"{jid}-update")
            if l: log_files += f" | File: {fn} - {l}"

    # Updates
    ws.update_cell(r, 6, next_stg) # Status
    if is_finish: ws.update_cell(r, 7, "Hoàn thành")
    if asn: ws.update_cell(r, 8, asn.split(' - ')[0])
    
    # Finance
    ws.update_cell(r, 14, fee)
    ws.update_cell(r, 15, 1 if is_paid else 0)
    
    # Deadline logic
    if next_stg != "8. Hoàn thành":
        hours = STAGE_SLA_HOURS.get(next_stg, 24)
        new_dl = (datetime.now() + timedelta(hours=hours) + timedelta(days=1 if datetime.now().weekday()>4 else 0)).strftime("%Y-%m-%d %H:%M:%S")
        ws.update_cell(r, 9, new_dl)

    # Log
    old_log = ws.cell(r, 11).value
    new_log = f"\n[{now_str}] {u}: {current_stg} -> {next_stg} | Note: {note}{log_files}"
    ws.update_cell(r, 11, old_log + new_log)
    
    clear_cache()
    send_telegram_msg(f"⚡ <b>UPDATE #{jid}</b>\n{current_stg} ➡ <b>{next_stg}</b>\n👤 {u}")
    st.toast("✅ Cập nhật thành công!", icon="💾")

def action_add_comment(jid, user, content):
    ws = get_worksheet("COMMENTS")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Simple ID generation for comment
    cid = int(datetime.now().timestamp())
    ws.append_row([cid, jid, user, content, ts])
    clear_cache()
    st.toast("Đã gửi bình luận!", icon="💬")

# --- 5. UI COMPONENTS ---

def render_sidebar(user, role, active_count):
    with st.sidebar:
        st.image("https://cdn-icons-png.flaticon.com/512/2921/2921226.png", width=50)
        st.markdown(f"### Xin chào, **{user}** 👋")
        st.caption(f"Vai trò: {role}")
        
        if active_count > 0:
            st.warning(f"🔔 Bạn có {active_count} việc cần làm")

        st.markdown("---")
        menu = st.radio("Điều hướng", 
            ["📊 Dashboard", "🏠 Việc của tôi", "📝 Tạo hồ sơ", "🗄️ Lưu trữ", "📅 Lịch biểu", "⚙️ Cài đặt"],
            index=1
        )
        
        st.markdown("---")
        if st.button("🔄 Làm mới dữ liệu", use_container_width=True):
            clear_cache()
            st.rerun()
            
        if st.button("🚪 Đăng xuất", use_container_width=True):
            st.session_state.logged_in = False
            st.rerun()
            
        return menu

def render_dashboard(df_jobs):
    st.title("📊 Dashboard Tổng Quan")
    
    if df_jobs.empty: st.info("Chưa có dữ liệu."); return

    # Metrics
    c1, c2, c3, c4 = st.columns(4)
    active_jobs = df_jobs[~df_jobs['status'].isin(['Hoàn thành', 'Đã xóa', 'Kết thúc sớm'])]
    revenue = df_jobs['survey_fee'].sum()
    debt = df_jobs[(df_jobs['is_paid'] == 0) & (df_jobs['survey_fee'] > 0) & (df_jobs['status'] != 'Đã xóa')]['survey_fee'].sum()
    
    with c1: st.markdown(f"<div class='metric-card'><h3>📝 {len(df_jobs)}</h3><p>Tổng hồ sơ</p></div>", unsafe_allow_html=True)
    with c2: st.markdown(f"<div class='metric-card'><h3>🔥 {len(active_jobs)}</h3><p>Đang xử lý</p></div>", unsafe_allow_html=True)
    with c3: st.markdown(f"<div class='metric-card'><h3>💰 {revenue:,.0f}</h3><p>Doanh thu (VNĐ)</p></div>", unsafe_allow_html=True)
    with c4: st.markdown(f"<div class='metric-card'><h3>⚠️ {debt:,.0f}</h3><p>Công nợ (VNĐ)</p></div>", unsafe_allow_html=True)
    
    st.markdown("### 📈 Biểu đồ & Thống kê")
    t1, t2 = st.tabs(["Tiến độ", "Tài chính"])
    
    with t1:
        st.bar_chart(df_jobs['current_stage'].value_counts())
    
    with t2:
        # Chart doanh thu theo tháng
        df_chart = df_jobs.copy()
        df_chart['month'] = df_chart['start_dt'].dt.strftime('%Y-%m')
        rev_by_month = df_chart.groupby('month')['survey_fee'].sum()
        st.line_chart(rev_by_month)

    # Export Data
    st.markdown("### 📤 Xuất dữ liệu & Sao lưu")
    if st.button("📥 Tải xuống toàn bộ dữ liệu (Excel)"):
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df_jobs.to_excel(writer, sheet_name='JOBS', index=False)
        st.download_button(label="Click để tải", data=buffer, file_name="backup_data.xlsx", mime="application/vnd.ms-excel")

def render_job_detail(job, comments_df, user, user_list):
    # Job Detail View inside an Expander or specific area
    st.markdown(f"## 📂 Hồ sơ: {job['customer_name']}")
    
    col_info, col_action = st.columns([1, 1.5])
    
    with col_info:
        st.markdown(f"""
        - **Mã:** `{job['id']}`
        - **Khách:** {job['customer_name']} - {job['customer_phone']}
        - **Đia chỉ:** {job['address']}
        - **Người làm:** {job['assigned_to']}
        """)
        
        # Files display
        with st.expander("📎 Tệp tin đính kèm", expanded=True):
            files = re.findall(r"File: (.*?) - (https?://[^\s]+)", str(job['logs']))
            if files:
                for name, link in files:
                    st.markdown(f"📄 [{name}]({link})")
            else:
                st.caption("Không có tệp tin.")

    with col_action:
        tab_proc, tab_chat, tab_log = st.tabs(["⚙️ Xử lý", "💬 Thảo luận", "📜 Lịch sử"])
        
        with tab_proc:
            if job['status'] in ['Hoàn thành', 'Đã xóa']:
                st.info(f"Hồ sơ đã {job['status']}")
            else:
                with st.form(key=f"frm_{job['id']}"):
                    note = st.text_area("Ghi chú xử lý")
                    f_up = st.file_uploader("Thêm file", accept_multiple_files=True)
                    
                    c_1, c_2 = st.columns(2)
                    cur_stage = job['current_stage']
                    
                    # Logic Next Stage
                    idx = STAGES_ORDER.index(cur_stage) if cur_stage in STAGES_ORDER else 0
                    next_stage_guess = STAGES_ORDER[idx+1] if idx < len(STAGES_ORDER)-1 else "8. Hoàn thành"
                    
                    with c_1: st.info(f"Hiện tại: **{cur_stage}**")
                    with c_2: st.write(f"Tiếp theo: **{next_stage_guess}**")

                    asn = st.selectbox("Chuyển giao cho", user_list, index=0)
                    
                    st.divider()
                    st.caption("💰 Cập nhật tài chính")
                    fee = st.number_input("Phí dịch vụ", value=int(job['survey_fee']), step=100000)
                    paid = st.checkbox("Đã thanh toán", value=bool(job['is_paid']))
                    finish = st.checkbox("🏁 Đánh dấu Hoàn thành hồ sơ")

                    if st.form_submit_button("💾 Cập nhật", type="primary"):
                        action_update_stage(job['id'], cur_stage, note, f_up, user, asn, fee, paid, finish)
                        st.rerun()

        with tab_chat:
            # Show comments
            job_comments = comments_df[comments_df['job_id'].astype(str) == str(job['id'])]
            if not job_comments.empty:
                for _, c in job_comments.iterrows():
                    st.markdown(f"**{c['user']}** ({c['timestamp']}): {c['content']}")
                    st.divider()
            
            txt_comment = st.text_input("Viết bình luận...", key=f"cmt_{job['id']}")
            if st.button("Gửi", key=f"btn_cmt_{job['id']}"):
                if txt_comment:
                    action_add_comment(job['id'], user, txt_comment)
                    st.rerun()

        with tab_log:
            st.text_area("", job['logs'], height=200, disabled=True)

# --- 6. MAIN APP FLOW ---

def main():
    inject_custom_css()
    
    if 'logged_in' not in st.session_state: st.session_state.logged_in = False
    
    # --- LOGIN SCREEN ---
    if not st.session_state.logged_in:
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2:
            st.title("🔐 ĐĂNG NHẬP HỆ THỐNG")
            with st.form("login_form"):
                u = st.text_input("Tên đăng nhập")
                p = st.text_input("Mật khẩu", type="password")
                if st.form_submit_button("Đăng nhập", type="primary", use_container_width=True):
                    _, df_users, _ = fetch_all_data()
                    user_row = df_users[(df_users['username'] == u) & (df_users['password'] == make_hash(p))]
                    if not user_row.empty:
                        st.session_state.logged_in = True
                        st.session_state.user = u
                        st.session_state.role = user_row.iloc[0]['role']
                        st.toast(f"Xin chào {u}!", icon="👋")
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error("Sai thông tin đăng nhập!")
        return

    # --- MAIN APP ---
    user = st.session_state.user
    role = st.session_state.role
    
    # Load Data
    df_jobs, df_users, df_comments = fetch_all_data()
    user_list = [f"{r['username']} - {r['fullname']}" for i, r in df_users.iterrows()]
    
    # Check Deadlines (Auto Reminder Logic on Load)
    if 'checked_deadline' not in st.session_state:
        my_urgent = df_jobs[
            (df_jobs['assigned_to'].str.contains(user, na=False)) & 
            (df_jobs['deadline_dt'] < datetime.now() + timedelta(days=1)) &
            (df_jobs['status'] != 'Hoàn thành')
        ]
        if not my_urgent.empty:
            st.toast(f"⚠️ Bạn có {len(my_urgent)} hồ sơ sắp/quá hạn!", icon="🔥")
        st.session_state.checked_deadline = True
    
    # Tính số lượng việc cần làm để hiện badge sidebar
    my_active_count = len(df_jobs[
        (df_jobs['assigned_to'].str.contains(user, na=False)) & 
        (~df_jobs['status'].isin(['Hoàn thành', 'Đã xóa']))
    ]) if role != "Quản lý" else len(df_jobs[~df_jobs['status'].isin(['Hoàn thành', 'Đã xóa'])])

    menu = render_sidebar(user, role, my_active_count)

    if menu == "📊 Dashboard":
        render_dashboard(df_jobs)

    elif menu == "🏠 Việc của tôi":
        st.title("📋 Quản lý hồ sơ")
        
        # --- ADVANCED FILTER ---
        with st.expander("🔍 Bộ lọc & Tìm kiếm nâng cao", expanded=False):
            c_f1, c_f2, c_f3 = st.columns(3)
            with c_f1: search_txt = st.text_input("Từ khóa (Tên, Mã, SĐT)")
            with c_f2: filter_status = st.multiselect("Trạng thái", STAGES_ORDER + ["Hoàn thành"], default=[])
            with c_f3: 
                if role == "Quản lý": filter_user = st.multiselect("Người thực hiện", user_list)
                else: filter_user = []

        # Filter Logic
        filtered_df = df_jobs.copy()
        if role != "Quản lý": filtered_df = filtered_df[filtered_df['assigned_to'].str.contains(user, na=False)]
        
        if search_txt:
            filtered_df = filtered_df[
                filtered_df['customer_name'].str.contains(search_txt, case=False) | 
                filtered_df['customer_phone'].str.contains(search_txt) |
                filtered_df['id'].str.contains(search_txt)
            ]
        if filter_status:
            filtered_df = filtered_df[filtered_df['current_stage'].isin(filter_status)]
        if filter_user:
            # Simple regex join for multiselect
            pat = '|'.join([u.split(' - ')[0] for u in filter_user])
            filtered_df = filtered_df[filtered_df['assigned_to'].str.contains(pat, na=False)]

        # --- LIST VIEW ---
        st.caption(f"Tìm thấy {len(filtered_df)} hồ sơ")
        
        # Pagination simple
        items_per_page = 10
        if 'page' not in st.session_state: st.session_state.page = 0
        
        start = st.session_state.page * items_per_page
        end = start + items_per_page
        
        # Render List
        for i, row in filtered_df.iloc[start:end].iterrows():
            with st.container():
                # Card Styling
                deadline_str = row['deadline_dt'].strftime("%d/%m") if pd.notna(row['deadline_dt']) else "N/A"
                is_late = pd.notna(row['deadline_dt']) and datetime.now() > row['deadline_dt'] and row['status'] != 'Hoàn thành'
                
                status_color = "badge-success" if row['status'] == 'Hoàn thành' else ("badge-danger" if is_late else "badge-warning")
                
                col_c1, col_c2, col_c3 = st.columns([4, 2, 1])
                with col_c1:
                    st.markdown(f"**#{row['id']} - {row['customer_name']}**")
                    st.caption(f"📞 {row['customer_phone']} | 📍 {row['address']}")
                with col_c2:
                    st.markdown(f"<span class='badge {status_color}'>{row['current_stage']}</span>", unsafe_allow_html=True)
                    if is_late: st.markdown("<small style='color:red'>Quá hạn!</small>", unsafe_allow_html=True)
                    else: st.caption(f"Hạn: {deadline_str}")
                with col_c3:
                    if st.button("Chi tiết", key=f"view_{row['id']}"):
                        st.session_state.selected_job = row['id']

                st.markdown("---")

        # Pagination controls
        c_prev, c_next = st.columns(2)
        if c_prev.button("Previous") and st.session_state.page > 0: st.session_state.page -= 1; st.rerun()
        if c_next.button("Next") and end < len(filtered_df): st.session_state.page += 1; st.rerun()

        # --- MODAL / DETAIL VIEW ---
        # Streamlit doesn't have native modals yet, so we render at top or use session_state to switch view
        if 'selected_job' in st.session_state:
            job_data = df_jobs[df_jobs['id'] == st.session_state.selected_job]
            if not job_data.empty:
                st.markdown("---")
                render_job_detail(job_data.iloc[0], df_comments, user, user_list)
                if st.button("❌ Đóng chi tiết"):
                    del st.session_state.selected_job
                    st.rerun()

    elif menu == "📝 Tạo hồ sơ":
        st.title("Tạo hồ sơ mới")
        with st.form("add_job_form"):
            col1, col2 = st.columns(2)
            n = col1.text_input("Tên khách hàng")
            p = col2.text_input("Số điện thoại")
            a = st.text_input("Địa chỉ")
            proc = st.selectbox("Loại thủ tục", PROCEDURES_LIST)
            files = st.file_uploader("File đính kèm", accept_multiple_files=True)
            assign = st.selectbox("Giao việc cho", user_list)
            
            if st.form_submit_button("🚀 Khởi tạo", type="primary"):
                if n and assign:
                    action_add_job(n, p, a, proc, files, user, assign)
                    st.rerun()
                else:
                    st.error("Thiếu thông tin quan trọng!")

    elif menu == "📅 Lịch biểu":
        st.title("📅 Lịch làm việc")
        # Simple Calendar Grid
        cal = calendar.Calendar()
        now = datetime.now()
        year = now.year
        month = now.month
        
        days = cal.monthdayscalendar(year, month)
        st.subheader(f"Tháng {month}/{year}")
        
        cols = st.columns(7)
        headers = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"]
        for i, h in enumerate(headers): cols[i].write(f"**{h}**")
        
        for week in days:
            cols = st.columns(7)
            for i, day in enumerate(week):
                if day == 0: cols[i].write("")
                else:
                    d_obj = datetime(year, month, day).date()
                    # Find deadlines on this day
                    tasks = df_jobs[df_jobs['deadline_dt'].dt.date == d_obj]
                    
                    with cols[i]:
                        st.markdown(f"**{day}**")
                        if not tasks.empty:
                            for _, t in tasks.iterrows():
                                st.markdown(f"<small style='color:red'>◉ {t['customer_name']}</small>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
