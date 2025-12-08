import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta, date
import time
import requests
import threading
import hashlib
import re
import gspread
import base64
import calendar
from google.oauth2.service_account import Credentials

# --- 1. CẤU HÌNH HỆ THỐNG & CONSTANTS ---
st.set_page_config(page_title="Đo Đạc Cloud V3-Pro", page_icon="☁️", layout="wide")

# Config Secrets & API
TELEGRAM_TOKEN = "8514665869:AAHUfTHgNlEEK_Yz6yYjZa-1iR645Cgr190"
TELEGRAM_CHAT_ID = "-5055192262"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbyEMEGyS_sVCA4eyVRFXxnOuGqMnJOKOIqZqKxi4HpYBcpr7U72WUXCoKLm20BQomVC/exec"
DRIVE_FOLDER_ID = "1SrARuA1rgKLZmoObGor-GkNx33F6zNQy"

# Workflow Config
ROLES = ["Quản lý", "Nhân viên", "Chưa cấp quyền"]
STAGES_ORDER = ["1. Tạo mới", "2. Đo đạc", "3. Hoàn thiện trích đo", "4. Làm hồ sơ", "5. Ký hồ sơ", "6. Lấy hồ sơ", "7. Nộp hồ sơ", "8. Hoàn thành"]
PROCEDURES_LIST = ["Cấp lần đầu", "Cấp đổi", "Chuyển quyền", "Tách thửa", "Thừa kế", "Cung cấp thông tin", "Đính chính"]
STAGE_SLA_HOURS = {"1. Tạo mới": 0, "2. Đo đạc": 24, "3. Hoàn thiện trích đo": 24, "4. Làm hồ sơ": 24, "5. Ký hồ sơ": 72, "6. Lấy hồ sơ": 24, "7. Nộp hồ sơ": 360}

WORKFLOW_FULL = {
    "1. Tạo mới": "2. Đo đạc", "2. Đo đạc": "3. Hoàn thiện trích đo", 
    "3. Hoàn thiện trích đo": "4. Làm hồ sơ", "4. Làm hồ sơ": "5. Ký hồ sơ", 
    "5. Ký hồ sơ": "6. Lấy hồ sơ", "6. Lấy hồ sơ": "7. Nộp hồ sơ", 
    "7. Nộp hồ sơ": "8. Hoàn thành", "8. Hoàn thành": None
}
WORKFLOW_SHORT = {
    "1. Tạo mới": "4. Làm hồ sơ", "4. Làm hồ sơ": "5. Ký hồ sơ", 
    "5. Ký hồ sơ": "6. Lấy hồ sơ", "6. Lấy hồ sơ": "7. Nộp hồ sơ", 
    "7. Nộp hồ sơ": "8. Hoàn thành", "8. Hoàn thành": None
}

# --- 2. CSS & UI STYLING ---
def inject_custom_css():
    st.markdown("""
    <style>
        .block-container { padding-top: 2rem; padding-bottom: 2rem; }
        .metric-card { background-color: white; border: 1px solid #e0e0e0; border-radius: 8px; padding: 15px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
        .metric-value { font-size: 24px; font-weight: bold; color: #2c3e50; }
        .metric-label { font-size: 14px; color: #7f8c8d; margin-bottom: 5px; }
        div[data-testid="stExpander"] { border: none; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-radius: 8px; }
        .stButton>button { border-radius: 6px; }
        .status-badge { padding: 4px 8px; border-radius: 12px; font-size: 0.8rem; font-weight: 600; display: inline-block; }
    </style>
    """, unsafe_allow_html=True)

def get_status_badge_html(status, deadline_str, logs):
    now = datetime.now()
    try: deadline = pd.to_datetime(deadline_str)
    except: deadline = None
    
    color, bg, text = "#28a745", "#e6fffa", "Đang thực hiện"
    
    if status == "Tạm dừng":
        if "Hoàn thành - Chưa thanh toán" in str(logs):
            color, bg, text = "#fd7e14", "#fff3cd", "⚠️ Xong - Chưa TT"
        else:
            color, bg, text = "#6c757d", "#f8f9fa", "⛔ Tạm dừng"
    elif status == "Hoàn thành":
        color, bg, text = "#004085", "#cce5ff", "✅ Hoàn thành"
    elif status == "Đã xóa":
        color, bg, text = "#343a40", "#e2e6ea", "🗑️ Đã xóa"
    elif status == "Kết thúc sớm":
        color, bg, text = "#343a40", "#e2e6ea", "⏹️ Kết thúc"
    elif deadline and now > deadline:
        color, bg, text = "#dc3545", "#ffe6e6", "🔴 Quá hạn"
    elif deadline and now <= deadline <= now + timedelta(hours=24):
        color, bg, text = "#fd7e14", "#fff3cd", "⚠️ Sắp đến hạn"

    return f'<span class="status-badge" style="background-color:{bg}; color:{color}; border:1px solid {color}">{text}</span>'

# --- 3. GOOGLE SERVICES & UTILS ---
def get_gcp_creds(): 
    return Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)

@st.cache_resource
def get_client():
    creds = get_gcp_creds()
    return gspread.authorize(creds)

def get_sheet(sheet_name="DB_DODAC", worksheet_index=0):
    try: 
        client = get_client()
        sh = client.open(sheet_name)
        return sh.get_worksheet(worksheet_index)
    except: return None

def get_users_sheet():
    client = get_client()
    sh = client.open("DB_DODAC")
    try: return sh.worksheet("USERS")
    except: 
        ws = sh.add_worksheet(title="USERS", rows="100", cols="5")
        ws.append_row(["username", "password", "fullname", "role"])
        return ws

def get_audit_sheet():
    client = get_client()
    sh = client.open("DB_DODAC")
    try: return sh.worksheet("AUDIT_LOGS")
    except:
        ws = sh.add_worksheet(title="AUDIT_LOGS", rows="1000", cols="4")
        ws.append_row(["Timestamp", "User", "Action", "Details"])
        return ws

# --- 4. BUSINESS LOGIC HELPER ---
def safe_int(value):
    try: return int(float(str(value).replace(",", "").replace(".", ""))) if pd.notna(value) and value != "" else 0
    except: return 0

def extract_proc_from_log(log_text):
    match = re.search(r'Khởi tạo \((.*?)\)', str(log_text))
    return match.group(1) if match else "Khác"

def get_next_stage_dynamic(current_stage, proc_name):
    if proc_name in ["Cung cấp thông tin", "Đính chính"]: return WORKFLOW_SHORT.get(current_stage)
    return WORKFLOW_FULL.get(current_stage)

def calculate_deadline(start_date, hours_to_add):
    if hours_to_add == 0: return None
    current_date = start_date; added_hours = 0
    while added_hours < hours_to_add:
        current_date += timedelta(hours=1)
        if current_date.weekday() < 5: added_hours += 1
    return current_date

def generate_unique_name(jid, start_time, name, phone, addr, proc_name):
    try:
        d_obj = datetime.strptime(str(start_time), "%Y-%m-%d %H:%M:%S")
        date_str = d_obj.strftime('%d%m%y')
    except: date_str = "000000"
    jid_str = str(jid); seq = jid_str[-2:]
    return f"{date_str}-{seq} {name}"

def upload_file_via_script(file_obj, sub_folder_name):
    if not file_obj: return None, None
    try:
        file_content = file_obj.read()
        file_base64 = base64.b64encode(file_content).decode('utf-8')
        payload = {"filename": file_obj.name, "mime_type": file_obj.type, "file_base64": file_base64, "folder_id": DRIVE_FOLDER_ID, "sub_folder_name": sub_folder_name}
        response = requests.post(APPS_SCRIPT_URL, json=payload)
        res_json = response.json()
        if res_json.get("status") == "success": return res_json.get("link"), file_obj.name
    except Exception as e: st.error(f"Upload Error: {e}")
    return None, None

def send_telegram_msg(msg):
    if not TELEGRAM_TOKEN: return
    def run(): 
        try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"})
        except: pass
    threading.Thread(target=run).start()

def log_to_audit(user, action, details):
    def _log():
        try: ws = get_audit_sheet(); ws.append_row([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user, action, details])
        except: pass
    threading.Thread(target=_log).start()

# --- 5. DATA FETCHING (CACHED) ---
@st.cache_data(ttl=60)
def get_all_jobs_df():
    sh = get_sheet()
    if sh is None: return pd.DataFrame()
    data = sh.get_all_records()
    df = pd.DataFrame(data)
    if not df.empty:
        df['id'] = df['id'].apply(safe_int)
        df['survey_fee'] = df['survey_fee'].apply(safe_int)
        df['deposit'] = df['deposit'].apply(safe_int)
        df['is_paid'] = df['is_paid'].apply(safe_int)
        df['start_dt'] = pd.to_datetime(df['start_time'], errors='coerce')
        df['deadline_dt'] = pd.to_datetime(df['deadline'], errors='coerce')
    return df

@st.cache_data(ttl=60)
def get_all_users_cached():
    sh = get_users_sheet()
    return pd.DataFrame(sh.get_all_records()) if sh else pd.DataFrame()

def get_active_users_list():
    df = get_all_users_cached()
    if df.empty: return []
    return df[df['role']!='Chưa cấp quyền'].apply(lambda x: f"{x['username']} - {x['fullname']}", axis=1).tolist()

# --- 6. ACTION FUNCTIONS (WRITE) ---
def add_job_action(n, p, a, proc, f, u, asn):
    sh = get_sheet(); now = datetime.now(); now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    
    # ID Gen
    df = get_all_jobs_df()
    prefix = int(now.strftime('%y%m%d'))
    if df.empty: jid = int(f"{prefix}01"); seq_str = "01"
    else:
        today_ids = [str(x) for x in df['id'] if str(x).startswith(str(prefix))]
        seq = (max([int(x[-2:]) for x in today_ids]) + 1) if today_ids else 1
        jid = int(f"{prefix}{seq:02}"); seq_str = f"{seq:02}"
    
    full_name_str = f"{prefix}-{seq_str} {n}"
    link, fname, log_file_str = "", "", ""
    if f:
        for uf in f:
            l, n_f = upload_file_via_script(uf, full_name_str)
            if l: log_file_str += f" | File: {n_f} - {l}"; link = l 

    dl = (now + timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
    log = f"[{now_str}] {u}: Khởi tạo ({proc}) -> Giao: {asn.split(' - ')[0] if asn else ''}{log_file_str}"
    
    sh.append_row([jid, now_str, n, f"'{p}", a, "1. Tạo mới", "Đang xử lý", asn.split(" - ")[0] if asn else "", dl, link, log, 0, 0, 0, 0])
    
    get_all_jobs_df.clear() # Clear cache
    log_to_audit(u, "CREATE_JOB", f"ID: {jid}")
    send_telegram_msg(f"🚀 <b>MỚI #{seq_str} ({proc})</b>\n📂 {n}\n👉 {asn}")

def update_stage_action(jid, curr_stg, next_stg, note, files, user, assign_to, fee, is_paid):
    sh = get_sheet()
    try: ids = sh.col_values(1); r = ids.index(str(jid)) + 1
    except: return
    
    now = datetime.now()
    row_vals = sh.row_values(r)
    proc_name = extract_proc_from_log(row_vals[10])
    
    # Upload files
    file_log = ""
    if files:
        folder_name = generate_unique_name(jid, row_vals[1], row_vals[2], "", "", "")
        for f in files:
            l, n = upload_file_via_script(f, folder_name)
            if l: file_log += f" | File: {n} - {l}"

    # Calculate deadline
    if next_stg == "8. Hoàn thành": 
        new_dl = row_vals[8]
        status = "Hoàn thành"
    else:
        sla = STAGE_SLA_HOURS.get(next_stg, 24)
        new_dl = calculate_deadline(now, sla).strftime("%Y-%m-%d %H:%M:%S") if sla > 0 else (now + timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
        status = "Đang xử lý"

    # Update sheet
    sh.update_cell(r, 6, next_stg)
    sh.update_cell(r, 7, status)
    if assign_to: sh.update_cell(r, 8, assign_to.split(" - ")[0])
    sh.update_cell(r, 9, new_dl)
    
    # Update Money
    sh.update_cell(r, 14, safe_int(fee))
    sh.update_cell(r, 15, 1 if is_paid else 0)

    # Log
    old_log = sh.cell(r, 11).value
    new_log = f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] {user}: {curr_stg}->{next_stg} | {note}{file_log}"
    sh.update_cell(r, 11, old_log + new_log)
    
    get_all_jobs_df.clear()
    log_to_audit(user, "UPDATE_STAGE", f"{jid}: {next_stg}")
    send_telegram_msg(f"✅ <b>CẬP NHẬT {jid}</b>\n{curr_stg} ➡ <b>{next_stg}</b>\n👤 {user}")

def update_finance_only(jid, deposit, fee, is_paid, user):
    sh = get_sheet()
    try: ids = sh.col_values(1); r = ids.index(str(jid)) + 1
    except: return
    sh.update_cell(r, 13, 1 if deposit else 0)
    sh.update_cell(r, 14, safe_int(fee))
    sh.update_cell(r, 15, 1 if is_paid else 0)
    get_all_jobs_df.clear()
    log_to_audit(user, "UPDATE_FINANCE", f"{jid}: {fee}")

# --- 7. UI COMPONENTS ---

@st.dialog("📋 Chi tiết hồ sơ", width="large")
def show_job_dialog(jid, user, role, user_list):
    # Fetch fresh data for this specific job
    df = get_all_jobs_df()
    job = df[df['id'] == jid].iloc[0]
    
    st.markdown(f"### {job['customer_name']} <span style='font-size:16px; color:#555'>#{job['id']}</span>", unsafe_allow_html=True)
    st.caption(f"📍 {job['address']} | 📞 {job['customer_phone']}")
    st.markdown(get_status_badge_html(job['status'], job['deadline'], job['logs']), unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["⚙️ Xử lý", "📂 File & Log", "💰 Tài chính"])
    
    proc_name = extract_proc_from_log(job['logs'])
    
    with tab1:
        if job['status'] in ['Hoàn thành', 'Đã xóa']:
            st.info(f"Hồ sơ đã {job['status']}.")
        else:
            current = job['current_stage']
            nxt = get_next_stage_dynamic(current, proc_name) or "8. Hoàn thành"
            
            with st.form(f"action_{jid}"):
                c1, c2 = st.columns(2)
                c1.write(f"Bước hiện tại: **{current}**")
                c2.write(f"Bước tiếp theo: **{nxt}**")
                
                note = st.text_area("Ghi chú xử lý")
                files = st.file_uploader("Đính kèm file", accept_multiple_files=True)
                
                assign = st.selectbox("Giao việc (Tùy chọn)", ["Giữ nguyên"] + user_list)
                assign_val = assign if assign != "Giữ nguyên" else ""
                
                # Auto finance prompt at finish
                is_finishing = nxt == "8. Hoàn thành"
                fee_val = job['survey_fee']
                paid_val = job['is_paid'] == 1
                
                if is_finishing:
                    st.markdown("---")
                    st.warning("⚠️ Xác nhận thanh toán trước khi hoàn thành:")
                    col_f1, col_f2 = st.columns(2)
                    fee_val = col_f1.number_input("Tổng phí", value=safe_int(job['survey_fee']))
                    paid_val = col_f2.checkbox("Đã thanh toán đủ", value=(job['is_paid']==1))

                if st.form_submit_button("✅ Chuyển bước", type="primary", use_container_width=True):
                    update_stage_action(jid, current, nxt, note, files, user, assign_val, fee_val, paid_val)
                    st.success("Đã cập nhật!"); time.sleep(1); st.rerun()

    with tab2:
        # Extract files from logs
        file_matches = re.findall(r"File: (.*?) - (https?://[^\s]+)", str(job['logs']))
        if file_matches:
            for fname, link in file_matches:
                st.markdown(f"📄 [{fname}]({link})")
        else:
            st.caption("Chưa có file.")
        st.divider()
        st.text_area("Nội dung Log:", value=job['logs'], height=200, disabled=True)

    with tab3:
        fee = st.number_input("Phí dịch vụ", value=safe_int(job['survey_fee']), key=f"f_{jid}")
        paid = st.checkbox("Đã thanh toán", value=(job['is_paid']==1), key=f"p_{jid}")
        if st.button("Lưu tài chính", key=f"s_{jid}"):
            update_finance_only(jid, job['deposit'], fee, paid, user)
            st.success("Đã lưu!"); time.sleep(0.5); st.rerun()

# --- 8. PAGE RENDERERS ---

def render_dashboard(user, role):
    df = get_all_jobs_df()
    if df.empty: st.info("Chưa có dữ liệu."); return
    
    active = df[~df['status'].isin(['Đã xóa'])]
    
    # KPI Top
    k1, k2, k3, k4 = st.columns(4)
    my_jobs = active[active['assigned_to'].str.contains(user, na=False)] if role != "Quản lý" else active
    
    urgent = my_jobs[(my_jobs['deadline_dt'] <= datetime.now() + timedelta(hours=24)) & (my_jobs['status'] != 'Hoàn thành')]
    overdue = my_jobs[(my_jobs['deadline_dt'] < datetime.now()) & (my_jobs['status'] != 'Hoàn thành')]
    
    k1.metric("Hồ sơ của bạn", len(my_jobs))
    k2.metric("Đang xử lý", len(my_jobs[my_jobs['status']=='Đang xử lý']))
    k3.metric("Sắp đến hạn", len(urgent), delta_color="inverse")
    k4.metric("Quá hạn", len(overdue), delta_color="inverse")
    
    st.markdown("### 📋 Danh sách hồ sơ")
    
    # Filters
    f1, f2 = st.columns([3, 1])
    search = f1.text_input("🔍 Tìm kiếm (Tên, SĐT, Mã)", placeholder="Nhập từ khóa...")
    filter_stt = f2.selectbox("Trạng thái", ["Tất cả", "Đang xử lý", "Hoàn thành", "Quá hạn"])
    
    display_df = my_jobs.copy()
    if search:
        s = search.lower()
        display_df = display_df[display_df.apply(lambda x: s in str(x['id']) or s in str(x['customer_name']).lower() or s in str(x['customer_phone']), axis=1)]
    
    if filter_stt == "Đang xử lý": display_df = display_df[display_df['status'] == 'Đang xử lý']
    elif filter_stt == "Hoàn thành": display_df = display_df[display_df['status'] == 'Hoàn thành']
    elif filter_stt == "Quá hạn": display_df = display_df[(display_df['deadline_dt'] < datetime.now()) & (display_df['status'] != 'Hoàn thành')]

    # Table View
    header = st.columns([1, 3, 2, 2, 1.5, 0.5])
    header[0].write("**Mã**")
    header[1].write("**Khách hàng**")
    header[2].write("**Quy trình**")
    header[3].write("**Trạng thái/Hạn**")
    header[4].write("**Người làm**")
    
    user_list = get_active_users_list()

    for _, row in display_df.iterrows():
        with st.container():
            c = st.columns([1, 3, 2, 2, 1.5, 0.5])
            c[0].write(f"**{str(row['id'])[-4:]}**")
            c[1].write(f"{row['customer_name']}\n<span style='font-size:0.8em; color:gray'>{row['address']}</span>", unsafe_allow_html=True)
            c[2].caption(row['current_stage'])
            c[3].markdown(get_status_badge_html(row['status'], str(row['deadline']), row['logs']), unsafe_allow_html=True)
            c[4].write(row['assigned_to'].split(' - ')[0] if row['assigned_to'] else "-")
            if c[5].button("👁️", key=f"v_{row['id']}"):
                show_job_dialog(row['id'], user, role, user_list)
            st.markdown("<hr style='margin:5px 0'>", unsafe_allow_html=True)

def render_calendar(user, role, user_list):
    st.title("📅 Lịch & Tiến Độ")
    df = get_all_jobs_df()
    if df.empty: st.info("Trống"); return
    
    active = df[df['status']!='Đã xóa'].copy()
    active['start_dt'] = pd.to_datetime(active['start_time'])
    
    view = st.radio("Chế độ:", ["Lịch tháng", "Timeline (Gantt)"], horizontal=True)
    
    if view == "Lịch tháng":
        cal_col = st.columns(7)
        days = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"]
        for i, d in enumerate(days): cal_col[i].markdown(f"**{d}**")
        
        now = datetime.now()
        cal = calendar.monthcalendar(now.year, now.month)
        
        for week in cal:
            cols = st.columns(7)
            for i, day in enumerate(week):
                with cols[i]:
                    if day != 0:
                        st.caption(str(day))
                        # Find jobs starting or ending
                        d_obj = date(now.year, now.month, day)
                        todays = active[active['start_dt'].dt.date == d_obj]
                        for _, row in todays.iterrows():
                            if st.button(f"🟢 {row['customer_name']}", key=f"c_{day}_{row['id']}"):
                                show_job_dialog(row['id'], user, role, user_list)
    else:
        # Timeline
        chart_data = active[(active['start_dt'] > datetime.now() - timedelta(days=60)) & (active['status']!='Hoàn thành')].copy()
        if not chart_data.empty:
            chart_data['End'] = chart_data['deadline_dt'].fillna(datetime.now() + timedelta(days=30))
            
            c = alt.Chart(chart_data).mark_bar().encode(
                x='start_dt',
                x2='End',
                y=alt.Y('customer_name', sort='x'),
                color='current_stage',
                tooltip=['customer_name', 'current_stage', 'assigned_to']
            ).properties(height=400).interactive()
            st.altair_chart(c, use_container_width=True)
        else:
            st.info("Không có dữ liệu timeline gần đây.")

def render_finance(user):
    st.title("💰 Quản Lý Công Nợ")
    df = get_all_jobs_df()
    if df.empty: return

    active = df[df['status']!='Đã xóa'].copy()
    total_rev = active['survey_fee'].sum()
    collected = active[active['is_paid']==1]['survey_fee'].sum()
    debt = total_rev - collected
    
    c1, c2, c3 = st.columns(3)
    c1.metric("Tổng Doanh Thu", f"{total_rev:,.0f}")
    c2.metric("Đã Thu", f"{collected:,.0f}")
    c3.metric("Còn Nợ", f"{debt:,.0f}", delta_color="inverse")
    
    st.progress(collected/total_rev if total_rev > 0 else 0)
    
    st.subheader("Danh sách chưa thu tiền")
    debt_list = active[active['is_paid']==0][['id', 'customer_name', 'survey_fee', 'status']]
    
    for _, row in debt_list.iterrows():
        with st.container(border=True):
            cl1, cl2, cl3, cl4 = st.columns([1, 4, 2, 2])
            cl1.write(f"#{row['id']}")
            cl2.write(row['customer_name'])
            cl3.write(f"**{row['survey_fee']:,.0f} đ**")
            if cl4.button("💸 Thu ngay", key=f"pay_{row['id']}"):
                update_finance_only(row['id'], 1, row['survey_fee'], 1, user)
                st.toast("Đã cập nhật!"); time.sleep(1); st.rerun()

def render_reports():
    st.title("📊 Báo Cáo Quản Trị")
    df = get_all_jobs_df()
    if df.empty: return
    
    active = df[df['status']!='Đã xóa'].copy()
    active['Month'] = active['start_dt'].dt.strftime('%Y-%m')
    
    # 1. Chart Doanh thu
    revenue_chart = active.groupby('Month')['survey_fee'].sum().reset_index()
    c1 = alt.Chart(revenue_chart).mark_bar().encode(
        x='Month', y='survey_fee', tooltip=['Month', 'survey_fee']
    ).properties(title="Doanh thu theo tháng")
    st.altair_chart(c1, use_container_width=True)
    
    # 2. Chart Trạng thái
    stt_chart = active['status'].value_counts().reset_index()
    stt_chart.columns = ['Status', 'Count']
    c2 = alt.Chart(stt_chart).mark_arc().encode(
        theta='Count', color='Status', tooltip=['Status', 'Count']
    ).properties(title="Tỷ lệ trạng thái")
    st.altair_chart(c2, use_container_width=True)

# --- 9. MAIN APP ---

def main():
    inject_custom_css()
    
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    
    # LOGIN SCREEN
    if not st.session_state['logged_in']:
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2:
            st.title("🔐 ĐĂNG NHẬP")
            with st.form("login"):
                u = st.text_input("Username")
                p = st.text_input("Password", type="password")
                if st.form_submit_button("Login", type="primary"):
                    # Hardcode admin for rescue if needed, otherwise fetch from sheet
                    if u == "admin" and p == "admin123":
                         st.session_state['logged_in'] = True; st.session_state['user'] = u; st.session_state['role'] = "Quản lý"
                         st.rerun()
                    
                    users = get_all_users_cached()
                    user_row = users[users['username'] == u]
                    if not user_row.empty and user_row.iloc[0]['password'] == hashlib.sha256(p.encode()).hexdigest():
                        st.session_state['logged_in'] = True
                        st.session_state['user'] = u
                        st.session_state['role'] = user_row.iloc[0]['role']
                        st.rerun()
                    else:
                        st.error("Sai thông tin!")
        return

    # LOGGED IN
    user = st.session_state['user']
    role = st.session_state['role']
    user_list = get_active_users_list()
    
    with st.sidebar:
        st.write(f"👤 **{user}** ({role})")
        menu = st.radio("Menu", ["🏠 Trang Chủ", "📝 Tạo Hồ Sơ", "📅 Lịch Biểu", "💰 Công Nợ", "📊 Báo Cáo"])
        if st.button("Đăng xuất"):
            st.session_state['logged_in'] = False
            st.rerun()
            
    if menu == "🏠 Trang Chủ":
        render_dashboard(user, role)
    
    elif menu == "📝 Tạo Hồ Sơ":
        st.title("📝 Tạo Hồ Sơ Mới")
        with st.form("new_job"):
            col1, col2 = st.columns(2)
            n = col1.text_input("Tên khách hàng", required=True)
            p = col2.text_input("SĐT", required=True)
            a = st.text_input("Địa chỉ")
            proc = st.selectbox("Thủ tục", PROCEDURES_LIST)
            files = st.file_uploader("File đính kèm", accept_multiple_files=True)
            assign = st.selectbox("Giao cho", user_list)
            
            if st.form_submit_button("🚀 Tạo ngay", type="primary"):
                if n and assign:
                    add_job_action(n, p, a, proc, files, user, assign)
                    st.success("Đã tạo hồ sơ thành công!"); time.sleep(1); st.rerun()
                else: st.error("Thiếu thông tin bắt buộc")
                
    elif menu == "📅 Lịch Biểu":
        render_calendar(user, role, user_list)
        
    elif menu == "💰 Công Nợ":
        render_finance(user)
        
    elif menu == "📊 Báo Cáo":
        render_reports()

if __name__ == "__main__":
    main()
