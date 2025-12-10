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
from google.oauth2.service_account import Credentials

# --- 1. CẤU HÌNH & KHỞI TẠO ---
st.set_page_config(page_title="Đo Đạc Cloud V4-Speed", page_icon="⚡", layout="wide")

# CẤU HÌNH API
TELEGRAM_TOKEN = "8514665869:AAHUfTHgNlEEK_Yz6yYjZa-1iR645Cgr190"
TELEGRAM_CHAT_ID = "-5055192262"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbyEMEGyS_sVCA4eyVRFXxnOuGqMnJOKOIqZqKxi4HpYBcpr7U72WUXCoKLm20BQomVC/exec"
DRIVE_FOLDER_ID = "1SrARuA1rgKLZmoObGor-GkNx33F6zNQy"

# DANH MỤC DỮ LIỆU
STAGES_ORDER = ["1. Đo đạc", "2. Hoàn thiện trích đo", "3. Làm hồ sơ", "4. Ký hồ sơ", "5. Lấy hồ sơ", "6. Nộp hồ sơ", "7. Hoàn thành"]
WORKFLOW_MAP = {
    "1. Đo đạc": "2. Hoàn thiện trích đo", "2. Hoàn thiện trích đo": "3. Làm hồ sơ",
    "3. Làm hồ sơ": "4. Ký hồ sơ", "4. Ký hồ sơ": "5. Lấy hồ sơ", 
    "5. Lấy hồ sơ": "6. Nộp hồ sơ", "6. Nộp hồ sơ": "7. Hoàn thành", "7. Hoàn thành": None
}
STAGE_SLA = {"1. Đo đạc": 24, "2. Hoàn thiện trích đo": 24, "3. Làm hồ sơ": 24, "4. Ký hồ sơ": 72, "5. Lấy hồ sơ": 24, "6. Nộp hồ sơ": 360}

# CSS TỐI ƯU GIAO DIỆN
st.markdown("""
<style>
    .metric-card { border: 1px solid #e0e0e0; padding: 10px; border-radius: 8px; text-align: center; background-color: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .status-badge { padding: 4px 8px; border-radius: 12px; font-size: 0.8rem; font-weight: bold; }
    .stButton>button { width: 100%; border-radius: 6px; }
    div[data-testid="stExpander"] { border: none; box-shadow: 0 1px 2px rgba(0,0,0,0.1); background-color: white; margin-bottom: 10px; border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# --- 2. XỬ LÝ DỮ LIỆU & KẾT NỐI (CORE) ---

def get_gcp_creds():
    return Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)

# --- CACHING QUAN TRỌNG: Chỉ tải dữ liệu khi cần ---
@st.cache_data(ttl=600) # Tự động cache trong 10 phút
def fetch_all_data():
    try:
        creds = get_gcp_creds()
        client = gspread.authorize(creds)
        sh = client.open("DB_DODAC").sheet1
        data = sh.get_all_records()
        df = pd.DataFrame(data)
        
        if df.empty: return pd.DataFrame()

        # Xử lý dữ liệu hàng loạt (Vectorization) để tăng tốc
        df['id'] = pd.to_numeric(df['id'], errors='coerce').fillna(0).astype(int)
        df['start_dt'] = pd.to_datetime(df['start_time'], errors='coerce')
        df['deadline_dt'] = pd.to_datetime(df['deadline'], errors='coerce')
        df['survey_fee'] = pd.to_numeric(df['survey_fee'], errors='coerce').fillna(0).astype(int)
        df['is_paid'] = pd.to_numeric(df['is_paid'], errors='coerce').fillna(0).astype(int)
        
        # Tính toán trạng thái quá hạn
        now = datetime.now()
        df['is_late'] = (df['deadline_dt'] < now) & (~df['status'].isin(['Hoàn thành', 'Đã xóa', 'Kết thúc sớm', 'Tạm dừng']))
        df['is_urgent'] = (df['deadline_dt'] >= now) & (df['deadline_dt'] <= now + timedelta(hours=24)) & (~df['status'].isin(['Hoàn thành', 'Đã xóa']))
        
        return df
    except Exception as e:
        st.error(f"Lỗi kết nối Google: {e}")
        return pd.DataFrame()

def clear_data_cache():
    fetch_all_data.clear()
    
# Các hàm tiện ích nhỏ
def safe_int(val): 
    try: return int(float(str(val).replace(",", "").replace(".", "")))
    except: return 0

def extract_proc(log):
    m = re.search(r'Khởi tạo \((.*?)\)', str(log))
    return m.group(1) if m else "Khác"

def calculate_deadline(start_date, hours):
    if hours == 0: return None
    curr = start_date; added = 0
    while added < hours:
        curr += timedelta(hours=1)
        if curr.weekday() < 5: added += 1 # Chỉ tính T2-T6
    return curr

# --- 3. TƯƠNG TÁC DỮ LIỆU (WRITE) ---

def run_async(func, *args):
    threading.Thread(target=func, args=args).start()

def send_tele(msg):
    if not TELEGRAM_TOKEN: return
    try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"})
    except: pass

def log_audit(user, action, detail):
    try:
        creds = get_gcp_creds(); client = gspread.authorize(creds)
        client.open("DB_DODAC").worksheet("AUDIT_LOGS").append_row([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user, action, detail])
    except: pass

def upload_file(f, folder_name):
    if not f: return None, None
    try:
        content = base64.b64encode(f.read()).decode('utf-8')
        res = requests.post(APPS_SCRIPT_URL, json={"filename": f.name, "mime_type": f.type, "file_base64": content, "folder_id": DRIVE_FOLDER_ID, "sub_folder_name": folder_name})
        if res.status_code == 200 and res.json().get("status") == "success": return res.json().get("link"), f.name
    except: pass
    return None, None

def update_gsheet_cell(jid, col_idx, val, row_idx=None):
    # Hàm cập nhật generic
    creds = get_gcp_creds(); client = gspread.authorize(creds)
    sh = client.open("DB_DODAC").sheet1
    if not row_idx:
        ids = sh.col_values(1)
        try: row_idx = ids.index(str(jid)) + 1
        except: return False
    sh.update_cell(row_idx, col_idx, val)
    return row_idx

# --- 4. LOGIC NGHIỆP VỤ ---

def action_add_job(name, phone, addr, proc, files, user, assign_to):
    df = fetch_all_data() # Lấy cache để tính ID
    now = datetime.now()
    prefix = int(now.strftime('%y%m%d'))
    
    # Sinh ID mới
    today_ids = [i for i in df['id'].tolist() if str(i).startswith(str(prefix))]
    seq = (max([int(str(i)[-2:]) for i in today_ids]) + 1) if today_ids else 1
    new_id = int(f"{prefix}{seq:02}")
    
    # Upload file
    link, fname, log_file = "", "", ""
    full_name = f"{new_id} {name} {phone}"
    if files:
        l, n = upload_file(files[0], full_name)
        if l: link = l; fname = n; log_file = f" | File: {n} - {l}"

    # Deadline
    dl = calculate_deadline(now, STAGE_SLA.get("1. Đo đạc", 24))
    dl_str = dl.strftime("%Y-%m-%d %H:%M:%S") if dl else ""
    
    # Write to Sheet
    creds = get_gcp_creds(); client = gspread.authorize(creds)
    sh = client.open("DB_DODAC").sheet1
    assign_clean = assign_to.split(' - ')[0] if assign_to else ""
    log_init = f"[{now}] {user}: Khởi tạo ({proc}) -> 1. Đo đạc -> Giao: {assign_clean}{log_file}"
    
    sh.append_row([new_id, now.strftime("%Y-%m-%d %H:%M:%S"), name, f"'{phone}", addr, "1. Đo đạc", "Đang xử lý", assign_clean, dl_str, link, log_init, 0, 0, 0, 0])
    
    clear_data_cache() # XÓA CACHE ĐỂ LOAD LẠI
    run_async(send_tele, f"🆕 <b>HỒ SƠ MỚI #{new_id}</b>\nKhách: {name}\nThủ tục: {proc}\nGiao: {assign_clean}")
    run_async(log_audit, user, "CREATE", f"ID {new_id}")

def action_update_stage(job, note, files, user, assign_to):
    creds = get_gcp_creds(); client = gspread.authorize(creds)
    sh = client.open("DB_DODAC").sheet1
    try:
        r = sh.find(str(job['id'])).row
    except: return st.error("Không tìm thấy hồ sơ!")

    cur = job['current_stage']
    proc = extract_proc(job['logs'])
    
    # Logic chuyển bước
    nxt = WORKFLOW_MAP.get(cur, "7. Hoàn thành")
    if proc == "Chỉ đo đạc" and cur == "1. Đo đạc": nxt = "2. Hoàn thiện trích đo"
    if not nxt: nxt = "7. Hoàn thành"

    # Deadline mới
    hours = STAGE_SLA.get(nxt, 24)
    new_dl = calculate_deadline(datetime.now(), hours)
    new_dl_str = new_dl.strftime("%Y-%m-%d %H:%M:%S") if new_dl else ""

    # Xử lý file
    log_file = ""
    if files:
        for f in files:
            l, n = upload_file(f, f"{job['id']} {job['customer_name']}")
            if l: log_file += f"\nFile: {n} - {l}"
    
    # Cập nhật Sheet
    sh.update_cell(r, 6, nxt) # Stage
    if nxt != "7. Hoàn thành": sh.update_cell(r, 9, new_dl_str) # Deadline
    else: sh.update_cell(r, 7, "Hoàn thành") # Status
    
    assign_msg = ""
    if assign_to:
        clean_assign = assign_to.split(' - ')[0]
        sh.update_cell(r, 8, clean_assign)
        assign_msg = f" -> Giao: {clean_assign}"

    old_log = sh.cell(r, 11).value
    new_log = f"\n[{datetime.now().strftime('%d/%m %H:%M')}] {user}: {cur} -> {nxt}{assign_msg}\nNote: {note}{log_file}"
    sh.update_cell(r, 11, old_log + new_log)
    
    clear_data_cache()
    run_async(send_tele, f"✅ <b>CẬP NHẬT #{job['id']}</b>\n{cur} ➡ <b>{nxt}</b>\nUser: {user}")
    st.toast("Đã chuyển bước thành công!")

def action_update_finance(job_id, fee, is_paid, user):
    r = update_gsheet_cell(job_id, 14, fee) # Col 14: Fee
    if r:
        update_gsheet_cell(job_id, 15, 1 if is_paid else 0, row_idx=r) # Col 15: Is Paid
        clear_data_cache()
        st.toast("Đã lưu tài chính!")

# --- 5. GIAO DIỆN NGƯỜI DÙNG (UI) ---

def render_login():
    st.markdown("<h2 style='text-align: center;'>☁️ ĐO ĐẠC CLOUD V4</h2>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login"):
            u = st.text_input("Tên đăng nhập")
            p = st.text_input("Mật khẩu", type="password")
            if st.form_submit_button("ĐĂNG NHẬP", use_container_width=True):
                # Hardcode login tạm thời để test, bạn kết nối lại sheet USER nếu cần
                creds = get_gcp_creds(); client = gspread.authorize(creds)
                try:
                    us = client.open("DB_DODAC").worksheet("USERS").get_all_records()
                    valid = next((r for r in us if r['username']==u and r['password']==hashlib.sha256(p.encode()).hexdigest()), None)
                    if valid:
                        st.session_state.logged_in = True
                        st.session_state.user = u
                        st.session_state.role = valid['role']
                        st.rerun()
                    else: st.error("Sai thông tin!")
                except: st.error("Lỗi kết nối User DB")

def render_job_item(row, user, role, all_users):
    # Card hiển thị thông tin
    with st.container():
        # Header Card: ID - Tên - Trạng thái
        c1, c2, c3, c4 = st.columns([1.5, 4, 2, 0.5])
        
        # Icon trạng thái
        stt_icon = "🔴" if row['is_late'] else ("🟡" if row['is_urgent'] else "🟢")
        if row['status'] == "Hoàn thành": stt_icon = "✅"
        elif row['status'] == "Tạm dừng": stt_icon = "⛔"
        
        with c1: st.markdown(f"**#{row['id']}** {stt_icon}")
        with c2: 
            st.markdown(f"**{row['customer_name']}**")
            st.caption(f"📞 {row['customer_phone']} | 📍 {row['address']}")
        with c3:
            st.info(f"{row['current_stage']}", icon="📌")
            if row['assigned_to']: st.caption(f"👤 {row['assigned_to']}")
        
        # Nút mở rộng
        expanded = st.session_state.get(f"open_{row['id']}", False)
        with c4:
            if st.button("👁️", key=f"btn_{row['id']}"):
                st.session_state[f"open_{row['id']}"] = not expanded
                st.rerun()

    # Phần chi tiết (chỉ hiện khi bấm nút)
    if expanded:
        with st.container():
            t1, t2, t3 = st.tabs(["⚙️ Xử lý", "💰 Tài chính", "📜 Lịch sử"])
            
            with t1:
                # Form xử lý nhanh
                with st.form(f"act_{row['id']}"):
                    note = st.text_area("Ghi chú/Kết quả:", rows=2)
                    files = st.file_uploader("Đính kèm file:", accept_multiple_files=True)
                    
                    c_sel, c_btn = st.columns([2, 1])
                    with c_sel:
                        idx = all_users.index(row['assigned_to']) if row['assigned_to'] in all_users else 0
                        assign = st.selectbox("Chuyển cho:", [""] + all_users, index=0 if not row['assigned_to'] else all_users.index(row['assigned_to'])+1)
                    
                    with c_btn:
                        st.write("") # Spacer
                        if st.form_submit_button("✅ Chuyển Bước Kế", type="primary"):
                            action_update_stage(row, note, files, user, assign)
                            st.rerun()
                
                # Nút phụ
                col_sub1, col_sub2 = st.columns(2)
                if col_sub1.button("Tạm dừng hồ sơ", key=f"p_{row['id']}"):
                    update_gsheet_cell(row['id'], 7, "Tạm dừng")
                    clear_data_cache(); st.rerun()
                if row['status'] == 'Tạm dừng' and col_sub2.button("Tiếp tục", key=f"r_{row['id']}"):
                    update_gsheet_cell(row['id'], 7, "Đang xử lý")
                    clear_data_cache(); st.rerun()

            with t2:
                # Tài chính
                with st.form(f"fin_{row['id']}"):
                    c_f1, c_f2 = st.columns(2)
                    fee = c_f1.number_input("Phí dịch vụ:", value=row['survey_fee'], step=50000)
                    paid = c_f2.checkbox("Đã thanh toán đủ", value=(row['is_paid']==1))
                    if st.form_submit_button("Lưu Tài Chính"):
                        action_update_finance(row['id'], fee, paid, user)
                        st.rerun()

            with t3:
                st.text_area("Log", row['logs'], height=200, disabled=True)
            st.divider()

def main_app():
    user = st.session_state.user
    role = st.session_state.role
    
    # SIDEBAR
    with st.sidebar:
        st.title(f"👤 {user}")
        if st.button("🔄 LÀM MỚI DỮ LIỆU", type="primary"):
            clear_data_cache()
            st.rerun()
        
        st.markdown("---")
        menu = st.radio("Menu", ["🏠 Trang Chủ", "📝 Tạo Mới", "📊 Báo Cáo", "🗄️ Lưu Trữ"])
        
        if st.button("Đăng xuất"):
            st.session_state.logged_in = False
            st.rerun()

    # LOAD DATA
    df = fetch_all_data()
    if df.empty and menu != "📝 Tạo Mới":
        st.warning("Đang tải dữ liệu hoặc chưa có dữ liệu...")
        return

    # Lấy danh sách user cho dropdown
    all_users_list = []
    try:
        creds = get_gcp_creds(); client = gspread.authorize(creds)
        u_sheet = client.open("DB_DODAC").worksheet("USERS")
        all_users_list = [r['username'] for r in u_sheet.get_all_records() if r['role'] != 'Chưa cấp quyền']
    except: all_users_list = [user]

    # --- TRANG CHỦ ---
    if menu == "🏠 Trang Chủ":
        # KPI Metrics
        active_df = df[~df['status'].isin(['Hoàn thành', 'Đã xóa', 'Kết thúc sớm'])]
        late_df = active_df[active_df['is_late']]
        urgent_df = active_df[active_df['is_urgent']]
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tổng đang làm", len(active_df))
        c2.metric("🔴 Quá hạn", len(late_df))
        c3.metric("🟡 Gấp (24h)", len(urgent_df))
        c4.metric("Của tôi", len(active_df[active_df['assigned_to'].str.contains(user, na=False)]))
        
        st.divider()
        
        # Filter & Search
        col_s, col_f = st.columns([3, 1])
        search = col_s.text_input("🔍 Tìm kiếm (Tên, SĐT, ID...)", placeholder="Nhập từ khóa...")
        filter_mode = col_f.selectbox("Lọc theo:", ["Việc của tôi", "Toàn bộ", "Quá hạn"])
        
        # Apply Filter
        view_df = active_df.copy()
        if filter_mode == "Việc của tôi": view_df = view_df[view_df['assigned_to'].str.contains(user, na=False)]
        elif filter_mode == "Quá hạn": view_df = late_df
        
        if search:
            s = search.lower()
            view_df = view_df[view_df.apply(lambda r: s in str(r['customer_name']).lower() or s in str(r['customer_phone']) or s in str(r['id']), axis=1)]

        # Render List
        st.caption(f"Hiển thị {len(view_df)} hồ sơ")
        for idx, row in view_df.sort_values(by=['is_late', 'deadline_dt'], ascending=[False, True]).iterrows():
            render_job_item(row, user, role, all_users_list)

    # --- TẠO MỚI ---
    elif menu == "📝 Tạo Mới":
        st.subheader("Tạo Hồ Sơ Mới")
        with st.form("new_job"):
            c1, c2 = st.columns(2)
            n = c1.text_input("Tên khách hàng *")
            p = c2.text_input("Số điện thoại *")
            a = st.text_input("Địa chỉ")
            proc = st.selectbox("Loại thủ tục", ["Cấp đổi", "Cấp lần đầu", "Tách thửa", "Chuyển quyền", "Chỉ đo đạc", "Cung cấp thông tin"])
            
            f = st.file_uploader("File đính kèm (Sổ đỏ/CMND)")
            assign = st.selectbox("Giao việc cho:", all_users_list)
            
            if st.form_submit_button("TẠO HỒ SƠ", type="primary"):
                if n and p:
                    action_add_job(n, p, a, proc, [f] if f else [], user, assign)
                    st.success("Đã tạo xong! Chuyển về trang chủ...")
                    time.sleep(1)
                    st.rerun()
                else: st.error("Thiếu tên hoặc SĐT")

    # --- BÁO CÁO ---
    elif menu == "📊 Báo Cáo":
        st.title("Báo Cáo Doanh Thu & Hiệu Suất")
        if role != "Quản lý":
            st.warning("Chỉ dành cho Quản lý")
        else:
            # Stats cơ bản
            total_rev = df['survey_fee'].sum()
            unpaid = df[df['is_paid']==0]['survey_fee'].sum()
            
            k1, k2 = st.columns(2)
            k1.metric("Tổng Doanh Thu (Dự kiến)", f"{total_rev:,.0f} đ")
            k2.metric("Công Nợ Phải Thu", f"{unpaid:,.0f} đ", delta_color="inverse")
            
            st.subheader("Công nợ chi tiết")
            debt_df = df[(df['is_paid']==0) & (df['survey_fee']>0)][['id', 'customer_name', 'customer_phone', 'survey_fee', 'assigned_to']]
            st.dataframe(debt_df, use_container_width=True)

    # --- LƯU TRỮ ---
    elif menu == "🗄️ Lưu Trữ":
        st.subheader("Kho Hồ Sơ Đã Hoàn Thành")
        archive = df[df['status'].isin(['Hoàn thành', 'Đã xóa'])]
        st.dataframe(archive[['id', 'customer_name', 'start_time', 'status', 'logs']], use_container_width=True)


# --- MAIN ENTRY ---
if __name__ == "__main__":
    if 'logged_in' not in st.session_state: st.session_state.logged_in = False
    
    if not st.session_state.logged_in:
        render_login()
    else:
        main_app()
