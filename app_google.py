import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import time
import requests
import threading
import hashlib
import re
import gspread
import base64 
from google.oauth2.service_account import Credentials

# --- 1. CẤU HÌNH HỆ THỐNG ---
TELEGRAM_TOKEN = "8514665869:AAHUfTHgNlEEK_Yz6yYjZa-1iR645Cgr190"
TELEGRAM_CHAT_ID = "-5046493421"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# ==============================================================================
# KEY KẾT NỐI
APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbyEMEGyS_sVCA4eyVRFXxnOuGqMnJOKOIqZqKxi4HpYBcpr7U72WUXCoKLm20BQomVC/exec"
DRIVE_FOLDER_ID = "1SrARuA1rgKLZmoObGor-GkNx33F6zNQy"
# ==============================================================================

ROLES = ["Quản lý", "Nhân viên", "Chưa cấp quyền"]
STAGES_ORDER = ["1. Tạo mới", "2. Đo đạc", "3. Làm hồ sơ", "4. Ký hồ sơ", "5. Lấy hồ sơ", "6. Nộp hồ sơ", "7. Hoàn thành"]

PROCEDURES_LIST = ["Cấp lần đầu", "Cấp đổi", "Chuyển quyền"]

WORKFLOW_DEFAULT = {
    "1. Tạo mới": "2. Đo đạc", "2. Đo đạc": "3. Làm hồ sơ", "3. Làm hồ sơ": "4. Ký hồ sơ", 
    "4. Ký hồ sơ": "5. Lấy hồ sơ", "5. Lấy hồ sơ": "6. Nộp hồ sơ", "6. Nộp hồ sơ": "7. Hoàn thành", "7. Hoàn thành": None
}

# --- 2. HÀM HỖ TRỢ ---
def safe_int(value):
    try:
        if pd.isna(value) or value == "" or value is None: return 0
        return int(float(str(value).replace(",", "").replace(".", "")))
    except: return 0

# [MỚI] Hàm viết tắt thủ tục
def get_proc_abbr(proc_name):
    mapping = {
        "Cấp lần đầu": "CLD",
        "Cấp đổi": "CD",
        "Chuyển quyền": "CQ"
    }
    # Mặc định lấy ký tự đầu nếu không khớp
    return mapping.get(proc_name, "K")

# [CẬP NHẬT] Hàm sinh mã hiển thị đầy đủ
def generate_full_name(jid, start, name, phone="", addr="", proc_name=""):
    try: d = datetime.strptime(str(start), "%Y-%m-%d %H:%M:%S").strftime('%d%m%y')
    except: d = datetime.now().strftime('%d%m%y')
    
    abbr = get_proc_abbr(proc_name) if proc_name else ""
    proc_str = f"-{abbr}" if abbr else ""
    
    # Định dạng: 271125-1764219494-CD Lê Trung 0962630491 Thiều Xuân
    return f"{d}-{int(jid)}{proc_str} {name} {phone} {addr}"

# [CẬP NHẬT] Hàm tìm thủ tục từ trong Log (Vì database không có cột riêng)
def extract_proc_from_log(log_text):
    # Tìm chuỗi trong ngoặc sau chữ Khởi tạo. VD: Khởi tạo (Cấp đổi)
    match = re.search(r'Khởi tạo \((.*?)\)', str(log_text))
    return match.group(1) if match else ""

def extract_files_from_log(log_text):
    pattern = r"File: (.*?) - (https?://[^\s]+)"
    matches = re.findall(pattern, str(log_text))
    if not matches:
        raw_links = re.findall(r'(https?://[^\s]+)', str(log_text))
        return [("File cũ", l) for l in raw_links]
    return matches

def get_drive_id(link):
    try:
        match = re.search(r'/d/([a-zA-Z0-9_-]+)', link)
        return match.group(1) if match else None
    except: return None

# --- 3. KẾT NỐI DATABASE ---
def get_gcp_creds(): 
    return Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)

def get_sheet(sheet_name="DB_DODAC"):
    creds = get_gcp_creds(); client = gspread.authorize(creds)
    try: return client.open(sheet_name).sheet1
    except: return None

def get_users_sheet():
    creds = get_gcp_creds(); client = gspread.authorize(creds)
    try:
        sh = client.open("DB_DODAC")
        try: return sh.worksheet("USERS")
        except: 
            ws = sh.add_worksheet(title="USERS", rows="100", cols="5")
            ws.append_row(["username", "password", "fullname", "role"])
            return ws
    except: return None

# --- 4. CÁC HÀM XỬ LÝ FILE ---
def upload_to_drive(file_obj, sub_folder_name):
    if not file_obj: return None, None
    status_box = st.empty()
    status_box.info(f"☁️ Đang tải '{file_obj.name}' vào thư mục: {sub_folder_name}...")
    try:
        file_content = file_obj.read()
        file_base64 = base64.b64encode(file_content).decode('utf-8')
        payload = {
            "filename": file_obj.name,
            "mime_type": file_obj.type,
            "file_base64": file_base64,
            "folder_id": DRIVE_FOLDER_ID,
            "sub_folder_name": sub_folder_name
        }
        response = requests.post(APPS_SCRIPT_URL, json=payload)
        if response.status_code == 200:
            res_json = response.json()
            if res_json.get("status") == "success":
                status_box.success("✅ Upload thành công!")
                time.sleep(1); status_box.empty()
                return res_json.get("link"), file_obj.name
    except: pass
    status_box.error("❌ Lỗi Upload"); return None, None

def delete_file_system(job_id, file_link, file_name):
    file_id = get_drive_id(file_link)
    if file_id:
        try: requests.post(APPS_SCRIPT_URL, json={"action": "delete", "file_id": file_id})
        except: pass
    sh = get_sheet()
    cell = sh.find(str(job_id))
    if cell:
        r = cell.row
        current_log = sh.cell(r, 11).value
        target_str_1 = f" | File: {file_name} - {file_link}"
        target_str_2 = f" | File: {file_link}" 
        new_log = current_log.replace(target_str_1, "").replace(target_str_2, "")
        sh.update_cell(r, 11, new_log)
        current_main_link = sh.cell(r, 10).value
        if current_main_link == file_link: sh.update_cell(r, 10, "")

# --- 5. LOGIC HỆ THỐNG ---
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
    try: cell = sh.find(u); row = sh.row_values(cell.row); return row if row[1] == make_hash(p) else None
    except: return None

def create_user(u, p, n):
    sh = get_users_sheet(); 
    if not sh: return False
    try: 
        if sh.find(u): return False
        sh.append_row([u, make_hash(p), n, "Chưa cấp quyền"])
        return True
    except: return False

def get_all_users(): sh = get_users_sheet(); return pd.DataFrame(sh.get_all_records())
def update_user_role(u, r): sh = get_users_sheet(); c = sh.find(u); sh.update_cell(c.row, 4, r)
def get_active_users_list(): df = get_all_users(); return df[df['role']!='Chưa cấp quyền'].apply(lambda x: f"{x['username']} - {x['fullname']}", axis=1).tolist() if not df.empty else []

def get_all_jobs_df():
    sh = get_sheet(); data = sh.get_all_records(); df = pd.DataFrame(data)
    if not df.empty:
        df['id'] = df['id'].apply(safe_int)
        if 'deposit' not in df.columns: df['deposit'] = 0
        if 'survey_fee' not in df.columns: df['survey_fee'] = 0
        if 'is_paid' not in df.columns: df['is_paid'] = 0
        if 'file_link' not in df.columns: df['file_link'] = ""
    return df

# --- 6. XỬ LÝ HỒ SƠ ---
def add_job(n, p, a, proc, f, u, asn, d, is_survey, deposit_ok, fee_amount):
    sh = get_sheet(); now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    date_code = now.strftime('%d%m%Y') # Ngày tiếp nhận (271125)
    dl = (now+timedelta(days=d)).strftime("%Y-%m-%d %H:%M:%S")
    jid = int(time.time()) # Số thứ tự (ID)
    
    # [CẬP NHẬT] Tên thư mục Google Drive: Ngày-ID-ThủTục Tên SĐT ĐịaChỉ
    abbr = get_proc_abbr(proc)
    sub_folder = f"{date_code}-{jid}-{abbr} {n} {p} {a}"
    
    link, fname = upload_to_drive(f, sub_folder)
    
    log = f"[{now_str}] {u}: Khởi tạo ({proc})"
    if link: log += f" | File: {fname} - {link}"
    
    asn_clean = asn.split(" - ")[0] if asn else ""
    sv_flag = 1 if is_survey else 0; dep_flag = 1 if deposit_ok else 0
    
    sh.append_row([jid, now_str, n, p, a, "1. Tạo mới", "Đang xử lý", asn_clean, dl, link, log, sv_flag, dep_flag, fee_amount, 0])
    
    # Mã thông báo Telegram
    code = f"{date_code}-{jid}-{abbr} {n}"
    type_msg = f"({proc.upper()})"
    money_msg = "✅ Đã thu tạm ứng" if deposit_ok else "❌ Chưa thu tạm ứng"
    file_msg = f"\n📎 {fname}: {link}" if link else ""
    
    send_telegram_msg(f"🚀 <b>MỚI #{jid} {type_msg}</b>\n📂 <b>{code}</b>\n📍 {a}\n👉 {asn_clean}\n💰 {money_msg}{file_msg}")

def update_stage(jid, stg, nt, f, u, asn, d, is_survey, deposit_ok, fee_amount, is_paid):
    sh = get_sheet(); cell = sh.find(str(jid))
    if cell:
        r = cell.row; now = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); lnk = ""; fname = ""
        c_name = sh.cell(r, 3).value; start_t = sh.cell(r, 2).value
        
        # Upload vào folder cũ (sử dụng ID_Tên để tìm, Apps Script sẽ tự tìm)
        sub_folder = f"{int(jid)}_{c_name}" 
        if f: lnk, fname = upload_to_drive(f, sub_folder)
        
        nxt = "7. Hoàn thành" if is_survey==1 and stg=="3. Làm hồ sơ" else WORKFLOW_DEFAULT.get(stg)
        if nxt:
            sh.update_cell(r, 6, nxt)
            if asn: sh.update_cell(r, 8, asn.split(" - ")[0])
            sh.update_cell(r, 9, (datetime.now()+timedelta(days=d)).strftime("%Y-%m-%d %H:%M:%S"))
            sh.update_cell(r, 13, 1 if deposit_ok else 0); sh.update_cell(r, 14, safe_int(fee_amount)); sh.update_cell(r, 15, 1 if is_paid else 0)
            olog = sh.cell(r, 11).value
            nlog = f"\n[{now}] {u}: {stg}->{nxt} | Note: {nt}"
            if lnk: nlog += f" | File: {fname} - {lnk}"
            sh.update_cell(r, 11, olog + nlog)
            if nxt=="7. Hoàn thành": sh.update_cell(r, 7, "Hoàn thành")
            
            # Gửi Tele (không cần thủ tục ở đây cho gọn)
            code = f"{int(jid)} {c_name}" 
            send_telegram_msg(f"✅ <b>CẬP NHẬT</b>\n📂 <b>{code}</b>\n{stg} ➡ <b>{nxt}</b>\n👤 {u}")

def update_finance_only(jid, deposit_ok, fee_amount, is_paid, u):
    sh = get_sheet(); cell = sh.find(str(jid))
    if cell:
        r = cell.row
        sh.update_cell(r, 13, 1 if deposit_ok else 0); sh.update_cell(r, 14, safe_int(fee_amount)); sh.update_cell(r, 15, 1 if is_paid else 0)
        c_name = sh.cell(r, 3).value
        send_telegram_msg(f"💰 <b>TÀI CHÍNH</b>\n📂 <b>{int(jid)} {c_name}</b>\n👤 {u}\nPhí: {fee_amount:,} VNĐ")

def pause_job(jid, rs, u):
    sh = get_sheet(); cell = sh.find(str(jid))
    if cell:
        r = cell.row; sh.update_cell(r, 7, "Tạm dừng"); c_name = sh.cell(r, 3).value;
        olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{datetime.now()}] {u}: TẠM DỪNG: {rs}")
        send_telegram_msg(f"⛔ <b>TẠM DỪNG</b>\n📂 <b>{int(jid)} {c_name}</b>\n👤 Bởi: {u}\n📝 Lý do: {rs}")

def resume_job(jid, u):
    sh = get_sheet(); cell = sh.find(str(jid))
    if cell:
        r = cell.row; sh.update_cell(r, 7, "Đang xử lý"); c_name = sh.cell(r, 3).value; 
        olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{datetime.now()}] {u}: KHÔI PHỤC")
        send_telegram_msg(f"▶️ <b>KHÔI PHỤC</b>\n📂 <b>{int(jid)} {c_name}</b>\n👤 Bởi: {u}")

def terminate_job(jid, rs, u):
    sh = get_sheet(); cell = sh.find(str(jid))
    if cell:
        r = cell.row; sh.update_cell(r, 7, "Kết thúc sớm"); c_name = sh.cell(r, 3).value; 
        olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{datetime.now()}] {u}: KẾT THÚC SỚM: {rs}")
        send_telegram_msg(f"⏹️ <b>KẾT THÚC SỚM</b>\n📂 <b>{int(jid)} {c_name}</b>\n👤 Bởi: {u}\n📝 Lý do: {rs}")

def render_progress_bar(current_stage, status):
    try: idx = STAGES_ORDER.index(current_stage)
    except: idx = 0
    color = "#dc3545" if status in ["Tạm dừng", "Kết thúc sớm"] else "#ffc107"
    st.markdown(f"""<style>.step-container {{display: flex; justify-content: space-between; margin-bottom: 15px;}} .step-item {{flex: 1; text-align: center; position: relative;}} .step-item:not(:last-child)::after {{content: ''; position: absolute; top: 15px; left: 50%; width: 100%; height: 2px; background: #e0e0e0; z-index: -1;}} .step-circle {{width: 30px; height: 30px; margin: 0 auto 5px; border-radius: 50%; line-height: 30px; color: white; font-weight: bold; font-size: 12px;}} .done {{background: #28a745;}} .active {{background: {color}; color: black;}} .pending {{background: #e9ecef; color: #999;}}</style>""", unsafe_allow_html=True)
    h = '<div class="step-container">'; 
    for i, s in enumerate(STAGES_ORDER):
        cls = "done" if i < idx else "active" if i == idx else "pending"
        ico = "✓" if i < idx else str(i+1)
        if i == idx and status == "Tạm dừng": ico = "⛔"; 
        if i == idx and status == "Kết thúc sớm": ico = "⏹️"
        h += f'<div class="step-item"><div class="step-circle {cls}">{ico}</div><div style="font-size:11px">{s.split(". ")[1]}</div></div>'
    st.markdown(h+'</div>', unsafe_allow_html=True)

# --- 7. UI MAIN ---
st.set_page_config(page_title="Đo Đạc Cloud Pro", page_icon="☁️", layout="wide")

if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'uploader_key' not in st.session_state: st.session_state['uploader_key'] = 0

if not st.session_state['logged_in']:
    st.title("🔐 Đăng nhập"); c1, c2 = st.columns(2)
    with c1:
        u = st.text_input("User"); p = st.text_input("Pass", type='password')
        if st.button("Login"):
            d = login_user(u, p)
            if d: st.session_state['logged_in']=True; st.session_state['user']=d[0]; st.session_state['role']=d[3]; st.rerun()
            else: st.error("Sai thông tin!")
    with c2:
        nu = st.text_input("User Mới"); np = st.text_input("Pass Mới", type='password'); nn = st.text_input("Họ Tên")
        if st.button("Đăng Ký"): 
            if create_user(nu, np, nn): st.success("OK! Chờ duyệt.")
            else: st.error("Trùng tên!")
else:
    user = st.session_state['user']; role = st.session_state['role']
    st.sidebar.title(f"👤 {user}"); st.sidebar.info(f"{role}")
    if st.sidebar.button("Đăng xuất"): st.session_state['logged_in']=False; st.rerun()
    
    menu = ["🏠 Việc Của Tôi", "🔍 Tra Cứu", "📝 Tạo Hồ Sơ", "📊 Báo Cáo"]; 
    if role == "Quản lý": menu.insert(1, "💰 Công Nợ"); menu.append("👥 Nhân Sự")
    sel = st.sidebar.radio("Menu", menu)

    if sel == "🏠 Việc Của Tôi":
        st.title("📋 Tiến trình hồ sơ")
        df = get_all_jobs_df()
        if df.empty: st.info("Trống!")
        else:
            if role != "Quản lý": my_df = df[(df['assigned_to'].astype(str) == user) & (~df['status'].isin(['Hoàn thành', 'Kết thúc sớm']))]
            else: my_df = df[~df['status'].isin(['Hoàn thành', 'Kết thúc sớm'])]
            
            if my_df.empty: st.info("Hết việc!")
            else:
                now = datetime.now(); my_df['dl_dt'] = pd.to_datetime(my_df['deadline'])
                over = my_df[my_df['dl_dt'] < now]; soon = my_df[(my_df['dl_dt'] >= now) & (my_df['dl_dt'] <= now + timedelta(days=1))]
                k1, k2, k3, k4 = st.columns(4)
                k1.metric("🔴 Quá Hạn", len(over), border=True); k2.metric("🟡 Gấp", len(soon), border=True); k3.metric("🟢 Tổng", len(my_df), border=True); st.divider()

                for i, j in my_df.iterrows():
                    # [CẬP NHẬT] Lấy thủ tục từ Log để hiển thị lên Header
                    proc_name = extract_proc_from_log(j['logs'])
                    code_display = generate_full_name(j['id'], j['start_time'], j['customer_name'], j['customer_phone'], j['address'], proc_name)
                    
                    icon = "⛔" if j['status']=='Tạm dừng' else "⏹️" if j['status']=='Kết thúc sớm' else ("🔴" if j['dl_dt'] < now else "🟡" if j['dl_dt'] <= now+timedelta(days=1) else "🟢")
                    
                    with st.expander(f"{icon} {code_display} | {j['current_stage']}"):
                        render_progress_bar(j['current_stage'], j['status'])
                        t1, t2, t3, t4 = st.tabs(["ℹ️ Thông tin & File", "⚙️ Xử lý Hồ sơ", "💰 Tài Chính", "📜 Nhật ký"])
                        
                        with t1:
                            st.subheader(f"👤 {j['customer_name']}")
                            if safe_int(j.get('is_survey_only')) == 1: st.warning("🛠️ CHỈ ĐO ĐẠC")
                            # Hiển thị thêm thủ tục ở phần chi tiết
                            if proc_name: st.info(f"Thủ tục: {proc_name}")
                            
                            c1, c2 = st.columns(2); c1.write(f"📞 **{j['customer_phone']}**"); c2.write(f"📍 {j['address']}")
                            c1.write(f"⏰ Hạn: **{j['deadline']}**"); c2.write(f"Trạng thái: {j['status']}")
                            st.markdown("---"); st.markdown("**📂 File đính kèm:**")
                            
                            file_list = extract_files_from_log(j['logs'])
                            if j['file_link'] and j['file_link'] not in [lnk for _, lnk in file_list]:
                                file_list.insert(0, ("File gốc", j['file_link']))
                            
                            if not file_list: st.caption("Chưa có file.")
                            else:
                                for idx, (fname, link) in enumerate(file_list):
                                    file_id = get_drive_id(link)
                                    down_link = f"https://drive.google.com/uc?export=download&id={file_id}" if file_id else link
                                    
                                    with st.container(border=True):
                                        c_icon, c_name, c_act = st.columns([0.5, 4, 2])
                                        c_icon.markdown("📎")
                                        c_name.markdown(f"**{fname}**")
                                        col_v, col_d, col_x = c_act.columns(3)
                                        col_v.link_button("👁️", link, help="Xem")
                                        col_d.link_button("⬇️", down_link, help="Tải")
                                        if role == "Quản lý":
                                            with col_x.popover("🗑️", help="Xóa File"):
                                                st.write("Bạn chắc chắn xóa?")
                                                if st.button("Xóa ngay", key=f"del_{j['id']}_{idx}"):
                                                    delete_file_system(j['id'], link, fname)
                                                    st.toast("Đã xóa file!"); time.sleep(1); st.rerun()

                        with t2:
                            if j['status'] in ['Tạm dừng', 'Kết thúc sớm']:
                                st.error(f"HỒ SƠ ĐANG: {j['status'].upper()}")
                                if j['status'] == 'Tạm dừng' and st.button("▶️ Tiếp tục", key=f"r{j['id']}"): resume_job(j['id'], user); st.rerun()
                            else:
                                with st.form(f"f{j['id']}"):
                                    nt = st.text_area("Ghi chú")
                                    fl = st.file_uploader("Upload File", key=f"up_{j['id']}_{st.session_state['uploader_key']}")
                                    cur = j['current_stage']; nxt = "7. Hoàn thành" if safe_int(j.get('is_survey_only'))==1 and cur=="3. Làm hồ sơ" else WORKFLOW_DEFAULT.get(cur)
                                    if nxt and nxt!="7. Hoàn thành":
                                        st.write(f"Chuyển sang: **{nxt}**"); asn = st.selectbox("Giao", get_active_users_list()); d = st.number_input("Hạn (Ngày)", value=2)
                                    else: st.info("Kết thúc"); asn=""; d=0
                                    
                                    if st.form_submit_button("✅ Chuyển bước"): 
                                        dep = 1 if safe_int(j.get('deposit'))==1 else 0; money = safe_int(j.get('survey_fee')); pdone = 1 if safe_int(j.get('is_paid'))==1 else 0
                                        update_stage(j['id'], cur, nt, fl, user, asn, d, safe_int(j.get('is_survey_only')), dep, money, pdone)
                                        st.session_state['uploader_key'] += 1
                                        st.success("Xong!"); time.sleep(0.5); st.rerun()
                                
                                c_stop1, c_stop2 = st.columns(2)
                                if c_stop1.button("⏸️ Dừng", key=f"p{j['id']}"): st.session_state[f'pm_{j['id']}'] = True
                                if c_stop2.button("⏹️ Kết thúc", key=f"t{j['id']}"): st.session_state[f'tm_{j['id']}'] = True
                                
                                if st.session_state.get(f'pm_{j['id']}', False):
                                    rs = st.text_input("Lý do dừng:", key=f"rs{j['id']}"); 
                                    if st.button("OK", key=f"okp{j['id']}"): pause_job(j['id'], rs, user); st.rerun()
                                if st.session_state.get(f'tm_{j['id']}', False):
                                    rst = st.text_input("Lý do kết thúc:", key=f"rst{j['id']}"); 
                                    if st.button("OK", key=f"okt{j['id']}"): terminate_job(j['id'], rst, user); st.rerun()

                        with t3:
                            with st.form(f"money_{j['id']}"):
                                dep_ok = st.checkbox("Đã tạm ứng?", value=safe_int(j.get('deposit'))==1)
                                fee = st.number_input("Phí đo đạc", value=safe_int(j.get('survey_fee')), step=100000)
                                paid_ok = st.checkbox("Đã thu đủ?", value=safe_int(j.get('is_paid'))==1)
                                if st.form_submit_button("💾 Lưu"): update_finance_only(j['id'], dep_ok, fee, paid_ok, user); st.success("Lưu!"); st.rerun()

                        with t4:
                            raw_logs = str(j['logs']).split('\n')
                            for log_line in raw_logs:
                                if log_line.strip(): st.text(re.sub(r'\| File: .*', '', log_line))

    elif sel == "📝 Tạo Hồ Sơ":
        st.title("Tạo Hồ Sơ")
        with st.form("new"):
            c1, c2 = st.columns(2); n = c1.text_input("Tên Khách Hàng"); p = c2.text_input("SĐT"); 
            a = st.text_input("Địa chỉ")
            
            c3, c4 = st.columns([1, 1])
            with c3: is_sv = st.checkbox("🛠️ CHỈ ĐO ĐẠC")
            with c4: proc = st.selectbox("Thủ tục", PROCEDURES_LIST)
            
            f = st.file_uploader("File", key=f"new_up_{st.session_state['uploader_key']}")
            st.markdown("---"); st.write("💰 **Phí:**"); c_m1, c_m2 = st.columns(2); dep_ok = c_m1.checkbox("Đã tạm ứng?"); fee_val = c_m2.number_input("Phí:", value=0, step=100000)
            asn = st.selectbox("Giao:", get_active_users_list()); d = st.number_input("Hạn (Ngày)", value=1)
            
            if st.form_submit_button("Tạo Hồ Sơ"):
                if n and asn: 
                    add_job(n, p, a, proc, f, user, asn, d, is_sv, dep_ok, fee_val)
                    st.session_state['uploader_key'] += 1
                    st.success("OK! Đã lưu."); st.rerun()
                else: st.error("Thiếu thông tin quan trọng!")

    elif sel == "💰 Công Nợ":
        st.title("💰 Quản Lý Công Nợ")
        try:
            df = get_all_jobs_df()
            if not df.empty:
                unpaid = df[df['is_paid'].apply(safe_int) == 0]
                st.metric("Tổng hồ sơ chưa thu tiền", len(unpaid))
                if not unpaid.empty:
                    unpaid['Mã'] = unpaid.apply(lambda x: generate_full_name(x['id'], x['start_time'], x['customer_name']), axis=1)
                    st.dataframe(unpaid[['Mã', 'customer_phone', 'survey_fee', 'deposit']], use_container_width=True)
                else: st.success("Sạch nợ!")
        except: pass

    elif sel == "🔍 Tra Cứu":
        st.title("Tra Cứu"); q = st.text_input("Tìm kiếm")
        if q:
            df = get_all_jobs_df(); res = df[df.apply(lambda r: q.lower() in str(r).lower(), axis=1)]; st.dataframe(res)

    elif sel == "📊 Báo Cáo":
        st.title("Thống Kê"); df = get_all_jobs_df()
        if not df.empty: st.bar_chart(df['current_stage'].value_counts()); st.dataframe(df)
            
    elif sel == "👥 Nhân Sự":
        if role == "Quản lý":
            st.title("Phân Quyền"); df = get_all_users()
            for i, u in df.iterrows():
                c1, c2 = st.columns([2, 2]); c1.write(f"**{u['username']}** ({u['fullname']})")
                if u['username']!=user:
                    idx = ROLES.index(u['role']) if u['role'] in ROLES else 2
                    nr = c2.selectbox("Quyền", ROLES, index=idx, key=u['username'])
                    if nr!=u['role']: update_user_role(u['username'], nr); st.toast("Lưu!"); st.rerun()
        else: st.error("Cấm truy cập!")
