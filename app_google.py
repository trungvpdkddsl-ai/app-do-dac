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

# --- 1. CẤU HÌNH HỆ THỐNG ---
TELEGRAM_TOKEN = "8514665869:AAHUfTHgNlEEK_Yz6yYjZa-1iR645Cgr190"
TELEGRAM_CHAT_ID = "-5055192262" 
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# KEY KẾT NỐI
APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbyEMEGyS_sVCA4eyVRFXxnOuGqMnJOKOIqZqKxi4HpYBcpr7U72WUXCoKLm20BQomVC/exec"
DRIVE_FOLDER_ID = "1SrARuA1rgKLZmoObGor-GkNx33F6zNQy"

ROLES = ["Quản lý", "Nhân viên", "Chưa cấp quyền"]

# [CẬP NHẬT] QUY TRÌNH MỚI (THÊM HOÀN THIỆN TRÍCH ĐO)
STAGES_ORDER = ["1. Tạo mới", "2. Đo đạc", "3. Hoàn thiện trích đo", "4. Làm hồ sơ", "5. Ký hồ sơ", "6. Lấy hồ sơ", "7. Nộp hồ sơ", "8. Hoàn thành"]

WORKFLOW_DEFAULT = {
    "1. Tạo mới": "2. Đo đạc", 
    "2. Đo đạc": "3. Hoàn thiện trích đo", 
    "3. Hoàn thiện trích đo": "4. Làm hồ sơ",
    "4. Làm hồ sơ": "5. Ký hồ sơ", 
    "5. Ký hồ sơ": "6. Lấy hồ sơ", 
    "6. Lấy hồ sơ": "7. Nộp hồ sơ", 
    "7. Nộp hồ sơ": "8. Hoàn thành", 
    "8. Hoàn thành": None
}

PROCEDURES_LIST = ["Cấp lần đầu", "Cấp đổi", "Chuyển quyền"]

# [CẬP NHẬT] SLA (GIỜ)
# Tạo mới = 0 (Không tính hạn, chờ bấm)
# Hoàn thiện trích đo = 24h
STAGE_SLA_HOURS = {
    "1. Tạo mới": 0,       
    "2. Đo đạc": 48,       
    "3. Hoàn thiện trích đo": 24, # Mới
    "4. Làm hồ sơ": 24,    
    "5. Ký hồ sơ": 72,     
    "6. Lấy hồ sơ": 24,    
    "7. Nộp hồ sơ": 360,   
}

# --- 2. HÀM HỖ TRỢ & KẾT NỐI ---
def safe_int(value):
    try: return int(float(str(value).replace(",", "").replace(".", ""))) if pd.notna(value) and value != "" else 0
    except: return 0

def get_proc_abbr(proc_name):
    return {"Cấp lần đầu": "CLD", "Cấp đổi": "CD", "Chuyển quyền": "CQ"}.get(proc_name, "K")

def extract_proc_from_log(log_text):
    match = re.search(r'Khởi tạo \((.*?)\)', str(log_text))
    return match.group(1) if match else ""

def check_bottleneck(logs, current_stage):
    if current_stage == "8. Hoàn thành" or not logs: return False, 0, 0
    
    # Nếu bước hiện tại SLA = 0 (như Tạo mới) -> Không bao giờ quá hạn
    limit = STAGE_SLA_HOURS.get(current_stage, 0)
    if limit == 0: return False, 0, 0

    try:
        # Tìm mốc thời gian chuyển sang bước HIỆN TẠI gần nhất
        # Log mẫu: [2023-11-28 10:00:00] user: A->B
        matches = re.findall(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]', str(logs))
        if matches:
            last_dt = datetime.strptime(matches[-1], "%Y-%m-%d %H:%M:%S")
            hours_passed = int((datetime.now() - last_dt).total_seconds() / 3600)
            return hours_passed >= limit, hours_passed, limit
    except: pass
    return False, 0, 0

def generate_unique_name(jid, start_time, name, phone, addr, proc_name):
    try:
        jid_str = str(jid); seq = jid_str[-2:] 
        d_obj = datetime.strptime(str(start_time), "%Y-%m-%d %H:%M:%S")
        date_str = d_obj.strftime('%d%m%y')
    except: date_str = "000000"; seq = "00"
    abbr = get_proc_abbr(proc_name) if proc_name else ""
    proc_str = f"-{abbr}" if abbr else ""
    clean_phone = str(phone).replace("'", "")
    return f"{date_str}-{seq}{proc_str} {name} {clean_phone} {addr}"

def extract_files_from_log(log_text):
    pattern = r"File: (.*?) - (https?://[^\s]+)"
    matches = re.findall(pattern, str(log_text))
    if not matches:
        raw_links = re.findall(r'(https?://[^\s]+)', str(log_text))
        return [("File cũ", l) for l in raw_links]
    return matches

def render_contact_buttons(phone):
    if not phone: return ""
    clean_phone = re.sub(r'\D', '', str(phone))
    if len(clean_phone) < 9: return f"<span style='color: gray;'>SĐT: {phone}</span>"
    zalo_link = f"https://zalo.me/{clean_phone}"; call_link = f"tel:{clean_phone}"
    return f"""<div style="display: flex; gap: 10px; margin-bottom: 10px;"><a href="{zalo_link}" target="_blank" style="text-decoration: none;"><div style="background-color: #0068FF; color: white; padding: 6px 12px; border-radius: 6px; font-weight: bold; font-size: 14px;">💬 Chat Zalo</div></a><a href="{call_link}" style="text-decoration: none;"><div style="background-color: #28a745; color: white; padding: 6px 12px; border-radius: 6px; font-weight: bold; font-size: 14px;">📞 Gọi Điện</div></a></div>"""

def calculate_deadline(start_date, hours_to_add):
    # Tính deadline dựa trên giờ làm việc (đơn giản hóa: cộng thẳng giờ, bỏ qua T7 CN nếu cần phức tạp hơn)
    # Ở đây cộng thẳng để đơn giản, nếu cần chính xác T7 CN thì dùng hàm cũ nhưng đổi đơn vị sang giờ
    # Nếu SLA = 0 -> Không có deadline
    if hours_to_add == 0: return None
    
    current_date = start_date
    added_hours = 0
    # Cộng từng giờ một, né T7 CN
    while added_hours < hours_to_add:
        current_date += timedelta(hours=1)
        if current_date.weekday() < 5: # 0-4 là T2-T6
            added_hours += 1
    return current_date

def get_drive_id(link):
    try: match = re.search(r'/d/([a-zA-Z0-9_-]+)', link); return match.group(1) if match else None
    except: return None

def get_gcp_creds(): 
    return Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)

def get_sheet(sheet_name="DB_DODAC"):
    try: creds = get_gcp_creds(); client = gspread.authorize(creds); return client.open(sheet_name).sheet1
    except: return None

def get_users_sheet():
    try:
        creds = get_gcp_creds(); client = gspread.authorize(creds); sh = client.open("DB_DODAC")
        try: return sh.worksheet("USERS")
        except: 
            ws = sh.add_worksheet(title="USERS", rows="100", cols="5")
            ws.append_row(["username", "password", "fullname", "role"]); return ws
    except: return None

def get_audit_sheet():
    try:
        creds = get_gcp_creds(); client = gspread.authorize(creds); sh = client.open("DB_DODAC")
        try: return sh.worksheet("AUDIT_LOGS")
        except: 
            ws = sh.add_worksheet(title="AUDIT_LOGS", rows="1000", cols="4")
            ws.append_row(["Timestamp", "User", "Action", "Details"]); return ws
    except: return None

def log_to_audit(user, action, details):
    def _log():
        try:
            ws = get_audit_sheet()
            if ws: ws.append_row([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user, action, details])
        except: pass
    threading.Thread(target=_log).start()

def upload_to_drive(file_obj, sub_folder_name):
    if not file_obj: return None, None
    try:
        file_content = file_obj.read()
        file_base64 = base64.b64encode(file_content).decode('utf-8')
        payload = {"filename": file_obj.name, "mime_type": file_obj.type, "file_base64": file_base64, "folder_id": DRIVE_FOLDER_ID, "sub_folder_name": sub_folder_name}
        response = requests.post(APPS_SCRIPT_URL, json=payload)
        if response.status_code == 200:
            res_json = response.json()
            if res_json.get("status") == "success": return res_json.get("link"), file_obj.name
    except: pass
    return None, None

def find_row_index(sh, jid):
    try: ids = sh.col_values(1); return ids.index(str(jid)) + 1
    except: return None

def delete_file_system(job_id, file_link, file_name, user):
    file_id = get_drive_id(file_link)
    if file_id:
        try: requests.post(APPS_SCRIPT_URL, json={"action": "delete", "file_id": file_id})
        except: pass
    sh = get_sheet(); r = find_row_index(sh, job_id)
    if r:
        current_log = sh.cell(r, 11).value
        pattern = r"(\s*\|\s*)?File: .*? - " + re.escape(file_link)
        new_log = re.sub(pattern, "", str(current_log))
        sh.update_cell(r, 11, new_log)
        if sh.cell(r, 10).value == file_link: sh.update_cell(r, 10, "")
        log_to_audit(user, "DELETE_FILE", f"Job {job_id}: Deleted file {file_name}")

def make_hash(p): return hashlib.sha256(str.encode(p)).hexdigest()

def send_telegram_msg(msg):
    if not TELEGRAM_TOKEN: return
    def run(): 
        try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"})
        except Exception as e: print(f"Tele Error: {e}")
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
        sh.append_row([u, make_hash(p), n, "Chưa cấp quyền"]); return True
    except: return False

@st.cache_data(ttl=60)
def get_all_users_cached():
    sh = get_users_sheet()
    if sh is None: return pd.DataFrame()
    return pd.DataFrame(sh.get_all_records())

def get_all_users(): return get_all_users_cached()
def update_user_role(u, r): sh = get_users_sheet(); c = sh.find(u); sh.update_cell(c.row, 4, r); get_all_users_cached.clear()
def get_active_users_list(): 
    df = get_all_users_cached()
    if df.empty: return []
    return df[df['role']!='Chưa cấp quyền'].apply(lambda x: f"{x['username']} - {x['fullname']}", axis=1).tolist()

def get_all_jobs_df():
    sh = get_sheet(); 
    if sh is None: return pd.DataFrame()
    data = sh.get_all_records(); df = pd.DataFrame(data)
    if not df.empty:
        df['id'] = df['id'].apply(safe_int)
        if 'deposit' not in df.columns: df['deposit'] = 0
        if 'survey_fee' not in df.columns: df['survey_fee'] = 0
        if 'is_paid' not in df.columns: df['is_paid'] = 0
        if 'file_link' not in df.columns: df['file_link'] = ""
        if 'start_time' in df.columns: df['start_dt'] = pd.to_datetime(df['start_time'], errors='coerce').dt.date
    return df

def get_daily_sequence_id():
    df = get_all_jobs_df(); now = datetime.now(); prefix = int(now.strftime('%y%m%d')) 
    if df.empty: return int(f"{prefix}01"), "01"
    today_ids = [str(jid) for jid in df['id'].tolist() if str(jid).startswith(str(prefix))]
    if not today_ids: seq = 1
    else: max_seq = max([int(jid[-2:]) for jid in today_ids]); seq = max_seq + 1
    return int(f"{prefix}{seq:02}"), f"{seq:02}"

# --- 3. LOGIC NGHIỆP VỤ ---
def add_job(n, p, a, proc, f, u, asn, d, is_survey, deposit_ok, fee_amount):
    sh = get_sheet(); now = datetime.now(); now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    date_code = now.strftime('%d%m%Y')
    
    # [CẬP NHẬT] Tạo mới = Không có deadline (d=0) hoặc nếu có thì tính theo SLA
    # Nhưng theo yêu cầu: Tạo mới không có thời gian -> Bắt buộc bấm chuyển
    # Ta để d=0 (Không có hạn) hoặc set deadline rất xa
    
    jid, seq_str = get_daily_sequence_id()
    phone_db = f"'{p}" 
    full_name_str = generate_unique_name(jid, now_str, n, p, a, proc)
    
    link = ""; fname = ""; log_file_str = ""
    if f: 
        for uploaded_file in f:
            l, n_f = upload_to_drive(uploaded_file, full_name_str)
            if l: log_file_str += f" | File: {n_f} - {l}"; link = l; fname = n_f

    assign_info = f" -> Giao: {asn.split(' - ')[0]}" if asn else ""
    log = f"[{now_str}] {u}: Khởi tạo ({proc}){assign_info}{log_file_str}"
    
    asn_clean = asn.split(" - ")[0] if asn else ""
    sv_flag = 1 if is_survey else 0; dep_flag = 1 if deposit_ok else 0
    
    # Lưu Deadline rỗng cho bước Tạo mới (hoặc +365 ngày để không báo đỏ)
    dl = (now + timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
    
    sh.append_row([jid, now_str, n, phone_db, a, "1. Tạo mới", "Đang xử lý", asn_clean, dl, link, log, sv_flag, dep_flag, fee_amount, 0])
    log_to_audit(u, "CREATE_JOB", f"ID: {jid}, Name: {n}")
    
    type_msg = f"({proc.upper()})"
    money_msg = "✅ Đã thu tạm ứng" if deposit_ok else "❌ Chưa thu tạm ứng"
    file_msg = f"\n📎 Có {len(f)} file đính kèm" if f else ""
    assign_msg = f"👉 <b>{asn_clean}</b>"
    send_telegram_msg(f"🚀 <b>MỚI #{seq_str} {type_msg}</b>\n📂 <b>{full_name_str}</b>\n{assign_msg}\n💰 {money_msg}{file_msg}")

# [CẬP NHẬT] Bỏ tham số 'd' (Hạn thủ công)
def update_stage(jid, stg, nt, f_list, u, asn, is_survey, deposit_ok, fee_amount, is_paid, result_date=None):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        row_data = sh.row_values(r)
        proc_name = extract_proc_from_log(row_data[10])
        full_code = generate_unique_name(jid, row_data[1], row_data[2], row_data[3], row_data[4], proc_name)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_file_str = ""
        if f_list:
            for uploaded_file in f_list:
                l, n_f = upload_to_drive(uploaded_file, full_code)
                if l: log_file_str += f" | File: {n_f} - {l}"

        nxt = "8. Hoàn thành" if is_survey==1 and stg=="4. Làm hồ sơ" else WORKFLOW_DEFAULT.get(stg)
        
        if nxt:
            sh.update_cell(r, 6, nxt)
            assign_str = ""; assign_tele = ""
            if asn: 
                assign_clean = asn.split(" - ")[0]; sh.update_cell(r, 8, assign_clean)
                assign_str = f" -> Giao: {assign_clean}"; assign_tele = f"\n👉 Giao: <b>{assign_clean}</b>"
            
            # [LOGIC SLA TỰ ĐỘNG]
            if result_date:
                new_deadline = result_date.strftime("%Y-%m-%d %H:%M:%S")
                sh.update_cell(r, 9, new_deadline); nt += f" (Hẹn trả: {result_date.strftime('%d/%m/%Y')})"
            else:
                # Tính deadline dựa trên SLA cấu hình
                hours_to_add = STAGE_SLA_HOURS.get(nxt, 24)
                if hours_to_add > 0:
                    new_dl = calculate_deadline(datetime.now(), hours_to_add)
                    sh.update_cell(r, 9, new_dl.strftime("%Y-%m-%d %H:%M:%S"))
                else:
                    # Nếu bước tiếp theo là 0 giờ (chờ bấm), set xa để không báo đỏ
                    new_dl = datetime.now() + timedelta(days=365)
                    sh.update_cell(r, 9, new_dl.strftime("%Y-%m-%d %H:%M:%S"))
            
            sh.update_cell(r, 13, 1 if deposit_ok else 0); sh.update_cell(r, 14, safe_int(fee_amount)); sh.update_cell(r, 15, 1 if is_paid else 0)
            olog = sh.cell(r, 11).value
            nlog = f"\n[{now}] {u}: {stg}->{nxt}{assign_str} | Note: {nt}{log_file_str}"
            sh.update_cell(r, 11, olog + nlog)
            if nxt=="8. Hoàn thành": sh.update_cell(r, 7, "Hoàn thành")
            
            log_to_audit(u, "UPDATE_STAGE", f"ID: {jid}, {stg} -> {nxt}")
            send_telegram_msg(f"✅ <b>CẬP NHẬT</b>\n📂 <b>{full_code}</b>\n{stg} ➡ <b>{nxt}</b>\n👤 {u}{assign_tele}")

def return_to_previous_stage(jid, current_stage, reason, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        try:
            curr_idx = STAGES_ORDER.index(current_stage)
            if curr_idx > 0:
                prev_stage = STAGES_ORDER[curr_idx - 1]
                sh.update_cell(r, 6, prev_stage)
                
                # [MỚI] TÍNH LẠI THỜI GIAN KHI TRẢ HỒ SƠ
                # Reset deadline cho bước trước đó để không bị báo quá hạn oan
                hours_to_add = STAGE_SLA_HOURS.get(prev_stage, 24)
                new_dl = calculate_deadline(datetime.now(), hours_to_add)
                if new_dl: sh.update_cell(r, 9, new_dl.strftime("%Y-%m-%d %H:%M:%S"))
                
                olog = sh.cell(r, 11).value
                nlog = f"\n[{datetime.now()}] {u}: ⬅️ TRẢ HỒ SƠ ({current_stage} -> {prev_stage}) | Lý do: {reason}"
                sh.update_cell(r, 11, olog + nlog)
                row_data = sh.row_values(r)
                full_code = generate_unique_name(jid, row_data[1], row_data[2], row_data[3], row_data[4], extract_proc_from_log(row_data[10]))
                log_to_audit(u, "RETURN_JOB", f"ID: {jid}, {current_stage} -> {prev_stage}")
                send_telegram_msg(f"↩️ <b>TRẢ HỒ SƠ</b>\n📂 <b>{full_code}</b>\n{current_stage} ➡ <b>{prev_stage}</b>\n👤 Bởi: {u}\n⚠️ Lý do: {reason}")
                return True
        except: return False
    return False

# [MỚI] HÀM SỬA THÔNG TIN KHÁCH HÀNG (ADMIN)
def update_customer_info(jid, new_name, new_phone, new_addr, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        # Cập nhật DB
        sh.update_cell(r, 3, new_name)
        sh.update_cell(r, 4, f"'{new_phone}") # Giữ số 0
        sh.update_cell(r, 5, new_addr)
        
        # Ghi log
        olog = sh.cell(r, 11).value
        nlog = f"\n[{datetime.now()}] {u}: ✏️ ADMIN SỬA THÔNG TIN KHÁCH"
        sh.update_cell(r, 11, olog + nlog)
        
        log_to_audit(u, "EDIT_INFO", f"ID: {jid}")
        st.toast("Đã cập nhật thông tin!")

def update_finance_only(jid, deposit_ok, fee_amount, is_paid, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        row_data = sh.row_values(r)
        full_code = generate_unique_name(jid, row_data[1], row_data[2], row_data[3], row_data[4], extract_proc_from_log(row_data[10]))
        sh.update_cell(r, 13, 1 if deposit_ok else 0); sh.update_cell(r, 14, safe_int(fee_amount)); sh.update_cell(r, 15, 1 if is_paid else 0)
        log_to_audit(u, "UPDATE_FINANCE", f"ID: {jid}, Fee: {fee_amount}")
        send_telegram_msg(f"💰 <b>TÀI CHÍNH</b>\n📂 <b>{full_code}</b>\n👤 {u}\nPhí: {fee_amount:,} VNĐ")

def pause_job(jid, rs, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        row_data = sh.row_values(r)
        full_code = generate_unique_name(jid, row_data[1], row_data[2], row_data[3], row_data[4], extract_proc_from_log(row_data[10]))
        sh.update_cell(r, 7, "Tạm dừng")
        olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{datetime.now()}] {u}: TẠM DỪNG: {rs}")
        log_to_audit(u, "PAUSE_JOB", f"ID: {jid}")
        send_telegram_msg(f"⛔ <b>TẠM DỪNG</b>\n📂 <b>{full_code}</b>\n👤 Bởi: {u}\n📝 Lý do: {rs}")

def resume_job(jid, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        row_data = sh.row_values(r)
        full_code = generate_unique_name(jid, row_data[1], row_data[2], row_data[3], row_data[4], extract_proc_from_log(row_data[10]))
        sh.update_cell(r, 7, "Đang xử lý")
        olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{datetime.now()}] {u}: KHÔI PHỤC")
        log_to_audit(u, "RESUME_JOB", f"ID: {jid}")
        send_telegram_msg(f"▶️ <b>KHÔI PHỤC</b>\n📂 <b>{full_code}</b>\n👤 Bởi: {u}")

def terminate_job(jid, rs, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        row_data = sh.row_values(r)
        full_code = generate_unique_name(jid, row_data[1], row_data[2], row_data[3], row_data[4], extract_proc_from_log(row_data[10]))
        sh.update_cell(r, 7, "Kết thúc sớm")
        olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{datetime.now()}] {u}: KẾT THÚC SỚM: {rs}")
        log_to_audit(u, "TERMINATE_JOB", f"ID: {jid}")
        send_telegram_msg(f"⏹️ <b>KẾT THÚC SỚM</b>\n📂 <b>{full_code}</b>\n👤 Bởi: {u}\n📝 Lý do: {rs}")

def move_to_trash(jid, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        sh.update_cell(r, 7, "Đã xóa"); log_to_audit(u, "MOVE_TO_TRASH", f"ID: {jid}"); st.toast("Đã chuyển vào thùng rác!")

def restore_from_trash(jid, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        sh.update_cell(r, 7, "Đang xử lý"); log_to_audit(u, "RESTORE_JOB", f"ID: {jid}"); st.toast("Đã khôi phục hồ sơ!")

def delete_forever(jid, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        sh.delete_rows(r); log_to_audit(u, "DELETE_FOREVER", f"ID: {jid}"); st.toast("Đã xóa vĩnh viễn!")

def scan_bottlenecks(df):
    bottlenecks = []
    for _, j in df.iterrows():
        is_stuck, hours, limit = check_bottleneck(j['logs'], j['current_stage'])
        if is_stuck and j['status'] == "Đang xử lý":
            proc_name = extract_proc_from_log(j['logs'])
            name = generate_unique_name(j['id'], j['start_time'], j['customer_name'], "", "", proc_name)
            bottlenecks.append(f"⚠️ **{name}**\n- Kẹt ở: {j['current_stage']}\n- Thời gian: {hours}h (Giới hạn: {limit}h)")
    return bottlenecks

# --- 4. UI COMPONENTS ---
def render_progress_bar(current_stage, status):
    try: idx = STAGES_ORDER.index(current_stage)
    except: idx = 0
    color = "#dc3545" if status in ["Tạm dừng", "Kết thúc sớm", "Đã xóa"] else "#ffc107"
    st.markdown(f"""<style>.step-container {{display: flex; justify-content: space-between; margin-bottom: 15px;}} .step-item {{flex: 1; text-align: center; position: relative;}} .step-item:not(:last-child)::after {{content: ''; position: absolute; top: 15px; left: 50%; width: 100%; height: 2px; background: #e0e0e0; z-index: -1;}} .step-circle {{width: 30px; height: 30px; margin: 0 auto 5px; border-radius: 50%; line-height: 30px; color: white; font-weight: bold; font-size: 12px;}} .done {{background: #28a745;}} .active {{background: {color}; color: black;}} .pending {{background: #e9ecef; color: #999;}}</style>""", unsafe_allow_html=True)
    h = '<div class="step-container">'; 
    for i, s in enumerate(STAGES_ORDER):
        cls = "done" if i < idx else "active" if i == idx else "pending"
        ico = "✓" if i < idx else str(i+1)
        if i == idx and status == "Tạm dừng": ico = "⛔"
        if i == idx and status == "Kết thúc sớm": ico = "⏹️"
        h += f'<div class="step-item"><div class="step-circle {cls}">{ico}</div><div style="font-size:11px">{s.split(". ")[1]}</div></div>'
    st.markdown(h+'</div>', unsafe_allow_html=True)

def render_job_card(j, user, role, user_list):
    proc_name = extract_proc_from_log(j['logs'])
    code_display = generate_unique_name(j['id'], j['start_time'], j['customer_name'], j['customer_phone'], j['address'], proc_name)
    now = datetime.now()
    
    # Logic hiển thị hạn
    try: dl_dt = pd.to_datetime(j['deadline'])
    except: dl_dt = now + timedelta(days=365)
    
    dl_status = "Đang xử lý"
    # Nếu bước hiện tại là Tạo mới hoặc Hoàn thành thì không báo quá hạn
    if j['current_stage'] in ["1. Tạo mới", "8. Hoàn thành"]:
        dl_str = "Không có hạn"
        icon = "🟢"
    else:
        dl_str = dl_dt.strftime("%d/%m/%Y %H:%M")
        dl_status = "HÔM NAY" if dl_dt.date() == now.date() else f"Còn {(dl_dt - now).days} ngày"
        if dl_dt < now: dl_status = "QUÁ HẠN"
        icon = "⛔" if j['status']=='Tạm dừng' else "⏹️" if j['status']=='Kết thúc sớm' else ("🔴" if dl_dt < now else "🟡" if dl_dt <= now+timedelta(days=1) else "🟢")
    
    # Cảnh báo điểm nghẽn
    is_stuck, hours, limit = check_bottleneck(j['logs'], j['current_stage'])
    stuck_alert = f" | ⚠️ KẸT {hours}H" if is_stuck and j['status'] == "Đang xử lý" else ""
    
    with st.expander(f"{icon} {code_display} | {j['current_stage']}{stuck_alert}"):
        if is_stuck and j['status'] == "Đang xử lý": st.error(f"⚠️ **CẢNH BÁO CHẬM:** Hồ sơ kẹt {hours}h (Max {limit}h)")
        else: st.info(f"⏱️ Thời gian xử lý: **{hours}h** / Định mức: {limit}h")

        if j['current_stage'] not in ["1. Tạo mới", "8. Hoàn thành"]:
            st.write(f"📅 **Hạn bước này: {dl_str}** | Trạng thái: **{dl_status}**")
            
        render_progress_bar(j['current_stage'], j['status'])
        t1, t2, t3, t4 = st.tabs(["ℹ️ Thông tin & File", "⚙️ Xử lý Hồ sơ", "💰 Tài Chính", "📜 Nhật ký"])
        
        with t1:
            st.subheader(f"👤 {j['customer_name']}")
            
            # [MỚI] ADMIN SỬA THÔNG TIN
            if role == "Quản lý":
                with st.popover("✏️ Sửa Thông Tin"):
                    new_n = st.text_input("Tên", j['customer_name'])
                    new_p = st.text_input("SĐT", j['customer_phone'])
                    new_a = st.text_input("Địa chỉ", j['address'])
                    if st.button("Lưu Thay Đổi", key=f"edit_{j['id']}"):
                        update_customer_info(j['id'], new_n, new_p, new_a, user)
                        time.sleep(1); st.rerun()

            if safe_int(j.get('is_survey_only')) == 1: st.warning("🛠️ CHỈ ĐO ĐẠC")
            if proc_name: st.info(f"Thủ tục: {proc_name}")
            st.markdown(render_contact_buttons(j['customer_phone']), unsafe_allow_html=True)
            c1, c2 = st.columns(2); c1.caption(f"📞 SĐT Gốc: {j['customer_phone']}"); c2.write(f"📍 {j['address']}")
            st.markdown("---"); st.markdown("**📂 File đính kèm:**")
            file_list = extract_files_from_log(j['logs'])
            if j['file_link'] and j['file_link'] not in [lnk for _, lnk in file_list]: file_list.insert(0, ("File gốc", j['file_link']))
            if not file_list: st.caption("Chưa có file.")
            else:
                for idx, (fname, link) in enumerate(file_list):
                    file_id = get_drive_id(link)
                    down_link = f"https://drive.google.com/uc?export=download&id={file_id}" if file_id else link
                    with st.container(border=True):
                        c_icon, c_name, c_act = st.columns([0.5, 4, 2])
                        c_icon.markdown("📎"); c_name.markdown(f"**{fname}**")
                        col_v, col_d, col_x = c_act.columns(3)
                        col_v.link_button("👁️", link, help="Xem"); col_d.link_button("⬇️", down_link, help="Tải")
                        if role == "Quản lý":
                            with col_x.popover("🗑️", help="Xóa File"):
                                st.write("Xóa file này?")
                                if st.button("Xóa ngay", key=f"del_{j['id']}_{idx}_{int(time.time())}"):
                                    delete_file_system(j['id'], link, fname, user); st.toast("Đã xóa file!"); time.sleep(1); st.rerun()
            if role == "Quản lý":
                st.divider()
                with st.container():
                    with st.popover("🗑️ Xóa Hồ Sơ (Vào thùng rác)", use_container_width=True):
                        st.warning("Hồ sơ sẽ được chuyển vào Thùng Rác.")
                        if st.button("XÁC NHẬN XÓA", key=f"soft_del_{j['id']}", type="primary"):
                            move_to_trash(j['id'], user); time.sleep(1); st.rerun()

        with t2:
            if j['status'] in ['Tạm dừng', 'Kết thúc sớm']:
                st.error(f"HỒ SƠ ĐANG: {j['status'].upper()}")
                if j['status'] == 'Tạm dừng' and st.button("▶️ Tiếp tục", key=f"r{j['id']}"): resume_job(j['id'], user); st.rerun()
            else:
                with st.form(f"f{j['id']}"):
                    nt = st.text_area("Ghi chú")
                    fl = st.file_uploader("Upload File", accept_multiple_files=True, key=f"up_{j['id']}_{st.session_state['uploader_key']}")
                    cur = j['current_stage']; nxt = "8. Hoàn thành" if safe_int(j.get('is_survey_only'))==1 and cur=="4. Làm hồ sơ" else WORKFLOW_DEFAULT.get(cur)
                    result_date = None
                    if nxt and nxt!="8. Hoàn thành":
                        st.write(f"Chuyển sang: **{nxt}**")
                        if cur == "7. Nộp hồ sơ":
                            st.info("📅 Thông tin nộp hồ sơ")
                            col_n1, col_n2 = st.columns(2)
                            submit_date = col_n1.date_input("Ngày nộp", datetime.now())
                            result_date = col_n2.date_input("Ngày trả kết quả", datetime.now() + timedelta(days=15))
                            asn = st.selectbox("Giao theo dõi", user_list); d = 0 
                        else:
                            asn = st.selectbox("Giao", user_list)
                            # [MỚI] Tự động tính hạn SLA, không cho nhập tay
                            sla = STAGE_SLA_HOURS.get(nxt, 0)
                            if sla > 0: st.caption(f"⏱️ Thời hạn quy định cho bước sau: {sla} giờ")
                            else: st.caption("⏱️ Bước sau không quy định thời hạn.")
                            d = 0 # Dummy
                    else: st.info("Kết thúc"); asn=""; d=0
                    if st.form_submit_button("✅ Chuyển bước"): 
                        dep = 1 if safe_int(j.get('deposit'))==1 else 0; money = safe_int(j.get('survey_fee')); pdone = 1 if safe_int(j.get('is_paid'))==1 else 0
                        update_stage(j['id'], cur, nt, fl, user, asn, d, safe_int(j.get('is_survey_only')), dep, money, pdone, result_date)
                        st.session_state['uploader_key'] += 1; st.success("Xong!"); time.sleep(0.5); st.rerun()
                
                c_stop1, c_stop2, c_back = st.columns([1, 1, 1])
                if c_stop1.button("⏸️ Dừng", key=f"p{j['id']}"): st.session_state[f'pm_{j['id']}'] = True
                if c_stop2.button("⏹️ Kết thúc", key=f"t{j['id']}"): st.session_state[f'tm_{j['id']}'] = True
                with c_back.popover("⬅️ Trả hồ sơ", use_container_width=True):
                    reason_back = st.text_input("Lý do:", key=f"reason_back_{j['id']}")
                    if st.button("Xác nhận", key=f"btn_back_{j['id']}"):
                        if return_to_previous_stage(j['id'], j['current_stage'], reason_back, user): st.success("Đã trả hồ sơ!"); time.sleep(1); st.rerun()
                        else: st.error("Lỗi!")

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

# --- 8. UI MAIN ---
st.set_page_config(page_title="Đo Đạc Cloud Pro", page_icon="☁️", layout="wide")
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'uploader_key' not in st.session_state: st.session_state['uploader_key'] = 0
if 'job_filter' not in st.session_state: st.session_state['job_filter'] = 'all'

if not st.session_state['logged_in']:
    st.title("🔐 CỔNG ĐĂNG NHẬP")
    c1, c2 = st.columns(2)
    # [CẬP NHẬT] Giao diện đăng nhập rõ ràng
    with c1:
        st.subheader("Đăng Nhập")
        u = st.text_input("User", key="login_u"); p = st.text_input("Pass", type='password', key="login_p")
        if st.button("Đăng Nhập", type="primary"):
            d = login_user(u, p)
            if d: st.session_state['logged_in']=True; st.session_state['user']=d[0]; st.session_state['role']=d[3]; st.rerun()
            else: st.error("Sai thông tin!")
    with c2:
        st.subheader("Đăng Ký Mới")
        nu = st.text_input("User Mới", key="reg_u"); np = st.text_input("Pass Mới", type='password', key="reg_p"); nn = st.text_input("Họ Tên", key="reg_n")
        if st.button("Đăng Ký"): 
            if create_user(nu, np, nn): st.success("OK! Chờ duyệt.")
            else: st.error("Trùng tên!")
else:
    user = st.session_state['user']; role = st.session_state['role']
    st.sidebar.title(f"👤 {user}"); st.sidebar.info(f"{role}")
    if st.sidebar.button("Đăng xuất"): st.session_state['logged_in']=False; st.rerun()
    
    menu = ["🏠 Việc Của Tôi", "🔍 Tra Cứu", "📝 Tạo Hồ Sơ", "📊 Báo Cáo"]
    if role == "Quản lý": menu.extend(["💰 Công Nợ", "👥 Nhân Sự", "🗑️ Thùng Rác", "🛡️ Nhật Ký"])
    sel = st.sidebar.radio("Menu", menu)

    user_list = get_active_users_list()

    if sel == "🏠 Việc Của Tôi":
        c_note, c_title = st.columns([1, 2])
        df = get_all_jobs_df()
        if df.empty: 
            with c_title: st.title("📋 Tiến trình hồ sơ")
            st.info("Trống!")
        else:
            active_df = df[df['status'] != 'Đã xóa']
            if role != "Quản lý": my_df = active_df[(active_df['assigned_to'].astype(str) == user) & (~active_df['status'].isin(['Hoàn thành', 'Kết thúc sớm']))]
            else: my_df = active_df[~active_df['status'].isin(['Hoàn thành', 'Kết thúc sớm'])]
            
            now = datetime.now()
            # Xử lý ngày tháng an toàn
            my_df['dl_dt'] = pd.to_datetime(my_df['deadline'], errors='coerce')
            # Nếu không có deadline (NaT), set xa để không báo đỏ
            my_df['dl_dt'] = my_df['dl_dt'].fillna(now + timedelta(days=365))
            
            warning_window = now + timedelta(hours=48)
            # Chỉ cảnh báo những hồ sơ có deadline hợp lệ (không phải xa tít) và sắp hết hạn
            warning_jobs = my_df[(my_df['dl_dt'] > now) & (my_df['dl_dt'] <= warning_window) & (my_df['dl_dt'] < now + timedelta(days=300))]
            
            if role == "Quản lý":
                if c_note.button("🔍 Quét Hồ Sơ Chậm (Bottleneck)"):
                    issues = scan_bottlenecks(active_df)
                    if issues:
                        msg = "🚨 **CẢNH BÁO ĐIỂM NGHẼN:**\n\n" + "\n\n".join(issues)
                        send_telegram_msg(msg)
                        st.toast(f"Đã gửi {len(issues)} cảnh báo qua Telegram!")
                    else: st.toast("Hệ thống hoạt động tốt, không có điểm nghẽn.")

            with c_note:
                if not warning_jobs.empty:
                    st.warning(f"🔔 **Sắp hết hạn: {len(warning_jobs)} hồ sơ (48h)**")
            
            with c_title: st.title("📋 Tiến trình hồ sơ")

            if my_df.empty: st.info("Hết việc!")
            else:
                over = my_df[my_df['dl_dt'] < now]
                soon = my_df[(my_df['dl_dt'] >= now) & (my_df['dl_dt'] <= now + timedelta(days=1))] 
                k1, k2, k3 = st.columns(3)
                if k1.button(f"🔴 Quá Hạn ({len(over)})", use_container_width=True): st.session_state['job_filter'] = 'overdue'
                if k2.button(f"🟡 Gấp ({len(soon)})", use_container_width=True): st.session_state['job_filter'] = 'urgent'
                if k3.button(f"🟢 Tổng ({len(my_df)})", use_container_width=True): st.session_state['job_filter'] = 'all'
                
                if st.session_state['job_filter'] == 'overdue': display_df = over
                elif st.session_state['job_filter'] == 'urgent': display_df = soon
                else: display_df = my_df
                
                st.divider()
                st.caption(f"Đang hiển thị: {st.session_state['job_filter'].upper()} ({len(display_df)} hồ sơ)")
                for i, j in display_df.iterrows(): render_job_card(j, user, role, user_list)

    # ... (Các phần khác giữ nguyên)
    elif sel == "📝 Tạo Hồ Sơ":
        st.title("Tạo Hồ Sơ")
        with st.form("new"):
            c1, c2 = st.columns(2); n = c1.text_input("Tên Khách Hàng"); p = c2.text_input("SĐT"); a = st.text_input("Địa chỉ")
            c3, c4 = st.columns([1, 1])
            with c3: is_sv = st.checkbox("🛠️ CHỈ ĐO ĐẠC")
            with c4: proc = st.selectbox("Thủ tục", PROCEDURES_LIST)
            f = st.file_uploader("File (Có thể chọn nhiều)", accept_multiple_files=True, key=f"new_up_{st.session_state['uploader_key']}")
            st.markdown("---"); st.write("💰 **Phí:**"); c_m1, c_m2 = st.columns(2); dep_ok = c_m1.checkbox("Đã tạm ứng?"); fee_val = c_m2.number_input("Phí:", value=0, step=100000)
            asn = st.selectbox("Giao:", user_list); d = st.number_input("Hạn (Ngày)", value=1)
            if st.form_submit_button("Tạo Hồ Sơ"):
                if n and asn: 
                    add_job(n, p, a, proc, f, user, asn, d, is_sv, dep_ok, fee_val)
                    st.session_state['uploader_key'] += 1; st.success("OK! Hồ sơ mới đã tạo."); st.rerun()
                else: st.error("Thiếu thông tin!")

    elif sel == "💰 Công Nợ":
        st.title("💰 Quản Lý Công Nợ")
        try:
            df = get_all_jobs_df()
            active_df = df[df['status'] != 'Đã xóa']
            if not active_df.empty:
                unpaid = active_df[active_df['is_paid'].apply(safe_int) == 0]
                st.metric("Tổng hồ sơ chưa thu tiền", len(unpaid))
                if not unpaid.empty:
                    unpaid['Mã'] = unpaid.apply(lambda x: generate_unique_name(x['id'], x['start_time'], x['customer_name'], x['customer_phone'], x['address'], extract_proc_from_log(x['logs'])), axis=1)
                    st.dataframe(unpaid[['Mã', 'survey_fee', 'deposit']], use_container_width=True)
                else: st.success("Sạch nợ!")
        except: pass

    elif sel == "🔍 Tra Cứu":
        st.title("Tra Cứu Thông Minh")
        with st.container(border=True):
            c_filter_1, c_filter_2 = st.columns([2, 1])
            with c_filter_1: q = st.text_input("🔍 Từ khóa", placeholder="Ví dụ: 271125, Lê Trung...")
            with c_filter_2:
                today = date.today(); first_day = today.replace(day=1)
                date_range = st.date_input("📅 Khoảng thời gian", (first_day, today), format="DD/MM/YYYY")
        df = get_all_jobs_df()
        active_df = df[df['status'] != 'Đã xóa']
        if not active_df.empty and 'start_dt' in active_df.columns:
            if isinstance(date_range, tuple) and len(date_range) == 2:
                start_d, end_d = date_range; mask_date = (active_df['start_dt'] >= start_d) & (active_df['start_dt'] <= end_d)
                filtered_df = active_df.loc[mask_date]
            else: filtered_df = active_df
            if q:
                search_df = filtered_df.copy()
                search_df['display_id'] = search_df.apply(lambda x: generate_unique_name(x['id'], x['start_time'], x['customer_name'], x['customer_phone'], x['address'], extract_proc_from_log(x['logs'])), axis=1)
                m1 = search_df['display_id'].astype(str).str.contains(q, case=False, na=False)
                m2 = search_df['customer_name'].astype(str).str.contains(q, case=False, na=False)
                m3 = search_df['customer_phone'].astype(str).str.contains(q, case=False, na=False)
                m4 = search_df['address'].astype(str).str.contains(q, case=False, na=False)
                final_res = search_df[m1 | m2 | m3 | m4]
            else: final_res = filtered_df
            st.divider()
            if not final_res.empty:
                st.success(f"Tìm thấy {len(final_res)} hồ sơ phù hợp.")
                for i, j in final_res.iterrows(): render_job_card(j, user, role, user_list)
            else: st.warning("Không tìm thấy hồ sơ nào.")
        elif active_df.empty: st.info("Chưa có dữ liệu.")

    elif sel == "📊 Báo Cáo":
        st.title("📊 Báo Cáo & Thống Kê")
        df = get_all_jobs_df()
        active_df = df[df['status'] != 'Đã xóa']
        if not active_df.empty:
            col_d1, col_d2 = st.columns(2)
            today = date.today(); first_day = today.replace(day=1)
            start_d = col_d1.date_input("Từ ngày", first_day); end_d = col_d2.date_input("Đến ngày", today)
            active_df['start_dt'] = pd.to_datetime(active_df['start_time']).dt.date
            mask = (active_df['start_dt'] >= start_d) & (active_df['start_dt'] <= end_d)
            filtered_df = active_df.loc[mask]
            st.divider()
            if filtered_df.empty: st.warning("Không có dữ liệu.")
            else:
                total_jobs = len(filtered_df)
                total_revenue = filtered_df['survey_fee'].apply(safe_int).sum()
                total_unpaid = filtered_df[filtered_df['is_paid'].apply(safe_int) == 0]['survey_fee'].apply(safe_int).sum()
                k1, k2, k3 = st.columns(3)
                k1.metric("Tổng Hồ Sơ", total_jobs, border=True)
                k2.metric("Doanh Thu", f"{total_revenue:,} đ", border=True)
                k3.metric("Công Nợ", f"{total_unpaid:,} đ", delta_color="inverse", border=True)
                st.divider()
                st.subheader("📌 Tỉ lệ hoàn thành")
                stage_counts = filtered_df['current_stage'].value_counts()
                for stage in STAGES_ORDER:
                    count = stage_counts.get(stage, 0)
                    if count > 0:
                        pct = (count / total_jobs); c_lab, c_bar = st.columns([1, 3])
                        c_lab.write(f"**{stage}**: {count} ({int(pct*100)}%)"); c_bar.progress(pct)
                st.divider()
                st.subheader("📄 Danh sách chi tiết")
                view_df = filtered_df.copy()
                view_df['Mã Hồ Sơ'] = view_df.apply(lambda x: generate_unique_name(x['id'], x['start_time'], x['customer_name'], "", "", ""), axis=1)
                view_df['Phí'] = view_df['survey_fee'].apply(lambda x: f"{safe_int(x):,} đ")
                final_view = view_df[['Mã Hồ Sơ', 'customer_name', 'current_stage', 'assigned_to', 'Phí']]
                final_view.columns = ['Mã', 'Khách', 'Tiến Độ', 'Người Xử Lý', 'Phí']
                st.dataframe(final_view, use_container_width=True, hide_index=True)
            
    elif sel == "👥 Nhân Sự":
        if role == "Quản lý":
            st.title("Phân Quyền"); df = get_all_users()
            for i, u in df.iterrows():
                with st.container(border=True):
                    c1, c2 = st.columns([0.7, 0.3])
                    with c1: st.subheader(f"👤 {u['fullname']}"); st.caption(f"User: {u['username']}")
                    with c2:
                        if u['username']!=user:
                            idx = ROLES.index(u['role']) if u['role'] in ROLES else 2
                            nr = st.selectbox("", ROLES, index=idx, key=u['username'], label_visibility="collapsed")
                            if nr!=u['role']: update_user_role(u['username'], nr); st.toast("Đã lưu!"); time.sleep(0.5); st.rerun()
                        else: st.info("Admin")
        else: st.error("Cấm truy cập!")

    elif sel == "🗑️ Thùng Rác":
        if role == "Quản lý":
            st.title("🗑️ Thùng Rác (Hồ sơ đã xóa)")
            df = get_all_jobs_df()
            trash_df = df[df['status'] == 'Đã xóa']
            if trash_df.empty: st.success("Thùng rác trống!")
            else:
                for i, j in trash_df.iterrows():
                    render_job_card(j, user, role, user_list)
        else: st.error("Cấm truy cập!")

    elif sel == "🛡️ Nhật Ký":
        if role == "Quản lý":
            st.title("🛡️ Nhật Ký Hệ Thống (Audit Logs)")
            audit_sheet = get_audit_sheet()
            if audit_sheet:
                audit_data = audit_sheet.get_all_records()
                audit_df = pd.DataFrame(audit_data)
                st.dataframe(audit_df, use_container_width=True)
            else: st.error("Không kết nối được nhật ký.")
        else: st.error("Cấm truy cập!")
