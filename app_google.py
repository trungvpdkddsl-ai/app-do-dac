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
import calendar
import io
from google.oauth2.service_account import Credentials

# --- 1. CẤU HÌNH HỆ THỐNG ---
st.set_page_config(page_title="Đo Đạc Cloud V3-Pro (Restored)", page_icon="☁️", layout="wide")

TELEGRAM_TOKEN = "8514665869:AAHUfTHgNlEEK_Yz6yYjZa-1iR645Cgr190"
TELEGRAM_CHAT_ID = "-5055192262"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# KEY KẾT NỐI
APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbyEMEGyS_sVCA4eyVRFXxnOuGqMnJOKOIqZqKxi4HpYBcpr7U72WUXCoKLm20BQomVC/exec"
DRIVE_FOLDER_ID = "1SrARuA1rgKLZmoObGor-GkNx33F6zNQy"

ROLES = ["Quản lý", "Nhân viên", "Chưa cấp quyền"]
STAGES_ORDER = ["1. Tạo mới", "2. Đo đạc", "3. Hoàn thiện trích đo", "4. Làm hồ sơ", "5. Ký hồ sơ", "6. Lấy hồ sơ", "7. Nộp hồ sơ", "8. Hoàn thành"]
PROCEDURES_LIST = ["Cấp lần đầu", "Cấp đổi", "Chuyển quyền", "Tách thửa", "Thừa kế", "Cung cấp thông tin", "Đính chính"]

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

# SLA (GIỜ)
STAGE_SLA_HOURS = {"1. Tạo mới": 0, "2. Đo đạc": 24, "3. Hoàn thiện trích đo": 24, "4. Làm hồ sơ": 24, "5. Ký hồ sơ": 72, "6. Lấy hồ sơ": 24, "7. Nộp hồ sơ": 360}

# --- 2. HÀM HỖ TRỢ & KẾT NỐI ---
def safe_int(value):
    try: return int(float(str(value).replace(",", "").replace(".", ""))) if pd.notna(value) and value != "" else 0
    except: return 0

def get_proc_abbr(proc_name):
    mapping = {
        "Cấp lần đầu": "CLD", "Cấp đổi": "CD", "Chuyển quyền": "CQ", 
        "Tách thửa": "TT", "Thừa kế": "TK", 
        "Cung cấp thông tin": "CCTT", "Đính chính": "DC"
    }
    return mapping.get(proc_name, "K")

def extract_proc_from_log(log_text):
    match = re.search(r'Khởi tạo \((.*?)\)', str(log_text))
    return match.group(1) if match else ""

def get_next_stage_dynamic(current_stage, proc_name):
    if proc_name in ["Cung cấp thông tin", "Đính chính"]: return WORKFLOW_SHORT.get(current_stage)
    return WORKFLOW_FULL.get(current_stage)

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

def calculate_deadline(start_date, hours_to_add):
    if hours_to_add == 0: return None
    current_date = start_date; added_hours = 0
    while added_hours < hours_to_add:
        current_date += timedelta(hours=1)
        if current_date.weekday() < 5: added_hours += 1
    return current_date

def get_drive_id(link):
    try: match = re.search(r'/d/([a-zA-Z0-9_-]+)', link); return match.group(1) if match else None
    except: return None

# --- HELPER UI & CSS ---
def get_status_badge_html(row):
    status = row['status']
    deadline = pd.to_datetime(row['deadline'], errors='coerce')
    now = datetime.now()
    logs = str(row.get('logs', ''))

    color = "#28a745" # Green (Mặc định)
    bg_color = "#e6fffa"
    text = "Đang thực hiện"
    
    if status == "Tạm dừng":
        if "Hoàn thành - Chưa thanh toán" in logs:
            color = "#fd7e14"; bg_color = "#fff3cd"; text = "⚠️ Xong - Chưa TT"
        else:
            color = "#6c757d"; bg_color = "#f8f9fa"; text = "⛔ Tạm dừng"
    elif status == "Hoàn thành":
        color = "#004085"; bg_color = "#cce5ff"; text = "✅ Hoàn thành"
    elif status == "Đã xóa":
        color = "#343a40"; bg_color = "#e2e6ea"; text = "🗑️ Đã xóa"
    elif status == "Kết thúc sớm":
        color = "#343a40"; bg_color = "#e2e6ea"; text = "⏹️ Kết thúc"
    else:
        if pd.notna(deadline) and now > deadline:
            color = "#dc3545"; bg_color = "#ffe6e6"; text = "🔴 Quá hạn"
        elif pd.notna(deadline) and now <= deadline <= now + timedelta(hours=24):
            color = "#fd7e14"; bg_color = "#fff3cd"; text = "⚠️ Sắp đến hạn"

    return f"""<span style='background-color: {bg_color}; color: {color}; padding: 3px 8px; border-radius: 12px; font-weight: bold; font-size: 11px; border: 1px solid {color}; white-space: nowrap;'>{text}</span>"""

def inject_custom_css():
    st.markdown("""
    <style>
        .row-header { font-weight: bold; color: #333; border-bottom: 2px solid #ddd; padding-bottom: 5px; margin-bottom: 10px; font-size: 13px; }
        .job-row { padding: 8px 0; border-bottom: 1px solid #f0f0f0; align-items: center; }
        .customer-name { color: #d63031; font-weight: bold; font-size: 14px; margin-bottom: 2px; }
        .sub-text { font-size: 12px; color: #555; display: block; margin-top: 0px; }
        .proc-name { color: #0984e3; font-weight: 600; font-size: 13px; }
        .stage-tag { font-size: 11px; font-weight: bold; color: #2d3436; background: #dfe6e9; padding: 2px 6px; border-radius: 4px; }
        .time-text { font-size: 11px; line-height: 1.3; color: #333; }
        .compact-btn button { padding: 0px 8px !important; min-height: 28px !important; height: 28px !important; font-size: 12px !important; margin-top: 0px !important; }
        div[data-testid="stExpanderDetails"] { padding-top: 10px !important; }
        hr { margin: 10px 0px !important; }
    </style>
    """, unsafe_allow_html=True)

# --- GOOGLE SHEETS & DRIVE API (WITH CACHING RESTORED FOR PERFORMANCE) ---
def get_gcp_creds(): 
    return Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)

@st.cache_resource
def get_gspread_client():
    creds = get_gcp_creds()
    return gspread.authorize(creds)

def get_sheet(sheet_name="DB_DODAC"):
    try: client = get_gspread_client(); return client.open(sheet_name).sheet1
    except: return None

def get_users_sheet():
    try:
        client = get_gspread_client(); sh = client.open("DB_DODAC")
        try: return sh.worksheet("USERS")
        except: 
            ws = sh.add_worksheet(title="USERS", rows="100", cols="5")
            ws.append_row(["username", "password", "fullname", "role"]); return ws
    except: return None

def get_audit_sheet():
    try:
        client = get_gspread_client(); sh = client.open("DB_DODAC")
        try: return sh.worksheet("AUDIT_LOGS")
        except: 
            ws = sh.add_worksheet(title="AUDIT_LOGS", rows="1000", cols="4")
            ws.append_row(["Timestamp", "User", "Action", "Details"]); return ws
    except: return None

# --- FILE UPLOAD & ACTIONS ---
def upload_file_via_script(file_obj, sub_folder_name):
    if not file_obj: return None, None
    try:
        file_content = file_obj.read()
        file_base64 = base64.b64encode(file_content).decode('utf-8')
        payload = {"filename": file_obj.name, "mime_type": file_obj.type, "file_base64": file_base64, "folder_id": DRIVE_FOLDER_ID, "sub_folder_name": sub_folder_name}
        response = requests.post(APPS_SCRIPT_URL, json=payload)
        if response.status_code == 200:
            res_json = response.json()
            if res_json.get("status") == "success": return res_json.get("link"), file_obj.name
    except Exception as e: st.error(f"Lỗi Upload: {e}")
    return None, None

def find_row_index(sh, jid):
    try: ids = sh.col_values(1); return ids.index(str(jid)) + 1
    except: return None

def delete_file_system(job_id, file_link, file_name, user):
    file_id = get_drive_id(file_link)
    if file_id: requests.post(APPS_SCRIPT_URL, json={"action": "delete", "file_id": file_id})
    sh = get_sheet(); r = find_row_index(sh, job_id)
    if r:
        current_log = sh.cell(r, 11).value
        new_log = re.sub(r"(\s*\|\s*)?File: .*? - " + re.escape(file_link), "", str(current_log))
        sh.update_cell(r, 11, new_log)
        if sh.cell(r, 10).value == file_link: sh.update_cell(r, 10, "")
        log_to_audit(user, "DELETE_FILE", f"Job {job_id}: Deleted file {file_name}")
        get_all_jobs_df.clear()

# --- AUTH & UTILS ---
def make_hash(p): return hashlib.sha256(str.encode(p)).hexdigest()

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

def login_user(u, p):
    sh = get_users_sheet(); 
    if not sh: return None
    try: cell = sh.find(u); row = sh.row_values(cell.row); return row if row[1] == make_hash(p) else None
    except: return None

def create_user(u, p, n):
    if not re.match(r'^[a-zA-Z0-9_]+$', u): return False
    sh = get_users_sheet(); 
    if not sh: return False
    try: 
        if sh.find(u): return False
        sh.append_row([u, make_hash(p), n, "Chưa cấp quyền"]); get_all_users_cached.clear(); return True
    except: return False

def delete_user_permanently(u):
    sh = get_users_sheet()
    try: cell = sh.find(u); sh.delete_rows(cell.row); get_all_users_cached.clear(); return True
    except: return False

@st.cache_data(ttl=300)
def get_all_users_cached():
    sh = get_users_sheet()
    return pd.DataFrame(sh.get_all_records()) if sh else pd.DataFrame()

def get_all_users(): return get_all_users_cached()

def update_user_role(u, r): 
    sh = get_users_sheet(); c = sh.find(u); sh.update_cell(c.row, 4, r); get_all_users_cached.clear()

def get_active_users_list(): 
    df = get_all_users_cached()
    if df.empty: return []
    return df[df['role']!='Chưa cấp quyền'].apply(lambda x: f"{x['username']} - {x['fullname']}", axis=1).tolist()

@st.cache_data(ttl=60)
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

# --- SCHEDULER ---
def run_schedule_check():
    while True:
        now = datetime.now()
        if (now.hour == 8 or now.hour == 13) and now.minute < 5:
            try:
                df = get_all_jobs_df()
                if not df.empty:
                    active_df = df[df['status'] != 'Đã xóa']
                    active_df['dl_dt'] = pd.to_datetime(active_df['deadline'], errors='coerce')
                    urgent = active_df[(active_df['dl_dt'] > now) & (active_df['dl_dt'] <= now + timedelta(hours=24))]
                    if not urgent.empty:
                        msg_list = []
                        for _, j in urgent.iterrows():
                            p_name = extract_proc_from_log(j['logs'])
                            name = generate_unique_name(j['id'], j['start_time'], j['customer_name'], "", "", p_name)
                            left = int((j['dl_dt'] - now).total_seconds() / 3600)
                            msg_list.append(f"🔸 <b>{name}</b> (Còn {left}h) - {j['assigned_to']}")
                        send_telegram_msg(f"⏰ <b>CẢNH BÁO 24H ({len(msg_list)} hồ sơ):</b>\n\n" + "\n".join(msg_list))
                        time.sleep(300)
            except: pass
        time.sleep(60)

if 'scheduler_started' not in st.session_state:
    threading.Thread(target=run_schedule_check, daemon=True).start()
    st.session_state['scheduler_started'] = True

# --- LOGIC ADD/UPDATE ---
def add_job(n, p, a, proc, f, u, asn):
    sh = get_sheet(); now = datetime.now(); now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    jid, seq_str = get_daily_sequence_id()
    phone_db = f"'{p}" 
    full_name_str = generate_unique_name(jid, now_str, n, p, a, proc)
    link = ""; fname = ""; log_file_str = ""
    if f: 
        for uploaded_file in f:
            l, n_f = upload_file_via_script(uploaded_file, full_name_str)
            if l: log_file_str += f" | File: {n_f} - {l}"; link = l; fname = n_f

    dl_dt = now + timedelta(days=365) 
    dl = dl_dt.strftime("%Y-%m-%d %H:%M:%S")

    assign_info = f" -> Giao: {asn.split(' - ')[0]}" if asn else ""
    log = f"[{now_str}] {u}: Khởi tạo ({proc}){assign_info}{log_file_str}"
    asn_clean = asn.split(" - ")[0] if asn else ""
    
    sh.append_row([jid, now_str, n, phone_db, a, "1. Tạo mới", "Đang xử lý", asn_clean, dl, link, log, 0, 0, 0, 0])
    get_all_jobs_df.clear()
    log_to_audit(u, "CREATE_JOB", f"ID: {jid}, Name: {n}")
    
    type_msg = f"({proc.upper()})"
    file_msg = f"\n📎 Có {len(f)} file đính kèm" if f else ""
    assign_msg = f"👉 <b>{asn_clean}</b>"
    send_telegram_msg(f"🚀 <b>MỚI #{seq_str} {type_msg}</b>\n📂 <b>{full_name_str}</b>\n{assign_msg}\n{file_msg}")

def update_stage(jid, stg, nt, f_list, u, asn, d, is_survey, deposit_ok, fee_amount, is_paid, result_date=None):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        row_data = sh.row_values(r)
        proc_name = extract_proc_from_log(row_data[10])
        full_code = generate_unique_name(jid, row_data[1], row_data[2], row_data[3], row_data[4], proc_name)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_file_str = ""
        if f_list:
            for uploaded_file in f_list:
                l, n_f = upload_file_via_script(uploaded_file, full_code); 
                if l: log_file_str += f" | File: {n_f} - {l}"
        
        if nt == "Đã nhận kết quả đúng hạn." or nt == "Đã nhận kết quả sớm." or nt == "Hoàn thành (Đã TT)":
            nxt = "8. Hoàn thành"
        else:
            nxt = get_next_stage_dynamic(stg, proc_name)
            if not nxt: nxt = "8. Hoàn thành"

        if nxt:
            sh.update_cell(r, 6, nxt)
            assign_str = ""; assign_tele = ""
            if asn: 
                assign_clean = asn.split(" - ")[0]; sh.update_cell(r, 8, assign_clean)
                assign_str = f" -> Giao: {assign_clean}"; assign_tele = f"\n👉 Giao: <b>{assign_clean}</b>"
            if result_date:
                new_deadline = result_date.strftime("%Y-%m-%d %H:%M:%S")
                sh.update_cell(r, 9, new_deadline); nt += f" (Hẹn trả: {result_date.strftime('%d/%m/%Y')})"
            else:
                if nxt == "8. Hoàn thành": pass
                else:
                    hours_to_add = STAGE_SLA_HOURS.get(nxt, 24)
                    if hours_to_add > 0:
                        new_dl = calculate_deadline(datetime.now(), hours_to_add)
                        sh.update_cell(r, 9, new_dl.strftime("%Y-%m-%d %H:%M:%S"))
                    else:
                        sh.update_cell(r, 9, (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S"))
            
            sh.update_cell(r, 13, 1 if deposit_ok else 0); sh.update_cell(r, 14, safe_int(fee_amount)); sh.update_cell(r, 15, 1 if is_paid else 0)
            olog = sh.cell(r, 11).value
            nlog = f"\n[{now}] {u}: {stg}->{nxt}{assign_str} | Note: {nt}{log_file_str}"
            sh.update_cell(r, 11, olog + nlog)
            if nxt=="8. Hoàn thành": sh.update_cell(r, 7, "Hoàn thành")
            get_all_jobs_df.clear()
            log_to_audit(u, "UPDATE_STAGE", f"ID: {jid}, {stg} -> {nxt}")
            send_telegram_msg(f"✅ <b>CẬP NHẬT</b>\n📂 <b>{full_code}</b>\n{stg} ➡ <b>{nxt}</b>\n👤 {u}{assign_tele}")

def update_deadline_custom(jid, new_date, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        new_dl_str = datetime.combine(new_date, datetime.max.time()).strftime("%Y-%m-%d %H:%M:%S")
        sh.update_cell(r, 9, new_dl_str)
        olog = sh.cell(r, 11).value
        nlog = f"\n[{datetime.now()}] {u}: 📅 CẬP NHẬT NGÀY HẸN TRẢ: {new_date.strftime('%d/%m/%Y')}"
        sh.update_cell(r, 11, olog + nlog)
        get_all_jobs_df.clear()
        log_to_audit(u, "UPDATE_DEADLINE", f"ID: {jid} -> {new_date}")
        st.toast("Đã lưu ngày hẹn mới!")

def return_to_previous_stage(jid, current_stage, reason, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        try:
            curr_idx = STAGES_ORDER.index(current_stage)
            row_data = sh.row_values(r)
            proc_name = extract_proc_from_log(row_data[10])
            prev_stage = None
            temp_idx = curr_idx - 1
            while temp_idx >= 0:
                candidate = STAGES_ORDER[temp_idx]
                if proc_name in ["Cung cấp thông tin", "Đính chính"]:
                     if candidate in ["2. Đo đạc", "3. Hoàn thiện trích đo"]:
                         temp_idx -= 1; continue
                prev_stage = candidate; break

            if prev_stage:
                sh.update_cell(r, 6, prev_stage)
                hours_to_add = STAGE_SLA_HOURS.get(prev_stage, 24)
                new_dl = calculate_deadline(datetime.now(), hours_to_add)
                if new_dl: sh.update_cell(r, 9, new_dl.strftime("%Y-%m-%d %H:%M:%S"))
                olog = sh.cell(r, 11).value
                nlog = f"\n[{datetime.now()}] {u}: ⬅️ TRẢ HỒ SƠ ({current_stage} -> {prev_stage}) | Lý do: {reason}"
                sh.update_cell(r, 11, olog + nlog)
                full_code = generate_unique_name(jid, row_data[1], row_data[2], row_data[3], row_data[4], proc_name)
                get_all_jobs_df.clear()
                log_to_audit(u, "RETURN_JOB", f"ID: {jid}, {current_stage} -> {prev_stage}")
                send_telegram_msg(f"↩️ <b>TRẢ HỒ SƠ</b>\n📂 <b>{full_code}</b>\n{current_stage} ➡ <b>{prev_stage}</b>\n👤 Bởi: {u}\n⚠️ Lý do: {reason}")
                return True
        except: return False
    return False

def update_customer_info(jid, new_name, new_phone, new_addr, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        sh.update_cell(r, 3, new_name); sh.update_cell(r, 4, f"'{new_phone}"); sh.update_cell(r, 5, new_addr)
        olog = sh.cell(r, 11).value; nlog = f"\n[{datetime.now()}] {u}: ✏️ ADMIN SỬA THÔNG TIN"
        sh.update_cell(r, 11, olog + nlog); log_to_audit(u, "EDIT_INFO", f"ID: {jid}"); get_all_jobs_df.clear(); st.toast("Đã cập nhật thông tin!")

def update_finance_only(jid, deposit_ok, fee_amount, is_paid, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        row_data = sh.row_values(r)
        full_code = generate_unique_name(jid, row_data[1], row_data[2], row_data[3], row_data[4], extract_proc_from_log(row_data[10]))
        sh.update_cell(r, 13, 1 if deposit_ok else 0); sh.update_cell(r, 14, safe_int(fee_amount)); sh.update_cell(r, 15, 1 if is_paid else 0)
        get_all_jobs_df.clear()
        log_to_audit(u, "UPDATE_FINANCE", f"ID: {jid}, Fee: {fee_amount}")
        send_telegram_msg(f"💰 <b>TÀI CHÍNH</b>\n📂 <b>{full_code}</b>\n👤 {u}\nPhí: {fee_amount:,} VNĐ")

def pause_job(jid, rs, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        row_data = sh.row_values(r)
        full_code = generate_unique_name(jid, row_data[1], row_data[2], row_data[3], row_data[4], extract_proc_from_log(row_data[10]))
        sh.update_cell(r, 7, "Tạm dừng"); olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{datetime.now()}] {u}: TẠM DỪNG: {rs}")
        get_all_jobs_df.clear()
        log_to_audit(u, "PAUSE_JOB", f"ID: {jid}")
        send_telegram_msg(f"⛔ <b>TẠM DỪNG</b>\n📂 <b>{full_code}</b>\n👤 Bởi: {u}\n📝 Lý do: {rs}")

def resume_job(jid, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        row_data = sh.row_values(r)
        full_code = generate_unique_name(jid, row_data[1], row_data[2], row_data[3], row_data[4], extract_proc_from_log(row_data[10]))
        sh.update_cell(r, 7, "Đang xử lý"); olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{datetime.now()}] {u}: KHÔI PHỤC")
        get_all_jobs_df.clear()
        log_to_audit(u, "RESUME_JOB", f"ID: {jid}")
        send_telegram_msg(f"▶️ <b>KHÔI PHỤC</b>\n📂 <b>{full_code}</b>\n👤 Bởi: {u}")

def terminate_job(jid, rs, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r:
        row_data = sh.row_values(r)
        full_code = generate_unique_name(jid, row_data[1], row_data[2], row_data[3], row_data[4], extract_proc_from_log(row_data[10]))
        sh.update_cell(r, 7, "Kết thúc sớm"); olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{datetime.now()}] {u}: KẾT THÚC SỚM: {rs}")
        get_all_jobs_df.clear()
        log_to_audit(u, "TERMINATE_JOB", f"ID: {jid}")
        send_telegram_msg(f"⏹️ <b>KẾT THÚC SỚM</b>\n📂 <b>{full_code}</b>\n👤 Bởi: {u}\n📝 Lý do: {rs}")

def move_to_trash(jid, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r: sh.update_cell(r, 7, "Đã xóa"); get_all_jobs_df.clear(); log_to_audit(u, "MOVE_TO_TRASH", f"ID: {jid}"); st.toast("Đã chuyển vào thùng rác!")

def restore_from_trash(jid, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r: sh.update_cell(r, 7, "Đang xử lý"); get_all_jobs_df.clear(); log_to_audit(u, "RESTORE_JOB", f"ID: {jid}"); st.toast("Đã khôi phục hồ sơ!")

def delete_forever(jid, u):
    sh = get_sheet(); r = find_row_index(sh, jid)
    if r: sh.delete_rows(r); get_all_jobs_df.clear(); log_to_audit(u, "DELETE_FOREVER", f"ID: {jid}"); st.toast("Đã xóa vĩnh viễn!")

# --- UI COMPONENTS ---
def change_menu(new_menu):
    st.session_state['menu_selection'] = new_menu

def render_square_menu(role):
    st.markdown("""<style>div.stButton > button {width: 100%; height: 80px; border-radius: 12px; border: 1px solid #ddd; background-color: #f8f9fa; color: #333; font-weight: bold; font-size: 14px; transition: all 0.3s ease; margin-bottom: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);} div.stButton > button:hover {background-color: #e2e6ea; border-color: #adb5bd; transform: translateY(-2px); box-shadow: 0 4px 8px rgba(0,0,0,0.1);} div.stButton > button:active { background-color: #dae0e5; transform: translateY(0); }</style>""", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        st.button("🏠 Việc Của Tôi", on_click=change_menu, args=("🏠 Việc Của Tôi",))
        st.button("📝 Tạo Hồ Sơ", on_click=change_menu, args=("📝 Tạo Hồ Sơ",))
        if role == "Quản lý":
             st.button("💰 Công Nợ", on_click=change_menu, args=("💰 Công Nợ",))
             st.button("🗑️ Thùng Rác", on_click=change_menu, args=("🗑️ Thùng Rác",))
    with c2:
        st.button("📅 Lịch Biểu", on_click=change_menu, args=("📅 Lịch Biểu",))
        st.button("🗄️ Lưu Trữ", on_click=change_menu, args=("🗄️ Lưu Trữ",)) 
        st.button("📊 Báo Cáo", on_click=change_menu, args=("📊 Báo Cáo",))
        if role == "Quản lý":
            st.button("👥 Nhân Sự", on_click=change_menu, args=("👥 Nhân Sự",))
            st.button("🛡️ Nhật Ký", on_click=change_menu, args=("🛡️ Nhật Ký",))

# --- RENDER CARD CONTENT (FORM CHI TIẾT) ---
def render_job_card_content(j, user, role, user_list):
    try: dl_dt = pd.to_datetime(j['deadline'])
    except: dl_dt = datetime.now() + timedelta(days=365)
    proc_name = extract_proc_from_log(j['logs'])

    # --- 1. THÔNG TIN KHÁCH HÀNG ---
    c_info1, c_info2 = st.columns([1, 1])
    with c_info1:
        st.markdown(f"👤 **{j['customer_name']}**")
        st.markdown(f"<span style='font-size:13px'>📞 {j['customer_phone']}</span>", unsafe_allow_html=True)
    with c_info2:
        st.markdown(f"<span style='font-size:13px'>📍 {j['address']}</span>", unsafe_allow_html=True)
        if role == "Quản lý":
            with st.popover("✏️ Sửa"):
                new_n = st.text_input("Tên", j['customer_name'], key=f"en_{j['id']}")
                new_p = st.text_input("SĐT", j['customer_phone'], key=f"ep_{j['id']}")
                new_a = st.text_input("Đ/c", j['address'], key=f"ea_{j['id']}")
                if st.button("Lưu", key=f"sv_{j['id']}"):
                    update_customer_info(j['id'], new_n, new_p, new_a, user); time.sleep(1); st.rerun()

    st.markdown("---")

    # --- 2. TABS CHỨC NĂNG ---
    t1, t2, t3, t4 = st.tabs(["📂 File & Hồ sơ", "⚙️ Xử lý", "💰 Tài Chính", "📜 Nhật ký"])
    
    with t1:
        st.markdown("###### 📎 Danh sách file:")
        file_list = extract_files_from_log(j['logs'])
        if j['file_link'] and j['file_link'] not in [lnk for _, lnk in file_list]: 
            file_list.insert(0, ("File gốc", j['file_link']))
        
        if not file_list: 
            st.caption("Chưa có file nào.")
        else:
            with st.container():
                st.markdown('<div class="compact-btn">', unsafe_allow_html=True)
                for idx, (fname, link) in enumerate(file_list):
                    file_id = get_drive_id(link)
                    down_link = f"https://drive.google.com/uc?export=download&id={file_id}" if file_id else link
                    
                    c_ico, c_name, c_view, c_down, c_del = st.columns([0.15, 3.5, 0.4, 0.4, 0.4])
                    with c_ico: st.write("📄")
                    with c_name: st.markdown(f"<span style='font-size:13px; position:relative; top:2px'>{fname}</span>", unsafe_allow_html=True)
                    with c_view: st.link_button("👁️", link, help="Xem file") 
                    with c_down: st.link_button("⬇️", down_link, help="Tải về")
                    with c_del:
                        if role == "Quản lý":
                            if st.button("✕", key=f"del_{j['id']}_{idx}", help="Xóa file"):
                                delete_file_system(j['id'], link, fname, user)
                                st.toast("Đã xóa!"); time.sleep(1); st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

    with t2:
        if j['status'] in ['Tạm dừng', 'Kết thúc sớm']:
            st.error(f"TRẠNG THÁI: {j['status'].upper()}")
            if j['status'] == 'Tạm dừng' and st.button("▶️ Tiếp tục", key=f"r{j['id']}"): resume_job(j['id'], user); st.rerun()
        
        elif j['current_stage'] == "7. Nộp hồ sơ":
            st.info("🏢 **ĐANG CHỜ KẾT QUẢ TỪ CƠ QUAN CHỨC NĂNG**")
            c_d, c_b = st.columns([2,1])
            new_date = c_d.date_input("Hẹn trả:", value=dl_dt.date(), key=f"d7_{j['id']}", label_visibility="collapsed")
            if c_b.button("Lưu hẹn", key=f"s7_{j['id']}"):
                 update_deadline_custom(j['id'], new_date, user); st.rerun()
            
            st.divider()
            st.write("🏁 **Xác nhận kết quả:**")
            # Logic mới: Tách nút thành 2 lựa chọn thanh toán
            c_pay_yes, c_pay_no = st.columns(2)
            
            if c_pay_yes.button("✅ Đã TT - Kết thúc", type="primary", use_container_width=True, key=f"fin_pay_{j['id']}"):
                 # Cập nhật tiền -> Hoàn thành
                 update_finance_only(j['id'], 1, safe_int(j.get('survey_fee')), 1, user)
                 update_stage(j['id'], "7. Nộp hồ sơ", "Hoàn thành (Đã TT)", [], user, "", 0, safe_int(j.get('is_survey_only')), 1, safe_int(j.get('survey_fee')), 1)
                 st.rerun()

            if c_pay_no.button("⛔ Chưa TT - Treo HS", use_container_width=True, key=f"fin_notpay_{j['id']}"):
                 # Cập nhật tiền (chưa) -> Tạm dừng với lý do đặc biệt
                 update_finance_only(j['id'], 1, safe_int(j.get('survey_fee')), 0, user)
                 pause_job(j['id'], "Hoàn thành - Chưa thanh toán", user)
                 st.rerun()
            
            st.divider()
            c1, c2 = st.columns(2)
            if c1.button("⏸️ Dừng", key=f"p{j['id']}", use_container_width=True): st.session_state[f'pm_{j['id']}'] = True
            with c2.popover("⬅️ Trả hồ sơ", use_container_width=True):
                reason = st.text_input("Lý do:", key=f"rb_{j['id']}")
                if st.button("Xác nhận", key=f"cb_{j['id']}"): return_to_previous_stage(j['id'], j['current_stage'], reason, user); st.rerun()

        else:
            with st.form(f"f{j['id']}"):
                nt = st.text_area("Ghi chú xử lý:", height=60)
                fl = st.file_uploader("Thêm file:", accept_multiple_files=True, key=f"up_{j['id']}_{st.session_state['uploader_key']}")
                
                cur = j['current_stage']; nxt = get_next_stage_dynamic(cur, proc_name)
                if not nxt: nxt = "8. Hoàn thành"
                
                c_next, c_assign = st.columns([1, 1])
                with c_next: st.write(f"➡️ **{nxt}**")
                with c_assign:
                    if nxt != "8. Hoàn thành":
                        idx = 0
                        if user_list and j['assigned_to'] in user_list: idx = user_list.index(j['assigned_to'])
                        asn = st.selectbox("Giao việc:", user_list, index=idx, label_visibility="collapsed")
                    else: asn = ""

                if st.form_submit_button("✅ Chuyển bước", type="primary", use_container_width=True): 
                    dep = 1 if safe_int(j.get('deposit'))==1 else 0; money = safe_int(j.get('survey_fee')); pdone = 1 if safe_int(j.get('is_paid'))==1 else 0
                    update_stage(j['id'], cur, nt, fl, user, asn, 0, safe_int(j.get('is_survey_only')), dep, money, pdone, None)
                    st.session_state['uploader_key'] += 1; st.rerun()
            
            c_pause, c_term, c_back = st.columns(3)
            if c_pause.button("⏸️", key=f"p{j['id']}", help="Tạm dừng"): st.session_state[f'pm_{j['id']}'] = True
            if c_term.button("⏹️", key=f"t{j['id']}", help="Kết thúc sớm"): st.session_state[f'tm_{j['id']}'] = True
            with c_back.popover("⬅️", help="Trả hồ sơ"):
                reason = st.text_input("Lý do:", key=f"rb_{j['id']}")
                if st.button("Trả về", key=f"cb_{j['id']}"): return_to_previous_stage(j['id'], j['current_stage'], reason, user); st.rerun()

        if st.session_state.get(f'pm_{j['id']}', False):
            rs = st.text_input("Lý do dừng:", key=f"rs{j['id']}")
            if st.button("Xác nhận dừng", key=f"okp{j['id']}"): pause_job(j['id'], rs, user); st.rerun()
            
        if st.session_state.get(f'tm_{j['id']}', False):
            rst = st.text_input("Lý do kết thúc:", key=f"rst{j['id']}")
            # Kết thúc sớm không cần check thanh toán nữa
            if st.button("Xác nhận kết thúc", key=f"okt{j['id']}"): terminate_job(j['id'], rst, user); st.rerun()

    # --- TAB 3: TÀI CHÍNH ---
    with t3:
        with st.form(f"mon_{j['id']}"):
            # Logic riêng cho Tách thửa
            if "Tách thửa" in proc_name:
                st.write("💰 **Chi phí Tách thửa**")
                c1, c2 = st.columns([2, 1])
                fee_val = c1.number_input("Số tiền:", value=safe_int(j.get('survey_fee')), step=100000)
                paid_status = c2.checkbox("Đã thanh toán", value=safe_int(j.get('is_paid'))==1)
                
                if st.form_submit_button("💾 Lưu TC", use_container_width=True): 
                    update_finance_only(j['id'], 0, fee_val, paid_status, user)
                    st.success("Đã lưu"); st.rerun()
            else:
                # Logic mặc định (Đo đạc = 1.5M)
                st.write("💰 **Chi phí Đo đạc**")
                is_collected = st.checkbox("✅ Đã thu tiền đo đạc (1.500.000 VNĐ)", value=safe_int(j.get('is_paid'))==1)
                
                if st.form_submit_button("💾 Lưu TC", use_container_width=True): 
                    final_fee = 1500000 if is_collected else 0
                    update_finance_only(j['id'], 0, final_fee, is_collected, user)
                    st.success("Đã lưu"); st.rerun()
    
    with t4:
        st.text_area("", j['logs'], height=150, disabled=True, label_visibility="collapsed")

# --- RENDER LIST VIEW ---
def render_complex_list_view(df, user, role, user_list):
    inject_custom_css()

    # Tỉ lệ cột: [Mã, Thủ tục, Chủ hồ sơ, Thời gian, Người làm, Trạng thái, Toggle]
    cols_cfg = [1.5, 1.5, 2.5, 2.5, 1.5, 1.2, 0.5]
    h1, h2, h3, h4, h5, h6, h7 = st.columns(cols_cfg)
    h1.markdown("**Mã Hồ Sơ**")
    h2.markdown("**Thủ tục**")
    h3.markdown("**Chủ hồ sơ**")
    h4.markdown("**Thời gian quy định**")
    h5.markdown("**Người thực hiện**")
    h6.markdown("**Trạng thái**")
    h7.markdown("**🔍**")

    if df.empty:
        st.info("Không có dữ liệu.")
        return

    for index, row in df.iterrows():
        short_id = str(row['id'])
        if len(short_id) > 6: display_id = f"{short_id[:-2]}-{short_id[-2:]}"
        else: display_id = short_id

        proc_name = extract_proc_from_log(row['logs'])
        if not proc_name: proc_name = "Đo đạc khác"
        
        start_dt = pd.to_datetime(row['start_time'])
        try: dl_dt = pd.to_datetime(row['deadline'])
        except: dl_dt = None
        
        overdue_msg = ""
        if dl_dt and datetime.now() > dl_dt:
            diff = datetime.now() - dl_dt
            d = diff.days; h = diff.seconds // 3600
            overdue_msg = f"<span style='color:red; font-weight:bold'>Đã quá hạn {d} ngày {h} giờ</span><br>"
        
        assignee = row['assigned_to'].split(' - ')[0] if row['assigned_to'] else "Chưa giao"
        current_step_name = row['current_stage'].split('. ')[1] if '. ' in row['current_stage'] else row['current_stage']

        with st.container(border=True):
            c1, c2, c3, c4, c5, c6, c7 = st.columns(cols_cfg)
            
            with c1: 
                if st.button(display_id, key=f"btn_code_{row['id']}"):
                    st.session_state[f"exp_{row['id']}"] = not st.session_state.get(f"exp_{row['id']}", False)
                    st.rerun()
            
            with c2: st.markdown(f"<div class='proc-name'>{proc_name}</div>", unsafe_allow_html=True)
            
            with c3:
                clean_phone = str(row['customer_phone']).replace("'", "")
                if st.button(f"{row['customer_name']}\n({clean_phone})", key=f"btn_name_{row['id']}", help="Xem chi tiết"):
                    st.session_state[f"exp_{row['id']}"] = not st.session_state.get(f"exp_{row['id']}", False)
                    st.rerun()
                st.caption(f"📍 {row['address']}")

            with c4:
                date_fmt = "%d/%m/%Y %H:%M"
                dl_str = dl_dt.strftime(date_fmt) if dl_dt else "Không giới hạn"
                start_str = start_dt.strftime(date_fmt)
                st.markdown(f"""<div class='time-text'>{overdue_msg}• Nhận: {start_str}<br>• Hạn: <b>{dl_str}</b></div>""", unsafe_allow_html=True)
            
            with c5:
                st.markdown(f"""<div>👤 <b>{assignee}</b></div><div class='stage-tag'>{current_step_name}</div>""", unsafe_allow_html=True)
            
            with c6: st.markdown(get_status_badge_html(row), unsafe_allow_html=True)
            
            with c7:
                expand_key = f"exp_{row['id']}"
                btn_label = "🔼" if st.session_state.get(expand_key, False) else "🔽"
                if st.button(btn_label, key=f"btn_expand_{row['id']}", help="Xem chi tiết & Xử lý"):
                    st.session_state[expand_key] = not st.session_state.get(expand_key, False)
                    st.rerun()

            if st.session_state.get(f"exp_{row['id']}", False):
                st.markdown("---")
                render_job_card_content(row, user, role, user_list)

# --- UI MAIN ---
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'uploader_key' not in st.session_state: st.session_state['uploader_key'] = 0
if 'job_filter' not in st.session_state: st.session_state['job_filter'] = 'all'
if 'menu_selection' not in st.session_state: st.session_state['menu_selection'] = "🏠 Việc Của Tôi"

if 'user' in st.query_params and not st.session_state['logged_in']:
    saved_user = st.query_params['user']
    st.session_state['logged_in'] = True; st.session_state['user'] = saved_user
    df_u = get_all_users_cached()
    if not df_u.empty:
        st.session_state['role'] = df_u[df_u['username'] == saved_user]['role'].values[0] if saved_user in df_u['username'].values else "Nhân viên"

if not st.session_state['logged_in']:
    st.title("🔐 CỔNG ĐĂNG NHẬP")
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Đăng Nhập")
        u = st.text_input("User", key="login_u"); p = st.text_input("Pass", type='password', key="login_p")
        remember = st.checkbox("Ghi nhớ đăng nhập")
        if st.button("Đăng Nhập", type="primary"):
            d = login_user(u, p)
            if d: 
                st.session_state['logged_in']=True; st.session_state['user']=d[0]; st.session_state['role']=d[3]
                if remember: st.query_params["user"] = u
                st.rerun()
            else: st.error("Sai thông tin!")
    with c2:
        st.subheader("Đăng Ký Mới")
        nu = st.text_input("User Mới", key="reg_u"); np = st.text_input("Pass Mới", type='password', key="reg_p"); nn = st.text_input("Họ Tên", key="reg_n")
        if st.button("Đăng Ký"): 
            if create_user(nu, np, nn): st.success("OK! Chờ duyệt.")
            else: st.error("Lỗi hoặc tên trùng!")
else:
    user = st.session_state['user']; role = st.session_state['role']
    with st.sidebar:
        st.title(f"👤 {user}"); st.info(f"{role}")
        df = get_all_jobs_df()
        if not df.empty:
            now = datetime.now(); active_df = df[df['status'] != 'Đã xóa']; active_df['dl_dt'] = pd.to_datetime(active_df['deadline'], errors='coerce')
            urgent = active_df[(active_df['dl_dt'] > now) & (active_df['dl_dt'] <= now + timedelta(hours=24))]
            if not urgent.empty:
                st.warning(f"🔥 **CẢNH BÁO: {len(urgent)} hồ sơ < 24h**")
                if role == "Quản lý":
                    counts = urgent['assigned_to'].value_counts()
                    for u_name, c in counts.items(): st.caption(f"- {u_name}: {c}")
                else:
                    my_urgent = urgent[urgent['assigned_to'].str.contains(user, na=False)]
                    if not my_urgent.empty: st.error(f"Bạn có {len(my_urgent)} hồ sơ gấp!")
        st.markdown("---"); render_square_menu(role); st.markdown("---")
        if st.button("Đăng xuất"): st.session_state['logged_in']=False; st.query_params.clear(); st.rerun()

    sel = st.session_state['menu_selection']; user_list = get_active_users_list()
    
    if sel == "🏠 Việc Của Tôi":
        st.title("📋 Tiến trình hồ sơ")
        
        if df.empty: st.info("Trống!")
        else:
            active_df = df[df['status'] != 'Đã xóa']
            if role != "Quản lý": user_filtered_df = active_df[active_df['assigned_to'].astype(str) == user]
            else: user_filtered_df = active_df
            
            my_df = user_filtered_df[~user_filtered_df['status'].isin(['Hoàn thành', 'Kết thúc sớm'])]
            now = datetime.now()
            my_df['dl_dt'] = pd.to_datetime(my_df['deadline'], errors='coerce')
            my_df['dl_dt'] = my_df['dl_dt'].fillna(now + timedelta(days=365))
            
            count_overdue = len(my_df[(my_df['dl_dt'] < now) & (my_df['status'] != 'Tạm dừng')])
            count_soon = len(my_df[(my_df['dl_dt'] >= now) & (my_df['dl_dt'] <= now + timedelta(hours=24)) & (my_df['status'] != 'Tạm dừng')])
            count_paused = len(my_df[my_df['status'] == 'Tạm dừng'])
            count_total = len(my_df)

            if my_df.empty: 
                st.info("Không có hồ sơ nào đang xử lý. (Kiểm tra mục 'Lưu Trữ' để xem hồ sơ đã xong)")
            else:
                k1, k2, k3, k4 = st.columns(4)
                if k1.button(f"🔴 Quá Hạn ({count_overdue})", use_container_width=True): st.session_state['job_filter'] = 'overdue'
                if k2.button(f"🟡 Sắp đến hạn ({count_soon})", use_container_width=True): st.session_state['job_filter'] = 'urgent'
                if k3.button(f"⛔ Tạm dừng ({count_paused})", use_container_width=True): st.session_state['job_filter'] = 'paused'
                if k4.button(f"🟢 Tổng ({count_total})", use_container_width=True): st.session_state['job_filter'] = 'all'

                st.write("")
                with st.expander("🔎 Bộ lọc tìm kiếm & Thời gian", expanded=False):
                    f_c1, f_c2, f_c3, f_c4, f_c5 = st.columns([2, 1.5, 1.5, 1, 1.5])
                    with f_c1:
                        search_kw = st.text_input("🔍 Từ khóa (Tên, SĐT, Mã, Đ/c)", placeholder="Nhập để tìm...", key="s_kw")
                    with f_c2:
                        filter_stages = ["Tất cả"] + STAGES_ORDER
                        sel_stage = st.selectbox("📌 Quy trình", filter_stages, key="s_stage")
                    with f_c3:
                        filter_users = ["Tất cả"] + user_list
                        sel_user = st.selectbox("👤 Người làm", filter_users, key="s_user")
                    with f_c4:
                        time_option = st.selectbox("📅 Thời gian", ["Tất cả", "Tháng này", "Khoảng ngày"], key="s_time_opt")
                    with f_c5:
                        d_range = None
                        if time_option == "Khoảng ngày":
                            d_range = st.date_input("Chọn ngày", [], key="s_date_rng")
                        elif time_option == "Tháng này":
                            st.info(f"Tháng {datetime.now().month}/{datetime.now().year}")

                if st.session_state['job_filter'] == 'overdue': display_df = my_df[(my_df['dl_dt'] < now) & (my_df['status'] != 'Tạm dừng')]
                elif st.session_state['job_filter'] == 'urgent': display_df = my_df[(my_df['dl_dt'] >= now) & (my_df['dl_dt'] <= now + timedelta(hours=24)) & (my_df['status'] != 'Tạm dừng')]
                elif st.session_state['job_filter'] == 'paused': display_df = my_df[my_df['status'] == 'Tạm dừng']
                else: display_df = my_df

                if search_kw:
                    search_kw = search_kw.lower()
                    display_df['search_str'] = display_df.apply(lambda x: f"{x['id']} {x['customer_name']} {x['customer_phone']} {x['address']} {extract_proc_from_log(x['logs'])}".lower(), axis=1)
                    display_df = display_df[display_df['search_str'].str.contains(search_kw, na=False)]

                if sel_stage != "Tất cả": display_df = display_df[display_df['current_stage'] == sel_stage]
                if sel_user != "Tất cả":
                    u_filter = sel_user.split(' - ')[0]
                    display_df = display_df[display_df['assigned_to'].astype(str).str.contains(u_filter, na=False)]
                if 'start_dt' in display_df.columns:
                    if time_option == "Tháng này":
                        start_month = date.today().replace(day=1)
                        display_df = display_df[display_df['start_dt'] >= start_month]
                    elif time_option == "Khoảng ngày" and d_range and len(d_range) == 2:
                        display_df = display_df[(display_df['start_dt'] >= d_range[0]) & (display_df['start_dt'] <= d_range[1])]

                st.divider()
                
                filter_map = {'overdue': '🔴 QUÁ HẠN', 'urgent': '🟡 SẮP ĐẾN HẠN (<24h)', 'paused': '⛔ TẠM DỪNG', 'all': '🟢 TẤT CẢ'}
                cur_filter = st.session_state.get('job_filter', 'all')
                st.caption(f"Đang hiển thị: **{filter_map.get(cur_filter, 'Tất cả')}** ({len(display_df)} hồ sơ)")
                
                if display_df.empty:
                    st.warning("Không tìm thấy hồ sơ nào phù hợp bộ lọc.")
                else:
                    display_df = display_df.sort_values(by=['status', 'id'], ascending=[True, False])
                    render_complex_list_view(display_df, user, role, user_list)

    elif sel == "🗄️ Lưu Trữ":
        st.title("🗄️ Kho Lưu Trữ Hồ Sơ")
        if df.empty: st.info("Chưa có dữ liệu.")
        else:
            archive_df = df[df['status'].isin(['Hoàn thành', 'Kết thúc sớm'])].copy()
            if archive_df.empty: st.info("Chưa có hồ sơ nào đã hoàn thành.")
            else:
                archive_df['start_dt'] = pd.to_datetime(archive_df['start_time'], errors='coerce')
                archive_df['year'] = archive_df['start_dt'].dt.year
                archive_df['month'] = archive_df['start_dt'].dt.month
                unique_years = sorted(archive_df['year'].dropna().unique().astype(int), reverse=True)
                if not unique_years: unique_years = [datetime.now().year]

                with st.container(border=True):
                    c_filter_y, c_filter_m = st.columns(2)
                    sel_year = c_filter_y.selectbox("📅 Chọn Năm", unique_years)
                    sel_month = c_filter_m.selectbox("📅 Chọn Tháng", range(1, 13), index=datetime.now().month-1)
                
                filtered_archive = archive_df[(archive_df['year'] == sel_year) & (archive_df['month'] == sel_month)]
                
                count_total = len(filtered_archive)
                total_rev = filtered_archive['survey_fee'].apply(safe_int).sum()
                count_done = len(filtered_archive[filtered_archive['status']=='Hoàn thành'])
                
                m1, m2, m3 = st.columns(3)
                m1.metric("Tổng Hồ Sơ", count_total)
                m2.metric("Doanh Thu", f"{total_rev:,.0f} đ")
                m3.metric("Hoàn thành", count_done)
                
                st.divider()
                if filtered_archive.empty: st.warning(f"Không có hồ sơ nào trong tháng {sel_month}/{sel_year}.")
                else:
                    st.subheader("📋 Bảng Số Liệu Chi Tiết")
                    display_table = filtered_archive[['id', 'start_time', 'customer_name', 'customer_phone', 'address', 'status', 'survey_fee']].copy()
                    display_table['survey_fee'] = display_table['survey_fee'].apply(safe_int)
                    display_table.columns = ['Mã', 'Ngày tạo', 'Khách hàng', 'SĐT', 'Địa chỉ', 'Trạng thái', 'Phí đo đạc']
                    st.dataframe(display_table, use_container_width=True, hide_index=True)

    elif sel == "📝 Tạo Hồ Sơ":
        st.title("Tạo Hồ Sơ")
        c1, c2 = st.columns(2); n = c1.text_input("Tên Khách Hàng"); p = c2.text_input("SĐT"); a = st.text_input("Địa chỉ")
        c3, c4 = st.columns([1, 1]); 
        with c3: is_sv = st.checkbox("🛠️ CHỈ ĐO ĐẠC")
        with c4: proc = st.selectbox("Thủ tục", PROCEDURES_LIST)
        st.markdown("---")
        f = st.file_uploader("File (Có thể chọn nhiều)", accept_multiple_files=True, key=f"new_up_{st.session_state['uploader_key']}")
        st.markdown("---")
        asn = st.selectbox("Giao việc cho:", user_list)
        if st.button("Tạo Hồ Sơ", type="primary"):
            if n and asn: 
                add_job(n, p, a, proc, f, user, asn)
                st.session_state['uploader_key'] += 1
                st.success("OK! Hồ sơ mới đã tạo.")
                st.rerun()
            else: st.error("Thiếu tên hoặc người giao việc!")

    elif sel == "📅 Lịch Biểu":
        st.title("📅 Lịch Làm Việc")
        df = get_all_jobs_df()
        if not df.empty:
            active_df = df[df['status'] != 'Đã xóa']
            c_y, c_m = st.columns(2)
            now = datetime.now()
            sel_year = c_y.number_input("Năm", 2020, 2030, now.year)
            sel_month = c_m.number_input("Tháng", 1, 12, now.month)
            
            active_df['start_dt_only'] = pd.to_datetime(active_df['start_time'], errors='coerce').dt.date
            active_df['deadline_dt_only'] = pd.to_datetime(active_df['deadline'], errors='coerce').dt.date
            
            cal = calendar.monthcalendar(sel_year, sel_month)
            days_cols = st.columns(7)
            days_names = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"]
            for i, d in enumerate(days_names): days_cols[i].markdown(f"**{d}**", unsafe_allow_html=True)
                
            for week in cal:
                week_cols = st.columns(7)
                for i, day in enumerate(week):
                    with week_cols[i]:
                        if day != 0:
                            st.markdown(f"#### {day}")
                            current_date = date(sel_year, sel_month, day)
                            
                            starts = active_df[active_df['start_dt_only'] == current_date]
                            for _, s in starts.iterrows(): 
                                with st.container(border=True):
                                    st.caption("📌 Nhận mới")
                                    if st.button(f"{s['customer_name']}", key=f"cal_start_{s['id']}"):
                                        st.session_state['selected_cal_id'] = s['id']

                            ends = active_df[active_df['deadline_dt_only'] == current_date]
                            for _, e in ends.iterrows(): 
                                if e['status'] != 'Hoàn thành': 
                                    with st.container(border=True):
                                        st.caption("⚠️ Hạn chót")
                                        if st.button(f"{e['customer_name']}", key=f"cal_end_{e['id']}"):
                                            st.session_state['selected_cal_id'] = e['id']
                            st.divider()
            
            # HIỂN THỊ CHI TIẾT DƯỚI LỊCH KHI BẤM
            if 'selected_cal_id' in st.session_state:
                st.markdown("---")
                st.subheader("🔎 Chi tiết hồ sơ từ Lịch")
                job_data = active_df[active_df['id'] == st.session_state['selected_cal_id']]
                if not job_data.empty:
                    render_job_card_content(job_data.iloc[0], user, role, user_list)

    elif sel == "💰 Công Nợ":
        st.title("💰 Quản Lý Công Nợ")
        if df.empty: st.info("Chưa có dữ liệu.")
        else:
            active_df = df[df['status'] != 'Đã xóa'].copy()
            active_df['fee_float'] = active_df['survey_fee'].apply(safe_int)
            active_df['paid_bool'] = active_df['is_paid'].apply(safe_int)
            unpaid_df = active_df[active_df['paid_bool'] == 0]
            
            total_receivable = unpaid_df['fee_float'].sum()
            count_debtors = len(unpaid_df)
            
            c1, c2 = st.columns(2)
            c1.metric("Tổng Phải Thu", f"{total_receivable:,.0f} VNĐ")
            c2.metric("Số Hồ Sơ Còn Nợ", count_debtors)
            
            st.markdown("### 📋 Danh sách chi tiết")
            if not unpaid_df.empty:
                display_debt = pd.DataFrame()
                display_debt['Mã HS'] = unpaid_df['id']
                # FIX LỖI TYPE ERROR Ở ĐÂY
                display_debt['Khách Hàng'] = unpaid_df['customer_name'].astype(str) + " - " + unpaid_df['customer_phone'].astype(str)
                display_debt['Phí Đo Đạc'] = unpaid_df['fee_float']
                display_debt['Trạng Thái'] = "Chưa thu đủ"
                st.dataframe(display_debt, use_container_width=True, hide_index=True)
            else: st.success("Tuyệt vời! Không còn công nợ.")

    elif sel == "📊 Báo Cáo":
        st.title("📊 Dashboard Quản Trị")
        active_df = df[df['status'] != 'Đã xóa'].copy()
        if not active_df.empty:
            active_df['start_dt'] = pd.to_datetime(active_df['start_time'], errors='coerce')
            active_df['month_year'] = active_df['start_dt'].dt.to_period('M')
            active_df['fee_float'] = active_df['survey_fee'].apply(safe_int)
            
            with st.container(border=True):
                view_mode = st.radio("Chế độ xem:", ["Tháng này", "Toàn bộ"], horizontal=True)
            
            now = datetime.now()
            current_month = now.strftime('%Y-%m')
            filtered_df = active_df[active_df['start_dt'].dt.strftime('%Y-%m') == current_month] if view_mode == "Tháng này" else active_df

            tab1, tab2 = st.tabs(["🏢 Sức Khỏe Doanh Nghiệp", "👥 Hiệu Suất Nhân Sự"])
            with tab1:
                col1, col2, col3, col4 = st.columns(4)
                total_jobs = len(filtered_df)
                total_rev = filtered_df['fee_float'].sum()
                completed_jobs = len(filtered_df[filtered_df['status'] == 'Hoàn thành'])
                total_debt = filtered_df[filtered_df['is_paid'].apply(safe_int) == 0]['fee_float'].sum()

                col1.metric("Tổng Hồ Sơ", total_jobs)
                col2.metric("Doanh Thu", f"{total_rev:,.0f} đ")
                col3.metric("Công Nợ", f"{total_debt:,.0f} đ")
                col4.metric("Tỷ lệ Hoàn thành", f"{int(completed_jobs/total_jobs*100) if total_jobs>0 else 0}%")

                st.markdown("---")
                c_chart1, c_chart2 = st.columns([2, 1])
                with c_chart1:
                    st.subheader("📈 Xu Hướng")
                    if not active_df.empty:
                        trend_data = active_df.groupby('month_year').agg({'id': 'count', 'fee_float': 'sum'}).rename(columns={'id': 'Số hồ sơ', 'fee_float': 'Doanh thu'})
                        trend_data.index = trend_data.index.astype(str)
                        st.line_chart(trend_data['Doanh thu'], color="#28a745", use_container_width=True)
                with c_chart2:
                    st.subheader("🍰 Nguồn Việc")
                    filtered_df['proc_type'] = filtered_df['logs'].apply(extract_proc_from_log)
                    st.dataframe(filtered_df['proc_type'].value_counts(), use_container_width=True)

            with tab2:
                st.subheader("🏆 Bảng Xếp Hạng")
                staff_metrics = []
                for u in user_list:
                    u_all = filtered_df[filtered_df['assigned_to'] == u]
                    doing = u_all[~u_all['status'].isin(['Hoàn thành', 'Đã xóa', 'Kết thúc sớm'])]
                    done = u_all[u_all['status'] == 'Hoàn thành']
                    staff_metrics.append({"Nhân viên": u.split(' - ')[0], "Đang làm": len(doing), "Đã xong": len(done)})
                st.dataframe(pd.DataFrame(staff_metrics), use_container_width=True, hide_index=True)

    elif sel == "👥 Nhân Sự":
        if role == "Quản lý":
            st.title("Phân Quyền"); df = get_all_users()
            for i, u in df.iterrows():
                with st.container(border=True):
                    c1, c2, c3 = st.columns([0.6, 0.3, 0.1])
                    with c1: st.subheader(f"👤 {u['fullname']}"); st.caption(f"User: {u['username']}")
                    with c2:
                        if u['username']!=user:
                            idx = ROLES.index(u['role']) if u['role'] in ROLES else 2; nr = st.selectbox("", ROLES, index=idx, key=u['username'], label_visibility="collapsed")
                            if nr!=u['role']: update_user_role(u['username'], nr); st.toast("Đã lưu!"); time.sleep(0.5); st.rerun()
                        else: st.info("Admin")
                    with c3:
                        if u['username']!=user:
                            if st.button("🗑️", key=f"del_u_{u['username']}"): delete_user_permanently(u['username']); st.rerun()
        else: st.error("Cấm truy cập!")

    elif sel == "🗑️ Thùng Rác":
        if role == "Quản lý":
            st.title("🗑️ Thùng Rác"); trash_df = df[df['status'] == 'Đã xóa']
            if trash_df.empty: st.success("Thùng rác trống!")
            else:
                render_complex_list_view(trash_df, user, role, user_list)
        else: st.error("Cấm truy cập!")

    elif sel == "🛡️ Nhật Ký":
        if role == "Quản lý":
            st.title("🛡️ Nhật Ký Hệ Thống"); audit_sheet = get_audit_sheet()
            if audit_sheet: st.dataframe(pd.DataFrame(audit_sheet.get_all_records()), use_container_width=True)
        else: st.error("Cấm truy cập!")
