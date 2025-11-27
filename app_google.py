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

def get_proc_abbr(proc_name):
    mapping = {"Cấp lần đầu": "CLD", "Cấp đổi": "CD", "Chuyển quyền": "CQ"}
    return mapping.get(proc_name, "K")

def extract_proc_from_log(log_text):
    match = re.search(r'Khởi tạo \((.*?)\)', str(log_text))
    return match.group(1) if match else ""

def generate_display_name(jid, start_time, name, phone, addr, proc_name):
    try:
        jid_str = str(jid)
        seq = jid_str[-2:] 
        d_obj = datetime.strptime(str(start_time), "%Y-%m-%d %H:%M:%S")
        date_str = d_obj.strftime('%d%m%y')
    except:
        date_str = "000000"; seq = "00"
    abbr = get_proc_abbr(proc_name) if proc_name else ""
    proc_str = f"-{abbr}" if abbr else ""
    return f"{date_str}-{seq}{proc_str} {name} {phone} {addr}"

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
    
    # [FIX] Dùng hàm tìm dòng mới an toàn hơn
    r = find_row_index(sh, job_id)
    if r:
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

def get_daily_sequence_id():
    df = get_all_jobs_df()
    now = datetime.now()
    prefix = int(now.strftime('%y%m%d')) 
    if df.empty: return int(f"{prefix}01"), "01"
    today_ids = [str(jid) for jid in df['id'].tolist() if str(jid).startswith(str(prefix))]
    if not today_ids: seq = 1
    else: max_seq = max([int(jid[-2:]) for jid in today_ids]); seq = max_seq + 1
    seq_str = f"{seq:02}"
    return int(f"{prefix}{seq_str}"), seq_str

# [QUAN TRỌNG] HÀM TÌM DÒNG AN TOÀN (SỬA LỖI API ERROR)
def find_row_index(sh, jid):
    try:
        # Lấy toàn bộ cột ID (Cột 1) về để tìm trong Python
        # Cách này không tốn quota tìm kiếm và không bị lỗi APIError
        ids = sh.col_values(1)
        # Cộng 1 vì list python bắt đầu từ 0, sheet bắt đầu từ 1
        return ids.index(str(jid)) + 1
    except ValueError:
        return None
    except Exception:
        return None

# --- 6. XỬ LÝ HỒ SƠ ---
def add_job(n, p, a, proc, f, u, asn, d, is_survey, deposit_ok, fee_amount):
    sh = get_sheet(); now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    date_code = now.strftime('%d%m%Y')
    dl = (now+timedelta(days=d)).strftime("%Y-%m-%d %H:%M:%S")
    
    jid, seq_str = get_daily_sequence_id()
    abbr = get_proc_abbr(proc)
    sub_folder = f"{date_code}-{seq_str}-{abbr} {n} {p} {a}"
    
    link, fname = upload_to_drive(f, sub_folder)
    log = f"[{now_str}] {u}: Khởi tạo ({proc})"
    if link: log += f" | File: {fname} - {link}"
    asn_clean = asn.split(" - ")[0] if asn else ""
    sv_flag = 1 if is_survey else 0; dep_flag = 1 if deposit_ok else 0
    sh.append_row([jid, now_str, n, p, a, "1. Tạo mới", "Đang xử lý", asn_clean, dl, link, log, sv_flag, dep_flag, fee_amount, 0])
    
    code_display = f"{date_code}-{seq_str}-{abbr} {n}"
    type_msg = f"({proc.upper()})"
    money_msg = "✅ Đã thu tạm ứng" if deposit_ok else "❌ Chưa thu tạm ứng"
    file_msg = f"\n📎 {fname}: {link}" if link else ""
    send_telegram_msg(f"🚀 <b>MỚI #{seq_str} {type_msg}</b>\n📂 <b>{code_display}</b>\n📍 {a}\n👉 {asn_clean}\n💰 {money_msg}{file_msg}")

# [ĐÃ SỬA] Update Stage dùng hàm tìm dòng mới
def update_stage(jid, stg, nt, f, u, asn, d, is_survey, deposit_ok, fee_amount, is_paid):
    sh = get_sheet()
    r = find_row_index(sh, jid) # <-- Dùng hàm tìm an toàn
    
    if r:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); lnk = ""; fname = ""
        c_name = sh.cell(r, 3).value
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
            
            jid_str = str(jid)[-2:]
            send_telegram_msg(f"✅ <b>CẬP NHẬT #{jid_str}</b>\n{stg} ➡ <b>{nxt}</b>\n👤 {u}")

# [ĐÃ SỬA] Update Finance dùng hàm tìm dòng mới
def update_finance_only(jid, deposit_ok, fee_amount, is_paid, u):
    sh = get_sheet()
    r = find_row_index(sh, jid)
    if r:
        sh.update_cell(r, 13, 1 if deposit_ok else 0); sh.update_cell(r, 14, safe_int(fee_amount)); sh.update_cell(r, 15, 1 if is_paid else 0)
        send_telegram_msg(f"💰 <b>TÀI CHÍNH</b>\n👤 {u}\nPhí: {fee_amount:,} VNĐ")

# [ĐÃ SỬA] Pause Job dùng hàm tìm dòng mới
def pause_job(jid, rs, u):
    sh = get_sheet()
    r = find_row_index(sh, jid)
    if r:
        sh.update_cell(r, 7, "Tạm dừng");
        olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{datetime.now()}] {u}: TẠM DỪNG: {rs}")
        send_telegram_msg(f"⛔ <b>TẠM DỪNG</b>\n👤 Bởi: {u}\n📝 Lý do: {rs}")

# [ĐÃ SỬA] Resume Job dùng hàm tìm dòng mới
def resume_job(jid, u):
    sh = get_sheet()
    r = find_row_index(sh, jid)
    if r:
        sh.update_cell(r, 7, "Đang xử lý");
        olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{datetime.now()}] {u}: KHÔI PHỤC")
        send_telegram_msg(f"▶️ <b>KHÔI PHỤC</b>\n👤 Bởi: {u}")

# [ĐÃ SỬA] Terminate Job dùng hàm tìm dòng mới (FIX LỖI CỦA BẠN)
def terminate_job(jid, rs, u):
    sh = get_sheet()
    r = find_row_index(sh, jid) # <-- Thay thế find() bằng hàm này
    if r:
        sh.update_cell(r, 7, "Kết thúc sớm");
        olog = sh.cell(r, 11).value; sh.update_cell(r, 11, olog + f"\n[{datetime.now()}] {u}: KẾT THÚC SỚM: {rs}")
        send_telegram_msg(f"⏹️ <b>KẾT THÚC SỚM</b>\n👤 Bởi: {u}\n📝 Lý do: {rs}")

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
                    proc_name = extract_proc_from_log(j['logs'])
                    code_display = generate_display_name(j['id'], j['start_time'], j['customer_name'], j['customer_phone'], j['address'], proc_name)
                    icon = "⛔" if j['status']=='Tạm dừng' else "⏹️" if j['status']=='Kết thúc sớm' else ("🔴" if j['dl_dt'] < now else "🟡" if j['dl_dt'] <= now+timedelta(days=1) else "🟢")
                    
                    with st.expander(f"{icon} {code_display} | {j['current_stage']}"):
                        render_progress_bar(j['current_stage'], j['status'])
                        t1, t2, t3, t4 = st.tabs(["ℹ️ Thông tin & File", "⚙️ Xử lý Hồ sơ", "💰 Tài Chính", "📜 Nhật ký"])
                        
                        with t1:
                            st.subheader(f"👤 {j['customer_name']}")
                            if safe_int(j.get('is_survey_only')) == 1: st.warning("🛠️ CHỈ ĐO ĐẠC")
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
                                            with col_x.popover("🗑️", help="Xóa"):
                                                st.write("Xóa file này?")
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
                    st.success("OK! Hồ sơ mới đã tạo."); st.rerun()
                else: st.error("Thiếu thông tin!")

    elif sel == "💰 Công Nợ":
        st.title("💰 Quản Lý Công Nợ")
        try:
            df = get_all_jobs_df()
            if not df.empty:
                unpaid = df[df['is_paid'].apply(safe_int) == 0]
                st.metric("Tổng hồ sơ chưa thu tiền", len(unpaid))
                if not unpaid.empty:
                    unpaid['Mã'] = unpaid.apply(lambda x: generate_display_name(x['id'], x['start_time'], x['customer_name'], x['customer_phone'], x['address'], extract_proc_from_log(x['logs'])), axis=1)
                    st.dataframe(unpaid[['Mã', 'survey_fee', 'deposit']], use_container_width=True)
                else: st.success("Sạch nợ!")
        except: pass

    elif sel == "🔍 Tra Cứu":
        st.title("Tra Cứu Thông Minh")
        q = st.text_input("Nhập mã hồ sơ, tên, hoặc SĐT")
        if q:
            df = get_all_jobs_df()
            if not df.empty:
                search_df = df.copy()
                search_df['display_id'] = search_df.apply(lambda x: generate_display_name(x['id'], x['start_time'], x['customer_name'], x['customer_phone'], x['address'], extract_proc_from_log(x['logs'])), axis=1)
                m1 = search_df['display_id'].astype(str).str.contains(q, case=False, na=False)
                m2 = search_df['customer_name'].astype(str).str.contains(q, case=False, na=False)
                m3 = search_df['customer_phone'].astype(str).str.contains(q, case=False, na=False)
                res = search_df[m1 | m2 | m3]
                if not res.empty:
                    st.success(f"Tìm thấy {len(res)} hồ sơ:")
                    view = res[['display_id', 'customer_name', 'current_stage', 'status']]
                    view.columns = ['Mã Hồ Sơ', 'Khách Hàng', 'Tiến Độ', 'Trạng Thái']
                    st.dataframe(view, use_container_width=True, hide_index=True)
                else: st.warning("Không tìm thấy kết quả phù hợp.")

    elif sel == "📊 Báo Cáo":
        st.title("📊 Báo Cáo & Thống Kê")
        df = get_all_jobs_df()
        if not df.empty:
            col_d1, col_d2 = st.columns(2)
            today = date.today()
            first_day = today.replace(day=1)
            start_d = col_d1.date_input("Từ ngày", first_day); end_d = col_d2.date_input("Đến ngày", today)
            df['start_dt'] = pd.to_datetime(df['start_time']).dt.date
            mask = (df['start_dt'] >= start_d) & (df['start_dt'] <= end_d)
            filtered_df = df.loc[mask]
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
                view_df['Mã Hồ Sơ'] = view_df.apply(lambda x: generate_display_name(x['id'], x['start_time'], x['customer_name'], "", "", ""), axis=1)
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
                    with c1:
                        st.subheader(f"👤 {u['fullname']}")
                        st.caption(f"User: {u['username']}")
                    with c2:
                        if u['username']!=user:
                            idx = ROLES.index(u['role']) if u['role'] in ROLES else 2
                            nr = st.selectbox("", ROLES, index=idx, key=u['username'], label_visibility="collapsed")
                            if nr!=u['role']: update_user_role(u['username'], nr); st.toast("Đã lưu!"); time.sleep(0.5); st.rerun()
                        else:
                            st.info("Admin")
        else: st.error("Cấm truy cập!")
