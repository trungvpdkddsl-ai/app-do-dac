import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def get_gcp_creds(): 
    return Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)

def get_jobs_data():
    """Lấy dữ liệu và lưu vào Session State để tối ưu tốc độ"""
    if 'jobs_df' not in st.session_state:
        creds = get_gcp_creds()
        client = gspread.authorize(creds)
        sh = client.open("DB_DODAC").sheet1
        data = sh.get_all_records()
        df = pd.DataFrame(data)
        
        # Tiền xử lý dữ liệu (như code cũ của bạn)
        if not df.empty:
            df['id'] = pd.to_numeric(df['id'], errors='coerce').fillna(0).astype(int)
            if 'receipt_code' not in df.columns: df['receipt_code'] = "" 
        
        st.session_state['jobs_df'] = df
    return st.session_state['jobs_df']

def force_refresh_data():
    """Chỉ gọi khi người dùng bấm nút làm mới thủ công"""
    if 'jobs_df' in st.session_state:
        del st.session_state['jobs_df']
    return get_jobs_data()

def update_stage_optimized(jid, stg, nxt, user, nt):
    """Vừa update Google Sheets, vừa update Session State (Tốc độ tức thì)"""
    # 1. Update Google Sheets (Giữ nguyên logic của bạn dùng gspread update_cell)
    # ... code sh.update_cell(...) ...
    
    # 2. Cập nhật trực tiếp vào bộ nhớ tạm (KHÔNG DÙNG clear_cache() nữa)
    if 'jobs_df' in st.session_state:
        df = st.session_state['jobs_df']
        idx = df.index[df['id'] == jid].tolist()
        if idx:
            df.at[idx[0], 'current_stage'] = nxt
            df.at[idx[0], 'logs'] += f"\n[{datetime.now()}] {user}: {stg}->{nxt} | Note: {nt}"
            st.session_state['jobs_df'] = df
            
    st.toast("✅ Đã cập nhật thành công!")