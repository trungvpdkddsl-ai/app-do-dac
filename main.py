import streamlit as st
import pandas as pd
# Import các module đã tách
from database import get_jobs_data, force_refresh_data, update_stage_optimized
from document_utils import generate_legal_document
from ai_assistant import ask_land_law_assistant
# from image_utils import create_a4_print_layout 

# ... [Các cấu hình ban đầu giữ nguyên] ...

# --- CÁCH GỌI TÍNH NĂNG 1: TRONG HÀM render_job_card_content ---
def render_job_card_content(j, user, role, user_list):
    # ... [Code hiển thị thông tin khách hàng của bạn] ...
    
    # [MỚI] Nút Tự động xuất biểu mẫu
    with st.expander("📄 Tự động xuất biểu mẫu", expanded=False):
        st.write("Hệ thống sẽ tự động điền thông tin khách hàng vào mẫu đơn.")
        template_choice = st.selectbox("Chọn mẫu đơn:", ["Đơn đề nghị đo đạc", "Hợp đồng dịch vụ"], key=f"tpl_{j['id']}")
        
        if st.button("Tạo File Word", key=f"btn_doc_{j['id']}"):
            with st.spinner("Đang tạo file..."):
                # Gỉa sử file mẫu lưu ở thư mục templates
                tpl_path = "templates/don_de_nghi_do_dac.docx" 
                doc_file = generate_legal_document(tpl_path, j)
                
                st.download_button(
                    label="⬇️ Tải file Word đã điền",
                    data=doc_file,
                    file_name=f"{j['customer_name']}_{template_choice}.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    key=f"dl_doc_{j['id']}"
                )

# ... [Các menu khác] ...

# --- CÁCH GỌI TÍNH NĂNG 2: THÊM MENU TRỢ LÝ AI ---
# Giả sử bạn thêm menu "🤖 Cố Vấn AI" vào hàm render_square_menu
elif sel == "🤖 Cố Vấn AI":
    st.title("⚖️ Trợ Lý Trí Tuệ Nhân Tạo - Pháp Lý Đất Đai")
    st.info("Trợ lý sử dụng mô hình AI local để tư vấn thủ tục, luật đất đai và các quy định về tách thửa.")
    
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Hiển thị lịch sử chat
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Box nhập câu hỏi
    if prompt := st.chat_input("Nhập câu hỏi pháp lý (VD: Điều kiện tách thửa đất ở nông thôn?)..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Đang tra cứu dữ liệu pháp lý..."):
                # Gọi thẳng vào model local đang chạy (có thể đổi tên model tùy bạn setup)
                response = ask_land_law_assistant(prompt, model_name="llama3") 
                st.markdown(response)
        st.session_state.messages.append({"role": "assistant", "content": response})