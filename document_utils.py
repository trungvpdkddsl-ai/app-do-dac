import io
from docxtpl import DocxTemplate
from datetime import datetime

def generate_legal_document(template_path, job_data):
    """
    Tạo file Word từ template và dữ liệu hồ sơ
    job_data: một dictionary hoặc pd.Series chứa thông tin dòng hiện tại
    """
    doc = DocxTemplate(template_path)
    
    # Định nghĩa các biến sẽ điền vào file Word
    context = {
        'ho_ten': job_data.get('customer_name', ''),
        'so_dien_thoai': str(job_data.get('customer_phone', '')).replace("'", ""),
        'dia_chi': job_data.get('address', ''),
        'thu_tuc': job_data.get('current_stage', ''), # Hoặc hàm extract_proc_from_log
        'ma_ho_so': job_data.get('id', ''),
        'ngay_thang_nam': datetime.now().strftime("Ngày %d tháng %m năm %Y")
    }
    
    doc.render(context)
    
    # Lưu ra buffer để Streamlit cho phép tải về
    file_stream = io.BytesIO()
    doc.save(file_stream)
    file_stream.seek(0)
    
    return file_stream