import requests
import json

def ask_land_law_assistant(query, model_name="llama3"):
    """
    Kết nối tới local server (Ollama) để tư vấn Luật đất đai.
    Đảm bảo Ollama đang chạy ở port 11434.
    """
    url = "http://localhost:11434/api/generate"
    
    system_prompt = """Bạn là một chuyên gia tư vấn pháp lý xuất sắc, am hiểu sâu sắc về Luật Đất đai Việt Nam hiện hành. 
    Nhiệm vụ của bạn là giải đáp các thắc mắc về cấp đổi sổ đỏ, đo đạc tách thửa, thừa kế và chuyển quyền sử dụng đất. 
    Hãy trả lời ngắn gọn, chính xác, trích dẫn luật nếu cần thiết và sử dụng tiếng Việt chuẩn."""
    
    payload = {
        "model": model_name,
        "prompt": f"{system_prompt}\n\nCâu hỏi của người dùng: {query}",
        "stream": False
    }
    
    try:
        response = requests.post(url, json=payload, timeout=60)
        if response.status_code == 200:
            return response.json().get('response', 'Không có phản hồi từ AI.')
        else:
            return f"Lỗi API: {response.status_code}"
    except requests.exceptions.ConnectionError:
        return "Không thể kết nối tới AI. Vui lòng kiểm tra xem Ollama/OpenClaw đã được khởi động chưa (localhost:11434)."
    except Exception as e:
        return f"Đã xảy ra lỗi: {str(e)}"