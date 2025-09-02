# Ví dụ về việc sử dụng proxy trong requests

import requests
from bs4 import BeautifulSoup
import time

url = 'https://www.glamira.fr/goanywhere-black-crossbody-bag-fm4wf9976black.html'

# Thông tin proxy của bạn (thay thế bằng thông tin thật)
# Sử dụng định dạng: 'http://user:password@host:port'
proxy_info = {
    "http": "http://yjodokld:7gurvqx86fo2@154.203.43.247:5536",
    "http": "https://yjodokld:7gurvqx86fo2@64.137.96.74:6641",
    "http": "https://yjodokld:7gurvqx86fo2@45.43.186.39:6257"
}

try:
    # Gửi yêu cầu thông qua proxy
    response = requests.get(url, proxies=proxy_info, timeout=10)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')

    # Tìm thẻ h1 với class là 'page-title'
    title_tag = soup.find('h1', class_='page-title')

    if title_tag:
        span = title_tag.find('span', class_='base')
        if span:
            product_name = span.get_text(strip=True)
            print("Product Name:", product_name)
        else:
            print("Không tìm thấy thẻ <span class='base'> bên trong <h1>")
    else:
        print("Không tìm thấy thẻ <h1 class='page-title'>")

except requests.exceptions.RequestException as e:
    print(f"Lỗi: {e}")
except Exception as e:
    print(f"Lỗi không xác định: {e}")

# Tạm dừng để tránh bị chặn IP
time.sleep(5)
