from bs4 import BeautifulSoup
import requests

url = 'https://www.glamira.fr/goanywhere-black-crossbody-bag-fm4wf9976black.html'

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
}

try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    title_tag = soup.find('h1', class_='page-title')
    if title_tag:
        span = title_tag.find('span', class_='base')
        if span:
            product_name = span.get_text(strip=True)
            print("Product Name:", product_name)
        else:
            print("Không tìm thấy <span class='base'> bên trong <h1>")
    else:
        print("Không tìm thấy <h1 class='page-title'>")

except requests.exceptions.RequestException as e:
    print("Lỗi khi tải trang:", e)
except Exception as e:
    print("Lỗi khi phân tích HTML:", e)
