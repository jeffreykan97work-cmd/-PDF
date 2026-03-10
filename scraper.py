from playwright.sync_api import sync_playwright
from datetime import datetime
import os

def generate_pdf():
    # 獲取當前年月 (如果係排程執行，就會自動攞當月時間)
    now = datetime.now()
    target_year = now.year
    target_month = now.month

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        print(f"正在前往氣象局網站...")
        page.goto("https://www.smg.gov.mo/zh/activity")
        page.wait_for_timeout(3000) # 等待載入
        
        # 簡單示範：直接將成個網頁截圖轉做 PDF (為咗確保你第一次測試一定成功)
        # 如果你想精準提取文字再排版，可以日後再修改呢部分代碼
        
        pdf_filename = f"SMG_News_{target_year}_{target_month:02d}.pdf"
        page.pdf(path=pdf_filename, format="A4")
        
        browser.close()
        print(f"✅ 成功生成 PDF：{pdf_filename}")

if __name__ == "__main__":
    generate_pdf()
