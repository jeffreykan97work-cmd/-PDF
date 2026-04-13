from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from datetime import date
import os
import re
import logging
import subprocess
import shutil
from pypdf import PdfWriter, PdfReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── 配置 ──────────────────────────────────────────────────────────────────
SOURCES = [
    {"name": "subpage_73", "url": "https://www.smg.gov.mo/zh/subpage/73"},
    {"name": "news", "url": "https://www.smg.gov.mo/zh/news"},
    {"name": "activity", "url": "https://www.smg.gov.mo/zh/activity"},
    {"name": "subpage_124", "url": "https://www.smg.gov.mo/zh/subpage/124"},
    {"name": "climate", "url": "https://www.smg.gov.mo/zh/climate"},
    {"name": "seasonal", "url": "https://www.smg.gov.mo/zh/seasonal"},
    {"name": "holiday_weather", "url": "https://www.smg.gov.mo/zh/news/Holiday_weather"},
    {"name": "chat_info", "url": "https://www.smg.gov.mo/zh/chat-info"},
]

BASE_URL = "https://www.smg.gov.mo"
MAX_BYTES = 8 * 1024 * 1024
MAX_PAGES = 30
LANGUAGES = [("zh", "中文"), ("en", "English"), ("pt", "Português")]
NO_CONTENT_MARKERS = ["no related content", "nenhum conteúdo relacionado", "nenhum conteudo relacionado", "404", "not found"]

WAIT_IMAGES_JS = """
() => new Promise(resolve => {
    const imgs = [...document.images].filter(i => !i.complete);
    if (imgs.length === 0) return resolve();
    let n = imgs.length;
    imgs.forEach(i => {
        i.onload = i.onerror = () => { if (--n === 0) resolve(); };
    });
    setTimeout(resolve, 8000);
})
"""

PRINT_CSS = """
@media print {
    header, nav, footer, .navbar, .site-header, .breadcrumb, .footer, .sidebar, .related-links { display: none !important; }
    .content-area, .news-detail-content { width: 100% !important; margin: 0 !important; padding: 0 !important; }
    * { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }
}
"""

# ── 工具函數 ────────────────────────────────────────────────────────────────

def get_target_month():
    today = date.today()
    y, m = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
    return y, m

def to_lang_url(url, lang):
    for l in ["zh", "en", "pt"]:
        if f"/{l}/" in url:
            return url.replace(f"/{l}/", f"/{lang}/")
    return url

def sanitize_filename(filename, max_length=120):
    if not filename or filename.strip() == "": return "Untitled"
    filename = re.sub(r'\s+', ' ', filename).strip()
    filename = re.sub(r'[\\/*?:"<>|]', "", filename)
    return filename[:max_length]

# ── 抓取核心 ────────────────────────────────────────────────────────────────

def extract_article_links(page):
    results = []
    # 修正 1：擴大連結匹配範圍，包含 chat-info/ 數字結尾的連結
    elements = page.query_selector_all("a[href*='-detail'], a[href*='chat-info/']")
    
    for el in elements:
        href = el.get_attribute("href")
        if not href or "page/" in href: continue # 排除分頁按鈕
        
        full_url = href if href.startswith("http") else BASE_URL + href
        
        # 修正 2：強化標題獲取，先找內部的 .title 或特定標籤
        title = el.evaluate("node => node.querySelector('.title, .subject, h3, h4')?.innerText || node.innerText")
        title = title.split('\n')[0].strip()
        
        # 獲取日期
        container = el.evaluate("el => el.closest('li, tr, div.item, .list-item, .news-item')?.innerText || ''")
        date_match = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", container)
        
        if date_match:
            date_str = f"{int(date_match.group(1)):04d}-{int(date_match.group(2)):02d}-{int(date_match.group(3)):02d}"
            results.append({
                "url": full_url,
                "text": title,
                "date_str": date_str
            })
    return results

def download_and_merge_article(page, item, tmp_dir, individual_dir, seq):
    """下載中英葡版本並合併為單一檔案"""
    lang_pdfs = []
    final_title = item['text']
    
    for lang, label in LANGUAGES:
        target_url = to_lang_url(item['url'], lang)
        temp_pdf = os.path.join(tmp_dir, f"temp_{seq}_{lang}.pdf")
        
        try:
            page.goto(target_url, wait_until="networkidle", timeout=30000)
            
            # 修正 3：如果原本沒標題，在進入詳情頁時從頁面最大的 H 標籤抓取 (對應圖 2 的標題)
            if not final_title or final_title == "無標題":
                page_h_title = page.evaluate("() => document.querySelector('h1, h2, .news-detail-title, .title')?.innerText")
                if page_h_title:
                    final_title = page_h_title.strip()

            # 檢查有無內容
            body_text = page.inner_text("body").lower()
            if any(m in body_text for m in NO_CONTENT_MARKERS):
                continue
            
            page.evaluate(WAIT_IMAGES_JS)
            # 移除導航欄等雜物
            page.evaluate("""() => {
                const s = ['header', 'nav', 'footer', '.site-header', '.breadcrumb', '.navbar', '.header-wrapper'];
                s.forEach(sel => document.querySelectorAll(sel).forEach(el => el.remove()));
            }""")
            page.add_style_tag(content=PRINT_CSS)
            
            page.pdf(
                path=temp_pdf,
                format="A4",
                print_background=True,
                margin={"top": "1.5cm", "bottom": "1.5cm", "left": "1.5cm", "right": "1.5cm"}
            )
            
            if os.path.exists(temp_pdf) and os.path.getsize(temp_pdf) > 2000:
                lang_pdfs.append(temp_pdf)
        except Exception as e:
            log.error(f"    - [{label}] 失敗: {e}")

    if lang_pdfs:
        # 修正 4：合併該文章的所有語系 PDF
        clean_title = sanitize_filename(final_title)
        output_name = f"{seq:02d}_{item['date_str']}_{clean_title}.pdf"
        output_path = os.path.join(individual_dir, output_name)
        
        writer = PdfWriter()
        for pdf in lang_pdfs:
            reader = PdfReader(pdf)
            for p in reader.pages:
                writer.add_page(p)
        
        with open(output_path, "wb") as f:
            writer.write(f)
        return output_path
    return None

# ── 主程序 ──────────────────────────────────────────────────────────────────

def main(year=None, month=None):
    if not year or not month:
        year, month = get_target_month()
        
    log.info(f"🚀 開始抓取 SMG 報告: {year}-{month:02d}")
    
    tmp_dir = f"smg_tmp_{year}_{month:02d}"
    individual_dir = f"SMG_Individual_News_{year}_{month:02d}"
    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(individual_dir, exist_ok=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={'width': 1280, 'height': 1600})
        page = context.new_page()
        
        # 1. 搜集連結
        all_items = []
        for src in SOURCES:
            log.info(f"正在掃描來源: {src['name']}")
            try:
                page.goto(src['url'], wait_until="networkidle", timeout=30000)
                # 簡單分頁處理
                for p_num in range(1, 4): # 抓前 3 頁通常夠了
                    if p_num > 1:
                        new_url = f"{src['url'].rstrip('/')}/page/{p_num}"
                        try:
                            page.goto(new_url, wait_until="networkidle", timeout=10000)
                        except: break
                    
                    found = extract_article_links(page)
                    if not found: break
                    
                    for f in found:
                        ly, lm = map(int, f['date_str'].split('-')[:2])
                        if ly == year and lm == month:
                            all_items.append(f)
                        elif ly < year or (ly == year and lm < month):
                            break
            except Exception as e:
                log.error(f"掃描 {src['name']} 出錯: {e}")
        
        # 2. 去重與排序
        unique_items = {x['url']: x for x in all_items}.values()
        sorted_items = sorted(unique_items, key=lambda x: x['date_str'])
        log.info(f"📊 找到 {len(sorted_items)} 篇符合條件的文章")
        
        # 3. 處理每篇文章 (合併語系)
        final_files = []
        for i, item in enumerate(sorted_items):
            log.info(f"({i+1}/{len(sorted_items)}) 正在處理: {item['date_str']} - {item['text']}")
            merged_path = download_and_merge_article(page, item, tmp_dir, individual_dir, i+1)
            if merged_path:
                final_files.append(merged_path)
        
        browser.close()

    # 4. 最終總月報
    if final_files:
        report_name = f"SMG_Monthly_Report_{year}_{month:02d}.pdf"
        writer = PdfWriter()
        for f in final_files:
            writer.append(f)
        with open(report_name, "wb") as f:
            writer.write(f)
        log.info(f"✅ 任務完成！檔案：{report_name}")
    else:
        log.warning("❌ 未找到任何文章。")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    args = parser.parse_args()
    main(args.year, args.month)
