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
MAX_BYTES = 8 * 1024 * 1024 # 稍微放寬壓縮閾值
MAX_PAGES = 30
LANGUAGES = [("zh", "中文"), ("en", "English"), ("pt", "Português")]
# 增加更多無內容判定
NO_CONTENT_MARKERS = ["no related content", "nenhum conteúdo relacionado", "nenhum conteudo relacionado", "404", "Not Found"]

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
    # 更強健的語系替換邏輯
    for l in ["zh", "en", "pt"]:
        if f"/{l}/" in url:
            return url.replace(f"/{l}/", f"/{lang}/")
    return url

def build_page_url(base_url, page_num):
    if page_num <= 1: return base_url
    return f"{base_url.rstrip('/')}/page/{page_num}"

def clean_page(page):
    page.evaluate("""() => {
        const selectors = ['header', 'nav', 'footer', '.site-header', '.breadcrumb', '.navbar', '.footer-wrapper', '.header-wrapper'];
        selectors.forEach(s => document.querySelectorAll(s).forEach(el => el.remove()));
    }""")
    page.add_style_tag(content=PRINT_CSS)

def sanitize_filename(filename, max_length=120):
    if not filename: return "Untitled"
    # 移除換行、多餘空格
    filename = re.sub(r'\s+', ' ', filename).strip()
    # 移除非法字元
    filename = re.sub(r'[\\/*?:"<>|]', "", filename)
    if len(filename) > max_length:
        filename = filename[:max_length].strip()
    return filename

# ── 抓取核心 ────────────────────────────────────────────────────────────────

def extract_article_links(page):
    results = []
    # 擴大搜索範圍，適應 chat-info 等不同頁面結構
    elements = page.query_selector_all("a[href*='news-detail']")
    
    for el in elements:
        href = el.get_attribute("href")
        if not href: continue
        full_url = href if href.startswith("http") else BASE_URL + href
        
        # 抓取標題：優先找內部的標題元素，否則用 inner_text
        title = el.evaluate("node => node.querySelector('.title, h3, h4')?.innerText || node.innerText")
        title = title.split('\n')[0].strip()
        
        # 尋找日期
        container_text = el.evaluate("el => el.closest('li, tr, div.item, .list-item, .news-item')?.innerText || ''")
        date_match = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", container_text)
        
        if date_match:
            date_str = f"{int(date_match.group(1)):04d}-{int(date_match.group(2)):02d}-{int(date_match.group(3)):02d}"
            results.append({
                "url": full_url,
                "text": title if title else "無標題",
                "date_str": date_str
            })
    return results

def collect_items_from_source(page, source, target_y, target_m):
    items = []
    seen_urls = set()
    for p_num in range(1, MAX_PAGES + 1):
        url = build_page_url(source['url'], p_num)
        log.info(f"  正在掃描 [{source['name']}]: {url}")
        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
            # 針對 chat_info 這種可能加載較慢的頁面稍微等待
            page.wait_for_timeout(1000)
        except Exception:
            break
            
        page_links = extract_article_links(page)
        if not page_links: break
        
        stop_source = False
        found_in_page = 0
        for link in page_links:
            if link['url'] in seen_urls: continue
            try:
                ly, lm = map(int, link['date_str'].split('-')[:2])
                if ly == target_y and lm == target_m:
                    items.append({**link, "source": source['name']})
                    seen_urls.add(link['url'])
                    found_in_page += 1
                elif (ly < target_y) or (ly == target_y and lm < target_m):
                    stop_source = True
            except: continue
            
        log.info(f"  第 {p_num} 頁找到 {found_in_page} 篇目標文章")
        if stop_source: break
    return items

# ── PDF 處理 ────────────────────────────────────────────────────────────────

def download_pdf_versions(page, item, tmp_dir, idx):
    downloaded = []
    base_name = f"item_{idx:03d}"
    
    for lang, label in LANGUAGES:
        target_url = to_lang_url(item['url'], lang)
        output_path = os.path.join(tmp_dir, f"{base_name}_{lang}.pdf")
        
        try:
            log.info(f"    -> 正在下載 {label} 版...")
            page.goto(target_url, wait_until="networkidle", timeout=30000)
            
            # 檢查是否真的有內容
            body_text = page.inner_text("body").lower()
            if any(marker in body_text for marker in NO_CONTENT_MARKERS):
                log.warning(f"    - {label} 版似乎無相關內容，跳過")
                continue
            
            page.evaluate(WAIT_IMAGES_JS)
            clean_page(page)
            
            page.pdf(
                path=output_path,
                format="A4",
                print_background=True,
                margin={"top": "1.5cm", "bottom": "1.5cm", "left": "1.5cm", "right": "1.5cm"}
            )
            
            if os.path.exists(output_path) and os.path.getsize(output_path) > 2000:
                downloaded.append(output_path)
        except Exception as e:
            log.error(f"    - {label} 版處理失敗: {e}")
            
    return downloaded

# ── 主程序 ──────────────────────────────────────────────────────────────────

def main(year=None, month=None):
    if not year or not month:
        year, month = get_target_month()
        
    log.info(f"🚀 開始執行 SMG 報告抓取任務: {year}年{month:02d}月")
    
    tmp_dir = f"smg_tmp_{year}_{month:02d}"
    individual_dir = f"SMG_Individual_News_{year}_{month:02d}"
    
    if os.path.exists(tmp_dir): shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir, exist_ok=True)
    if os.path.exists(individual_dir): shutil.rmtree(individual_dir)
    os.makedirs(individual_dir, exist_ok=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # 模擬常見視窗大小
        context = browser.new_context(viewport={'width': 1280, 'height': 1600})
        page = context.new_page()
        
        # 1. 收集
        all_items = []
        for src in SOURCES:
            all_items.extend(collect_items_from_source(page, src, year, month))
        
        # 2. 去重與排序
        unique_urls = set()
        deduplicated_items = []
        for item in all_items:
            if item['url'] not in unique_urls:
                unique_urls.add(item['url'])
                deduplicated_items.append(item)
        
        # 依日期排序
        deduplicated_items.sort(key=lambda x: x['date_str'])
        log.info(f"📊 篩選後共計 {len(deduplicated_items)} 篇文章")
        
        # 3. 處理每一篇 (合併三語)
        article_merged_paths = []
        for i, item in enumerate(deduplicated_items):
            seq = i + 1
            clean_title = sanitize_filename(item['text'])
            # 檔名：序號 + 日期 + 標題
            final_filename = f"{seq:02d}_{item['date_str']}_{clean_title}.pdf"
            final_path = os.path.join(individual_dir, final_filename)
            
            log.info(f"({seq}/{len(deduplicated_items)}) 處理文章: {item['text']}")
            
            # 抓取該文章的所有語系 PDF
            lang_pdfs = download_pdf_versions(page, item, tmp_dir, i)
            
            if lang_pdfs:
                # 在這裡執行「合併」操作：將中英葡 PDF 拼成一個檔案
                writer = PdfWriter()
                for pdf_file in lang_pdfs:
                    reader = PdfReader(pdf_file)
                    for pg in reader.pages:
                        writer.add_page(pg)
                
                with open(final_path, "wb") as f:
                    writer.write(f)
                
                article_merged_paths.append(final_path)
                log.info(f"    ✅ 已合併多語系並存至: {final_filename}")
            else:
                log.warning(f"    ❌ 無法獲取任何語系內容: {item['text']}")
            
        browser.close()

    # 4. 生成最終總月報
    if article_merged_paths:
        output_filename = f"SMG_Monthly_Report_{year}_{month:02d}.pdf"
        log.info(f"📦 正在製作最終總月報...")
        
        final_writer = PdfWriter()
        for p in article_merged_paths:
            final_writer.append(p)
            
        raw_final = os.path.join(tmp_dir, "raw_total.pdf")
        with open(raw_final, "wb") as f:
            final_writer.write(f)
            
        # 檢查是否需要壓縮 (調用之前定義的 gs 壓縮)
        shutil.copy(raw_final, output_filename)
        log.info(f"✨ 任務全部完成！總檔案：{output_filename}")
        log.info(f"📂 獨立檔案已存放於：{individual_dir}")
    else:
        log.error("結束：未找到符合條件的內容。")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    args = parser.parse_args()
    main(args.year, args.month)
