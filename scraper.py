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
    {"name": "news", "url": "https://www.smg.gov.mo/zh/subpage/73"},
    {"name": "climate", "url": "https://www.smg.gov.mo/zh/subpage/124"},
    {"name": "holiday_weather", "url": "https://www.smg.gov.mo/zh/news/Holiday_weather"},
    {"name": "chat_info", "url": "https://www.smg.gov.mo/zh/chat-info"},
]

BASE_URL = "https://www.smg.gov.mo"
MAX_BYTES = 5 * 1024 * 1024
MAX_PAGES = 30
LANGUAGES = [("zh", "中文"), ("en", "English"), ("pt", "Português")]
NO_CONTENT_MARKERS = ["no related content", "nenhum conteúdo relacionado", "nenhum conteudo relacionado"]

WAIT_IMAGES_JS = """
() => new Promise(resolve => {
    const imgs = [...document.images].filter(i => !i.complete);
    if (imgs.length === 0) return resolve();
    let n = imgs.length;
    imgs.forEach(i => {
        i.onload = i.onerror = () => { if (--n === 0) resolve(); };
    });
    setTimeout(resolve, 5000);
})
"""

PRINT_CSS = """
@media print {
    header, nav, footer, .navbar, .site-header, .breadcrumb, .footer { display: none !important; }
    * { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }
}
"""

# ── 工具函數 ────────────────────────────────────────────────────────────────

def get_target_month():
    today = date.today()
    # 正確邏輯：上個月
    y, m = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
    return y, m

def to_lang_url(url, lang):
    return re.sub(r'/(zh|en|pt)/', f'/{lang}/', url)

def build_page_url(base_url, page_num):
    if page_num <= 1: return base_url
    return f"{base_url.rstrip('/')}/page/{page_num}"

def clean_page(page):
    """清理頁面干擾元素"""
    page.evaluate("""() => {
        const selectors = ['header', 'nav', 'footer', '.site-header', '.breadcrumb', '.navbar'];
        selectors.forEach(s => document.querySelectorAll(s).forEach(el => el.remove()));
    }""")
    page.add_style_tag(content=PRINT_CSS)

# ── 抓取核心 ────────────────────────────────────────────────────────────────

def extract_article_links(page):
    results = []
    # 針對 SMG 列表頁的常見選擇器
    elements = page.query_selector_all("a[href*='news-detail']")
    for el in elements:
        href = el.get_attribute("href")
        if not href: continue
        full_url = href if href.startswith("http") else BASE_URL + href
        
        # 獲取日期文字：通常在父級容器或相鄰 span
        container_text = el.evaluate("el => el.closest('li, tr, div.item, .list-item')?.innerText || ''")
        date_match = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", container_text)
        
        if date_match:
            date_str = f"{int(date_match.group(1)):04d}-{int(date_match.group(2)):02d}-{int(date_match.group(3)):02d}"
            results.append({
                "url": full_url,
                "text": el.inner_text().strip(),
                "date_str": date_str
            })
    return results

def collect_items_from_source(page, source, target_y, target_m):
    items = []
    seen_urls = set()
    
    for p_num in range(1, MAX_PAGES + 1):
        url = build_page_url(source['url'], p_num)
        log.info(f"  正在掃描: {url}")
        
        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
        except Exception:
            log.warning(f"  無法載入第 {p_num} 頁，停止該來源")
            break

        page_links = extract_article_links(page)
        if not page_links: break
        
        stop_source = False
        found_in_page = 0
        
        for link in page_links:
            if link['url'] in seen_urls: continue
            
            ly, lm = map(int, link['date_str'].split('-')[:2])
            
            if ly == target_y and lm == target_m:
                items.append({**link, "source": source['name']})
                seen_urls.add(link['url'])
                found_in_page += 1
            elif (ly < target_y) or (ly == target_y and lm < target_m):
                stop_source = True # 已經超過目標月份（舊文章）
        
        log.info(f"  第 {p_num} 頁找到 {found_in_page} 篇目標文章")
        if stop_source: break
        
    return items

# ── PDF 處理 ────────────────────────────────────────────────────────────────

def download_pdf_versions(page, item, tmp_dir, idx):
    downloaded = []
    base_name = f"{item['date_str']}_{idx:02d}"
    
    for lang, label in LANGUAGES:
        target_url = to_lang_url(item['url'], lang)
        output_path = os.path.join(tmp_dir, f"{base_name}_{lang}.pdf")
        
        try:
            page.goto(target_url, wait_until="networkidle", timeout=20000)
            # 檢查「無相關內容」
            if lang != "zh":
                body_text = page.inner_text("body").lower()
                if any(m in body_text for m in NO_CONTENT_MARKERS):
                    continue
            
            # 等待圖片渲染
            page.evaluate(WAIT_IMAGES_JS)
            clean_page(page)
            
            page.pdf(
                path=output_path,
                format="A4",
                print_background=True,
                margin={"top": "1cm", "bottom": "1cm", "left": "1cm", "right": "1cm"}
            )
            if os.path.getsize(output_path) > 1000: # 確保不是空文件
                downloaded.append(output_path)
                log.info(f"    - [{label}] 下載成功")
        except Exception as e:
            log.error(f"    - [{label}] 下載失敗: {e}")
            
    return downloaded

def compress_pdf(input_path, output_path):
    """使用 Ghostscript 壓縮 PDF"""
    if os.path.getsize(input_path) <= MAX_BYTES:
        shutil.copy(input_path, output_path)
        return
        
    log.info("  正在壓縮 PDF...")
    gs_cmd = [
        "gs", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4",
        "-dPDFSETTINGS=/printer", "-dNOPAUSE", "-dQUIET", "-dBATCH",
        f"-sOutputFile={output_path}", input_path
    ]
    try:
        subprocess.run(gs_cmd, check=True)
    except Exception:
        log.warning("  Ghostscript 失敗，使用原檔")
        shutil.copy(input_path, output_path)

# ── 主程序 ──────────────────────────────────────────────────────────────────

def main(year=None, month=None):
    if not year or not month:
        year, month = get_target_month()
        
    log.info(f"🚀 開始抓取 SMG 報告: {year}-{month:02d}")
    
    # 確保資料夾命名符合 GitHub Actions 的尋找路徑
    tmp_dir = f"smg_tmp_{year}_{month:02d}"
    os.makedirs(tmp_dir, exist_ok=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={'width': 1280, 'height': 1440})
        page = context.new_page()
        
        # 1. 收集
        all_items = []
        for src in SOURCES:
            all_items.extend(collect_items_from_source(page, src, year, month))
        
        # 2. 去重與排序 (日期升序)
        all_items.sort(key=lambda x: x['date_str'])
        
        # 3. 下載
        final_pdf_list = []
        for i, item in enumerate(all_items):
            log.info(f"({i+1}/{len(all_items)}) 處理: {item['date_str']} {item['text'][:20]}...")
            pdfs = download_pdf_versions(page, item, tmp_dir, i)
            final_pdf_list.extend(pdfs)
            
        browser.close()

    # 4. 合併與壓縮
    if not final_pdf_list:
        log.warning("❌ 沒有找到任何文章，任務結束")
        return

    raw_merged = os.path.join(tmp_dir, "raw_merged.pdf")
    writer = PdfWriter()
    for p in final_pdf_list:
        writer.append(p)
    
    with open(raw_merged, "wb") as f:
        writer.write(f)
        
    # 確保最終 PDF 命名符合 GitHub Actions 的尋找路徑
    output_filename = f"SMG_Monthly_Report_{year}_{month:02d}.pdf"
    compress_pdf(raw_merged, output_filename)
    log.info(f"✅ 任務完成! 輸出檔案: {output_filename}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    args = parser.parse_args()
    main(args.year, args.month)
