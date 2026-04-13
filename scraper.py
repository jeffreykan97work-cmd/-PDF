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
    y, m = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
    return y, m

def to_lang_url(url, lang):
    return re.sub(r'/(zh|en|pt)/', f'/{lang}/', url)

def build_page_url(base_url, page_num):
    if page_num <= 1: return base_url
    return f"{base_url.rstrip('/')}/page/{page_num}"

def clean_page(page):
    page.evaluate("""() => {
        const selectors = ['header', 'nav', 'footer', '.site-header', '.breadcrumb', '.navbar'];
        selectors.forEach(s => document.querySelectorAll(s).forEach(el => el.remove()));
    }""")
    page.add_style_tag(content=PRINT_CSS)

def sanitize_filename(filename, max_length=100):
    """清理檔名中的非法字元、換行，並限制長度避免 OSError"""
    # 1. 移除換行符、製表符
    filename = re.sub(r'\s+', ' ', filename).strip()
    # 2. 移除系統不允許的非法字元
    filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
    # 3. 截斷長度 (保留空間給序號和副檔名)
    if len(filename) > max_length:
        filename = filename[:max_length].strip() + "..."
    return filename

# ── 抓取核心 ────────────────────────────────────────────────────────────────

def extract_article_links(page):
    results = []
    elements = page.query_selector_all("a[href*='news-detail']")
    for el in elements:
        href = el.get_attribute("href")
        if not href: continue
        full_url = href if href.startswith("http") else BASE_URL + href
        
        # 抓取文字並只保留第一行作為標題
        raw_text = el.inner_text().strip()
        title = raw_text.split('\n')[0].strip()
        
        container_text = el.evaluate("el => el.closest('li, tr, div.item, .list-item')?.innerText || ''")
        date_match = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", container_text)
        if date_match:
            date_str = f"{int(date_match.group(1)):04d}-{int(date_match.group(2)):02d}-{int(date_match.group(3)):02d}"
            results.append({
                "url": full_url,
                "text": title,
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
        except Exception:
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
                stop_source = True
        log.info(f"  第 {p_num} 頁找到 {found_in_page} 篇目標文章")
        if stop_source: break
    return items

# ── PDF 處理 ────────────────────────────────────────────────────────────────

def download_pdf_versions(page, item, tmp_dir, idx):
    downloaded = []
    base_name = f"temp_{idx:02d}"
    for lang, label in LANGUAGES:
        target_url = to_lang_url(item['url'], lang)
        output_path = os.path.join(tmp_dir, f"{base_name}_{lang}.pdf")
        try:
            page.goto(target_url, wait_until="networkidle", timeout=20000)
            if lang != "zh":
                body_text = page.inner_text("body").lower()
                if any(m in body_text for m in NO_CONTENT_MARKERS):
                    continue
            page.evaluate(WAIT_IMAGES_JS)
            clean_page(page)
            page.pdf(
                path=output_path,
                format="A4",
                print_background=True,
                margin={"top": "1cm", "bottom": "1cm", "left": "1cm", "right": "1cm"}
            )
            if os.path.getsize(output_path) > 1000:
                downloaded.append(output_path)
        except Exception as e:
            log.error(f"    - [{label}] 下載失敗: {e}")
    return downloaded

def compress_pdf(input_path, output_path):
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
        shutil.copy(input_path, output_path)

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
        context = browser.new_context(viewport={'width': 1280, 'height': 1440})
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
        deduplicated_items.sort(key=lambda x: x['date_str'])
        
        log.info(f"📊 共收集到 {len(deduplicated_items)} 篇不重複文章")
        
        # 3. 下載與單則合併
        article_merged_files = []
        for i, item in enumerate(deduplicated_items):
            seq = i + 1
            # 強化：清理標題並限制長度
            clean_title = sanitize_filename(item['text'])
            final_individual_name = f"{seq:02d}_{clean_title}.pdf"
            final_individual_path = os.path.join(individual_dir, final_individual_name)
            
            log.info(f"({seq}/{len(deduplicated_items)}) 處理: {item['date_str']} {item['text'][:20]}...")
            lang_pdfs = download_pdf_versions(page, item, tmp_dir, i)
            
            if lang_pdfs:
                item_writer = PdfWriter()
                for lp in lang_pdfs:
                    item_writer.append(lp)
                with open(final_individual_path, "wb") as f:
                    item_writer.write(f)
                article_merged_files.append(final_individual_path)
            
        browser.close()

    # 4. 最終合併與壓縮
    if not article_merged_files:
        log.warning("❌ 沒有找到任何文章，任務結束")
        return

    raw_merged = os.path.join(tmp_dir, "raw_merged.pdf")
    final_writer = PdfWriter()
    for p in article_merged_files:
        final_writer.append(p)
    with open(raw_merged, "wb") as f:
        final_writer.write(f)
        
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
