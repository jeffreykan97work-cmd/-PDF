from __future__ import annotations
import argparse
import logging
import os
import re
import shutil
import subprocess
from datetime import date
from pathlib import Path
from typing import Optional

from playwright.sync_api import Browser, Page, sync_playwright, TimeoutError as PWTimeout
from pypdf import PdfReader, PdfWriter

# ── Logging Setup ──────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
BASE_URL = "https://www.smg.gov.mo"
SOURCES: list[dict] = [
    {"name": "activity",        "url": f"{BASE_URL}/zh/activity"},
    {"name": "news",            "url": f"{BASE_URL}/zh/news"},
    {"name": "holiday_weather", "url": f"{BASE_URL}/zh/news/Holiday_weather"},
    {"name": "chat_info",       "url": f"{BASE_URL}/zh/chat-info"},
    {"name": "seasonal",        "url": f"{BASE_URL}/zh/seasonal"},
    {"name": "climate",         "url": f"{BASE_URL}/zh/climate"},
]
LANG_CODES = ["zh", "en", "pt"]
MAX_FINAL_BYTES = 50 * 1024 * 1024 
MAX_SOURCE_PAGES = 80       
REQUEST_TIMEOUT = 90_000    

PDF_LINK_SELECTORS = ["a[href$='.pdf']", "a[href*='.pdf?']", "a[href*='/pdf/']", "a[href*='download']", "a[href*='attach']", "a[href*='file']"]
WAIT_IMAGES_JS = """
() => new Promise(resolve => {
    const imgs = [...document.images].filter(i => !i.complete);
    if (!imgs.length) return resolve();
    let n = imgs.length;
    imgs.forEach(i => { i.onload = i.onerror = () => { if (--n === 0) resolve(); }; });
    setTimeout(resolve, 8000);
})
"""

def get_target_month() -> tuple[int, int]:
    today = date.today()
    if today.month > 1: return today.year, today.month - 1
    return today.year - 1, 12

def sanitize_filename(name: str, max_len: int = 100) -> str:
    name = re.sub(r"\s+", " ", name).strip()
    return re.sub(r'[\\/*?:"<>|]', "", name)[:max_len] or "Untitled"

def switch_lang(url: str, target_lang: str) -> str:
    for lang in LANG_CODES:
        if f"/{lang}/" in url: return url.replace(f"/{lang}/", f"/{target_lang}/", 1)
    return url

def resolve_url(href: str) -> str:
    return href if href.startswith("http") else BASE_URL + href if href.startswith("/") else BASE_URL + "/" + href

def download_pdf(url: str, dest: Path, page: Page) -> bool:
    try:
        r = page.context.request.get(url, timeout=60000)
        if not r.ok: return False
        content = r.body()
        if not content.startswith(b"%PDF") and b"pdf" not in r.headers.get("content-type", "").lower() and not url.lower().endswith(".pdf"):
            return False
        dest.write_bytes(content)
        return dest.stat().st_size > 2000
    except Exception:
        dest.unlink(missing_ok=True)
        return False

def extract_article_links(page: Page) -> list[dict]:
    results = page.evaluate("""() => {
        const DATE_RE_1 = /(20\\d{2})[\\s\\-\\/年\\.]+(\\d{1,2})[\\s\\-\\/月\\.]+(\\d{1,2})/;
        const results = [];
        const links = Array.from(document.querySelectorAll('a[href], [onclick], [data-href], [data-url]'));
        
        links.forEach(el => {
            let href = el.getAttribute('href') || el.getAttribute('data-href') || el.getAttribute('data-url');
            if (!href || href === '#' || href.toLowerCase().includes('javascript:')) return;
            
            let node = el;
            let dateStr = null;
            let title = (el.innerText || '').trim();
            
            for (let i = 0; i < 15; i++) {
                if (!node || node.tagName === 'BODY' || node.tagName === 'HTML') break;
                const text = (node.innerText || '').replace(/\\u200B/g, '').trim();
                const match = text.match(DATE_RE_1);
                if (match) {
                    dateStr = match[1] + '-' + match[2].padStart(2,'0') + '-' + match[3].padStart(2,'0');
                    break;
                }
                node = node.parentElement;
            }
            if (dateStr) results.push({ href: href, date_str: dateStr, text: title });
        });
        return results;
    }""")
    
    unique_results = {}
    for item in items_data := results:
        url = resolve_url(item['href'])
        if url not in unique_results:
            unique_results[url] = item
    return list(unique_results.values())

def process_article(page: Page, item: dict, tmp_dir: Path, out_dir: Path, seq: int) -> Optional[Path]:
    # (此處保留原有的 PDF 處理邏輯，為節省空間省略，請複製你之前檔案中的函數)
    # ... (請確保此處包含完整的 process_article 函數) ...
    pass # 為了讓你複製完整，請確保此函數保留

def execute_scraping(year: int, month: int):
    log.info(f"🚀 [SUPER-V6-STOP] Execution launched — target: {year}-{month:02d}")
    
    # 建立臨時目錄
    tmp_dir = Path(f"smg_tmp_{year}_{month:02d}")
    tmp_dir.mkdir(exist_ok=True)
    
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = ctx.new_page()
        all_items = {}
        
        for src in SOURCES:
            log.info(f"📋 Scanning: {src['name']}")
            page.goto(src["url"], wait_until="domcontentloaded")
            
            for page_num in range(1, MAX_SOURCE_PAGES + 1):
                found = extract_article_links(page)
                
                # ── 核心邏輯：時間早停 ──
                stop_scanning = False
                for item in found:
                    try:
                        ly, lm = int(item["date_str"][:4]), int(item["date_str"][5:7])
                        # 如果發現文章日期已經早於目標月份，設置標記
                        if (ly, lm) < (year, month):
                            log.info(f"  Reached older date {ly}-{lm}. Stopping this source.")
                            stop_scanning = True
                            break
                        
                        if ly == year and lm == month:
                            all_items[item["url"]] = item
                    except: continue
                
                if stop_scanning: break
                # ... (模擬點擊下一頁邏輯) ...
                
        # ... (後續合併 PDF 邏輯) ...

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",  type=int, default=get_target_month()[0])
    parser.add_argument("--month", type=int, default=get_target_month()[1])
    args = parser.parse_args()
    execute_scraping(args.year, args.month)
