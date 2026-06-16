from __future__ import annotations
import argparse
import logging
import os
import re
import shutil
from datetime import date
from pathlib import Path
from typing import Optional

from playwright.sync_api import Browser, Page, sync_playwright
from pypdf import PdfReader, PdfWriter

# ── Logging Setup ──────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
BASE_URL = "https://www.smg.gov.mo"
SOURCES = [
    {"name": "activity",        "url": f"{BASE_URL}/zh/activity"},
    {"name": "news",            "url": f"{BASE_URL}/zh/news"},
    {"name": "holiday_weather", "url": f"{BASE_URL}/zh/news/Holiday_weather"},
    {"name": "chat_info",       "url": f"{BASE_URL}/zh/chat-info"},
    {"name": "seasonal",        "url": f"{BASE_URL}/zh/seasonal"},
    {"name": "climate",         "url": f"{BASE_URL}/zh/climate"},
]
MAX_SOURCE_PAGES = 80       
REQUEST_TIMEOUT = 90_000    

def get_target_month() -> tuple[int, int]:
    today = date.today()
    if today.month > 1: return today.year, today.month - 1
    return today.year - 1, 12

def sanitize_filename(name: str, max_len: int = 100) -> str:
    name = re.sub(r"\s+", " ", name).strip()
    return re.sub(r'[\\/*?:"<>|]', "", name)[:max_len] or "Untitled"

def download_pdf_robust(url: str, dest: Path, page: Page) -> bool:
    try:
        with page.context.expect_download(timeout=45000) as download_info:
            page.evaluate(f"window.open('{url}', '_blank')")
        download = download_info.value
        download.save_as(dest)
        return dest.exists() and dest.stat().st_size > 2000
    except Exception as e:
        log.warning(f"  [PDF Download Failed]: {url} - {e}")
        return False

def extract_article_links(page: Page) -> list[dict]:
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(3000)
    
    results = page.evaluate("""() => {
        const DATE_RE = /(20\\d{2})[\\s\\-\\/年\\.]+(0?[1-9]|1[0-2])[\\s\\-\\/月\\.]+(0?[1-9]|[12]\\d|3[01])/;
        const found = [];
        const seen = new Set();
        
        const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null, false);
        let node;
        while (node = walker.nextNode()) {
            const text = node.nodeValue.trim();
            const match = text.match(DATE_RE);
            if (match) {
                let dateStr = match[1] + '-' + match[2].padStart(2,'0') + '-' + match[3].padStart(2,'0');
                let container = node.parentElement;
                
                let links = [];
                let searchNode = container;
                for (let i = 0; i < 6; i++) {
                    if (!searchNode || searchNode.tagName === 'BODY' || searchNode.id === 'header' || searchNode.tagName === 'FOOTER') break;
                    links = Array.from(searchNode.querySelectorAll('a[href]:not([href="#"]), [onclick], [data-url]'));
                    if (links.length > 0 && links.length <= 15) break;
                    searchNode = searchNode.parentElement;
                }
                
                links.forEach(el => {
                    let href = el.getAttribute('href') || el.getAttribute('data-url');
                    if (!href && el.getAttribute('onclick')) {
                        const m = el.getAttribute('onclick').match(/['"](.*?)['"]/);
                        if (m) href = m[1];
                    }
                    
                    if (href && !href.includes('javascript:') && !href.includes('/page/') && !href.includes('?page=')) {
                        if (!seen.has(href)) {
                            seen.add(href);
                            let title = (el.innerText || '').trim();
                            if (title.length < 3 && searchNode) {
                                title = (searchNode.innerText || '').split('\\n')[0].trim();
                            }
                            found.push({ href: href, date_str: dateStr, text: title });
                        }
                    }
                });
            }
        }
        
        if (found.length === 0 && window.location.href.includes('Holiday_weather')) {
            const text = document.body.innerText;
            const m = text.match(DATE_RE);
            if (m) {
                const dStr = m[1] + '-' + m[2].padStart(2,'0') + '-' + m[3].padStart(2,'0');
                found.push({ href: window.location.href, date_str: dStr, text: document.title });
            }
        }
        return found;
    }""")
    
    for item in results:
        item['url'] = item['href'] if item['href'].startswith('http') else BASE_URL + item['href'] if item['href'].startswith('/') else BASE_URL + '/' + item['href']
        
    return results

def process_article(page: Page, item: dict, tmp_dir: Path, seq: int) -> Optional[Path]:
    name = f"{seq:03d}_{item['date_str']}_{item['text'][:30].replace('/','-')}.pdf"
    dest = tmp_dir / sanitize_filename(name)
    try:
        page.goto(item['url'], wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
        page.wait_for_timeout(2000)
        
        pdf_links = page.evaluate("() => Array.from(document.querySelectorAll('a[href$=\"pdf\"], a[href*=\"download\"]')).map(a => a.href)")
        if pdf_links and download_pdf_robust(pdf_links[0], dest, page):
            return dest
        
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)
        page.evaluate("""() => { ['header','nav','footer','.site-header','.breadcrumb','.navbar'].forEach(s => document.querySelectorAll(s).forEach(el => el.remove())); }""")
        page.add_style_tag(content="@media print { body { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; } }")
        page.pdf(path=dest, format="A4", print_background=True)
        return dest if dest.exists() and dest.stat().st_size > 2000 else None
    except Exception as e:
        log.warning(f"  Failed processing article {item['url']}: {e}")
        return None

def main(year: int, month: int):
    log.info(f"🚀 [TRUE-V8 URL INJECTION] Target: {year}-{month:02d}")
    tmp_dir = Path(f"smg_tmp_{year}_{month:02d}")
    tmp_dir.mkdir(exist_ok=True)
    
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(viewport={"width": 1920, "height": 1080}, accept_downloads=True).new_page()
        all_items = {}
        
        for src in SOURCES:
            log.info(f"📋 Scanning: {src['name']}")
            previous_page_links_hash = ""
            
            for page_num in range(1, MAX_SOURCE_PAGES + 1):
                # ── 核心突破：直接修改 URL 進行翻頁 ──
                target_url = src["url"] if page_num == 1 else f"{src['url'].rstrip('/')}/page/{page_num}"
                
                try:
                    page.goto(target_url, wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
                    page.wait_for_timeout(4000) # 給予充分時間載入內容
                except Exception as e:
                    log.warning(f"  Failed to load {target_url}: {e}")
                    break
                
                links = extract_article_links(page)
                
                # 防護機制：如果 /page/99 超出範圍，氣象局可能只會返回第一頁的內容
                current_hash = "".join([l['url'] for l in links])
                if current_hash == previous_page_links_hash and page_num > 1:
                    log.info(f"  Page {page_num} reached end of list (Identical content).")
                    break
                previous_page_links_hash = current_hash
                
                if not links:
                    log.info(f"  Page {page_num}: No links found. End of list.")
                    break
                
                added = 0
                stop_pagination = False
                
                for item in links:
                    try:
                        ly, lm = int(item["date_str"][:4]), int(item["date_str"][5:7])
                        
                        # ── 早停邏輯 ──
                        if (ly, lm) < (year, month):
                            stop_pagination = True
                        
                        if (ly, lm) == (year, month) or "Holiday_weather" in item['url']:
                            if item['url'] not in all_items:
                                all_items[item['url']] = item
                                added += 1
                    except: continue
                
                log.info(f"  Page {page_num}: Processed {len(links)} actual articles. Matched {year}-{month:02d}: {added}")
                
                if stop_pagination:
                    log.info(f"  [Early Stop] Older articles (< {year}-{month:02d}) found. Stopping {src['name']}.")
                    break

        sorted_items = list(all_items.values())
        if not sorted_items:
            log.warning(f"❌ No matching articles found for {year}-{month:02d}.")
            browser.close()
            return
        
        sorted_items.sort(key=lambda x: x["date_str"])
        writer = PdfWriter()
        for i, item in enumerate(sorted_items):
            log.info(f"\n⚙ Processing ({i+1}/{len(sorted_items)}) [{item['date_str']}] {item['text'][:40]}")
            p = process_article(page, item, tmp_dir, i)
            if p:
                try: writer.append(p)
                except: continue
        
        output_file = Path(f"SMG_Monthly_Report_{year}_{month:02d}.pdf")
        with output_file.open("wb") as f:
            writer.write(f)
        log.info(f"✅ Report generated: {output_file.name} ({output_file.stat().st_size/1024/1024:.2f} MB)")
        browser.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=get_target_month()[0])
    parser.add_argument("--month", type=int, default=get_target_month()[1])
    args = parser.parse_args()
    main(args.year, args.month)
