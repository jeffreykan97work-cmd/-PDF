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
        # 使用真實瀏覽器下載事件截取，繞過一切防護
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
        
        // 1. 尋找網頁上所有的文字節點，篩選出符合日期的文字
        const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null, false);
        let node;
        while (node = walker.nextNode()) {
            const text = node.nodeValue.trim();
            const match = text.match(DATE_RE);
            if (match) {
                let dateStr = match[1] + '-' + match[2].padStart(2,'0') + '-' + match[3].padStart(2,'0');
                let container = node.parentElement;
                
                // 2. 從日期出發，往上找尋包含連結的文章卡片 (最多找 6 層，避免抓到整個網頁)
                let links = [];
                let searchNode = container;
                for (let i = 0; i < 6; i++) {
                    if (!searchNode || searchNode.tagName === 'BODY' || searchNode.id === 'header' || searchNode.tagName === 'FOOTER') break;
                    links = Array.from(searchNode.querySelectorAll('a[href]:not([href="#"]), [onclick], [data-url]'));
                    
                    // 如果這個容器內有 1 到 15 個連結，代表這是一個合理的獨立文章卡片
                    if (links.length > 0 && links.length <= 15) {
                        break;
                    }
                    searchNode = searchNode.parentElement;
                }
                
                // 3. 將這些連結與這個日期綁定
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
        
        // 處理 Holiday_weather 這種沒有列表的單頁
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
    
    # 補全完整的 URL
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
        
        # 如果沒有實體 PDF 檔案，截取網頁畫面列印成 PDF
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
    log.info(f"🚀 [TRUE-V7-LOGIC] Target: {year}-{month:02d}")
    tmp_dir = Path(f"smg_tmp_{year}_{month:02d}")
    tmp_dir.mkdir(exist_ok=True)
    
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(viewport={"width": 1920, "height": 1080}, accept_downloads=True).new_page()
        all_items = []
        
        for src in SOURCES:
            log.info(f"📋 Scanning: {src['name']}")
            try:
                page.goto(src["url"], wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
            except Exception as e:
                log.warning(f"  Skip category {src['name']}: {e}")
                continue
            
            previous_page_links_hash = ""
            
            for page_num in range(1, MAX_SOURCE_PAGES + 1):
                links = extract_article_links(page)
                
                # ── 無限迴圈防護機制 ──
                current_hash = "".join([l['url'] for l in links])
                if current_hash == previous_page_links_hash:
                    log.info(f"  Page {page_num} content identical to previous page. End of pagination.")
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
                        # 檢查是否觸發早停
                        if (ly, lm) < (year, month):
                            stop_pagination = True
                        
                        # 符合目標月份，加入收集清單
                        if (ly, lm) == (year, month) or "Holiday_weather" in item['url']:
                            if not any(x['url'] == item['url'] for x in all_items):
                                all_items.append(item)
                                added += 1
                    except: continue
                
                log.info(f"  Page {page_num}: Processed {len(links)} actual articles. Matched {year}-{month:02d}: {added}")
                
                # 如果發現過期文章，煞掣！
                if stop_pagination:
                    log.info(f"  [Early Stop] Older articles found. Stopping {src['name']}.")
                    break
                
                # 翻頁點擊機制
                try:
                    action = page.evaluate("""() => {
                        const btns = Array.from(document.querySelectorAll('a, button, li, span'));
                        for (let el of btns) {
                            const t = (el.innerText || '').trim();
                            if ((t === '下一頁' || t === '下一页' || t === 'Next' || t === '>' || t === '»' || t.includes('載入更多')) && el.offsetParent !== null) {
                                if (!el.disabled && !el.classList.contains('disabled')) {
                                    el.click(); return 'clicked';
                                }
                            }
                        }
                        return null;
                    }""")
                    if action == 'clicked':
                        page.wait_for_timeout(4000)
                    else:
                        break
                except: break

        if not all_items:
            log.warning(f"❌ No matching articles found for {year}-{month:02d}.")
            browser.close()
            return
        
        all_items.sort(key=lambda x: x["date_str"])
        
        writer = PdfWriter()
        for i, item in enumerate(all_items):
            log.info(f"\n⚙ Processing ({i+1}/{len(all_items)}) [{item['date_str']}] {item['text']}")
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
