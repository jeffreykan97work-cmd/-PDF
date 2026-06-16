from __future__ import annotations
import argparse
import logging
import os
import re
import shutil
import subprocess
import threading
import time
import sys
import webbrowser
from datetime import date
from pathlib import Path
from typing import Optional
from flask import Flask, Response, jsonify, render_template_string, request, send_file
from playwright.sync_api import Browser, Page, sync_playwright, TimeoutError as PWTimeout
from pypdf import PdfReader, PdfWriter

# ── PyInstaller Playwright Path Configuration ────────────────────────────────
if getattr(sys, 'frozen', False):
    bundle_dir = sys._MEIPASS
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = os.path.join(bundle_dir, 'ms-playwright')
else:
    bundle_dir = os.path.dirname(os.path.abspath(__file__))

# ── Logging and Memory Buffer Setup ──────────────────────────────────────────
app_log_buffer: list[str] = []

class WebLogHandler(logging.Handler):
    def emit(self, record):
        try:
            log_message = self.format(record)
            app_log_buffer.append(log_message)
        except Exception:
            self.handleError(record)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)
log.addHandler(WebLogHandler())

# ── Configuration ─────────────────────────────────────────────────────────────
BASE_URL = "https://www.smg.gov.mo"
SOURCES: list[dict] = [
    {"name": "subpage_73",      "url": f"{BASE_URL}/zh/subpage/73"},
    {"name": "news",            "url": f"{BASE_URL}/zh/news"},
    {"name": "activity",        "url": f"{BASE_URL}/zh/activity"},
    {"name": "subpage_124",     "url": f"{BASE_URL}/zh/subpage/124"},
    {"name": "climate",         "url": f"{BASE_URL}/zh/climate"},
    {"name": "seasonal",        "url": f"{BASE_URL}/zh/seasonal"},
    {"name": "holiday_weather", "url": f"{BASE_URL}/zh/news/Holiday_weather"},
    {"name": "special_weather", "url": f"{BASE_URL}/zh/subpage/730"}, # 基於你的發現新增的來源
    {"name": "chat_info",       "url": f"{BASE_URL}/zh/chat-info"},
]
LANG_CODES = ["zh", "en", "pt"]
MAX_FINAL_BYTES = 10 * 1024 * 1024

MAX_SOURCE_PAGES = 80       # 終極掃描深度
REQUEST_TIMEOUT = 90_000    # 放寬超時限制

PDF_LINK_SELECTORS = ["a[href$='.pdf']", "a[href*='.pdf?']", "a[href*='/pdf/']", "a[href*='download']", "a[href*='attach']", "a[href*='file']"]
NO_CONTENT_MARKERS = ["no related content", "nenhum conteúdo relacionado", "nenhum conteudo relacionado", "404", "page not found"]

PRINT_CSS = """
@media print {
    header, nav, footer, .navbar, .site-header, .breadcrumb, .footer, .sidebar, .related-links { display: none !important; }
    .content-area, .news-detail-content { width: 100% !important; margin: 0 !important; padding: 0 !important; }
    * { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }
}
"""
WAIT_IMAGES_JS = """
() => new Promise(resolve => {
    const imgs = [...document.images].filter(i => !i.complete);
    if (!imgs.length) return resolve();
    let n = imgs.length;
    imgs.forEach(i => { i.onload = i.onerror = () => { if (--n === 0) resolve(); }; });
    setTimeout(resolve, 8000);
})
"""

# ── State Control Variables ─────────────────────────
scraper_running_status: bool = False
scraper_execution_result: dict = {"success": False, "filename": "", "message": "Idle"}

# ── Helper Utility Functions ──────────────────────────────────────────────────
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

# ── Core Downloader Routines ────────────────────────────────────
def download_pdf(url: str, dest: Path, page: Page) -> bool:
    """使用 Playwright 的 Browser Context 進行下載，確保攜帶所有 Cookie"""
    try:
        r = page.context.request.get(url, timeout=60000)
        if not r.ok:
            log.warning(f"    [WARNING] Download rejected by server: {url}")
            return False
        
        content = r.body()
        if not content.startswith(b"%PDF") and b"pdf" not in r.headers.get("content-type", "").lower() and not url.lower().endswith(".pdf"):
            return False
            
        dest.write_bytes(content)
        return dest.stat().st_size > 2000
    except Exception as exc:
        log.warning(f"    [WARNING] Download failed for {url[:80]}: {exc}")
        dest.unlink(missing_ok=True)
        return False

def extract_article_links(page: Page) -> list[dict]:
    results, seen_urls = [], set()
    
    page.wait_for_timeout(3000)
    page.evaluate("""() => {
        window.scrollTo(0, document.body.scrollHeight / 2);
        setTimeout(() => window.scrollTo(0, document.body.scrollHeight), 1500);
    }""")
    page.wait_for_timeout(3000)
    
    items_data = page.evaluate("""() => {
        const DATE_RE = /(20\\d{2})[\\s\\-\\/年\\.]+(\\d{1,2})[\\s\\-\\/月\\.]+(\\d{1,2})/;
        const results = [];
        
        // 【修復】擴增選擇器，強制包含 news-detail 及 subpage/730
        const links = Array.from(document.querySelectorAll('a[href*="news-detail"], a[href*="subpage/730"], a, [onclick], [data-href], [data-url]'));
        
        links.forEach(el => {
            let href = el.getAttribute('href') || el.getAttribute('data-href') || el.getAttribute('data-url');
            if (!href && el.getAttribute('onclick')) {
                const m = el.getAttribute('onclick').match(/['"](.*?)['"]/);
                if (m) href = m[1];
            }
            if (!href || href === '#' || href.toLowerCase().includes('javascript:')) return;
            
            let node = el;
            let dateStr = null;
            let title = (el.innerText || '').trim();
            
            // 【修復】優先嘗試從 URL 提取日期
            const urlDateMatch = href.match(/(20\\d{2})[\\-\\/](\\d{1,2})[\\-\\/](\\d{1,2})/);
            if (urlDateMatch) {
                dateStr = urlDateMatch[1] + '-' + urlDateMatch[2].padStart(2,'0') + '-' + urlDateMatch[3].padStart(2,'0');
            }
            
            for (let i = 0; i < 15; i++) {
                if (!node || node.tagName === 'BODY' || node.tagName === 'HTML') break;
                
                const text = (node.innerText || '').replace(/\\u200B/g, '').trim();
                
                // 如果 URL 沒有日期，從 DOM 文字中提取
                if (!dateStr) {
                    const match = text.match(DATE_RE);
                    if (match) {
                        dateStr = match[1] + '-' + match[2].padStart(2,'0') + '-' + match[3].padStart(2,'0');
                    }
                }
                
                if (dateStr) {
                    if (title.length < 4 || /閱讀|詳情|more|detail/i.test(title)) {
                        const heading = node.querySelector('h1, h2, h3, h4, .title, .subject, strong');
                        if (heading) {
                            title = heading.innerText.trim();
                        } else {
                            const lines = text.split('\\n').map(l => l.trim()).filter(l => l.length > 3 && !l.match(DATE_RE));
                            if (lines.length > 0) title = lines[0];
                        }
                    }
                    break;
                }
                node = node.parentElement;
            }
            
            if (dateStr) {
                const hl = href.toLowerCase();
                if (hl.includes('/page/') || hl.includes('?page=')) return;
                results.push({ href: href, date_str: dateStr, text: title.substring(0, 150) });
            }
        });
        
        // 【關鍵修復】如果頁面是「即時天氣預報」或找不到列表，直接把整個頁面當作一篇文章抓取！
        if (results.length === 0 || window.location.href.includes('Holiday_weather') || window.location.href.includes('subpage/730')) {
            const bodyText = document.body.innerText;
            const m = bodyText.match(DATE_RE);
            if (m) {
                const dateStr = m[1] + '-' + m[2].padStart(2,'0') + '-' + m[3].padStart(2,'0');
                const title = document.querySelector('h1, h2, .title')?.innerText || document.title;
                results.push({ href: window.location.href, date_str: dateStr, text: "Live Bulletin: " + title.substring(0, 100) });
            }
        }
        
        return results;
    }""")

    for item in items_data:
        href = item['href']
        hl = href.lower()
        if hl.endswith(('.jpg', '.png', '.zip', '.css', '.js')): continue
        if any(x in hl for x in ['facebook.com', 'twitter.com', 'youtube.com', 'instagram.com']): continue
        
        full_url = resolve_url(href)
        norm_url = full_url
        for lang in LANG_CODES: norm_url = norm_url.replace(f"/{lang}/", "/zh/", 1)
        if norm_url in seen_urls: continue
        seen_urls.add(norm_url)
        
        zh_url = full_url
        for lang in LANG_CODES: zh_url = zh_url.replace(f"/{lang}/", "/zh/", 1)
        
        results.append({
            "url": zh_url,
            "text": item['text'] if item['text'] else "Article", 
            "date_str": item['date_str']
        })
        
    return results

def find_pdf_links_on_page(page: Page) -> list[str]:
    found, seen = [], set()
    for selector in PDF_LINK_SELECTORS:
        try:
            for a in page.query_selector_all(selector):
                href = a.get_attribute("href") or ""
                if href and resolve_url(href) not in seen:
                    seen.add(resolve_url(href))
                    found.append(resolve_url(href))
        except Exception: continue
    extra = page.evaluate("""() => {
        const urls = [];
        document.querySelectorAll('[onclick],[data-url],[data-href],[data-file]').forEach(el => {
            const raw = el.getAttribute('onclick') || el.getAttribute('data-url') || el.getAttribute('data-href') || el.getAttribute('data-file') || '';
            const m = raw.match(/https?:\\/\\/[^'"\\s]+\\.pdf[^'"\\s]*/i);
            if (m) urls.push(m[0]);
        }); return urls;
    }""") or []
    for u in extra:
        if u not in seen:
            seen.add(u)
            found.append(u)
    return found

def process_article(page: Page, item: dict, tmp_dir: Path, out_dir: Path, seq: int) -> Optional[Path]:
    collected, final_title = [], item["text"] or "Untitled"
    lang_pdf_map = {lang: [] for lang in LANG_CODES}
    
    for lang in LANG_CODES:
        try:
            page.goto(switch_lang(item["url"], lang), wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
            page.wait_for_timeout(3000)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            
            if not final_title or len(final_title) < 3 or "閱讀" in final_title or "詳情" in final_title or final_title == "Article":
                h = page.evaluate("() => document.querySelector('h1, h2, .news-detail-title, .title')?.innerText || ''")
                if h: final_title = h.strip()
            
            if any(m in page.inner_text("body").lower() for m in NO_CONTENT_MARKERS): continue
            pdf_urls = find_pdf_links_on_page(page)
            if pdf_urls: lang_pdf_map[lang].extend(pdf_urls)
        except Exception as exc: log.warning(f"    [{lang}] Page load failed: {exc}")

    seen_pdf_urls = set()
    for lang in LANG_CODES:
        for url in lang_pdf_map[lang]:
            if url not in seen_pdf_urls:
                seen_pdf_urls.add(url)
                dest = tmp_dir / f"{seq:03d}_{lang}_{len(collected)}.pdf"
                log.info(f"    Downloading [{lang}]: {url[:80]}")
                if download_pdf(url, dest, page): collected.append((dest, LANG_CODES.index(lang)))

    if not collected:
        log.info(f"    No PDFs found — falling back to print-to-PDF")
        for lang in LANG_CODES:
            dest = tmp_dir / f"{seq:03d}_{lang}_print.pdf"
            try:
                page.goto(switch_lang(item["url"], lang), wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
                page.wait_for_timeout(2000)
                
                page.evaluate("""() => new Promise((resolve) => {
                    let totalHeight = 0;
                    let distance = 300;
                    let timer = setInterval(() => {
                        let scrollHeight = document.body.scrollHeight;
                        window.scrollBy(0, distance);
                        totalHeight += distance;
                        if(totalHeight >= scrollHeight){
                            clearInterval(timer);
                            window.scrollTo(0, 0);
                            setTimeout(resolve, 1500);
                        }
                    }, 150);
                })""")
                
                if any(m in page.inner_text("body").lower() for m in NO_CONTENT_MARKERS): continue
                page.evaluate(WAIT_IMAGES_JS)
                page.evaluate("""() => { ['header','nav','footer','.site-header','.breadcrumb','.navbar'].forEach(s => document.querySelectorAll(s).forEach(el => el.remove())); }""")
                page.add_style_tag(content=PRINT_CSS)
                page.pdf(path=str(dest), format="A4", print_background=True, margin={"top": "1.5cm", "bottom": "1.5cm", "left": "1.5cm", "right": "1.5cm"})
                if dest.exists() and dest.stat().st_size > 2000: collected.append((dest, LANG_CODES.index(lang)))
            except Exception as exc: log.warning(f"    [{lang}] print-to-PDF failed: {exc}")

    if not collected: return None
    collected.sort(key=lambda x: x[1])
    article_pdf = out_dir / f"{seq:03d}_{item['date_str']}_{sanitize_filename(final_title)}.pdf"
    writer = PdfWriter()
    for pdf_path, _ in collected:
        try:
            for p in PdfReader(str(pdf_path)).pages: writer.add_page(p)
        except Exception: continue
    if not writer.pages: return None
    with article_pdf.open("wb") as f: writer.write(f)
    return article_pdf

def compress_with_pypdf(src: Path, dst: Path) -> bool:
    try:
        writer = PdfWriter()
        for page in PdfReader(str(src)).pages:
            page.compress_content_streams()
            writer.add_page(page)
        writer.compress_identical_objects(remove_identicals=True, remove_orphans=True)
        with dst.open("wb") as f: writer.write(f)
        return dst.exists() and dst.stat().st_size > 1000
    except Exception: return False

def ensure_size_limit(src: Path, final_path: Path) -> Path:
    if src.stat().st_size <= MAX_FINAL_BYTES:
        shutil.copy(src, final_path)
        return final_path
    log.info("  Compressing oversized file...")
    tmp_py = final_path.with_suffix(".py.pdf")
    if compress_with_pypdf(src, tmp_py) and tmp_py.stat().st_size <= MAX_FINAL_BYTES:
        shutil.move(str(tmp_py), final_path)
        return final_path
    shutil.copy(src, final_path)
    return final_path

# ── Execution Worker Instance ──────────────────────────────────────────────────
def execute_scraping_worker(year: Optional[int], month: Optional[int]):
    global scraper_running_status, scraper_execution_result
    try:
        default_year, default_month = get_target_month()
        year = year if year else default_year
        month = month if month else default_month
        
        log.info(f"🚀 Execution launched — target: {year}-{month:02d} (Ultimate Deep Link Mode)")
        
        current_dir = Path(os.getcwd())
        tmp_dir = current_dir / f"smg_tmp_{year}_{month:02d}"
        tmp_dir.mkdir(exist_ok=True)
        
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(viewport={"width": 1920, "height": 1080}, user_agent=user_agent)
            page, all_items = ctx.new_page(), {}
            
            for src in SOURCES:
                log.info(f"📋 Scanning: {src['name']}")
                empty_pages_streak = 0
                
                try: 
                    page.goto(src["url"], wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
                    page.wait_for_timeout(3000)
                except Exception as e: 
                    log.warning(f"  Failed to load category {src['name']}: {e}")
                    continue 
                
                for page_num in range(1, MAX_SOURCE_PAGES + 1):
                    found = extract_article_links(page)
                    
                    if not found: 
                        empty_pages_streak += 1
                        log.info(f"  Page {page_num}: No links found.")
                        if empty_pages_streak >= 2:
                            log.info(f"  Reached end of list for {src['name']}.")
                            break
                    else:
                        empty_pages_streak = 0

                    added = 0
                    for item in found:
                        try: 
                            ly, lm = int(item["date_str"][:4]), int(item["date_str"][5:7])
                        except Exception: 
                            continue
                        
                        if ly == year and lm == month and item["url"] not in all_items:
                            all_items[item["url"]] = item
                            added += 1

                    log.info(f"  Page {page_num}: Found {len(found)} candidate links. Matched {year}-{month:02d}: {added}")
                    
                    try:
                        action = page.evaluate("""() => {
                            const links = Array.from(document.querySelectorAll('a, button, li, span'));
                            for (let el of links) {
                                const t = (el.innerText || '').trim();
                                const title = el.getAttribute('title') || '';
                                const aria = el.getAttribute('aria-label') || '';
                                const isNext = t === '下一頁' || t === '下一页' || t === 'Next' || t === '>' || t === '»' || title.includes('下一頁') || aria.includes('Next');
                                
                                if (isNext && el.offsetParent !== null) {
                                    const disabled = el.disabled || el.classList.contains('disabled') || (el.parentElement && el.parentElement.classList.contains('disabled'));
                                    if (!disabled) {
                                        el.click();
                                        return 'clicked';
                                    }
                                }
                            }
                            for (let el of links) {
                                const t = (el.innerText || '').trim();
                                if ((t.includes('載入更多') || t.includes('加载更多') || t.includes('Load More')) && el.offsetParent !== null) {
                                    el.click();
                                    return 'clicked';
                                }
                            }
                            return null;
                        }""")
                        
                        if action == 'clicked':
                            page.wait_for_timeout(4000) 
                        else:
                            log.info(f"  No 'Next' button found. Moving to next category.")
                            break
                    except Exception as e:
                        log.warning(f"  Pagination error: {e}")
                        break
            
            sorted_items = sorted(all_items.values(), key=lambda x: x["date_str"])
            if not sorted_items:
                scraper_execution_result = {"success": False, "filename": "", "message": f"No matching articles found for {year}-{month:02d}."}
                browser.close()
                return
            
            article_pdfs = []
            for idx, item in enumerate(sorted_items, 1):
                log.info(f"⚙ Processing ({idx}/{len(sorted_items)}) [{item['date_str']}] {item['text']}")
                pdf = process_article(page, item, tmp_dir, tmp_dir, idx)
                if pdf: article_pdfs.append(pdf)
            browser.close()
            
        if not article_pdfs:
            scraper_execution_result = {"success": False, "filename": "", "message": "No PDFs generated (Check if pages contain actual content)."}
            return
            
        raw = tmp_dir / "raw_merged.pdf"
        writer = PdfWriter()
        for f in article_pdfs:
            try: writer.append(str(f))
            except Exception: pass
        with raw.open("wb") as f: writer.write(f)
        
        final_filename = f"SMG_Monthly_Report_{year}_{month:02d}.pdf"
        ensure_size_limit(raw, current_dir / final_filename)
        scraper_execution_result = {"success": True, "filename": final_filename, "message": "Report generated successfully!"}
        
    except Exception as e:
        scraper_execution_result = {"success
