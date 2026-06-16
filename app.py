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
import requests
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
    {"name": "chat_info",       "url": f"{BASE_URL}/zh/chat-info"},
]
LANG_CODES = ["zh", "en", "pt"]
MAX_FINAL_BYTES = 10 * 1024 * 1024 # 放寬到 10MB，確保全部資料裝得落

# 【終極暴力設定】
MAX_SOURCE_PAGES = 80       # 掃描深度加大到 80 頁，確保搵勻全網
REQUEST_TIMEOUT = 90_000    # 放寬超時限制至 90 秒

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
def download_pdf(url: str, dest: Path, session: requests.Session) -> bool:
    try:
        r = session.get(url, timeout=45, stream=True)
        r.raise_for_status()
        chunks = []
        first = True
        for chunk in r.iter_content(65536):
            if first:
                if not chunk.startswith(b"%PDF") and "pdf" not in r.headers.get("content-type", "") and not url.lower().endswith(".pdf"):
                    return False
                first = False
            chunks.append(chunk)
        dest.write_bytes(b"".join(chunks))
        return dest.stat().st_size > 2000
    except Exception as exc:
        log.debug(f"    Download failed: {exc}")
        dest.unlink(missing_ok=True)
        return False

def extract_article_links(page: Page) -> list[dict]:
    results, seen_urls = [], set()
    
    # 暴力強制等待渲染，並模擬人類滾動頁面 (觸發 Lazy Loading)
    page.wait_for_timeout(3000)
    page.evaluate("""() => {
        window.scrollTo(0, document.body.scrollHeight / 2);
        setTimeout(() => window.scrollTo(0, document.body.scrollHeight), 1500);
    }""")
    page.wait_for_timeout(3000)
    
    items_data = page.evaluate("""() => {
        const DATE_RE = /(20\\d{2})[\\s\\-\\/年\\.]+(\\d{1,2})[\\s\\-\\/月\\.]+(\\d{1,2})/;
        const results = [];
        
        // 捕獲包含隱藏數據的連結
        const links = Array.from(document.querySelectorAll('a, [onclick], [data-href], [data-url]'));
        
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
            
            // 往上尋找 15 層，包攬任何排版結構
            for (let i = 0; i < 15; i++) {
                if (!node || node.tagName === 'BODY' || node.tagName === 'HTML') break;
                
                const text = (node.innerText || '').replace(/\\u200B/g, '').trim();
                const match = text.match(DATE_RE);
                
                if (match) {
                    dateStr = match[1] + '-' + match[2].padStart(2,'0') + '-' + match[3].padStart(2,'0');
                    
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
        
        return results;
    }""")

    for item in items_data:
        href = item['href']
        hl = href.lower()
        if hl.endswith(('.jpg', '.png', '.zip', '.css', '.js')): continue
        if any(x in hl for x in ['facebook.com', 'twitter.com', 'youtube.com', 'instagram.com']): continue
        
        full_url = resolve_url(href)
        
        # 網址去重
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

def process_article(page: Page, item: dict, tmp_dir: Path, out_dir: Path, seq: int, session: requests.Session) -> Optional[Path]:
    collected, final_title = [], item["text"] or "Untitled"
    lang_pdf_map = {lang: [] for lang in LANG_CODES}
    
    for lang in LANG_CODES:
        try:
            page.goto(switch_lang(item["url"], lang), wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
            page.wait_for_timeout(3000) # 給予內頁充分渲染時間
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
                if download_pdf(url, dest, session): collected.append((dest, LANG_CODES.index(lang)))

    if not collected:
        log.info(f"    No PDFs found — falling back to print-to-PDF")
        for lang in LANG_CODES:
            dest = tmp_dir / f"{seq:03d}_{lang}_print.pdf"
            try:
                page.goto(switch_lang(item["url"], lang), wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
                page.wait_for_timeout(3000)
                if any(m in page.inner_text("body").lower() for m in NO_CONTENT_MARKERS): continue
                page.evaluate(WAIT_IMAGES_JS)
                page.evaluate("""() => { ['header','nav','footer','.site-header','.breadcrumb','.navbar'].forEach(s => document.querySelectorAll(s).forEach(el => el.remove())); }""")
                page.add_style_tag(content=PRINT_CSS)
                page.pdf(path=str(dest), format="A4", print_background=True, margin={"top": "1.5cm", "bottom": "1.5cm", "left": "1.5cm", "right": "1.5cm"})
                if dest.exists() and dest.stat().st_size > 2000: collected.append((dest, LANG_CODES.index(lang)))
            except Exception as exc: log.warning(f"    [{lang}] print-to-PDF failed")

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
        
        log.info(f"🚀 Execution launched — target: {year}-{month:02d}")
        
        current_dir = Path(os.getcwd())
        tmp_dir = current_dir / f"smg_tmp_{year}_{month:02d}"
        tmp_dir.mkdir(exist_ok=True)
        
        session = requests.Session()
        session.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            # 使用全螢幕解像度，防止 Responsive Design 將網址收入 Menu
            ctx = browser.new_context(viewport={"width": 1920, "height": 1080}, user_agent=session.headers["User-Agent"])
            page, all_items = ctx.new_page(), {}
            
            for src in SOURCES:
                log.info(f"📋 Scanning: {src['name']}")
                empty_pages_streak = 0
                
                for page_num in range(1, MAX_SOURCE_PAGES + 1):
                    page_url = src["url"] if page_num == 1 else f"{src['url'].rstrip('/')}/page/{page_num}"
                    try: 
                        # 改為 domcontentloaded 防止 networkidle 卡死，並配合下方的強制 sleep
                        page.goto(page_url, wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
                    except Exception as e: 
                        log.warning(f"  Page {page_num} load error: {e}. Skipping to next page.")
                        continue # 【關鍵修復】Timeout 唔好 break，繼續試下一頁
                    
                    found = extract_article_links(page)
                    if not found: 
                        empty_pages_streak += 1
                        log.info(f"  Page {page_num}: No links found.")
                        if empty_pages_streak >= 3:
                            log.info(f"  3 consecutive empty pages. Moving to next source.")
                            break # 連續 3 頁冇嘢先當作到底
                        continue
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

                    log.info(f"  Page {page_num}: Found {len(found)} links. Matched {year}-{month:02d}: {added}")
            
            sorted_items = sorted(all_items.values(), key=lambda x: x["date_str"])
            if not sorted_items:
                scraper_execution_result = {"success": False, "filename": "", "message": f"No matching articles found for {year}-{month:02d}."}
                browser.close()
                return
            
            article_pdfs = []
            for idx, item in enumerate(sorted_items, 1):
                log.info(f"⚙ Processing ({idx}/{len(sorted_items)}) [{item['date_str']}] {item['text']}")
                pdf = process_article(page, item, tmp_dir, tmp_dir, idx, session)
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
        scraper_execution_result = {"success": False, "filename": "", "message": str(e)}
    finally:
        scraper_running_status = False

# ── Web Control Panel Embedded Portal (HTML Layout UI) ───────────────────────
CONTROL_PANEL_UI_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8"><title>SMG Report Engine Portal</title>
    <style>
        body { font-family: sans-serif; background: #eef2f3; padding: 30px; }
        .container { max-width: 900px; margin: auto; background: #fff; padding: 25px; border-radius: 10px; }
        input, select, button { padding: 10px; margin-bottom: 15px; width: 100%; box-sizing: border-box; }
        button { background: #3498db; color: #fff; border: none; cursor: pointer; font-weight: bold; }
        .console-box { background: #1e272e; color: #ced6e0; padding: 15px; height: 300px; overflow-y: scroll; font-family: monospace; white-space: pre-wrap; }
        .status-banner { padding: 12px; background: #f1f2f6; font-weight: bold; margin-bottom: 20px; }
    </style>
</head>
<body>
<div class="container">
    <h2>SMG Monthly PDF Scraper Console</h2>
    <label>Target Year:</label> <input type="number" id="inputYear" placeholder="Leave blank for default (last month)">
    <label>Target Month:</label> 
    <select id="inputMonth">
        <option value="">-- Default Last Month --</option>
        <option value="1">01</option><option value="2">02</option><option value="3">03</option><option value="4">04</option>
        <option value="5">05</option><option value="6">06</option><option value="7">07</option><option value="8">08</option>
        <option value="9">09</option><option value="10">10</option><option value="11">11</option><option value="12">12</option>
    </select>
    <button id="btnAction" onclick="triggerTask()">Launch Scraper Engine</button>
    <div id="statusBanner" class="status-banner">System Engine Status: Idle</div>
    <div id="downloadSection" style="display: none; padding:15px; background:#e8f4fd; margin-bottom:15px;">
        <a id="linkDownload" href="#" style="background:#2ed573; color:#fff; padding:10px; text-decoration:none;">Download PDF Report</a>
    </div>
    <div id="consoleLog" class="console-box">Waiting for process invocation...</div>
</div>
<script>
    let offset = 0, interval = null;
    function checkStatus() {
        fetch('/engine-status').then(r=>r.json()).then(d=>{
            document.getElementById('statusBanner').innerText = d.running ? "Status: Running..." : "Status: " + d.result.message;
            document.getElementById('btnAction').disabled = d.running;
            if(!d.running && d.result.filename) {
                document.getElementById('downloadSection').style.display = 'block';
                document.getElementById('linkDownload').href = "/retrieve-file?file=" + encodeURIComponent(d.result.filename);
                clearInterval(interval);
            }
        });
    }
    function fetchLogs() {
        fetch('/poll-logs?offset='+offset).then(r=>r.json()).then(d=>{
            if(d.logs.length) {
                const c = document.getElementById('consoleLog');
                d.logs.forEach(m => c.innerText += m + "\\n");
                offset += d.logs.length;
                c.scrollTop = c.scrollHeight;
            }
        });
    }
    function triggerTask() {
        offset = 0; document.getElementById('consoleLog').innerText = "";
        document.getElementById('downloadSection').style.display = 'none';
        fetch('/trigger-execution', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({year: document.getElementById('inputYear').value, month: document.getElementById('inputMonth').value})
        }).then(()=>{ clearInterval(interval); interval = setInterval(()=>{checkStatus(); fetchLogs();}, 1500); });
    }
    checkStatus();
</script>
</body>
</html>
"""

app = Flask(__name__)
@app.route('/')
def serve_index_portal(): return render_template_string(CONTROL_PANEL_UI_TEMPLATE)
@app.route('/trigger-execution', methods=['POST'])
def trigger_execution_endpoint():
    global scraper_running_status, scraper_execution_result, app_log_buffer
    if scraper_running_status: return jsonify({"status": "rejected"}), 400
    p = request.json or {}
    app_log_buffer.clear()
    scraper_execution_result = {"success": False, "filename": "", "message": "Started"}
    scraper_running_status = True
    threading.Thread(target=execute_scraping_worker, args=(int(p.get('year')) if p.get('year') else None, int(p.get('month')) if p.get('month') else None)).start()
    return jsonify({"status": "initiated"})
@app.route('/engine-status')
def get_engine_status_endpoint(): return jsonify({"running": scraper_running_status, "result": scraper_execution_result})
@app.route('/poll-logs')
def poll_logs_endpoint(): return jsonify({"logs": app_log_buffer[request.args.get('offset', 0, type=int):]})
@app.route('/retrieve-file')
def retrieve_file_endpoint(): 
    file_path = Path(os.getcwd()) / request.args.get('file', '')
    return send_file(file_path, as_attachment=True)

if __name__ == "__main__":
    print("Starting server and opening browser...")
    threading.Timer(1.5, lambda: webbrowser.open("http://127.0.0.1:5000")).start()
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False)
