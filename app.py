from __future__ import annotations
import logging
import os
import re
import shutil
import subprocess
import threading
import sys
import webbrowser
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests
from flask import Flask, jsonify, render_template_string, request, send_file
from playwright.sync_api import Page, sync_playwright
from pypdf import PdfWriter

if getattr(sys, "frozen", False):
    bundle_dir = sys._MEIPASS
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = os.path.join(bundle_dir, "ms-playwright")
else:
    bundle_dir = os.path.dirname(os.path.abspath(__file__))

app_log_buffer: list[str] = []

class WebLogHandler(logging.Handler):
    def emit(self, record):
        try:
            app_log_buffer.append(self.format(record))
        except Exception:
            self.handleError(record)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)
log.addHandler(WebLogHandler())

BASE_URL = "https://www.smg.gov.mo"
CMS_BASE = "https://cms.smg.gov.mo"
LANG_ORDER = ["zh", "pt", "en"]
LANG_CMS = {"zh": "zh_TW", "en": "en", "pt": "pt"}
NEWS_CODES = ["news", "normal", "important", "weather", "promote", "Holiday_weather", "seasonal", "question"]
SITECONTENT_CODES = ["chat-info"]
NAV_TIMEOUT = 60_000
RENDER_WAIT = 2_500
PDF_SIZE_LIMIT = 5 * 1024 * 1024
_COMPRESS_ATTEMPTS = [("ebook", 150), ("screen", 96), ("screen", 72)]
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (compatible; SMG-Monthly-Scraper/2.2)", "Accept": "application/json"})
scraper_running_status: bool = False
scraper_execution_result: dict = {"success": False, "filename": "", "message": "Idle", "files": []}

def get_target_month() -> tuple[int, int]:
    today = date.today()
    return (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)

def sanitize_filename(name: str, max_len: int = 100) -> str:
    name = re.sub(r"\s+", " ", name).strip()
    return re.sub(r'[\\/*?:"<>|]', "", name)[:max_len] or "Untitled"

def parse_startdate(raw: str) -> Optional[datetime]:
    if not raw:
        return None
    raw = raw.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw[:19], fmt)
        except ValueError:
            continue
    return None

def fetch_cms_json(url: str) -> list[dict]:
    try:
        r = SESSION.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and isinstance(data.get("data"), list):
            return data["data"]
        return []
    except Exception as e:
        log.warning(f"  CMS fetch failed {url}: {e}")
        return []

def extract_translation(item: dict, cms_lang: str) -> tuple[str, str]:
    tr = item.get("translations") or {}
    block: dict = {}
    if isinstance(tr, dict):
        block = tr.get(cms_lang) or {}
        if not isinstance(block, dict):
            block = {}
    title = (block.get("title") or "").strip()
    content = (block.get("content") or "").strip()
    if not title:
        title = (item.get("name") or "").strip()
    return title, content

def _ingest_rows(rows, fe_lang, cms_lang, year, month, source, url_builder, groups, group_dates, seen_keys):
    matched = 0
    for row in rows:
        aid = row.get("id")
        if aid is None:
            continue
        try:
            aid = int(aid)
        except (TypeError, ValueError):
            continue
        gkey = f"{source}:{aid}"
        if gkey in seen_keys:
            continue
        dt = parse_startdate(str(row.get("startdate") or ""))
        if not dt or dt.year != year or dt.month != month:
            continue
        title, content = extract_translation(row, cms_lang)
        if not title and not content:
            continue
        if not content and fe_lang != "zh":
            if not title or title.startswith("article-"):
                continue
        seen_keys.add(gkey)
        matched += 1
        groups[gkey][fe_lang] = {
            "id": aid, "gkey": gkey, "lang": fe_lang,
            "date_str": dt.strftime("%Y-%m-%d"), "datetime": dt,
            "title": title or f"article-{aid}", "content": content,
            "url": url_builder(fe_lang, aid), "source": source,
        }
        if gkey not in group_dates or dt < group_dates[gkey]:
            group_dates[gkey] = dt
    return matched

def collect_month_articles(year: int, month: int) -> list[dict]:
    groups: dict[str, dict[str, dict]] = defaultdict(dict)
    group_dates: dict[str, datetime] = {}
    for fe_lang in LANG_ORDER:
        cms_lang = LANG_CMS[fe_lang]
        log.info(f"\nFetching CMS lists for {fe_lang} ({cms_lang})")
        seen_keys: set[str] = set()
        for code in NEWS_CODES:
            rows = fetch_cms_json(f"{CMS_BASE}/{cms_lang}/api/news/{code}")
            matched = _ingest_rows(rows, fe_lang, cms_lang, year, month, code, lambda fl, aid: f"{BASE_URL}/{fl}/news-detail/{aid}", groups, group_dates, seen_keys)
            log.info(f"  news/{code}: {matched} in {year}-{month:02d} (total {len(rows)})")
        for code in SITECONTENT_CODES:
            rows = fetch_cms_json(f"{CMS_BASE}/{cms_lang}/api/sitecontent/{code}")
            matched = _ingest_rows(rows, fe_lang, cms_lang, year, month, f"sitecontent:{code}", lambda fl, aid, c=code: f"{BASE_URL}/{fl}/{c}/{aid}", groups, group_dates, seen_keys)
            log.info(f"  sitecontent/{code}: {matched} in {year}-{month:02d} (total {len(rows)})")
    ordered_keys = sorted(groups.keys(), key=lambda k: (group_dates.get(k) or datetime.min, k))
    flat: list[dict] = []
    for gkey in ordered_keys:
        for fe_lang in LANG_ORDER:
            if fe_lang in groups[gkey]:
                flat.append(groups[gkey][fe_lang])
    log.info(f"\nUnique articles: {len(ordered_keys)}")
    log.info(f"Total language variants to render: {len(flat)}")
    return flat

def _prepare_live_page_for_pdf(page: Page) -> None:
    try:
        page.emulate_media(media="screen")
    except Exception:
        pass
    try:
        page.add_style_tag(content="html, body { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; } .mega-menu, .dropdown-menu { display: none !important; } #menu-button { display: none !important; }")
    except Exception:
        pass

def process_article(page: Page, item: dict, tmp_dir: Path, seq: int) -> Optional[Path]:
    safe = (item["title"] or "untitled")[:30].replace("/", "-")
    dest = tmp_dir / sanitize_filename(f"{seq:03d}_{item['date_str']}_{item['lang']}_{safe}.pdf")
    url = (item.get("url") or "").strip()
    try:
        if not url:
            raise RuntimeError("missing article url")
        log.info(f"  Open page: {url}")
        page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        try:
            page.wait_for_function("() => { const h = document.querySelector('h1'); return !!(h && h.textContent && h.textContent.trim().length > 1); }", timeout=20_000)
        except Exception:
            page.wait_for_timeout(2_000)
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        page.wait_for_timeout(RENDER_WAIT)
        _prepare_live_page_for_pdf(page)
        page.pdf(path=str(dest), format="A4", print_background=True, margin={"top": "0mm", "bottom": "8mm", "left": "0mm", "right": "0mm"})
        if dest.exists() and dest.stat().st_size > 1_000:
            return dest
        log.warning(f"  PDF too small, skipping: {dest.name}")
        return None
    except Exception as e:
        log.warning(f"  Live page failed id={item['id']} [{item['lang']}]: {e}")
        return None

def compress_pdf(input_path: Path, output_path: Path) -> bool:
    input_size = input_path.stat().st_size
    if input_size <= PDF_SIZE_LIMIT:
        shutil.copy2(input_path, output_path)
        return True
    for gs_setting, img_dpi in _COMPRESS_ATTEMPTS:
        cmd = ["gs", "-dBATCH", "-dNOPAUSE", "-dQUIET", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.5", f"-dPDFSETTINGS=/{gs_setting}", "-dDownsampleColorImages=true", "-dDownsampleGrayImages=true", "-dDownsampleMonoImages=true", f"-dColorImageResolution={img_dpi}", f"-dGrayImageResolution={img_dpi}", f"-dMonoImageResolution={min(img_dpi * 2, 300)}", "-dCompressFonts=true", "-dEmbedAllFonts=true", f"-sOutputFile={output_path}", str(input_path)]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if result.returncode != 0:
                continue
        except FileNotFoundError:
            shutil.copy2(input_path, output_path)
            return False
        except subprocess.TimeoutExpired:
            continue
        out_size = output_path.stat().st_size if output_path.exists() else 0
        if out_size <= PDF_SIZE_LIMIT:
            return True
    if output_path.exists() and output_path.stat().st_size > 0:
        return False
    shutil.copy2(input_path, output_path)
    return False

def execute_scraping_worker(year: Optional[int], month: Optional[int]):
    global scraper_running_status, scraper_execution_result
    try:
        if not year or not month:
            year, month = get_target_month()
        log.info(f"SMG Monthly Scraper — Target: {year}-{month:02d}")
        current_dir = Path(os.getcwd())
        tmp_dir = current_dir / f"smg_tmp_{year}_{month:02d}"
        tmp_dir.mkdir(exist_ok=True)
        items = collect_month_articles(year, month)
        if not items:
            scraper_execution_result = {"success": False, "filename": "", "files": [], "message": "No matching articles found."}
            return
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(viewport={"width": 1400, "height": 900}, accept_downloads=True)
            page = ctx.new_page()
            writer = PdfWriter()
            for i, item in enumerate(items, 1):
                log.info(f"\n({i}/{len(items)}) [{item['date_str']}] [{item['lang'].upper()}] {item['title'][:50]}")
                pdf_path = process_article(page, item, tmp_dir, i)
                if pdf_path:
                    try:
                        writer.append(str(pdf_path))
                    except Exception as e:
                        log.warning(f"  Could not append {pdf_path.name}: {e}")
            if len(writer.pages) == 0:
                scraper_execution_result = {"success": False, "filename": "", "files": [], "message": "No pages rendered."}
                browser.close()
                return
            raw_output = current_dir / f"SMG_Monthly_Report_{year}_{month:02d}_raw.pdf"
            with raw_output.open("wb") as fh:
                writer.write(fh)
            final_filename = f"SMG_Monthly_Report_{year}_{month:02d}.pdf"
            output = current_dir / final_filename
            compress_pdf(raw_output, output)
            log.info(f"\nDone: {output.name}  ({output.stat().st_size / 1_048_576:.2f} MB)")
            raw_output.unlink(missing_ok=True)
            browser.close()
        scraper_execution_result = {"success": True, "filename": final_filename, "files": [final_filename], "message": f"Report generated: {final_filename}"}
    except Exception as e:
        log.error(f"Execution error: {e}")
        scraper_execution_result = {"success": False, "filename": "", "files": [], "message": str(e)}
    finally:
        scraper_running_status = False

CONTROL_PANEL_UI_TEMPLATE = """<!DOCTYPE html><html lang=\"zh\"><head><meta charset=\"UTF-8\"><title>SMG Report Engine Portal</title></head><body><h2>SMG Monthly PDF Scraper Console</h2><p>單一 PDF｜原網頁畫面</p><input type=\"number\" id=\"inputYear\" placeholder=\"Year\"><select id=\"inputMonth\"><option value=\"\">Default</option><option value=\"1\">01</option><option value=\"2\">02</option><option value=\"3\">03</option><option value=\"4\">04</option><option value=\"5\">05</option><option value=\"6\">06</option><option value=\"7\">07</option><option value=\"8\">08</option><option value=\"9\">09</option><option value=\"10\">10</option><option value=\"11\">11</option><option value=\"12\">12</option></select><button onclick=\"triggerTask()\">Launch</button><div id=\"statusBanner\"></div><div id=\"downloadLinks\"></div><pre id=\"consoleLog\"></pre><script>let offset=0,interval=null;function checkStatus(){fetch('/engine-status').then(r=>r.json()).then(d=>{document.getElementById('statusBanner').innerText=d.running?'Running':d.result.message;if(!d.running&&d.result.files&&d.result.files.length){const box=document.getElementById('downloadLinks');box.innerHTML='';d.result.files.forEach(f=>{const a=document.createElement('a');a.href='/retrieve-file?file='+encodeURIComponent(f);a.textContent='Download '+f;box.appendChild(a);});clearInterval(interval);}});}function fetchLogs(){fetch('/poll-logs?offset='+offset).then(r=>r.json()).then(d=>{if(d.logs.length){const c=document.getElementById('consoleLog');d.logs.forEach(m=>c.innerText+=m+'\\n');offset+=d.logs.length;}});}function triggerTask(){offset=0;document.getElementById('consoleLog').innerText='';fetch('/trigger-execution',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({year:document.getElementById('inputYear').value,month:document.getElementById('inputMonth').value})}).then(()=>{clearInterval(interval);interval=setInterval(()=>{checkStatus();fetchLogs();},1500);});}checkStatus();</script></body></html>"""

app = Flask(__name__)
@app.route('/')
def serve_index_portal():
    return render_template_string(CONTROL_PANEL_UI_TEMPLATE)
@app.route('/trigger-execution', methods=['POST'])
def trigger_execution_endpoint():
    global scraper_running_status, scraper_execution_result, app_log_buffer
    if scraper_running_status:
        return jsonify({"status": "rejected"}), 400
    p = request.json or {}
    app_log_buffer.clear()
    scraper_execution_result = {"success": False, "filename": "", "files": [], "message": "Started"}
    scraper_running_status = True
    threading.Thread(target=execute_scraping_worker, args=(int(p.get('year')) if p.get('year') else None, int(p.get('month')) if p.get('month') else None)).start()
    return jsonify({"status": "initiated"})
@app.route('/engine-status')
def get_engine_status_endpoint():
    return jsonify({"running": scraper_running_status, "result": scraper_execution_result})
@app.route('/poll-logs')
def poll_logs_endpoint():
    return jsonify({"logs": app_log_buffer[request.args.get('offset', 0, type=int):]})
@app.route('/retrieve-file')
def retrieve_file_endpoint():
    return send_file(Path(os.getcwd()) / request.args.get('file', ''), as_attachment=True)
if __name__ == '__main__':
    threading.Timer(1.5, lambda: webbrowser.open('http://127.0.0.1:5000')).start()
    app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False)
