from __future__ import annotations
import logging
import os
import re
import shutil
import subprocess
import threading
import time
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

# ── PyInstaller Playwright Path Configuration ────────────────────────────────
if getattr(sys, 'frozen', False):
    bundle_dir = sys._MEIPASS
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = os.path.join(bundle_dir, 'ms-playwright')
else:
    bundle_dir = os.path.dirname(os.path.abspath(__file__))

# ── Logging ──────────────────────────────────────────────────────────────────
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

# ── Config ─────────────────────────────────────────────────────────────────
BASE_URL = "https://www.smg.gov.mo"
CMS_BASE = "https://cms.smg.gov.mo"

LANG_ORDER = ["zh", "pt", "en"]  # 同一則：中文 → 葡文 → 英文
LANG_CMS = {"zh": "zh_TW", "en": "en", "pt": "pt"}

NEWS_CODES = [
    "news", "normal", "important", "weather", "promote", "Holiday_weather", "seasonal",
]

NAV_TIMEOUT = 60_000
RENDER_WAIT = 2_000
PDF_SIZE_LIMIT = 5 * 1024 * 1024
_COMPRESS_ATTEMPTS = [("ebook", 150), ("screen", 96), ("screen", 72)]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; SMG-Monthly-Scraper/2.0)",
    "Accept": "application/json",
})

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

def fetch_cms_list(cms_lang: str, code: str) -> list[dict]:
    url = f"{CMS_BASE}/{cms_lang}/api/news/{code}"
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
        log.warning(f"  CMS fetch failed {cms_lang}/{code}: {e}")
        return []

def title_from_item(item: dict, cms_lang: str) -> str:
    tr = item.get("translations") or {}
    if isinstance(tr, dict):
        block = tr.get(cms_lang) or next(iter(tr.values()), {}) or {}
        t = (block.get("title") or "").strip()
        if t:
            return t
    return (item.get("name") or f"article-{item.get('id')}").strip()

def collect_month_articles(year: int, month: int) -> list[dict]:
    groups: dict[int, dict[str, dict]] = defaultdict(dict)
    group_dates: dict[int, datetime] = {}

    for fe_lang in LANG_ORDER:
        cms_lang = LANG_CMS[fe_lang]
        log.info(f"\n🌐 Fetching CMS lists for {fe_lang} ({cms_lang})")
        seen_ids: set[int] = set()

        for code in NEWS_CODES:
            rows = fetch_cms_list(cms_lang, code)
            matched = 0
            for row in rows:
                aid = row.get("id")
                if aid is None:
                    continue
                try:
                    aid = int(aid)
                except (TypeError, ValueError):
                    continue
                if aid in seen_ids:
                    continue

                dt = parse_startdate(str(row.get("startdate") or ""))
                if not dt or dt.year != year or dt.month != month:
                    continue

                title = title_from_item(row, cms_lang)
                tr = row.get("translations") or {}
                if isinstance(tr, dict):
                    block = tr.get(cms_lang) or {}
                    if not (block.get("title") or "").strip() and fe_lang != "zh":
                        if not title or title.startswith("article-"):
                            continue

                seen_ids.add(aid)
                matched += 1
                groups[aid][fe_lang] = {
                    "id": aid,
                    "lang": fe_lang,
                    "date_str": dt.strftime("%Y-%m-%d"),
                    "datetime": dt,
                    "text": title[:80],
                    "url": f"{BASE_URL}/{fe_lang}/news/{aid}",
                    "source": code,
                }
                if aid not in group_dates or dt < group_dates[aid]:
                    group_dates[aid] = dt

            log.info(f"  {code}: {matched} in {year}-{month:02d} (total rows {len(rows)})")

    ordered_ids = sorted(groups.keys(), key=lambda i: (group_dates.get(i) or datetime.min, i))
    flat: list[dict] = []
    for aid in ordered_ids:
        for fe_lang in LANG_ORDER:  # zh → pt → en
            if fe_lang in groups[aid]:
                flat.append(groups[aid][fe_lang])

    log.info(f"\n📦 Unique articles (by id): {len(ordered_ids)}")
    log.info(f"📦 Total language variants to render: {len(flat)}")
    return flat

def download_pdf_robust(url: str, dest: Path, page: Page) -> bool:
    try:
        with page.context.expect_download(timeout=45_000) as dl:
            page.evaluate(f"window.open('{url}', '_blank')")
        dl.value.save_as(dest)
        return dest.exists() and dest.stat().st_size > 2_000
    except Exception as e:
        log.warning(f"  PDF download failed ({url}): {e}")
        return False

def process_article(page: Page, item: dict, tmp_dir: Path, seq: int) -> Optional[Path]:
    safe = item["text"][:30].replace("/", "-")
    dest = tmp_dir / sanitize_filename(f"{seq:03d}_{item['date_str']}_{item['lang']}_{safe}.pdf")
    try:
        page.goto(item["url"], wait_until="networkidle", timeout=NAV_TIMEOUT)
        page.wait_for_timeout(RENDER_WAIT)
        pdf_links = page.evaluate(
            "() => Array.from(document.querySelectorAll('a[href$=\".pdf\"],a[href*=\"download\"]')).map(a=>a.href)"
        )
        if pdf_links and download_pdf_robust(pdf_links[0], dest, page):
            return dest

        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_000)
        page.evaluate("""() => {
            ['header','nav','footer','#header','#footer','#nav',
             '.site-header','.breadcrumb','.cookie-bar','.back-to-top',
             '.navbar-top','.sticky-header']
            .forEach(s => document.querySelectorAll(s).forEach(el => el.remove()));
        }""")
        page.add_style_tag(content=(
            "@media print{body{-webkit-print-color-adjust:exact !important;"
            "print-color-adjust:exact !important}}"
        ))
        page.pdf(path=str(dest), format="A4", print_background=True)
        if dest.exists() and dest.stat().st_size > 2_000:
            return dest
        log.warning(f"  PDF too small, skipping: {dest.name}")
        return None
    except Exception as e:
        log.warning(f"  Failed processing {item['url']}: {e}")
        return None

def compress_pdf(input_path: Path, output_path: Path) -> bool:
    input_size = input_path.stat().st_size
    if input_size <= PDF_SIZE_LIMIT:
        log.info(f"  PDF is {input_size / 1_048_576:.2f} MB — already under limit, skipping compression")
        shutil.copy2(input_path, output_path)
        return True

    log.info(f"  PDF is {input_size / 1_048_576:.2f} MB — compressing…")
    for gs_setting, img_dpi in _COMPRESS_ATTEMPTS:
        cmd = [
            "gs", "-dBATCH", "-dNOPAUSE", "-dQUIET", "-sDEVICE=pdfwrite",
            "-dCompatibilityLevel=1.5", f"-dPDFSETTINGS=/{gs_setting}",
            "-dDownsampleColorImages=true", "-dDownsampleGrayImages=true", "-dDownsampleMonoImages=true",
            f"-dColorImageResolution={img_dpi}", f"-dGrayImageResolution={img_dpi}",
            f"-dMonoImageResolution={min(img_dpi * 2, 300)}",
            "-dCompressFonts=true", "-dEmbedAllFonts=true",
            f"-sOutputFile={output_path}", str(input_path),
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if result.returncode != 0:
                log.warning(f"  gs /{gs_setting} failed: {result.stderr[:200]}")
                continue
        except FileNotFoundError:
            log.warning("  Ghostscript (gs) not found — skipping compression")
            shutil.copy2(input_path, output_path)
            return False
        except subprocess.TimeoutExpired:
            log.warning(f"  gs /{gs_setting} timed out")
            continue

        out_size = output_path.stat().st_size if output_path.exists() else 0
        log.info(f"  /{gs_setting} @{img_dpi}dpi → {out_size / 1_048_576:.2f} MB"
                 + (" ✅" if out_size <= PDF_SIZE_LIMIT else " (still large)"))
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
        log.info(f"🚀 SMG Monthly Scraper — Target: {year}-{month:02d}")
        log.info("   Output: ONE PDF | order: date ASC, then zh → pt → en")

        current_dir = Path(os.getcwd())
        tmp_dir = current_dir / f"smg_tmp_{year}_{month:02d}"
        tmp_dir.mkdir(exist_ok=True)

        items = collect_month_articles(year, month)
        if not items:
            log.warning(f"❌ No articles found for {year}-{month:02d}.")
            scraper_execution_result = {"success": False, "filename": "", "files": [], "message": "No matching articles found."}
            return

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(viewport={"width": 1920, "height": 1080}, accept_downloads=True)
            page = ctx.new_page()

            writer = PdfWriter()
            for i, item in enumerate(items, 1):
                log.info(
                    f"\n⚙  ({i}/{len(items)}) [{item['date_str']}] "
                    f"[{item['lang'].upper()}] {item['text'][:50]}"
                )
                pdf_path = process_article(page, item, tmp_dir, i)
                if pdf_path:
                    try:
                        writer.append(str(pdf_path))
                    except Exception as e:
                        log.warning(f"  Could not append {pdf_path.name}: {e}")

            if len(writer.pages) == 0:
                log.warning("❌ No pages rendered.")
                scraper_execution_result = {"success": False, "filename": "", "files": [], "message": "No pages rendered."}
                browser.close()
                return

            raw_output = current_dir / f"SMG_Monthly_Report_{year}_{month:02d}_raw.pdf"
            with raw_output.open("wb") as fh:
                writer.write(fh)

            final_filename = f"SMG_Monthly_Report_{year}_{month:02d}.pdf"
            output = current_dir / final_filename
            log.info(f"🗜  Compressing → {output.name} (target ≤ 5 MB)…")
            compress_pdf(raw_output, output)
            log.info(f"\n✅ Done: {output.name}  ({output.stat().st_size / 1_048_576:.2f} MB)")
            raw_output.unlink(missing_ok=True)
            browser.close()

        scraper_execution_result = {
            "success": True,
            "filename": final_filename,
            "files": [final_filename],
            "message": f"Report generated: {final_filename}",
        }
    except Exception as e:
        log.error(f"❌ Execution error: {e}")
        scraper_execution_result = {"success": False, "filename": "", "files": [], "message": str(e)}
    finally:
        scraper_running_status = False

CONTROL_PANEL_UI_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh">
<head>
    <meta charset="UTF-8"><title>SMG Report Engine Portal</title>
    <style>
        body { font-family: sans-serif; background: #eef2f3; padding: 30px; }
        .container { max-width: 900px; margin: auto; background: #fff; padding: 25px; border-radius: 10px; }
        input, select, button { padding: 10px; margin-bottom: 15px; width: 100%; box-sizing: border-box; }
        button { background: #3498db; color: #fff; border: none; cursor: pointer; font-weight: bold; }
        .console-box { background: #1e272e; color: #ced6e0; padding: 15px; height: 350px; overflow-y: scroll; font-family: monospace; white-space: pre-wrap; }
        .status-banner { padding: 12px; background: #f1f2f6; font-weight: bold; margin-bottom: 20px; }
        .download-links a { display: inline-block; margin: 5px 8px 5px 0; background:#2ed573; color:#fff; padding:10px 14px; text-decoration:none; border-radius:4px; }
    </style>
</head>
<body>
<div class="container">
    <h2>SMG Monthly PDF Scraper Console</h2>
    <p style="color:#666;font-size:0.9em;">單一 PDF｜按日期排序｜同一則消息：中文 → 葡文 → 英文</p>
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
        <div class="download-links" id="downloadLinks"></div>
    </div>
    <div id="consoleLog" class="console-box">Waiting for process invocation...</div>
</div>
<script>
    let offset = 0, interval = null;
    function checkStatus() {
        fetch('/engine-status').then(r=>r.json()).then(d=>{
            document.getElementById('statusBanner').innerText = d.running ? "Status: Running..." : "Status: " + d.result.message;
            document.getElementById('btnAction').disabled = d.running;
            if(!d.running && d.result.files && d.result.files.length) {
                document.getElementById('downloadSection').style.display = 'block';
                const box = document.getElementById('downloadLinks');
                box.innerHTML = '';
                d.result.files.forEach(f => {
                    const a = document.createElement('a');
                    a.href = "/retrieve-file?file=" + encodeURIComponent(f);
                    a.textContent = "Download " + f;
                    box.appendChild(a);
                });
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
    threading.Thread(
        target=execute_scraping_worker,
        args=(
            int(p.get('year')) if p.get('year') else None,
            int(p.get('month')) if p.get('month') else None,
        ),
    ).start()
    return jsonify({"status": "initiated"})

@app.route('/engine-status')
def get_engine_status_endpoint():
    return jsonify({"running": scraper_running_status, "result": scraper_execution_result})

@app.route('/poll-logs')
def poll_logs_endpoint():
    return jsonify({"logs": app_log_buffer[request.args.get('offset', 0, type=int):]})

@app.route('/retrieve-file')
def retrieve_file_endpoint():
    file_path = Path(os.getcwd()) / request.args.get('file', '')
    return send_file(file_path, as_attachment=True)

if __name__ == "__main__":
    print("Starting server and opening browser...")
    threading.Timer(1.5, lambda: webbrowser.open("http://127.0.0.1:5000")).start()
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False)
