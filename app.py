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

if getattr(sys, 'frozen', False):
    bundle_dir = sys._MEIPASS
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = os.path.join(bundle_dir, 'ms-playwright')
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
LANG_LABEL = {"zh": "中文", "pt": "Português", "en": "English"}

SMG_HEADER_BG = f"{CMS_BASE}/uploads/image/5c62a3d6a7dca.jpg"
SMG_LOGO = {
    "zh": f"{BASE_URL}/assets/image/smg-logo-zh.png",
    "en": f"{BASE_URL}/assets/image/smg-logo-en.png",
    "pt": f"{BASE_URL}/assets/image/smg-logo-pt.png",
}
SMG_NAV = {
    "zh": ["首頁", "天氣和氣候", "天氣警告", "空氣質量", "地球物理", "科普天地", "資源共享", "公開資訊", "關於我們"],
    "en": ["Home", "Weather and Climate", "Warnings", "Air Quality", "Geophysics", "Corner of Science knowledge", "Sharing resources", "Open information", "About us"],
    "pt": ["Página Principal", "Tempo e clima", "Avisos", "Qualidade do ar", "Geofísica", "Ciência e Tecnologia", "Recursos", "Informação pública", "Sobre nós"],
}

NEWS_CODES = [
    "news", "normal", "important", "weather", "promote",
    "Holiday_weather", "seasonal", "question",
]
SITECONTENT_CODES = ["chat-info"]

NAV_TIMEOUT = 60_000
RENDER_WAIT = 1_500
PDF_SIZE_LIMIT = 5 * 1024 * 1024
_COMPRESS_ATTEMPTS = [("ebook", 150), ("screen", 96), ("screen", 72)]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; SMG-Monthly-Scraper/2.2)",
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
            matched = _ingest_rows(
                rows, fe_lang, cms_lang, year, month, code,
                lambda fl, aid: f"{BASE_URL}/{fl}/news/{aid}",
                groups, group_dates, seen_keys,
            )
            log.info(f"  news/{code}: {matched} in {year}-{month:02d} (total {len(rows)})")
        for code in SITECONTENT_CODES:
            rows = fetch_cms_json(f"{CMS_BASE}/{cms_lang}/api/sitecontent/{code}")
            matched = _ingest_rows(
                rows, fe_lang, cms_lang, year, month, f"sitecontent:{code}",
                lambda fl, aid, c=code: f"{BASE_URL}/{fl}/{c}",
                groups, group_dates, seen_keys,
            )
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

def build_article_html(item: dict) -> str:
    title = item["title"]
    content = item.get("content") or ""
    lang = item["lang"]
    date_str = item["date_str"]
    label = LANG_LABEL.get(lang, lang.upper())
    source_url = item.get("url", "")
    source_tag = item.get("source", "")
    logo_src = SMG_LOGO.get(lang, SMG_LOGO["zh"])
    nav_html = "".join(f"<li>{item_label}</li>" for item_label in SMG_NAV.get(lang, SMG_NAV["zh"]))
    content = re.sub(r'(src|href)=(["\'])\/uploads\/', rf'\1=\2{CMS_BASE}/uploads/', content)
    content = re.sub(r'(src|href)=(["\'])\/\/', r'\1=\2https://', content)
    return f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="utf-8">
<title>{title}</title>
<style>
  * {{ box-sizing: border-box; }}
  html, body {{
    margin: 0; padding: 0;
    font-family: "Noto Sans TC", "Noto Sans SC", "Microsoft YaHei",
                 "PingFang TC", "PingFang SC", "Helvetica Neue", Arial, sans-serif;
    font-size: 14px; line-height: 1.7; color: #222;
    background: #fff;
  }}
  .site-header {{
    width: 100%;
    background:
      linear-gradient(rgb(2, 186, 188), rgba(2, 186, 188, 0.4) 100%),
      url("{SMG_HEADER_BG}") center / cover no-repeat;
    min-height: 92px;
    display: flex;
    align-items: center;
    padding: 10px 28px;
    -webkit-print-color-adjust: exact;
    print-color-adjust: exact;
  }}
  .site-header img.logo {{
    height: 58px;
    width: auto;
    max-width: 360px;
    display: block;
  }}
  .site-nav {{
    width: 100%;
    background: #129ea3;
    color: #fff;
    -webkit-print-color-adjust: exact;
    print-color-adjust: exact;
  }}
  .site-nav ul {{
    list-style: none;
    margin: 0;
    padding: 0 20px;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    min-height: 40px;
  }}
  .site-nav li {{
    color: #fff;
    font-size: 13px;
    padding: 8px 12px;
    white-space: nowrap;
  }}
  .article {{
    max-width: 800px;
    margin: 0 auto;
    padding: 22px 32px 28px;
  }}
  .meta {{ font-size: 12px; color: #666; margin-bottom: 8px; border-bottom: 1px solid #ddd; padding-bottom: 8px; }}
  .meta span {{ margin-right: 16px; }}
  h1 {{ font-size: 20px; font-weight: 700; margin: 12px 0 20px; line-height: 1.4; color: #0e8c90; }}
  .body img {{ max-width: 100%; height: auto; display: block; margin: 12px auto; }}
  .body p {{ margin: 0 0 12px; }}
  .body table {{ border-collapse: collapse; width: 100%; margin: 12px 0; }}
  .body th, .body td {{ border: 1px solid #ccc; padding: 6px 8px; text-align: left; }}
  .footer {{ margin-top: 28px; padding-top: 10px; border-top: 1px solid #eee; font-size: 11px; color: #999; }}
  @media print {{
    body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
  }}
</style>
</head>
<body>
  <header class="site-header">
    <img class="logo" src="{logo_src}" alt="SMG">
  </header>
  <nav class="site-nav"><ul>{nav_html}</ul></nav>
  <div class="article">
    <div class="meta">
      <span>📅 {date_str}</span>
      <span>🌐 {label}</span>
      <span>#{item['id']}</span>
      <span>{source_tag}</span>
    </div>
    <h1>{title}</h1>
    <div class="body">{content if content else "<p><em>（此語言版本暫無正文內容）</em></p>"}</div>
    <div class="footer">Source: {source_url}</div>
  </div>
</body>
</html>"""

def process_article(page: Page, item: dict, tmp_dir: Path, seq: int) -> Optional[Path]:
    safe = (item["title"] or "untitled")[:30].replace("/", "-")
    dest = tmp_dir / sanitize_filename(f"{seq:03d}_{item['date_str']}_{item['lang']}_{safe}.pdf")
    try:
        html = build_article_html(item)
        page.set_content(html, wait_until="networkidle", timeout=NAV_TIMEOUT)
        page.wait_for_timeout(RENDER_WAIT)
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        page.pdf(path=str(dest), format="A4", print_background=True,
                 margin={"top": "0mm", "bottom": "10mm", "left": "0mm", "right": "0mm"})
        if dest.exists() and dest.stat().st_size > 1_000:
            return dest
        log.warning(f"  PDF too small, skipping: {dest.name}")
        return None
    except Exception as e:
        log.warning(f"  Failed processing id={item['id']} [{item['lang']}]: {e}")
        return None

def compress_pdf(input_path: Path, output_path: Path) -> bool:
    input_size = input_path.stat().st_size
    if input_size <= PDF_SIZE_LIMIT:
        log.info(f"  PDF is {input_size / 1_048_576:.2f} MB — already under limit")
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
            log.warning("  Ghostscript not found — skipping compression")
            shutil.copy2(input_path, output_path)
            return False
        except subprocess.TimeoutExpired:
            log.warning(f"  gs /{gs_setting} timed out")
            continue
        out_size = output_path.stat().st_size if output_path.exists() else 0
        log.info(f"  /{gs_setting} @{img_dpi}dpi → {out_size / 1_048_576:.2f} MB"
                 + (" OK" if out_size <= PDF_SIZE_LIMIT else " (still large)"))
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
        log.info("   Output: ONE PDF | official header | date ASC, then zh → pt → en")
        log.info("   Sources: news/* + sitecontent/chat-info")
        current_dir = Path(os.getcwd())
        tmp_dir = current_dir / f"smg_tmp_{year}_{month:02d}"
        tmp_dir.mkdir(exist_ok=True)
        items = collect_month_articles(year, month)
        if not items:
            log.warning(f"No articles found for {year}-{month:02d}.")
            scraper_execution_result = {"success": False, "filename": "", "files": [], "message": "No matching articles found."}
            return
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(viewport={"width": 1200, "height": 1600}, accept_downloads=True)
            page = ctx.new_page()
            writer = PdfWriter()
            for i, item in enumerate(items, 1):
                content_len = len(item.get("content") or "")
                log.info(
                    f"\n({i}/{len(items)}) [{item['date_str']}] "
                    f"[{item['lang'].upper()}] {item['title'][:50]} "
                    f"(body {content_len} chars) [{item.get('source','')}]"
                )
                pdf_path = process_article(page, item, tmp_dir, i)
                if pdf_path:
                    try:
                        writer.append(str(pdf_path))
                    except Exception as e:
                        log.warning(f"  Could not append {pdf_path.name}: {e}")
            if len(writer.pages) == 0:
                log.warning("No pages rendered.")
                scraper_execution_result = {"success": False, "filename": "", "files": [], "message": "No pages rendered."}
                browser.close()
                return
            raw_output = current_dir / f"SMG_Monthly_Report_{year}_{month:02d}_raw.pdf"
            with raw_output.open("wb") as fh:
                writer.write(fh)
            final_filename = f"SMG_Monthly_Report_{year}_{month:02d}.pdf"
            output = current_dir / final_filename
            log.info(f"Compressing → {output.name} (target ≤ 5 MB)…")
            compress_pdf(raw_output, output)
            log.info(f"\nDone: {output.name}  ({output.stat().st_size / 1_048_576:.2f} MB)")
            raw_output.unlink(missing_ok=True)
            browser.close()
        scraper_execution_result = {
            "success": True,
            "filename": final_filename,
            "files": [final_filename],
            "message": f"Report generated: {final_filename}",
        }
    except Exception as e:
        log.error(f"Execution error: {e}")
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
    <p style="color:#666;font-size:0.9em;">單一 PDF｜官網頁首｜按日期排序｜中文→葡文→英文｜含新聞 + 天氣Fun識(chat-info)</p>
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
