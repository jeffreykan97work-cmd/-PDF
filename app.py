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
from playwright.sync_api import Browser, Page, sync_playwright
from pypdf import PdfReader, PdfWriter

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
LOCALES = {"zh": "zh-TW", "pt": "pt-PT", "en": "en-US"}
ACCEPT_LANGUAGE = {"zh": "zh-TW,zh;q=0.9", "pt": "pt-PT,pt;q=0.9", "en": "en-US,en;q=0.9"}
CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
PASS1_ATTEMPTS = 3  # 第一輪：CMS 確認存在的語言版本最多嘗試次數
PASS2_ATTEMPTS = 4  # 第二輪補抓：延長等待後的嘗試次數
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

def fetch_cms_json(url: str, retries: int = 3) -> list[dict]:
    """取得 CMS 列表；暫時性失敗會重試，避免整個語言的列表因一次逾時而消失。"""
    for attempt in range(1, retries + 1):
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
            log.warning(f"  CMS fetch failed ({attempt}/{retries}) {url}: {e}")
            if attempt < retries:
                time.sleep(2 * attempt)
    return []


def _real_translation(row: dict, cms_lang: str) -> tuple[str, str]:
    """
    只讀取 translations 內「真正存在」的翻譯。
    不再回退到 row["name"]（那通常是中文），否則會把沒有葡文/英文的文章誤當成有該語言版本。
    """
    tr = row.get("translations")
    block = tr.get(cms_lang) if isinstance(tr, dict) else None
    if not isinstance(block, dict):
        return "", ""
    title = (block.get("title") or "").strip()
    content = (block.get("content") or "").strip()
    if not content and title.startswith("article-"):
        return "", ""
    return title, content


def _fetch_all_rows() -> tuple[dict, dict]:
    """
    抓取三種語言的全部列表（此階段不篩月份），並以「文章本身」歸戶：
      - 新聞：以 news-detail:{id} 歸戶，不再受 news / important / weather… 分類代碼影響
      - 站內內容：以 {code}:{id} 歸戶（例如 chat-info:{id}）
    回傳 (rows_by_article[gkey][lang] = row, meta[gkey] = {aid, url_seg})
    """
    rows_by_article: dict[str, dict[str, dict]] = defaultdict(dict)
    meta: dict[str, dict] = {}
    for fe_lang in LANG_ORDER:
        cms_lang = LANG_CMS[fe_lang]
        log.info(f"\nFetching CMS lists for {fe_lang} ({cms_lang})")
        sources = [
            (f"{CMS_BASE}/{cms_lang}/api/news/{code}", "news-detail", "news-detail", f"news/{code}")
            for code in NEWS_CODES
        ] + [
            (f"{CMS_BASE}/{cms_lang}/api/sitecontent/{code}", code, code, f"sitecontent/{code}")
            for code in SITECONTENT_CODES
        ]
        for api_url, ident, url_seg, label in sources:
            rows = fetch_cms_json(api_url)
            kept = 0
            for row in rows:
                try:
                    aid = int(row.get("id"))
                except (TypeError, ValueError):
                    continue
                gkey = f"{ident}:{aid}"
                meta.setdefault(gkey, {"aid": aid, "url_seg": url_seg})
                cur = rows_by_article[gkey].get(fe_lang)
                # 同一篇出現在多個分類時，優先保留「帶有真實翻譯」的那一筆
                if cur is None or (not any(_real_translation(cur, cms_lang)) and any(_real_translation(row, cms_lang))):
                    rows_by_article[gkey][fe_lang] = row
                kept += 1
            if not rows:
                log.warning(f"  {label}: 0 rows (可能是 API 失敗或此分類沒有資料)")
            else:
                log.info(f"  {label}: {kept} rows")
    return rows_by_article, meta


def collect_month_articles(year: int, month: int) -> list[dict]:
    """
    依「文章」為單位收集選定月份的所有語言版本（zh / pt / en 一律各產生一筆）：
      - 月份以 zh → pt → en 第一個有效的 startdate 為準，同一篇文章不會因各語言日期不同而被拆開或漏掉
      - 各語言的標題/內容會從「任何一個語言列表」的 translations 中取，不受單一列表缺漏影響
      - required=True 表示 CMS 確認有此語言；False 表示 CMS 查無，僅對官網頁面做一次探測
    """
    rows_by_article, meta = _fetch_all_rows()
    grouped: list[tuple[datetime, str, list[dict]]] = []

    for gkey, by_lang in rows_by_article.items():
        canon: Optional[datetime] = None
        for fe_lang in LANG_ORDER:
            row = by_lang.get(fe_lang)
            if row:
                canon = parse_startdate(str(row.get("startdate") or ""))
                if canon:
                    break
        if not canon or canon.year != year or canon.month != month:
            continue

        aid, seg = meta[gkey]["aid"], meta[gkey]["url_seg"]
        fallback_name = next((str(r.get("name")).strip() for r in by_lang.values() if r.get("name")), "") or f"article-{aid}"

        versions: list[dict] = []
        for fe_lang in LANG_ORDER:
            title = content = ""
            for src in [fe_lang] + [l for l in LANG_ORDER if l != fe_lang]:
                row = by_lang.get(src)
                if row:
                    title, content = _real_translation(row, LANG_CMS[fe_lang])
                    if title or content:
                        break
            versions.append({
                "id": aid, "gkey": gkey, "lang": fe_lang,
                "date_str": canon.strftime("%Y-%m-%d"), "datetime": canon,
                "title": title or fallback_name, "content": content,
                "url": f"{BASE_URL}/{fe_lang}/{seg}/{aid}", "seg": seg, "source": gkey,
                "required": bool(title or content),
            })
        grouped.append((canon, gkey, versions))

    grouped.sort(key=lambda g: (g[0], g[1]))
    flat = [v for _, _, versions in grouped for v in versions]
    log.info(f"\nUnique articles: {len(grouped)}")
    for fe_lang in LANG_ORDER:
        have = sum(1 for v in flat if v["lang"] == fe_lang and v["required"])
        log.info(f"  CMS 確認有 {fe_lang.upper()} 版本：{have}/{len(grouped)}")
    log.info(f"Total language variants to render: {len(flat)}")
    return flat


# ------------------------------------------------------------------
# 頁面擷取與驗證
# ------------------------------------------------------------------
_H1_READY_JS = "() => { const h = document.querySelector('h1'); return !!(h && h.textContent && h.textContent.trim().length > 1); }"

_SNAPSHOT_JS = """() => {
  const body = document.body ? (document.body.innerText || '') : '';
  const h1 = document.querySelector('h1');
  const main = document.querySelector('main, article, .news-detail, .detail-content, .content-box');
  const mainText = main ? (main.innerText || '') : body;
  return {
    url: location.href,
    h1: h1 ? (h1.textContent || '').trim() : '',
    bodyLen: body.trim().length,
    mainLen: mainText.trim().length,
    mainText: mainText.slice(0, 4000)
  };
}"""

_SCROLL_JS = """async () => {
  await new Promise((resolve) => {
    let y = 0;
    const timer = setInterval(() => {
      const h = document.body ? document.body.scrollHeight : 0;
      window.scrollBy(0, 500); y += 500;
      if (y >= h || y > 20000) { clearInterval(timer); window.scrollTo(0, 0); resolve(); }
    }, 80);
  });
}"""

_ERROR_KEYWORDS = ("404", "Page Not Found", "找不到頁面", "Página não encontrada", "Not Found")


def _cjk_ratio(text: str) -> float:
    chars = [c for c in text if not c.isspace()]
    if not chars:
        return 0.0
    return sum(1 for c in chars if CJK_RE.match(c)) / len(chars)


def validate_snapshot(snap: dict, lang: str, seg: str) -> tuple[bool, str]:
    """判斷頁面是否真的是「目標語言」的文章內容，而不是空白頁 / 404 / 被導向別的語言或列表頁。"""
    url_now = snap.get("url", "")
    if f"/{lang}/" not in url_now or f"/{seg}/" not in url_now:
        return False, f"網址不符（目前在 {url_now}）"

    h1 = snap.get("h1", "")
    main_len = snap.get("mainLen", 0)
    body_len = snap.get("bodyLen", 0)
    text = f"{h1} {snap.get('mainText', '')}"

    if not h1 and main_len < 80:
        return False, "正文尚未載入"
    if body_len < 300 and any(k in text for k in _ERROR_KEYWORDS):
        return False, "頁面為 404 / 無內容"

    ratio = _cjk_ratio(text)
    if lang == "zh" and ratio < 0.15:
        return False, f"內容不是中文（中日韓字元占比 {ratio:.0%}），疑似語系回退"
    if lang in ("pt", "en") and ratio > 0.35:
        return False, f"內容是中文（占比 {ratio:.0%}），疑似語系回退"
    return True, ""


def verify_pdf_file(path: Path) -> tuple[bool, str]:
    """確認輸出的 PDF 可讀、有頁面、不是空白頁。"""
    if not path.exists() or path.stat().st_size < 1_000:
        return False, "PDF 檔案過小或不存在"
    try:
        reader = PdfReader(str(path))
        if len(reader.pages) == 0:
            return False, "PDF 沒有頁面"
        text = "".join((pg.extract_text() or "") for pg in reader.pages[:3])
        if len(text.strip()) < 20 and path.stat().st_size < 20_000:
            return False, "PDF 疑似空白頁"
    except Exception as e:
        return False, f"PDF 無法讀取: {e}"
    return True, ""


def _prepare_live_page_for_pdf(page: Page) -> None:
    try:
        page.emulate_media(media="screen")
    except Exception:
        pass
    try:
        page.add_style_tag(content="html, body { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; } .mega-menu, .dropdown-menu { display: none !important; } #menu-button { display: none !important; }")
    except Exception:
        pass


def _wait_for_article(page: Page, extra_ms: int) -> None:
    try:
        page.wait_for_function(_H1_READY_JS, timeout=20_000 + extra_ms)
    except Exception:
        page.wait_for_timeout(2_000)
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass
    page.wait_for_timeout(RENDER_WAIT + extra_ms)


def dest_pdf_path(tmp_dir: Path, item: dict) -> Path:
    safe = (item["title"] or "untitled")[:30].replace("/", "-")
    return tmp_dir / sanitize_filename(f"{item['seq']:03d}_{item['date_str']}_{item['lang']}_{safe}.pdf")


def render_language_pdf(browser: Browser, item: dict, tmp_dir: Path, attempts: int, slow: bool = False) -> tuple[Optional[Path], str]:
    """
    擷取單一文章的單一語言 PDF。每次嘗試都使用全新的瀏覽器環境（獨立 cookie / localStorage），
    避免網站記住上一個語言而把 /pt/ 或 /zh/ 導回其他語言；擷取前先驗證內容語言正確，
    擷取後再驗證 PDF 非空白，失敗會自動延長等待並重試。
    回傳 (PDF 路徑或 None, 最後一次失敗原因)。
    """
    lang, url, seg = item["lang"], item["url"], item["seg"]
    dest = dest_pdf_path(tmp_dir, item)
    last_reason = ""
    for attempt in range(1, attempts + 1):
        dest.unlink(missing_ok=True)
        extra = (attempt - 1) * 2_000 + (3_000 if slow else 0)
        ctx = browser.new_context(
            viewport={"width": 1400, "height": 900},
            locale=LOCALES[lang],
            extra_http_headers={"Accept-Language": ACCEPT_LANGUAGE[lang]},
        )
        page = ctx.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
            _wait_for_article(page, extra)
            ok, reason = validate_snapshot(page.evaluate(_SNAPSHOT_JS), lang, seg)
            if not ok and f"/{lang}/" not in page.url:
                # 被網站導向其他語言：重新進入目標網址一次
                page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                _wait_for_article(page, extra)
                ok, reason = validate_snapshot(page.evaluate(_SNAPSHOT_JS), lang, seg)
            if not ok:
                last_reason = reason
                log.warning(f"  [{lang.upper()}] 第 {attempt}/{attempts} 次驗證未通過：{reason}")
                continue

            try:
                page.evaluate(_SCROLL_JS)  # 觸發懶載入圖片
                page.wait_for_timeout(800)
            except Exception:
                pass
            _prepare_live_page_for_pdf(page)
            page.pdf(path=str(dest), format="A4", print_background=True,
                     margin={"top": "0mm", "bottom": "8mm", "left": "0mm", "right": "0mm"})
            good, reason = verify_pdf_file(dest)
            if good:
                return dest, ""
            last_reason = reason
            log.warning(f"  [{lang.upper()}] 第 {attempt}/{attempts} 次輸出的 PDF 無效：{reason}")
        except Exception as e:
            last_reason = f"{type(e).__name__}: {e}"
            log.warning(f"  [{lang.upper()}] 第 {attempt}/{attempts} 次擷取發生錯誤：{last_reason}")
        finally:
            ctx.close()
    dest.unlink(missing_ok=True)
    return None, last_reason


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

def build_completeness_report(items: list[dict], results: dict, failures: dict, year: int, month: int) -> tuple[str, int, int]:
    """產生完整性報告文字；回傳 (報告內容, 三語齊全篇數, 擷取失敗版本數)。"""
    order: list[str] = []
    by_article: dict[str, dict[str, dict]] = defaultdict(dict)
    for it in items:
        if it["gkey"] not in by_article:
            order.append(it["gkey"])
        by_article[it["gkey"]][it["lang"]] = it

    def status(it: dict) -> str:
        if (it["gkey"], it["lang"]) in results:
            return "OK"
        return "缺(擷取失敗)" if it["required"] else "缺(官網無此版本)"

    complete = 0
    failed_versions = 0
    lines = [f"SMG {year}-{month:02d} 中葡英三語 PDF 完整性報告", "=" * 60]
    detail: list[str] = []
    for gkey in order:
        vs = by_article[gkey]
        sts = {l: status(vs[l]) for l in LANG_ORDER}
        if all(s == "OK" for s in sts.values()):
            complete += 1
        failed_versions += sum(1 for s in sts.values() if s == "缺(擷取失敗)")
        first = vs[LANG_ORDER[0]]
        detail.append(f"{first['date_str']} | {gkey:<22} | " + " | ".join(f"{l.upper()}:{sts[l]}" for l in LANG_ORDER))
        for l in LANG_ORDER:
            if sts[l] == "缺(擷取失敗)":
                detail.append(f"    └ {l.upper()} 原因：{failures.get((gkey, l), '未知')}  ({vs[l]['url']})")
    lines.append(f"文章總數：{len(order)}　三語齊全：{complete}　擷取失敗版本：{failed_versions}")
    lines.append("-" * 60)
    lines.extend(detail)
    return "\n".join(lines) + "\n", complete, failed_versions


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
        for seq, item in enumerate(items, 1):
            item["seq"] = seq

        results: dict[tuple[str, str], Path] = {}
        failures: dict[tuple[str, str], str] = {}

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            try:
                # 第一輪：CMS 確認有的語言最多重試 3 次；CMS 查無的語言只探測 1 次
                for item in items:
                    key = (item["gkey"], item["lang"])
                    log.info(f"\n({item['seq']}/{len(items)}) [{item['date_str']}] [{item['lang'].upper()}] {item['title'][:50]}")
                    attempts = PASS1_ATTEMPTS if item["required"] else 1
                    path, reason = render_language_pdf(browser, item, tmp_dir, attempts)
                    if path:
                        results[key] = path
                        if not item["required"]:
                            log.info("  CMS 未列出此語言，但官網頁面存在，已補入。")
                    else:
                        failures[key] = reason

                # 第二輪：對 CMS 確認存在、卻仍失敗的版本，延長等待再補抓
                retry_items = [it for it in items if it["required"] and (it["gkey"], it["lang"]) not in results]
                if retry_items:
                    log.info(f"\n[第二輪補抓] 共 {len(retry_items)} 個版本需要補抓")
                for item in retry_items:
                    key = (item["gkey"], item["lang"])
                    log.info(f"  補抓 ({item['seq']}) [{item['date_str']}] [{item['lang'].upper()}] {item['title'][:50]}")
                    path, reason = render_language_pdf(browser, item, tmp_dir, PASS2_ATTEMPTS, slow=True)
                    if path:
                        results[key] = path
                        failures.pop(key, None)
                        log.info("    補抓成功")
                    else:
                        failures[key] = reason
            finally:
                browser.close()

        writer = PdfWriter()
        for item in items:  # items 已依日期、文章、zh→pt→en 排序
            path = results.get((item["gkey"], item["lang"]))
            if path:
                try:
                    writer.append(str(path))
                except Exception as e:
                    log.warning(f"  Could not append {path.name}: {e}")
                    results.pop((item["gkey"], item["lang"]), None)
                    failures[(item["gkey"], item["lang"])] = f"合併失敗: {e}"
        if len(writer.pages) == 0:
            scraper_execution_result = {"success": False, "filename": "", "files": [], "message": "No pages rendered."}
            return

        raw_output = current_dir / f"SMG_Monthly_Report_{year}_{month:02d}_raw.pdf"
        with raw_output.open("wb") as fh:
            writer.write(fh)
        final_filename = f"SMG_Monthly_Report_{year}_{month:02d}.pdf"
        output = current_dir / final_filename
        compress_pdf(raw_output, output)
        log.info(f"\nDone: {output.name}  ({output.stat().st_size / 1_048_576:.2f} MB)")
        raw_output.unlink(missing_ok=True)

        report_text, complete, failed_versions = build_completeness_report(items, results, failures, year, month)
        report_name = f"SMG_Completeness_Report_{year}_{month:02d}.txt"
        (current_dir / report_name).write_text(report_text, encoding="utf-8")
        log.info("\n" + report_text)

        total_articles = len({it["gkey"] for it in items})
        message = f"Report generated: {final_filename}（{total_articles} 篇文章，三語齊全 {complete} 篇）"
        if failed_versions:
            message += f"；仍有 {failed_versions} 個版本擷取失敗，請查看 {report_name}"
        scraper_execution_result = {"success": True, "filename": final_filename, "files": [final_filename, report_name], "message": message}
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
def run_cli(argv: Optional[list[str]] = None) -> int:
    """
    命令列模式（給 GitHub Actions 使用）：python app.py --cli [--year Y] [--month M]
    年月皆留空 = 上個月。成功回傳 0，失敗回傳 1。
    """
    import argparse
    ap = argparse.ArgumentParser(description="SMG monthly PDF scraper (headless)")
    ap.add_argument("--cli", action="store_true", help="不啟動網頁介面，直接執行並結束")
    ap.add_argument("--year", type=int, default=None)
    ap.add_argument("--month", type=int, default=None)
    args = ap.parse_args(argv)
    if args.year and not args.month:
        ap.error("指定 --year 時也必須指定 --month")
    if args.month and not args.year:
        args.year = date.today().year
    if args.month is not None and not 1 <= args.month <= 12:
        ap.error("--month 必須介於 1 到 12")
    execute_scraping_worker(args.year, args.month)
    result = scraper_execution_result
    print(result.get("message", ""))
    return 0 if result.get("success") else 1


if __name__ == '__main__':
    if "--cli" in sys.argv[1:]:
        sys.exit(run_cli())
    threading.Timer(1.5, lambda: webbrowser.open('http://127.0.0.1:5000')).start()
    app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False)
