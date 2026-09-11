from __future__ import annotations
import argparse
import logging
import re
import subprocess
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests
from playwright.sync_api import Page, sync_playwright
from pypdf import PdfWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

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

NEWS_CODES = ["news", "normal", "important", "weather", "promote", "Holiday_weather", "seasonal", "question"]
SITECONTENT_CODES = ["chat-info"]

NAV_TIMEOUT = 60_000
RENDER_WAIT = 2_500
PDF_SIZE_LIMIT = 5 * 1024 * 1024
_COMPRESS_ATTEMPTS = [("ebook", 150), ("screen", 96), ("screen", 72)]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; SMG-Monthly-Scraper/2.2)",
    "Accept": "application/json",
})


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


def fetch_news_list(cms_lang: str, code: str) -> list[dict]:
    return fetch_cms_json(f"{CMS_BASE}/{cms_lang}/api/news/{code}")


def fetch_sitecontent_list(cms_lang: str, code: str) -> list[dict]:
    return fetch_cms_json(f"{CMS_BASE}/{cms_lang}/api/sitecontent/{code}")


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


def _ingest_rows(rows, fe_lang, cms_lang, year, month, source, url_builder, groups, group_dates, seen_keys) -> int:
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
            rows = fetch_news_list(cms_lang, code)
            matched = _ingest_rows(
                rows, fe_lang, cms_lang, year, month, code,
                lambda fl, aid: f"{BASE_URL}/{fl}/news-detail/{aid}",
                groups, group_dates, seen_keys,
            )
            log.info(f"  news/{code}: {matched} in {year}-{month:02d} (total rows {len(rows)})")
        for code in SITECONTENT_CODES:
            rows = fetch_sitecontent_list(cms_lang, code)
            matched = _ingest_rows(
                rows, fe_lang, cms_lang, year, month, f"sitecontent:{code}",
                lambda fl, aid, c=code: f"{BASE_URL}/{fl}/{c}/{aid}",
                groups, group_dates, seen_keys,
            )
            log.info(f"  sitecontent/{code}: {matched} in {year}-{month:02d} (total rows {len(rows)})")
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
        page.add_style_tag(content="""
          html, body { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }
          .mega-menu, .dropdown-menu { display: none !important; visibility: hidden !important; }
          #menu-button { display: none !important; }
        """)
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
            page.wait_for_function(
                "() => { const h = document.querySelector('h1'); return !!(h && h.textContent && h.textContent.trim().length > 1); }",
                timeout=20_000,
            )
        except Exception:
            page.wait_for_timeout(2_000)
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        page.wait_for_timeout(RENDER_WAIT)
        _prepare_live_page_for_pdf(page)
        page.pdf(
            path=str(dest),
            format="A4",
            print_background=True,
            margin={"top": "0mm", "bottom": "8mm", "left": "0mm", "right": "0mm"},
        )
        if dest.exists() and dest.stat().st_size > 1_000:
            return dest
        log.warning(f"  PDF too small, skipping: {dest.name}")
        return None
    except Exception as e:
        log.warning(f"  Live page failed id={item['id']} [{item['lang']}]: {e}")
        return None


def compress_pdf(input_path: Path, output_path: Path) -> bool:
    input_size = input_path.stat().st_size
    input_mb = input_size / 1_048_576
    if input_size <= PDF_SIZE_LIMIT:
        log.info(f"  PDF is {input_mb:.2f} MB — already under 5 MB, skipping compression")
        import shutil
        shutil.copy2(input_path, output_path)
        return True
    log.info(f"  PDF is {input_mb:.2f} MB — compressing…")
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
            log.error("  Ghostscript (gs) not found — skipping compression")
            import shutil
            shutil.copy2(input_path, output_path)
            return False
        except subprocess.TimeoutExpired:
            log.warning(f"  gs /{gs_setting} timed out")
            continue
        out_size = output_path.stat().st_size if output_path.exists() else 0
        out_mb = out_size / 1_048_576
        log.info(f"  /{gs_setting} @{img_dpi}dpi → {out_mb:.2f} MB" + (" OK" if out_size <= PDF_SIZE_LIMIT else " (still large)"))
        if out_size <= PDF_SIZE_LIMIT:
            return True
    if output_path.exists() and output_path.stat().st_size > 0:
        return False
    import shutil
    shutil.copy2(input_path, output_path)
    return False


def main(year: int, month: int) -> None:
    log.info(f"SMG Monthly Scraper — Target: {year}-{month:02d}")
    log.info("   Output: ONE PDF | live webpage capture | date ASC, then zh → pt → en")
    log.info("   Sources: news/* + sitecontent/chat-info")
    items = collect_month_articles(year, month)
    if not items:
        log.warning(f"No articles found for {year}-{month:02d}. Exiting.")
        return
    tmp_dir = Path(f"smg_tmp_{year}_{month:02d}")
    tmp_dir.mkdir(exist_ok=True)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1400, "height": 900}, accept_downloads=True)
        page = ctx.new_page()
        writer = PdfWriter()
        for i, item in enumerate(items, 1):
            log.info(
                f"\n({i}/{len(items)}) [{item['date_str']}] [{item['lang'].upper()}] {item['title'][:50]} [{item.get('source','')}]"
            )
            pdf_path = process_article(page, item, tmp_dir, i)
            if pdf_path:
                try:
                    writer.append(str(pdf_path))
                except Exception as e:
                    log.warning(f"  Could not append {pdf_path.name}: {e}")
        if len(writer.pages) == 0:
            log.warning("No pages rendered. Exiting.")
            browser.close()
            return
        raw_output = Path(f"SMG_Monthly_Report_{year}_{month:02d}_raw.pdf")
        with raw_output.open("wb") as fh:
            writer.write(fh)
        output = Path(f"SMG_Monthly_Report_{year}_{month:02d}.pdf")
        log.info(f"Compressing → {output.name} (target ≤ 5 MB)…")
        compress_pdf(raw_output, output)
        log.info(f"\nDone: {output.name}  ({output.stat().st_size / 1_048_576:.2f} MB)")
        raw_output.unlink(missing_ok=True)
        browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=get_target_month()[0])
    parser.add_argument("--month", type=int, default=get_target_month()[1])
    args = parser.parse_args()
    main(args.year, args.month)
