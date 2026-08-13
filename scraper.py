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

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
BASE_URL = "https://www.smg.gov.mo"
CMS_BASE = "https://cms.smg.gov.mo"

# Frontend path segment → CMS API language code
# Order within the same article: zh → pt → en (as requested)
LANG_ORDER = ["zh", "pt", "en"]
LANG_CMS = {
    "zh": "zh_TW",
    "en": "en",
    "pt": "pt",
}
LANG_LABEL = {
    "zh": "中文",
    "pt": "Português",
    "en": "English",
}

# CMS news category codes (/api/news/{code})
NEWS_CODES = [
    "news",
    "normal",           # 本局動態 / activity-related
    "important",
    "weather",          # 氣候資訊
    "promote",
    "Holiday_weather",  # Extra Info / holiday weather
    "seasonal",
    "question",         # 問題解說 Q&A
]

# CMS sitecontent codes (/api/sitecontent/{code}) — e.g. 天氣「Fun」識
SITECONTENT_CODES = [
    "chat-info",        # https://www.smg.gov.mo/{lang}/chat-info
]

NAV_TIMEOUT = 60_000
RENDER_WAIT = 1_500

PDF_SIZE_LIMIT = 5 * 1024 * 1024
_COMPRESS_ATTEMPTS = [
    ("ebook", 150),
    ("screen", 96),
    ("screen", 72),
]

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
    """Return (title, content_html) for the given CMS language."""
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


def _ingest_rows(
    rows: list[dict],
    fe_lang: str,
    cms_lang: str,
    year: int,
    month: int,
    source: str,
    url_builder,
    groups: dict,
    group_dates: dict,
    seen_keys: set,
) -> int:
    """Filter rows for target month and merge into groups. Returns match count."""
    matched = 0
    for row in rows:
        aid = row.get("id")
        if aid is None:
            continue
        try:
            aid = int(aid)
        except (TypeError, ValueError):
            continue

        # Namespace key so news id and sitecontent id never collide
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
            "id": aid,
            "gkey": gkey,
            "lang": fe_lang,
            "date_str": dt.strftime("%Y-%m-%d"),
            "datetime": dt,
            "title": title or f"article-{aid}",
            "content": content,
            "url": url_builder(fe_lang, aid),
            "source": source,
        }
        if gkey not in group_dates or dt < group_dates[gkey]:
            group_dates[gkey] = dt

    return matched


def collect_month_articles(year: int, month: int) -> list[dict]:
    """
    Collect articles for target month from all languages.
    Returns a flat list of render items, already sorted:
      1. by date ascending
      2. same article (shared id): zh → pt → en
    Each item carries full title + content HTML from CMS.
    Sources: /api/news/* and /api/sitecontent/chat-info
    """
    groups: dict[str, dict[str, dict]] = defaultdict(dict)
    group_dates: dict[str, datetime] = {}

    for fe_lang in LANG_ORDER:
        cms_lang = LANG_CMS[fe_lang]
        log.info(f"\n🌐 Fetching CMS lists for {fe_lang} ({cms_lang})")
        seen_keys: set[str] = set()

        # ── news categories ──
        for code in NEWS_CODES:
            rows = fetch_news_list(cms_lang, code)
            matched = _ingest_rows(
                rows, fe_lang, cms_lang, year, month,
                source=code,
                url_builder=lambda fl, aid: f"{BASE_URL}/{fl}/news/{aid}",
                groups=groups, group_dates=group_dates, seen_keys=seen_keys,
            )
            log.info(f"  news/{code}: {matched} in {year}-{month:02d} (total rows {len(rows)})")

        # ── sitecontent (天氣「Fun」識 etc.) ──
        for code in SITECONTENT_CODES:
            rows = fetch_sitecontent_list(cms_lang, code)
            matched = _ingest_rows(
                rows, fe_lang, cms_lang, year, month,
                source=f"sitecontent:{code}",
                url_builder=lambda fl, aid, c=code: f"{BASE_URL}/{fl}/{c}",
                groups=groups, group_dates=group_dates, seen_keys=seen_keys,
            )
            log.info(f"  sitecontent/{code}: {matched} in {year}-{month:02d} (total rows {len(rows)})")

    ordered_keys = sorted(groups.keys(), key=lambda k: (group_dates.get(k) or datetime.min, k))
    flat: list[dict] = []
    for gkey in ordered_keys:
        langs_present = groups[gkey]
        for fe_lang in LANG_ORDER:  # zh → pt → en
            if fe_lang in langs_present:
                flat.append(langs_present[fe_lang])

    log.info(f"\n📦 Unique articles: {len(ordered_keys)}")
    log.info(f"📦 Total language variants to render: {len(flat)}")
    return flat


# ── HTML → PDF rendering ───────────────────────────────────────────────────

def build_article_html(item: dict) -> str:
    """Build a clean, printable HTML document from CMS title + content."""
    title = item["title"]
    content = item.get("content") or ""
    lang = item["lang"]
    date_str = item["date_str"]
    label = LANG_LABEL.get(lang, lang.upper())
    source_url = item.get("url", "")
    source_tag = item.get("source", "")

    content = re.sub(
        r'(src|href)=(["\'])\/uploads\/',
        rf'\1=\2{CMS_BASE}/uploads/',
        content,
    )
    content = re.sub(
        r'(src|href)=(["\'])\/\/',
        r'\1=\2https://',
        content,
    )

    return f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{
    font-family: "Noto Sans TC", "Noto Sans SC", "Microsoft YaHei",
                 "PingFang TC", "PingFang SC", "Helvetica Neue",
                 Arial, sans-serif;
    font-size: 14px;
    line-height: 1.7;
    color: #222;
    max-width: 800px;
    margin: 0 auto;
    padding: 24px 32px;
  }}
  .meta {{
    font-size: 12px;
    color: #666;
    margin-bottom: 8px;
    border-bottom: 1px solid #ddd;
    padding-bottom: 8px;
  }}
  .meta span {{ margin-right: 16px; }}
  h1 {{
    font-size: 20px;
    font-weight: 700;
    margin: 12px 0 20px;
    line-height: 1.4;
    color: #111;
  }}
  .body img {{
    max-width: 100%;
    height: auto;
    display: block;
    margin: 12px auto;
  }}
  .body p {{ margin: 0 0 12px; }}
  .body table {{
    border-collapse: collapse;
    width: 100%;
    margin: 12px 0;
  }}
  .body th, .body td {{
    border: 1px solid #ccc;
    padding: 6px 8px;
    text-align: left;
  }}
  .footer {{
    margin-top: 28px;
    padding-top: 10px;
    border-top: 1px solid #eee;
    font-size: 11px;
    color: #999;
  }}
  @media print {{
    body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
  }}
</style>
</head>
<body>
  <div class="meta">
    <span>📅 {date_str}</span>
    <span>🌐 {label}</span>
    <span>#{item['id']}</span>
    <span>{source_tag}</span>
  </div>
  <h1>{title}</h1>
  <div class="body">
    {content if content else "<p><em>（此語言版本暫無正文內容）</em></p>"}
  </div>
  <div class="footer">Source: {source_url}</div>
</body>
</html>"""


def process_article(page: Page, item: dict, tmp_dir: Path, seq: int) -> Optional[Path]:
    safe = (item["title"] or "untitled")[:30].replace("/", "-")
    dest = tmp_dir / sanitize_filename(
        f"{seq:03d}_{item['date_str']}_{item['lang']}_{safe}.pdf"
    )

    try:
        html = build_article_html(item)
        page.set_content(html, wait_until="networkidle", timeout=NAV_TIMEOUT)
        page.wait_for_timeout(RENDER_WAIT)
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass

        page.pdf(
            path=str(dest),
            format="A4",
            print_background=True,
            margin={"top": "15mm", "bottom": "15mm", "left": "12mm", "right": "12mm"},
        )

        if dest.exists() and dest.stat().st_size > 1_000:
            return dest
        log.warning(f"  PDF too small, skipping: {dest.name}")
        return None

    except Exception as e:
        log.warning(f"  Failed processing id={item['id']} [{item['lang']}]: {e}")
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
            "gs",
            "-dBATCH", "-dNOPAUSE", "-dQUIET",
            "-sDEVICE=pdfwrite",
            "-dCompatibilityLevel=1.5",
            f"-dPDFSETTINGS=/{gs_setting}",
            "-dDownsampleColorImages=true",
            "-dDownsampleGrayImages=true",
            "-dDownsampleMonoImages=true",
            f"-dColorImageResolution={img_dpi}",
            f"-dGrayImageResolution={img_dpi}",
            f"-dMonoImageResolution={min(img_dpi * 2, 300)}",
            "-dCompressFonts=true",
            "-dEmbedAllFonts=true",
            f"-sOutputFile={output_path}",
            str(input_path),
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
        log.info(
            f"  /{gs_setting} @{img_dpi}dpi → {out_mb:.2f} MB"
            + (" ✅" if out_size <= PDF_SIZE_LIMIT else " (still large)")
        )
        if out_size <= PDF_SIZE_LIMIT:
            return True

    if output_path.exists() and output_path.stat().st_size > 0:
        final_mb = output_path.stat().st_size / 1_048_576
        log.warning(f"  ⚠️  Could not reach 5 MB target; final size: {final_mb:.2f} MB")
        return False

    import shutil
    shutil.copy2(input_path, output_path)
    return False


def main(year: int, month: int) -> None:
    log.info(f"🚀 SMG Monthly Scraper — Target: {year}-{month:02d}")
    log.info("   Output: ONE PDF | order: date ASC, then zh → pt → en")
    log.info("   Sources: news/* + sitecontent/chat-info (天氣Fun識)")

    items = collect_month_articles(year, month)
    if not items:
        log.warning(f"❌ No articles found for {year}-{month:02d}. Exiting.")
        return

    tmp_dir = Path(f"smg_tmp_{year}_{month:02d}")
    tmp_dir.mkdir(exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1200, "height": 1600},
            accept_downloads=True,
        )
        page = ctx.new_page()

        writer = PdfWriter()
        for i, item in enumerate(items, 1):
            content_len = len(item.get("content") or "")
            log.info(
                f"\n⚙  ({i}/{len(items)}) [{item['date_str']}] "
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
            log.warning("❌ No pages rendered. Exiting.")
            browser.close()
            return

        raw_output = Path(f"SMG_Monthly_Report_{year}_{month:02d}_raw.pdf")
        with raw_output.open("wb") as fh:
            writer.write(fh)

        raw_mb = raw_output.stat().st_size / 1_048_576
        log.info(f"\n📄 Raw merged PDF: {raw_output.name}  ({raw_mb:.2f} MB)")

        output = Path(f"SMG_Monthly_Report_{year}_{month:02d}.pdf")
        log.info(f"🗜  Compressing → {output.name} (target ≤ 5 MB)…")
        compress_pdf(raw_output, output)

        final_mb = output.stat().st_size / 1_048_576
        log.info(f"\n✅ Done: {output.name}  ({final_mb:.2f} MB)")
        raw_output.unlink(missing_ok=True)
        browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=get_target_month()[0])
    parser.add_argument("--month", type=int, default=get_target_month()[1])
    args = parser.parse_args()
    main(args.year, args.month)
