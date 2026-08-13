from __future__ import annotations
import argparse
import logging
import re
import subprocess
import time
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
LANG_PRIORITY = {lang: i for i, lang in enumerate(LANG_ORDER)}  # zh=0, pt=1, en=2

# CMS news category codes (from /api/newstype + seasonal)
# activity page loads important + normal; news page loads news; etc.
NEWS_CODES = [
    "news",
    "normal",           # 本局動態 / activity-related
    "important",
    "weather",          # 氣候資訊
    "promote",
    "Holiday_weather",  # Extra Info / holiday weather
    "seasonal",
]

NAV_TIMEOUT = 60_000
RENDER_WAIT = 2_000

PDF_SIZE_LIMIT = 5 * 1024 * 1024
_COMPRESS_ATTEMPTS = [
    ("ebook", 150),
    ("screen", 96),
    ("screen", 72),
]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; SMG-Monthly-Scraper/2.0)",
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
        # Prefer this language's title, fall back to any
        block = tr.get(cms_lang) or next(iter(tr.values()), {}) or {}
        t = (block.get("title") or "").strip()
        if t:
            return t
    return (item.get("name") or f"article-{item.get('id')}").strip()


def collect_month_articles(year: int, month: int) -> list[dict]:
    """
    Collect articles for target month from all languages.
    Returns a flat list of render items, already sorted:
      1. by date ascending
      2. same article (shared id): zh → pt → en
    """
    # id -> { lang -> item_meta }
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

                # Need a real title in this language (skip empty translation shells)
                title = title_from_item(row, cms_lang)
                # en/pt lists sometimes include empty zh shells — skip if title empty
                tr = row.get("translations") or {}
                if isinstance(tr, dict):
                    block = tr.get(cms_lang) or {}
                    if not (block.get("title") or "").strip() and fe_lang != "zh":
                        # still allow if any title exists for this lang key
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
                # Prefer earliest known date for sorting the group
                if aid not in group_dates or dt < group_dates[aid]:
                    group_dates[aid] = dt

            log.info(f"  {code}: {matched} in {year}-{month:02d} (total rows {len(rows)})")

    # Build ordered flat list
    ordered_ids = sorted(groups.keys(), key=lambda i: (group_dates.get(i) or datetime.min, i))
    flat: list[dict] = []
    for aid in ordered_ids:
        langs_present = groups[aid]
        for fe_lang in LANG_ORDER:  # zh → pt → en
            if fe_lang in langs_present:
                flat.append(langs_present[fe_lang])

    log.info(f"\n📦 Unique articles (by id): {len(ordered_ids)}")
    log.info(f"📦 Total language variants to render: {len(flat)}")
    return flat


# ── Article rendering ──────────────────────────────────────────────────────

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
    dest = tmp_dir / sanitize_filename(
        f"{seq:03d}_{item['date_str']}_{item['lang']}_{safe}.pdf"
    )

    try:
        page.goto(item["url"], wait_until="networkidle", timeout=NAV_TIMEOUT)
        page.wait_for_timeout(RENDER_WAIT)

        # Try embedded PDF first
        pdf_links: list[str] = page.evaluate(
            "() => Array.from(document.querySelectorAll('a[href$=\".pdf\"],a[href*=\"download\"]'))"
            ".map(a=>a.href)"
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

    items = collect_month_articles(year, month)
    if not items:
        log.warning(f"❌ No articles found for {year}-{month:02d}. Exiting.")
        return

    tmp_dir = Path(f"smg_tmp_{year}_{month:02d}")
    tmp_dir.mkdir(exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            accept_downloads=True,
        )
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
