"""
SMG Monthly PDF Report Scraper
================================
Strategy:
  1. For each source URL, enumerate article/detail page links filtered by target month
  2. On each detail page, collect all PDF download links (zh / en / pt versions)
  3. Download those PDFs directly (binary fetch); fall back to print-to-PDF only when no PDF is attached
  4. Merge per-article PDFs in date order, then merge everything into one monthly report
  5. If the final report exceeds 5 MB, compress with Ghostscript (fallback: pypdf page re-write)
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import subprocess
import time
from datetime import date
from pathlib import Path
from typing import Optional

import requests
from playwright.sync_api import Browser, Page, sync_playwright, TimeoutError as PWTimeout
from pypdf import PdfReader, PdfWriter

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
BASE_URL = "https://www.smg.gov.mo"

# Source listing pages to scan for article links
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

# Language codes in priority order; PDFs will be merged in this order per article
LANG_CODES = ["zh", "en", "pt"]

# PDF selectors – try these in order to find downloadable PDFs on a detail page
PDF_LINK_SELECTORS = [
    "a[href$='.pdf']",
    "a[href*='.pdf?']",
    "a[href*='/pdf/']",
    "a[href*='download']",
    "a[href*='attach']",
    "a[href*='file']",
]

MAX_FINAL_BYTES   = 5 * 1024 * 1024   # 5 MB hard limit
MAX_SOURCE_PAGES  = 5                  # how many listing pages to scan per source
REQUEST_TIMEOUT   = 30_000             # ms for Playwright

# Selectors that identify article link elements on listing pages
ARTICLE_LINK_SELECTOR = (
    "a[href*='-detail'], "
    "a[href*='/detail/'], "
    "a[href*='chat-info/'], "
    "a[href*='/news/'], "
    "a[href*='/activity/'], "
    "a[href*='/subpage/']"
)

# Text found on error / "no content" pages – skip these
NO_CONTENT_MARKERS = [
    "no related content",
    "nenhum conteúdo relacionado",
    "nenhum conteudo relacionado",
    "404",
    "not found",
    "page not found",
]

PRINT_CSS = """
@media print {
    header, nav, footer, .navbar, .site-header,
    .breadcrumb, .footer, .sidebar, .related-links { display: none !important; }
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

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_target_month() -> tuple[int, int]:
    today = date.today()
    if today.month > 1:
        return today.year, today.month - 1
    return today.year - 1, 12


def sanitize_filename(name: str, max_len: int = 100) -> str:
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    return name[:max_len] or "Untitled"


def switch_lang(url: str, target_lang: str) -> str:
    """Swap language segment in a smg.gov.mo URL."""
    for lang in LANG_CODES:
        if f"/{lang}/" in url:
            return url.replace(f"/{lang}/", f"/{target_lang}/", 1)
    return url


def is_smg_pdf_url(href: str) -> bool:
    return bool(href) and (".pdf" in href.lower() or "/pdf/" in href.lower())


def resolve_url(href: str) -> str:
    if href.startswith("http"):
        return href
    return BASE_URL + href if href.startswith("/") else BASE_URL + "/" + href


# ── PDF Download ──────────────────────────────────────────────────────────────

def download_pdf(url: str, dest: Path, session: requests.Session) -> bool:
    """
    Download a PDF from *url* to *dest*.
    Returns True on success, False on failure.
    """
    try:
        r = session.get(url, timeout=30, stream=True)
        r.raise_for_status()
        ct = r.headers.get("content-type", "")
        if "pdf" not in ct and not url.lower().endswith(".pdf"):
            # Check first bytes for PDF magic number
            first = next(r.iter_content(8), b"")
            if not first.startswith(b"%PDF"):
                log.debug(f"    Not a PDF: {url}")
                return False
            dest.write_bytes(first + b"".join(r.iter_content(65536)))
            return dest.stat().st_size > 2000
        with dest.open("wb") as f:
            for chunk in r.iter_content(65536):
                f.write(chunk)
        return dest.stat().st_size > 2000
    except Exception as exc:
        log.debug(f"    Download failed ({url}): {exc}")
        if dest.exists():
            dest.unlink(missing_ok=True)
        return False


# ── Link Extraction ───────────────────────────────────────────────────────────

def extract_article_links(page: Page) -> list[dict]:
    """
    Return a list of dicts  { url, text, date_str }  for every article link
    visible on the current listing page.
    """
    results: list[dict] = []
    seen_urls: set[str] = set()

    # 1) Try structured list items first (preferred – carries date context)
    rows = page.query_selector_all(
        "li.news-item, li.list-item, li.item, "
        "tr.news-row, tr.item-row, "
        "div.news-item, div.list-item, div.item"
    )

    if not rows:
        # Fallback: grab every anchor that looks like an article and infer date from sibling text
        rows = page.query_selector_all("a[href*='-detail'], a[href*='/detail/'], a[href*='chat-info/']")

    for row in rows:
        try:
            # Find the anchor (row might itself be an anchor)
            anchor = row if row.tag_name() == "a" else row.query_selector("a[href]")
            if not anchor:
                continue
            href = anchor.get_attribute("href") or ""
            if not href or "page/" in href:
                continue

            full_url = resolve_url(href)
            if full_url in seen_urls:
                continue

            # Title
            title_el = row.query_selector(".title, .subject, h3, h4, h2, .news-title")
            title = (title_el.inner_text() if title_el else anchor.inner_text()).strip()
            title = title.split("\n")[0].strip()

            # Date – look inside the row/container
            container_text = row.inner_text()
            date_match = re.search(r"(\d{4})[-/\.](\d{1,2})[-/\.](\d{1,2})", container_text)
            if not date_match:
                # Try the sibling or parent
                parent = row.evaluate("el => el.parentElement?.innerText || ''")
                date_match = re.search(r"(\d{4})[-/\.](\d{1,2})[-/\.](\d{1,2})", parent)
            if not date_match:
                continue

            date_str = (
                f"{int(date_match.group(1)):04d}-"
                f"{int(date_match.group(2)):02d}-"
                f"{int(date_match.group(3)):02d}"
            )
            seen_urls.add(full_url)
            results.append({"url": full_url, "text": title, "date_str": date_str})

        except Exception as exc:
            log.debug(f"  Link extraction row error: {exc}")
            continue

    return results


# ── PDF Discovery on Detail Pages ─────────────────────────────────────────────

def find_pdf_links_on_page(page: Page) -> list[str]:
    """
    Scan the current detail page for downloadable PDF links.
    Returns a list of absolute URLs, deduplicated.
    """
    found: list[str] = []
    seen: set[str] = set()

    for selector in PDF_LINK_SELECTORS:
        try:
            anchors = page.query_selector_all(selector)
            for a in anchors:
                href = a.get_attribute("href") or ""
                if not href:
                    continue
                full = resolve_url(href)
                if full not in seen:
                    seen.add(full)
                    found.append(full)
        except Exception:
            continue

    # Also search via JavaScript for any PDF URLs buried in onclick / data-* attributes
    extra = page.evaluate("""
        () => {
            const urls = [];
            document.querySelectorAll('[onclick], [data-url], [data-href], [data-file]').forEach(el => {
                const raw = el.getAttribute('onclick') || el.getAttribute('data-url')
                           || el.getAttribute('data-href') || el.getAttribute('data-file') || '';
                const m = raw.match(/https?:\\/\\/[^'"\\s]+\\.pdf[^'"\\s]*/i);
                if (m) urls.push(m[0]);
            });
            return urls;
        }
    """)
    for u in (extra or []):
        if u not in seen:
            seen.add(u)
            found.append(u)

    return found


def classify_pdf_lang(url: str, link_text: str) -> Optional[str]:
    """
    Guess the language of a PDF link based on URL path or anchor text.
    Returns 'zh', 'en', 'pt', or None (unknown / keep all).
    """
    combined = (url + " " + link_text).lower()
    if any(k in combined for k in ["/zh/", "_zh", "-zh", "中文", "chinese"]):
        return "zh"
    if any(k in combined for k in ["/en/", "_en", "-en", "english", "eng"]):
        return "en"
    if any(k in combined for k in ["/pt/", "_pt.", "-pt.", "_pt-", "-pt-", "portugu"]):
        return "pt"
    return None  # unknown – include it


# ── Per-Article Processing ────────────────────────────────────────────────────

def process_article(
    page: Page,
    item: dict,
    tmp_dir: Path,
    out_dir: Path,
    seq: int,
    session: requests.Session,
) -> Optional[Path]:
    """
    For a single article:
      1. Visit the detail page (zh, en, pt variants)
      2. Collect all PDF download links found
      3. Download them; fall back to print-to-PDF if none found
      4. Merge language versions into one per-article PDF
    Returns the path to the merged article PDF, or None on total failure.
    """
    collected_pdfs: list[Path] = []   # (path, lang_order)
    final_title = item["text"] or "Untitled"

    # ── Phase 1: collect PDF download links across language variants ──────────
    lang_pdf_map: dict[str, list[str]] = {lang: [] for lang in LANG_CODES}

    for lang in LANG_CODES:
        lang_url = switch_lang(item["url"], lang)
        try:
            page.goto(lang_url, wait_until="networkidle", timeout=REQUEST_TIMEOUT)

            # Refresh title from the actual detail page if still vague
            if not final_title or len(final_title) < 3:
                h = page.evaluate(
                    "() => document.querySelector('h1, h2, .news-detail-title, .title')?.innerText || ''"
                )
                if h:
                    final_title = h.strip()

            body_lower = page.inner_text("body").lower()
            if any(m in body_lower for m in NO_CONTENT_MARKERS):
                log.debug(f"    [{lang}] No content at {lang_url}")
                continue

            pdf_urls = find_pdf_links_on_page(page)
            if pdf_urls:
                log.info(f"    [{lang}] Found {len(pdf_urls)} PDF link(s)")
                lang_pdf_map[lang].extend(pdf_urls)
            else:
                log.info(f"    [{lang}] No PDF links – will use print-to-PDF")

        except Exception as exc:
            log.warning(f"    [{lang}] Page load failed: {exc}")

    # Deduplicate across languages (same URL shouldn't be downloaded twice)
    all_pdf_urls: list[tuple[str, str]] = []  # (url, lang)
    seen_pdf_urls: set[str] = set()
    for lang in LANG_CODES:
        for url in lang_pdf_map[lang]:
            if url not in seen_pdf_urls:
                seen_pdf_urls.add(url)
                all_pdf_urls.append((url, lang))

    # ── Phase 2: download found PDFs ─────────────────────────────────────────
    for idx, (pdf_url, lang) in enumerate(all_pdf_urls):
        dest = tmp_dir / f"{seq:03d}_{lang}_{idx}.pdf"
        log.info(f"    Downloading [{lang}]: {pdf_url[:80]}")
        if download_pdf(pdf_url, dest, session):
            collected_pdfs.append((dest, LANG_CODES.index(lang) if lang in LANG_CODES else 99))
        else:
            log.warning(f"    Download failed: {pdf_url}")

    # ── Phase 3: fallback print-to-PDF if no PDFs downloaded ─────────────────
    if not collected_pdfs:
        log.info(f"    Falling back to print-to-PDF for '{final_title}'")
        for lang in LANG_CODES:
            lang_url = switch_lang(item["url"], lang)
            dest = tmp_dir / f"{seq:03d}_{lang}_print.pdf"
            try:
                page.goto(lang_url, wait_until="networkidle", timeout=REQUEST_TIMEOUT)
                body_lower = page.inner_text("body").lower()
                if any(m in body_lower for m in NO_CONTENT_MARKERS):
                    continue
                page.evaluate(WAIT_IMAGES_JS)
                page.evaluate("""() => {
                    ['header','nav','footer','.site-header','.breadcrumb','.navbar']
                        .forEach(s => document.querySelectorAll(s).forEach(el => el.remove()));
                }""")
                page.add_style_tag(content=PRINT_CSS)
                page.pdf(
                    path=str(dest),
                    format="A4",
                    print_background=True,
                    margin={"top": "1.5cm", "bottom": "1.5cm", "left": "1.5cm", "right": "1.5cm"},
                )
                if dest.exists() and dest.stat().st_size > 2000:
                    collected_pdfs.append((dest, LANG_CODES.index(lang)))
            except Exception as exc:
                log.warning(f"    [{lang}] print-to-PDF failed: {exc}")

    if not collected_pdfs:
        log.warning(f"  ✗ No PDF content for: {final_title}")
        return None

    # ── Phase 4: merge language PDFs into one article PDF ────────────────────
    # Sort by language order (zh first, then en, then pt)
    collected_pdfs.sort(key=lambda x: x[1])

    clean_title = sanitize_filename(final_title)
    article_pdf = out_dir / f"{seq:03d}_{item['date_str']}_{clean_title}.pdf"

    writer = PdfWriter()
    for pdf_path, _ in collected_pdfs:
        try:
            reader = PdfReader(str(pdf_path))
            for p in reader.pages:
                writer.add_page(p)
        except Exception as exc:
            log.warning(f"    Could not read {pdf_path.name}: {exc}")

    if len(writer.pages) == 0:
        log.warning(f"  ✗ All PDFs unreadable for: {final_title}")
        return None

    with article_pdf.open("wb") as f:
        writer.write(f)

    log.info(f"  ✓ Article PDF: {article_pdf.name} ({article_pdf.stat().st_size // 1024} KB, {len(writer.pages)} pages)")
    return article_pdf


# ── PDF Compression ───────────────────────────────────────────────────────────

def compress_with_ghostscript(src: Path, dst: Path) -> bool:
    """Try to compress *src* into *dst* using Ghostscript. Returns True on success."""
    if not shutil.which("gs"):
        return False
    try:
        subprocess.run(
            [
                "gs", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4",
                "-dPDFSETTINGS=/ebook", "-dNOPAUSE", "-dQUIET", "-dBATCH",
                f"-sOutputFile={dst}", str(src),
            ],
            check=True,
            capture_output=True,
        )
        return dst.exists() and dst.stat().st_size > 1000
    except Exception as exc:
        log.warning(f"  Ghostscript compression failed: {exc}")
        return False


def compress_with_pypdf(src: Path, dst: Path) -> bool:
    """
    Lightweight fallback compression using pypdf page re-writing
    (removes duplicate objects, compresses streams).
    """
    try:
        reader = PdfReader(str(src))
        writer = PdfWriter()
        for page in reader.pages:
            page.compress_content_streams()
            writer.add_page(page)
        writer.compress_identical_objects(remove_identicals=True, remove_orphans=True)
        with dst.open("wb") as f:
            writer.write(f)
        return dst.exists() and dst.stat().st_size > 1000
    except Exception as exc:
        log.warning(f"  pypdf compression failed: {exc}")
        return False


def ensure_size_limit(src: Path, final_path: Path) -> Path:
    """
    Copy or compress *src* to *final_path*, ensuring the result is under MAX_FINAL_BYTES.
    Returns the final path used.
    """
    src_mb = src.stat().st_size / 1024 / 1024
    if src.stat().st_size <= MAX_FINAL_BYTES:
        shutil.copy(src, final_path)
        log.info(f"  Size OK: {src_mb:.2f} MB – no compression needed")
        return final_path

    log.info(f"  Size {src_mb:.2f} MB > 5 MB – compressing…")

    # Try Ghostscript first (best quality/ratio)
    gs_out = final_path.with_suffix(".gs_tmp.pdf")
    if compress_with_ghostscript(src, gs_out):
        gs_mb = gs_out.stat().st_size / 1024 / 1024
        log.info(f"  Ghostscript: {src_mb:.2f} MB → {gs_mb:.2f} MB")
        if gs_out.stat().st_size <= MAX_FINAL_BYTES:
            shutil.move(str(gs_out), final_path)
            return final_path
        # Still too big? Try again at /screen quality
        gs_out2 = final_path.with_suffix(".gs_screen.pdf")
        subprocess.run(
            ["gs", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4",
             "-dPDFSETTINGS=/screen", "-dNOPAUSE", "-dQUIET", "-dBATCH",
             f"-sOutputFile={gs_out2}", str(src)],
            check=True, capture_output=True,
        )
        if gs_out2.exists() and gs_out2.stat().st_size < gs_out.stat().st_size:
            shutil.move(str(gs_out2), final_path)
        else:
            shutil.move(str(gs_out), final_path)
        for f in [gs_out, gs_out2]:
            f.unlink(missing_ok=True)
        final_mb = final_path.stat().st_size / 1024 / 1024
        log.info(f"  Final size after GS: {final_mb:.2f} MB")
        return final_path

    # Fallback: pypdf
    py_out = final_path.with_suffix(".py_tmp.pdf")
    if compress_with_pypdf(src, py_out):
        py_mb = py_out.stat().st_size / 1024 / 1024
        log.info(f"  pypdf: {src_mb:.2f} MB → {py_mb:.2f} MB")
        shutil.move(str(py_out), final_path)
        return final_path

    # No compression worked – just copy as-is and warn
    log.warning(f"  ⚠ Could not compress below 5 MB. Copying as-is ({src_mb:.2f} MB).")
    shutil.copy(src, final_path)
    return final_path


# ── Main ──────────────────────────────────────────────────────────────────────

def main(year: Optional[int] = None, month: Optional[int] = None) -> None:
    if not year or not month:
        year, month = get_target_month()

    log.info(f"🚀 SMG Monthly Report Scraper – target: {year}-{month:02d}")

    tmp_dir = Path(f"smg_tmp_{year}_{month:02d}")
    out_dir = Path(f"smg_tmp_{year}_{month:02d}")  # individual PDFs go here too
    tmp_dir.mkdir(exist_ok=True)

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    })

    with sync_playwright() as pw:
        browser: Browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 1600},
            user_agent=session.headers["User-Agent"],
        )
        page: Page = ctx.new_page()

        # ── Step 1: Scan all source pages and collect article links ───────────
        all_items: dict[str, dict] = {}  # url → item (deduplication)

        for src in SOURCES:
            log.info(f"\n📋 Scanning source: {src['name']}  ({src['url']})")
            month_exhausted = False

            for page_num in range(1, MAX_SOURCE_PAGES + 1):
                if month_exhausted:
                    break

                page_url = src["url"] if page_num == 1 else f"{src['url'].rstrip('/')}/page/{page_num}"
                try:
                    page.goto(page_url, wait_until="networkidle", timeout=REQUEST_TIMEOUT)
                except PWTimeout:
                    log.warning(f"  Timeout loading page {page_num} of {src['name']}")
                    break
                except Exception as exc:
                    log.warning(f"  Error loading {page_url}: {exc}")
                    break

                found = extract_article_links(page)
                if not found:
                    log.info(f"  No links found on page {page_num} – stopping pagination")
                    break

                added_this_page = 0
                for item in found:
                    try:
                        ly, lm = int(item["date_str"][:4]), int(item["date_str"][5:7])
                    except Exception:
                        continue

                    if ly == year and lm == month:
                        if item["url"] not in all_items:
                            all_items[item["url"]] = item
                            added_this_page += 1
                    elif (ly < year) or (ly == year and lm < month):
                        # Older than target month – if MOST items are older, stop
                        pass  # Don't break early; page may be mixed

                log.info(f"  Page {page_num}: {len(found)} links found, {added_this_page} added for {year}-{month:02d}")

                # Stop paginating if nothing on this page was in our target month
                # AND the oldest item is clearly before target month
                oldest = min(found, key=lambda x: x["date_str"])
                oy, om = int(oldest["date_str"][:4]), int(oldest["date_str"][5:7])
                if (oy < year) or (oy == year and om < month):
                    month_exhausted = True

        sorted_items = sorted(all_items.values(), key=lambda x: x["date_str"])
        log.info(f"\n📊 Total unique articles for {year}-{month:02d}: {len(sorted_items)}")

        if not sorted_items:
            log.warning("❌ No articles found. Exiting.")
            browser.close()
            return

        # ── Step 2: Process each article ─────────────────────────────────────
        article_pdfs: list[Path] = []

        for idx, item in enumerate(sorted_items, start=1):
            log.info(f"\n({'':>3}{idx}/{len(sorted_items)}) {item['date_str']} – {item['text'][:60]}")
            pdf = process_article(page, item, tmp_dir, out_dir, idx, session)
            if pdf:
                article_pdfs.append(pdf)

        browser.close()

    # ── Step 3: Merge all articles into monthly report ────────────────────────
    if not article_pdfs:
        log.warning("❌ No article PDFs generated.")
        return

    log.info(f"\n📎 Merging {len(article_pdfs)} article PDFs…")
    raw_merged = tmp_dir / "raw_total_merged.pdf"
    writer = PdfWriter()
    for f in article_pdfs:
        try:
            writer.append(str(f))
        except Exception as exc:
            log.warning(f"  Could not append {f.name}: {exc}")

    with raw_merged.open("wb") as f:
        writer.write(f)

    raw_mb = raw_merged.stat().st_size / 1024 / 1024
    log.info(f"  Raw merged: {raw_mb:.2f} MB, {len(writer.pages)} pages")

    # ── Step 4: Compress if needed ────────────────────────────────────────────
    final_filename = f"SMG_Monthly_Report_{year}_{month:02d}.pdf"
    final_path = ensure_size_limit(raw_merged, Path(final_filename))

    final_mb = final_path.stat().st_size / 1024 / 1024
    log.info(
        f"\n✅ Done!  →  {final_filename}\n"
        f"   Articles: {len(article_pdfs)}   Pages: {len(PdfReader(str(final_path)).pages)}   Size: {final_mb:.2f} MB"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SMG Monthly PDF Report Scraper")
    parser.add_argument("--year",  type=int, default=None, help="Target year  (default: last month)")
    parser.add_argument("--month", type=int, default=None, help="Target month (default: last month)")
    args = parser.parse_args()
    main(args.year, args.month)
