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

LANG_CODES    = ["zh", "en", "pt"]
MAX_FINAL_BYTES  = 5 * 1024 * 1024
MAX_SOURCE_PAGES = 5
REQUEST_TIMEOUT  = 30_000  # ms

# ── These selectors WORK on SMG (proven by old scraper runs #22-#28) ─────────
ARTICLE_LINK_SELECTOR = "a[href*='-detail'], a[href*='chat-info/']"

PDF_LINK_SELECTORS = [
    "a[href$='.pdf']",
    "a[href*='.pdf?']",
    "a[href*='/pdf/']",
    "a[href*='download']",
    "a[href*='attach']",
    "a[href*='file']",
]

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
    for lang in LANG_CODES:
        if f"/{lang}/" in url:
            return url.replace(f"/{lang}/", f"/{target_lang}/", 1)
    return url


def resolve_url(href: str) -> str:
    if href.startswith("http"):
        return href
    return BASE_URL + href if href.startswith("/") else BASE_URL + "/" + href


def classify_pdf_lang(url: str, link_text: str) -> Optional[str]:
    combined = (url + " " + link_text).lower()
    if any(k in combined for k in ["/zh/", "_zh.", "-zh.", "_zh-", "-zh-", "中文", "chinese"]):
        return "zh"
    if any(k in combined for k in ["/en/", "_en.", "-en.", "_en-", "-en-", "english", "eng"]):
        return "en"
    if any(k in combined for k in ["/pt/", "_pt.", "-pt.", "_pt-", "-pt-", "portugu"]):
        return "pt"
    return None


# ── PDF Download ──────────────────────────────────────────────────────────────

def download_pdf(url: str, dest: Path, session: requests.Session) -> bool:
    try:
        r = session.get(url, timeout=30, stream=True)
        r.raise_for_status()
        chunks = []
        first = True
        for chunk in r.iter_content(65536):
            if first:
                if not chunk.startswith(b"%PDF"):
                    ct = r.headers.get("content-type", "")
                    if "pdf" not in ct and not url.lower().endswith(".pdf"):
                        return False
                first = False
            chunks.append(chunk)
        dest.write_bytes(b"".join(chunks))
        return dest.stat().st_size > 2000
    except Exception as exc:
        log.debug(f"    Download failed ({url}): {exc}")
        dest.unlink(missing_ok=True)
        return False


# ── Link Extraction (uses proven SMG selectors) ───────────────────────────────

def extract_article_links(page: Page) -> list[dict]:
    """
    Extract article links from the current listing page.
    Uses the selector pattern proven to work on SMG: a[href*='-detail'], a[href*='chat-info/']
    Date is found by inspecting the closest container element's text.
    """
    results: list[dict] = []
    seen_urls: set[str] = set()

    elements = page.query_selector_all(ARTICLE_LINK_SELECTOR)
    log.debug(f"    Raw anchor matches: {len(elements)}")

    for el in elements:
        try:
            href = el.get_attribute("href") or ""
            if not href or "page/" in href:
                continue

            full_url = resolve_url(href)
            if full_url in seen_urls:
                continue

            # Title: try child elements first, then the anchor's own text
            title = el.evaluate(
                "node => node.querySelector('.title, .subject, h3, h4, h2')?.innerText "
                "|| node.innerText"
            )
            title = (title or "").split("\n")[0].strip()

            # Date: look in the closest list/row/div container
            container = el.evaluate(
                "el => el.closest('li, tr, div.item, .list-item, .news-item, div')?.innerText || ''"
            )
            date_match = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", container)
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
            log.debug(f"  Link extraction error: {exc}")
            continue

    return results


# ── PDF Discovery on Detail Pages ─────────────────────────────────────────────

def find_pdf_links_on_page(page: Page) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()

    for selector in PDF_LINK_SELECTORS:
        try:
            for a in page.query_selector_all(selector):
                href = a.get_attribute("href") or ""
                if not href:
                    continue
                full = resolve_url(href)
                if full not in seen:
                    seen.add(full)
                    found.append(full)
        except Exception:
            continue

    # Also scan onclick / data-* attributes
    extra = page.evaluate("""
        () => {
            const urls = [];
            document.querySelectorAll('[onclick],[data-url],[data-href],[data-file]').forEach(el => {
                const raw = el.getAttribute('onclick') || el.getAttribute('data-url')
                           || el.getAttribute('data-href') || el.getAttribute('data-file') || '';
                const m = raw.match(/https?:\\/\\/[^'"\\s]+\\.pdf[^'"\\s]*/i);
                if (m) urls.push(m[0]);
            });
            return urls;
        }
    """) or []
    for u in extra:
        if u not in seen:
            seen.add(u)
            found.append(u)

    return found


# ── Per-Article Processing ────────────────────────────────────────────────────

def process_article(
    page: Page,
    item: dict,
    tmp_dir: Path,
    out_dir: Path,
    seq: int,
    session: requests.Session,
) -> Optional[Path]:
    collected: list[tuple[Path, int]] = []  # (path, lang_order)
    final_title = item["text"] or "Untitled"

    # Phase 1: find PDF download links across all language versions
    lang_pdf_map: dict[str, list[str]] = {lang: [] for lang in LANG_CODES}

    for lang in LANG_CODES:
        lang_url = switch_lang(item["url"], lang)
        try:
            page.goto(lang_url, wait_until="networkidle", timeout=REQUEST_TIMEOUT)

            if not final_title or len(final_title) < 3:
                h = page.evaluate(
                    "() => document.querySelector('h1, h2, .news-detail-title, .title')?.innerText || ''"
                )
                if h:
                    final_title = h.strip()

            body_lower = page.inner_text("body").lower()
            if any(m in body_lower for m in NO_CONTENT_MARKERS):
                continue

            pdf_urls = find_pdf_links_on_page(page)
            if pdf_urls:
                log.info(f"    [{lang}] Found {len(pdf_urls)} PDF link(s)")
                lang_pdf_map[lang].extend(pdf_urls)
            else:
                log.info(f"    [{lang}] No PDF links — will use print-to-PDF")

        except Exception as exc:
            log.warning(f"    [{lang}] Page load failed: {exc}")

    # Phase 2: download found PDFs
    seen_pdf_urls: set[str] = set()
    for lang in LANG_CODES:
        for url in lang_pdf_map[lang]:
            if url not in seen_pdf_urls:
                seen_pdf_urls.add(url)
                dest = tmp_dir / f"{seq:03d}_{lang}_{len(collected)}.pdf"
                log.info(f"    Downloading [{lang}]: {url[:80]}")
                if download_pdf(url, dest, session):
                    collected.append((dest, LANG_CODES.index(lang)))
                else:
                    log.warning(f"    Download failed: {url}")

    # Phase 3: fallback — print-to-PDF for all languages (original working approach)
    if not collected:
        log.info(f"    No PDFs found — falling back to print-to-PDF")
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
                    collected.append((dest, LANG_CODES.index(lang)))
            except Exception as exc:
                log.warning(f"    [{lang}] print-to-PDF failed: {exc}")

    if not collected:
        log.warning(f"  ✗ No content for: {final_title}")
        return None

    # Phase 4: merge language PDFs → single article PDF
    collected.sort(key=lambda x: x[1])
    clean_title  = sanitize_filename(final_title)
    article_pdf  = out_dir / f"{seq:03d}_{item['date_str']}_{clean_title}.pdf"

    writer = PdfWriter()
    for pdf_path, _ in collected:
        try:
            for p in PdfReader(str(pdf_path)).pages:
                writer.add_page(p)
        except Exception as exc:
            log.warning(f"    Unreadable: {pdf_path.name}: {exc}")

    if not writer.pages:
        return None

    with article_pdf.open("wb") as f:
        writer.write(f)

    log.info(f"  ✓ {article_pdf.name} ({article_pdf.stat().st_size//1024} KB, {len(writer.pages)} pages)")
    return article_pdf


# ── PDF Compression ───────────────────────────────────────────────────────────

def compress_with_ghostscript(src: Path, dst: Path, quality: str = "/ebook") -> bool:
    if not shutil.which("gs"):
        return False
    try:
        subprocess.run(
            ["gs", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4",
             f"-dPDFSETTINGS={quality}", "-dNOPAUSE", "-dQUIET", "-dBATCH",
             f"-sOutputFile={dst}", str(src)],
            check=True, capture_output=True,
        )
        return dst.exists() and dst.stat().st_size > 1000
    except Exception as exc:
        log.warning(f"  Ghostscript failed: {exc}")
        return False


def compress_with_pypdf(src: Path, dst: Path) -> bool:
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
    src_mb = src.stat().st_size / 1024 / 1024
    if src.stat().st_size <= MAX_FINAL_BYTES:
        shutil.copy(src, final_path)
        log.info(f"  Size OK: {src_mb:.2f} MB")
        return final_path

    log.info(f"  {src_mb:.2f} MB > 5 MB — compressing…")
    tmp_gs = final_path.with_suffix(".gs.pdf")
    if compress_with_ghostscript(src, tmp_gs, "/ebook"):
        if tmp_gs.stat().st_size <= MAX_FINAL_BYTES:
            shutil.move(str(tmp_gs), final_path)
            log.info(f"  GS /ebook → {final_path.stat().st_size/1024/1024:.2f} MB")
            return final_path
        # Still too big — try /screen
        tmp_gs2 = final_path.with_suffix(".gs2.pdf")
        compress_with_ghostscript(src, tmp_gs2, "/screen")
        best = min([tmp_gs, tmp_gs2], key=lambda f: f.stat().st_size if f.exists() else 999999999)
        shutil.move(str(best), final_path)
        for f in [tmp_gs, tmp_gs2]:
            f.unlink(missing_ok=True)
        log.info(f"  GS /screen → {final_path.stat().st_size/1024/1024:.2f} MB")
        return final_path

    tmp_py = final_path.with_suffix(".py.pdf")
    if compress_with_pypdf(src, tmp_py):
        shutil.move(str(tmp_py), final_path)
        log.info(f"  pypdf → {final_path.stat().st_size/1024/1024:.2f} MB")
        return final_path

    log.warning(f"  ⚠ Cannot compress below 5 MB — copying as-is ({src_mb:.2f} MB)")
    shutil.copy(src, final_path)
    return final_path


# ── Main ──────────────────────────────────────────────────────────────────────

def main(year: Optional[int] = None, month: Optional[int] = None) -> None:
    if not year or not month:
        year, month = get_target_month()

    log.info(f"🚀 SMG Monthly Report — target: {year}-{month:02d}")

    tmp_dir = Path(f"smg_tmp_{year}_{month:02d}")
    tmp_dir.mkdir(exist_ok=True)

    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    with sync_playwright() as pw:
        browser: Browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 1600},
            user_agent=session.headers["User-Agent"],
        )
        page: Page = ctx.new_page()

        # ── Step 1: collect article links ─────────────────────────────────────
        all_items: dict[str, dict] = {}

        for src in SOURCES:
            log.info(f"\n📋 Scanning: {src['name']}  ({src['url']})")

            for page_num in range(1, MAX_SOURCE_PAGES + 1):
                page_url = (
                    src["url"]
                    if page_num == 1
                    else f"{src['url'].rstrip('/')}/page/{page_num}"
                )
                try:
                    page.goto(page_url, wait_until="networkidle", timeout=REQUEST_TIMEOUT)
                except Exception as exc:
                    log.warning(f"  Could not load page {page_num}: {exc}")
                    break

                found = extract_article_links(page)
                if not found:
                    log.info(f"  Page {page_num}: no links — stopping")
                    break

                added = 0
                oldest_in_page_ym = (9999, 12)
                for item in found:
                    try:
                        ly = int(item["date_str"][:4])
                        lm = int(item["date_str"][5:7])
                    except Exception:
                        continue
                    if ly == year and lm == month:
                        if item["url"] not in all_items:
                            all_items[item["url"]] = item
                            added += 1
                    if (ly, lm) < oldest_in_page_ym:
                        oldest_in_page_ym = (ly, lm)

                log.info(f"  Page {page_num}: {len(found)} links, {added} matched {year}-{month:02d}")

                # Stop paginating once oldest item is clearly before target month
                if oldest_in_page_ym < (year, month):
                    break

        sorted_items = sorted(all_items.values(), key=lambda x: x["date_str"])
        log.info(f"\n📊 Found {len(sorted_items)} articles for {year}-{month:02d}")

        if not sorted_items:
            log.warning("❌ No articles found.")
            browser.close()
            return

        # ── Step 2: process each article ──────────────────────────────────────
        article_pdfs: list[Path] = []
        for idx, item in enumerate(sorted_items, 1):
            log.info(f"\n({idx}/{len(sorted_items)}) {item['date_str']} — {item['text'][:60]}")
            pdf = process_article(page, item, tmp_dir, tmp_dir, idx, session)
            if pdf:
                article_pdfs.append(pdf)

        browser.close()

    # ── Step 3: merge into monthly report ─────────────────────────────────────
    if not article_pdfs:
        log.warning("❌ No article PDFs generated.")
        return

    log.info(f"\n📎 Merging {len(article_pdfs)} PDFs…")
    raw = tmp_dir / "raw_merged.pdf"
    writer = PdfWriter()
    for f in article_pdfs:
        try:
            writer.append(str(f))
        except Exception as exc:
            log.warning(f"  Cannot append {f.name}: {exc}")

    with raw.open("wb") as f:
        writer.write(f)

    log.info(f"  Raw: {raw.stat().st_size/1024/1024:.2f} MB, {len(writer.pages)} pages")

    final = ensure_size_limit(raw, Path(f"SMG_Monthly_Report_{year}_{month:02d}.pdf"))
    log.info(
        f"\n✅ Done → {final.name}  "
        f"({final.stat().st_size/1024/1024:.2f} MB, "
        f"{len(PdfReader(str(final)).pages)} pages)"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",  type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()
    main(args.year, args.month)
