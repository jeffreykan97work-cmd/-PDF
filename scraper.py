from __future__ import annotations
import argparse
import logging
import re
import time
from datetime import date
from pathlib import Path
from typing import Optional

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

SOURCES = [
    {"name": "news",            "url": f"{BASE_URL}/zh/news"},
    {"name": "activity",        "url": f"{BASE_URL}/zh/activity"},
    {"name": "holiday_weather", "url": f"{BASE_URL}/zh/news/Holiday_weather"},
    {"name": "chat_info",       "url": f"{BASE_URL}/zh/chat-info"},
    {"name": "seasonal",        "url": f"{BASE_URL}/zh/seasonal"},
    {"name": "climate",         "url": f"{BASE_URL}/zh/climate"},
]

NAV_TIMEOUT   = 60_000   # ms — page navigation
RENDER_WAIT   = 5_000    # ms — after navigation, wait for Vue to render data
MAX_PAGES     = 50       # safety cap on pagination depth

# ── BUG-FIX 1: Date regex ──────────────────────────────────────────────────
# Original alternation (0?[1-9]|[12]\d|3[01]) matches only the first digit of
# two-digit values like "20" because "0?" matches empty and "[1-9]" matches "2".
# Fix: put two-digit alternatives FIRST so they are tried before single-digit.
DATE_RE = re.compile(
    r"(20\d{2})"                   # year
    r"[\s\-\/年\.]+"
    r"(1[0-2]|0?[1-9])"           # month — two-digit first
    r"[\s\-\/月\.]+"
    r"([12]\d|3[01]|0?[1-9])"     # day   — two-digit first
)

# JS version (injected into browser) — no Python escaping needed for /regex/
_JS_DATE_RE = r"(20\d{{2}})[\s\-/年.]+(1[0-2]|0?[1-9])[\s\-/月.]+([12]\d|3[01]|0?[1-9])"


def get_target_month() -> tuple[int, int]:
    today = date.today()
    return (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)


def sanitize_filename(name: str, max_len: int = 100) -> str:
    name = re.sub(r"\s+", " ", name).strip()
    return re.sub(r'[\\/*?:"<>|]', "", name)[:max_len] or "Untitled"


def parse_date_str(raw: str) -> Optional[str]:
    m = DATE_RE.search(raw)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"


# ── Core: extract article links from current page DOM ─────────────────────
_EXTRACT_JS = """
() => {
    const DATE_RE = /(20\\d{2})[\\s\\-\\/年.]+(1[0-2]|0?[1-9])[\\s\\-\\/月.]+([12]\\d|3[01]|0?[1-9])/;
    const found = [];
    const seen  = new Set();

    function abs(href) {
        if (!href) return null;
        if (href.startsWith('http')) return href;
        if (href.startsWith('/'))   return '""" + BASE_URL + """' + href;
        return '""" + BASE_URL + """/' + href;
    }

    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    let node;
    while (node = walker.nextNode()) {
        const text  = node.nodeValue.trim();
        const match = text.match(DATE_RE);
        if (!match) continue;

        // BUG-FIX: corrected group order — two-digit day captured properly
        const dateStr = match[1] + '-'
            + match[2].padStart(2, '0') + '-'
            + match[3].padStart(2, '0');

        let container = node.parentElement;
        let links = [];
        for (let i = 0; i < 8; i++) {
            if (!container || container.tagName === 'BODY') break;
            links = Array.from(container.querySelectorAll(
                'a[href]:not([href="#"]):not([href^="javascript"]), [data-url], [onclick]'
            ));
            if (links.length > 0 && links.length <= 20) break;
            container = container.parentElement;
        }

        links.forEach(el => {
            let href = el.getAttribute('href') || el.getAttribute('data-url');
            if (!href) {
                const oc = el.getAttribute('onclick') || '';
                const m2 = oc.match(/['"](\\/[^'"]+)['"]/);
                if (m2) href = m2[1];
            }
            // Skip pagination links themselves
            if (!href || /\\/page\\/\\d+/.test(href) || href.includes('?page=')) return;

            const url = abs(href);
            if (!url || seen.has(url)) return;
            seen.add(url);

            let title = (el.innerText || '').trim();
            if (title.length < 3 && container)
                title = (container.innerText || '').split('\\n')[0].trim();

            found.push({ url, date_str: dateStr, text: title.substring(0, 80) });
        });
    }

    // Holiday_weather: page itself is the article
    if (found.length === 0 && window.location.href.includes('Holiday_weather')) {
        const m = document.body.innerText.match(DATE_RE);
        if (m) found.push({
            url:      window.location.href,
            date_str: m[1] + '-' + m[2].padStart(2,'0') + '-' + m[3].padStart(2,'0'),
            text:     document.title,
        });
    }
    return found;
}
"""

# ── Pagination helpers ─────────────────────────────────────────────────────

# JS: return the max page number visible in the pagination bar.
# Tries multiple selector patterns to cover different CMS themes.
_MAX_PAGE_JS = """
() => {
    let max = 1;

    // Pattern A: numbered <a> or <button> inside a pagination container
    // Covers Bootstrap .pagination, custom .page-list, etc.
    const pgSelectors = [
        '.pagination a', '.pagination button', '.pagination li a',
        '.page-list a',  '.page-bar a',
        '[class*="pagin"] a', '[class*="pagin"] button',
        '[class*="page-num"]', '[class*="pageNum"]',
    ];
    pgSelectors.forEach(sel => {
        document.querySelectorAll(sel).forEach(el => {
            const n = parseInt((el.innerText || el.textContent || '').trim(), 10);
            if (!isNaN(n) && n > max) max = n;
        });
    });

    // Pattern B: text like "共 22 頁" / "Page 1 of 22"
    const bodyText = document.body.innerText;
    const m = bodyText.match(/共\\s*(\\d+)\\s*頁/) ||
              bodyText.match(/of\\s+(\\d+)\\s+page/i);
    if (m) max = Math.max(max, parseInt(m[1], 10));

    return max;
}
"""

# JS: click the pagination button whose visible text exactly matches `pageNum`.
# Returns true if the button was found and clicked, false otherwise.
_CLICK_PAGE_JS = """
(pageNum) => {
    const label = String(pageNum);
    const selectors = [
        '.pagination a', '.pagination button', '.pagination li a', '.pagination li button',
        '.page-list a',  '.page-bar a',
        '[class*="pagin"] a', '[class*="pagin"] button',
        '[class*="page-num"]', '[class*="pageNum"]',
    ];
    for (const sel of selectors) {
        for (const el of document.querySelectorAll(sel)) {
            if ((el.innerText || el.textContent || '').trim() === label) {
                el.click();
                return true;
            }
        }
    }
    return false;
}
"""

# JS: grab a stable fingerprint of the current article listing so we can
# detect when the Vue component has finished re-rendering after a page click.
_ARTICLE_FINGERPRINT_JS = """
() => {
    // Use the text of the first few visible article titles / dates as a hash.
    const texts = [];
    document.querySelectorAll('a[href], [class*="title"], [class*="date"]').forEach(el => {
        const t = (el.innerText || '').trim();
        if (t.length > 5) texts.push(t);
        if (texts.length >= 10) return;
    });
    return texts.join('|');
}
"""


def _wait_for_content_change(page: Page, old_fingerprint: str, timeout_ms: int = 10_000) -> bool:
    """
    Poll until the article listing DOM changes from old_fingerprint.
    Returns True when changed, False on timeout.
    """
    deadline = time.monotonic() + timeout_ms / 1000
    while time.monotonic() < deadline:
        try:
            new_fp = page.evaluate(_ARTICLE_FINGERPRINT_JS)
            if new_fp and new_fp != old_fingerprint:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def navigate_and_wait(page: Page, url: str) -> bool:
    """Navigate to url with networkidle wait so Vue renders its article list."""
    try:
        page.goto(url, wait_until="networkidle", timeout=NAV_TIMEOUT)
        page.wait_for_timeout(RENDER_WAIT)
        return True
    except Exception as e:
        log.warning(f"  Navigation failed ({url}): {e}")
        return False


def extract_page_articles(page: Page) -> list[dict]:
    try:
        return page.evaluate(_EXTRACT_JS) or []
    except Exception as e:
        log.warning(f"  DOM extraction failed: {e}")
        return []


def collect_source(
    page: Page,
    src: dict,
    year: int,
    month: int,
) -> dict[str, dict]:
    """
    Scan one source section across all its paginated listing pages.

    CONFIRMED behaviour (from screenshot): the SMG SPA changes article content
    when a page number button is clicked, but the browser URL never changes.
    URL-based navigation to /page/N therefore does NOT work.

    Fix: load page 1, detect the total page count from the rendered pagination
    bar, then click each numbered button in sequence and wait for the article
    list to re-render before extracting.
    """
    all_items: dict[str, dict] = {}
    source_name = src["name"]
    base_url = src["url"].rstrip("/")

    log.info(f"  Loading: {base_url}")
    if not navigate_and_wait(page, base_url):
        return {}

    # Detect total pages from the now-rendered pagination bar
    try:
        max_page = max(1, int(page.evaluate(_MAX_PAGE_JS)))
    except Exception:
        max_page = 1
    log.info(f"  Pagination: {max_page} page(s) detected")

    for page_num in range(1, min(max_page, MAX_PAGES) + 1):

        # ── Click the page-number button for pages 2+ ──────────────────────
        if page_num > 1:
            old_fp = page.evaluate(_ARTICLE_FINGERPRINT_JS)
            clicked = page.evaluate(_CLICK_PAGE_JS, page_num)

            if not clicked:
                log.warning(f"  Could not find page-{page_num} button — stopping")
                break

            # Wait for Vue to fetch and re-render the new article list
            changed = _wait_for_content_change(page, old_fp, timeout_ms=12_000)
            if not changed:
                log.warning(f"  Content did not change after clicking page {page_num} — stopping")
                break

            # Extra settle time for images / lazy elements
            page.wait_for_load_state("networkidle", timeout=15_000)

        # ── Extract articles from the current (rendered) listing ───────────
        articles = extract_page_articles(page)
        if not articles:
            log.info(f"  Page {page_num}: no articles found — stopping")
            break

        added       = 0
        found_older = False

        for item in articles:
            ds = item.get("date_str", "")
            if len(ds) < 10:
                continue
            try:
                ly, lm = int(ds[:4]), int(ds[5:7])
            except ValueError:
                continue

            if (ly, lm) < (year, month):
                found_older = True
            elif (ly, lm) == (year, month):
                url = item["url"]
                if url not in all_items:
                    all_items[url] = {**item, "source": source_name}
                    added += 1

        log.info(
            f"  Page {page_num}/{max_page}: {len(articles)} articles, "
            f"+{added} matched {year}-{month:02d}"
            + (" [older found → stop]" if found_older else "")
        )

        if found_older:
            break

    return all_items


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
    dest = tmp_dir / sanitize_filename(f"{seq:03d}_{item['date_str']}_{safe}.pdf")

    try:
        page.goto(item["url"], wait_until="networkidle", timeout=NAV_TIMEOUT)
        page.wait_for_timeout(2_000)

        # Try embedded PDF first
        pdf_links: list[str] = page.evaluate(
            "() => Array.from(document.querySelectorAll('a[href$=\".pdf\"],a[href*=\"download\"]'))"
            ".map(a=>a.href)"
        )
        if pdf_links and download_pdf_robust(pdf_links[0], dest, page):
            return dest

        # Full-page print-to-PDF
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_000)
        # BUG-FIX 5: remove only known chrome elements, not generic .navbar
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


# ── Entry point ────────────────────────────────────────────────────────────

def main(year: int, month: int) -> None:
    log.info(f"🚀 SMG Monthly Scraper — Target: {year}-{month:02d}")

    tmp_dir = Path(f"smg_tmp_{year}_{month:02d}")
    tmp_dir.mkdir(exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx  = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            accept_downloads=True,
        )
        page = ctx.new_page()

        all_items: dict[str, dict] = {}

        for src in SOURCES:
            log.info(f"\n📋 Source: {src['name']}")
            items = collect_source(page, src, year, month)
            before = len(all_items)
            all_items.update(items)
            log.info(f"  ✔ {src['name']}: {len(items)} found, "
                     f"{len(all_items)-before} new unique")

        if not all_items:
            log.warning(f"❌ No articles found for {year}-{month:02d}. Exiting.")
            browser.close()
            return

        sorted_items = sorted(all_items.values(), key=lambda x: x["date_str"])
        log.info(f"\n📦 Total unique articles to render: {len(sorted_items)}")

        writer = PdfWriter()
        for i, item in enumerate(sorted_items, 1):
            log.info(f"\n⚙  ({i}/{len(sorted_items)}) [{item['date_str']}] {item['text'][:50]}")
            pdf_path = process_article(page, item, tmp_dir, i)
            if pdf_path:
                try:
                    writer.append(str(pdf_path))
                except Exception as e:
                    log.warning(f"  Could not append {pdf_path.name}: {e}")

        output = Path(f"SMG_Monthly_Report_{year}_{month:02d}.pdf")
        with output.open("wb") as fh:
            writer.write(fh)

        mb = output.stat().st_size / 1_048_576
        log.info(f"\n✅ Done: {output.name}  ({mb:.2f} MB)")
        browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",  type=int, default=get_target_month()[0])
    parser.add_argument("--month", type=int, default=get_target_month()[1])
    args = parser.parse_args()
    main(args.year, args.month)
