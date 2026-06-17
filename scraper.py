from __future__ import annotations

import argparse
import logging
import re
import time
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

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

NAV_TIMEOUT   = 60_000   # ms
RENDER_WAIT   = 5_000    # ms
MAX_PAGES     = 50       # safety cap

# ── Date regex (fixed) ────────────────────────────────────────────────────
DATE_RE = re.compile(
    r"(20\d{2})"                   # year
    r"[\s\-\/年\.]+"
    r"(1[0-2]|0?[1-9])"           # month
    r"[\s\-\/月\.]+"
    r"([12]\d|3[01]|0?[1-9])"     # day
)

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


# ── Core: extract article links ────────────────────────────────────────────
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

# ── Pagination info extraction (IMPROVED) ────────────────────────────────
_PAGINATION_JS = """
() => {
    // Find all <a> tags that contain "page"
    const links = Array.from(document.querySelectorAll('a[href*="page"]'));
    let maxPage = 1;
    let template = null;
    const pageNumbers = [];

    links.forEach(a => {
        const href = a.getAttribute('href');
        // Try to extract page number from href
        let m = href.match(/\\/page\\/(\\d+)/);
        if (m) {
            const num = parseInt(m[1], 10);
            if (num > maxPage) maxPage = num;
            pageNumbers.push(num);
            // Build template by replacing the numeric part with {page}
            const candidate = href.replace(/\\/page\\/\\d+/, '/page/{page}');
            if (!template) template = candidate;
        }
        m = href.match(/[?&]page=(\\d+)/);
        if (m) {
            const num = parseInt(m[1], 10);
            if (num > maxPage) maxPage = num;
            pageNumbers.push(num);
            const candidate = href.replace(/[?&]page=\\d+/, '?page={page}');
            if (!template) template = candidate;
        }
    });

    // Also check text like "共 N 頁"
    const bodyText = document.body.innerText;
    const m2 = bodyText.match(/共\\s*(\\d+)\\s*頁/) || bodyText.match(/of\\s+(\\d+)\\s+pages?/i);
    if (m2) maxPage = Math.max(maxPage, parseInt(m2[1], 10));

    // If no template found, try to infer from current URL
    if (!template) {
        const currentUrl = window.location.href;
        let m = currentUrl.match(/\\/page\\/(\\d+)/);
        if (m) {
            template = currentUrl.replace(/\\/page\\/\\d+/, '/page/{page}');
        } else {
            m = currentUrl.match(/[?&]page=(\\d+)/);
            if (m) {
                template = currentUrl.replace(/[?&]page=\\d+/, '?page={page}');
            }
        }
    }

    // Ensure template is absolute (with origin)
    if (template && template.startsWith('/')) {
        template = window.location.origin + template;
    }

    return { maxPage, template };
}
"""


def navigate_and_wait(page: Page, url: str) -> bool:
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


def get_pagination_info(page: Page) -> tuple[int, Optional[str]]:
    """Return (max_page, template_absolute_url)"""
    try:
        result = page.evaluate(_PAGINATION_JS)
        return result.get("maxPage", 1), result.get("template")
    except Exception:
        return 1, None


def build_page_url(base_url: str, template: Optional[str], n: int) -> str:
    """Build absolute URL for page number n using template or fallback."""
    if template:
        url = template.replace("{page}", str(n))
        # If still relative (shouldn't happen after absolute conversion), make absolute
        if url.startswith('/'):
            parsed_base = urlparse(base_url)
            url = urlunparse((parsed_base.scheme, parsed_base.netloc, url, '', '', ''))
        return url
    else:
        # Fallback: try ?page=N
        parsed = urlparse(base_url)
        qs = parse_qs(parsed.query)
        qs["page"] = [str(n)]
        new_query = urlencode(qs, doseq=True)
        return urlunparse(parsed._replace(query=new_query))


# ── MODIFIED: collect_source with intelligent pagination ──────────────────
def collect_source(
    page: Page,
    src: dict,
    year: int,
    month: int,
) -> dict[str, dict]:
    all_items: dict[str, dict] = {}
    base_url = src["url"].rstrip("/")
    source_name = src["name"]

    # Load page 1
    log.info(f"  Loading page 1: {base_url}")
    if not navigate_and_wait(page, base_url):
        return {}

    # Get pagination info (max page and URL template)
    max_page, template = get_pagination_info(page)
    log.info(f"  Detected max page: {max_page}, template: {template}")

    page_num = 1
    empty_page_count = 0
    found_older = False

    while page_num <= MAX_PAGES:
        if page_num > 1:
            page_url = build_page_url(base_url, template, page_num)
            log.info(f"  Loading page {page_num}: {page_url}")
            if not navigate_and_wait(page, page_url):
                break

            # Verify URL changed (prevent infinite loop)
            if page.url == page_url or page.url == base_url:
                # If we're still on the same page, stop
                log.info(f"  Page {page_num} not reached (URL unchanged), stopping")
                break
        else:
            # Already on page 1
            pass

        articles = extract_page_articles(page)

        if not articles:
            empty_page_count += 1
            log.info(f"  Page {page_num}: no articles extracted (empty count {empty_page_count})")
            if empty_page_count >= 2 and page_num > 1:
                log.info("  Two consecutive empty pages → stopping")
                break
            page_num += 1
            continue
        else:
            empty_page_count = 0

        added = 0
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
            f"  Page {page_num}: {len(articles)} articles, "
            f"+{added} matched {year}-{month:02d}, "
            f"older_found={found_older}"
        )

        if found_older:
            break

        # Update pagination info dynamically (in case it changes)
        new_max, new_template = get_pagination_info(page)
        if new_max > max_page:
            max_page = new_max
            log.info(f"  Updated max_page to {max_page}")
        if new_template and new_template != template:
            template = new_template
            log.info(f"  Updated template to {template}")

        if page_num >= max_page:
            log.info(f"  Reached max_page {max_page}, stopping")
            break

        page_num += 1

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


# ── Entry point ────────────────────────────────────────────────────────────

def main(year: int, month: int) -> None:
    log.info(f"🚀 SMG Monthly Scraper — Target: {year}-{month:02d}")

    tmp_dir = Path(f"smg_tmp_{year}_{month:02d}")
    tmp_dir.mkdir(exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
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
