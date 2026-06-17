from __future__ import annotations
import argparse
import json
import logging
import re
import shutil
from datetime import date
from pathlib import Path
from typing import Optional

from playwright.sync_api import Browser, Page, Response, sync_playwright
from pypdf import PdfReader, PdfWriter

# ── Logging Setup ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
BASE_URL = "https://www.smg.gov.mo"
CMS_API  = "http://cms.smg.gov.mo"          # internal CMS API base

SOURCES = [
    {"name": "news",            "url": f"{BASE_URL}/zh/news"},
    {"name": "activity",        "url": f"{BASE_URL}/zh/activity"},
    {"name": "holiday_weather", "url": f"{BASE_URL}/zh/news/Holiday_weather"},
    {"name": "chat_info",       "url": f"{BASE_URL}/zh/chat-info"},
    {"name": "seasonal",        "url": f"{BASE_URL}/zh/seasonal"},
    {"name": "climate",         "url": f"{BASE_URL}/zh/climate"},
]

REQUEST_TIMEOUT   = 90_000      # ms
SCROLL_PAUSE_MS   = 2_000       # ms between scrolls when loading more
MAX_SCROLL_ROUNDS = 60          # safety cap on infinite-scroll attempts

# ── BUG-FIX 1: Date regex — two-digit day/month alternatives reordered ────
# Original:  (0?[1-9]|[12]\d|3[01])  → "0?" matches empty → "2" from "20" wins
# Fixed:     ([12]\d|3[01]|0?[1-9])  → two-digit patterns tried first
DATE_RE_PY = re.compile(
    r"(20\d{2})"                      # year
    r"[\s\-\/年\.]+"
    r"(1[0-2]|0?[1-9])"              # month  (1[0-2] before 0?[1-9])
    r"[\s\-\/月\.]+"
    r"([12]\d|3[01]|0?[1-9])"        # day    (two-digit first)
)

# Same fix for the JS regex injected into the browser.
# NOTE: forward-slash does NOT need escaping inside JS /regex/ literals.
# We define it as a normal string so Python doesn't misinterpret \s etc.
# The string is spliced into a JS regex literal: /.../ via f-string.
_JS_DATE_RE = "(20[0-9]{2})[\\s\\-/年.]+(1[0-2]|0?[1-9])[\\s\\-/月.]+([12][0-9]|3[01]|0?[1-9])"


def get_target_month() -> tuple[int, int]:
    today = date.today()
    if today.month > 1:
        return today.year, today.month - 1
    return today.year - 1, 12


def sanitize_filename(name: str, max_len: int = 100) -> str:
    name = re.sub(r"\s+", " ", name).strip()
    return re.sub(r'[\\/*?:"<>|]', "", name)[:max_len] or "Untitled"


def parse_date_str(raw: str) -> Optional[str]:
    """Return 'YYYY-MM-DD' or None."""
    m = DATE_RE_PY.search(raw)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"


# ── BUG-FIX 2: Network interception replaces /page/N URL hacking ──────────
# The SMG site is a Vue.js SPA; appending /page/2 to the URL just returns the
# same page-1 HTML shell — pagination is driven by API calls.  We intercept
# those XHR/fetch responses to collect article metadata without needing to
# reverse-engineer the exact API path.

def _intercept_articles_from_api(
    responses: list[dict],
    year: int,
    month: int,
    source_name: str,
) -> tuple[dict[str, dict], bool]:
    """
    Parse intercepted API responses for article metadata.
    Returns (new_items_dict, found_older) so the caller can decide whether
    to stop scrolling.
    """
    new_items: dict[str, dict] = {}
    found_older = False

    for resp_data in responses:
        try:
            payload = resp_data if isinstance(resp_data, dict) else {}
            # Typical SMG CMS shapes: { data: [ {id, releaseDate, title, ...} ] }
            # or  { content: [...] }  or  { list: [...] }
            for key in ("data", "content", "list", "items", "posts", "results"):
                if key in payload and isinstance(payload[key], list):
                    for art in payload[key]:
                        url, date_str, title = _extract_article_meta(art)
                        if not url or not date_str:
                            continue
                        ly, lm = int(date_str[:4]), int(date_str[5:7])
                        if (ly, lm) < (year, month):
                            found_older = True
                        elif (ly, lm) == (year, month):
                            if url not in new_items:
                                new_items[url] = {
                                    "url": url,
                                    "date_str": date_str,
                                    "text": title,
                                    "source": source_name,
                                }
        except Exception:
            pass

    return new_items, found_older


def _extract_article_meta(art: dict) -> tuple[Optional[str], Optional[str], str]:
    """Try common CMS field names to get (url, date_str, title)."""
    # URL
    url = None
    for f in ("url", "link", "href", "detailUrl", "path"):
        if f in art and isinstance(art[f], str) and art[f]:
            raw = art[f]
            url = raw if raw.startswith("http") else BASE_URL + raw
            break
    # ID-based fallback
    if not url:
        for id_f in ("id", "postId", "articleId"):
            if id_f in art and art[id_f]:
                url = f"{BASE_URL}/zh/news-detail/{art[id_f]}"
                break

    # Date
    date_str = None
    for f in ("releaseDate", "publishDate", "date", "createAt", "updateAt", "pubDate"):
        if f in art and isinstance(art[f], str):
            date_str = parse_date_str(art[f])
            if date_str:
                break

    # Title
    title = ""
    for f in ("title", "name", "subject", "heading"):
        val = art.get(f)
        if isinstance(val, dict):                     # {zh_TW: "...", zh_CN: "..."}
            val = val.get("zh_TW") or val.get("zh") or next(iter(val.values()), "")
        if isinstance(val, str) and val.strip():
            title = val.strip()
            break

    return url, date_str, title


def collect_articles_for_source(
    page: Page,
    src: dict,
    year: int,
    month: int,
) -> dict[str, dict]:
    """
    Navigate to a source listing, intercept API responses while scrolling,
    and fall back to DOM scraping when no API traffic is found.
    """
    intercepted_raw: list[dict] = []

    def _on_response(resp: Response) -> None:
        try:
            ct = resp.headers.get("content-type", "")
            if "json" not in ct:
                return
            url = resp.url
            # Only care about CMS / news-related API calls
            if not any(kw in url for kw in ("api", "news", "post", "article", "list")):
                return
            body = resp.json()
            if isinstance(body, (dict, list)):
                intercepted_raw.append(body if isinstance(body, dict) else {"data": body})
        except Exception:
            pass

    page.on("response", _on_response)

    log.info(f"  Loading: {src['url']}")
    try:
        page.goto(src["url"], wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
        page.wait_for_timeout(3_000)
    except Exception as e:
        log.warning(f"  Failed initial load of {src['url']}: {e}")
        page.remove_listener("response", _on_response)
        return {}

    all_items: dict[str, dict] = {}

    # ── Strategy A: Scroll to trigger pagination API calls ─────────────────
    # BUG-FIX 2 core: instead of navigating to /page/N (which doesn't work on
    # the SPA), we scroll to the bottom repeatedly so the Vue component fires
    # its own "load more" requests, which we intercept.
    for scroll_round in range(MAX_SCROLL_ROUNDS):
        snap = len(intercepted_raw)

        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(SCROLL_PAUSE_MS)

        # Also try clicking any visible "下一頁 / 更多 / load-more" button
        for selector in (
            "button.load-more", "a.load-more", "[class*='more']",
            "button:has-text('更多')", "button:has-text('下一頁')",
            "a:has-text('更多')", "a:has-text('下一頁')",
        ):
            try:
                btn = page.query_selector(selector)
                if btn and btn.is_visible():
                    btn.click()
                    page.wait_for_timeout(SCROLL_PAUSE_MS)
                    break
            except Exception:
                pass

        # Parse whatever new JSON arrived
        new_batch = intercepted_raw[snap:]
        if new_batch:
            new_items, found_older = _intercept_articles_from_api(
                new_batch, year, month, src["name"]
            )
            all_items.update(new_items)
            log.info(
                f"  scroll {scroll_round+1}: +{len(new_items)} matched "
                f"({'older found – stop' if found_older else 'continue'})"
            )
            if found_older:
                break
        else:
            # No new JSON and no change in page height → end of content
            prev_height = page.evaluate("document.body.scrollHeight")
            page.wait_for_timeout(500)
            new_height = page.evaluate("document.body.scrollHeight")
            if new_height == prev_height:
                log.info(f"  No new content after scroll {scroll_round+1}. Done.")
                break

    # ── Strategy B: DOM fallback when no API traffic was intercepted ────────
    if not all_items:
        log.info(f"  No API traffic captured — falling back to DOM scraping")
        dom_items = _extract_from_dom(page, year, month, src["name"])
        all_items.update(dom_items)

    page.remove_listener("response", _on_response)
    return all_items


def _extract_from_dom(
    page: Page,
    year: int,
    month: int,
    source_name: str,
) -> dict[str, dict]:
    """
    Improved DOM-based link extraction.  Uses the fixed JS date regex and
    handles Vue router-link elements and data-url attributes.
    """
    results = page.evaluate(f"""() => {{
        const DATE_RE = /{_JS_DATE_RE}/;
        const found = [];
        const seen = new Set();

        // Helper: build absolute URL
        function abs(href) {{
            if (!href) return null;
            if (href.startsWith('http')) return href;
            if (href.startsWith('/')) return '{BASE_URL}' + href;
            return '{BASE_URL}/' + href;
        }}

        // Walk every text node looking for a date
        const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
        let node;
        while (node = walker.nextNode()) {{
            const text = node.nodeValue.trim();
            const match = text.match(DATE_RE);
            if (!match) continue;

            // FIX: use corrected group order for two-digit day
            const dateStr = match[1] + '-'
                + match[2].padStart(2, '0') + '-'
                + match[3].padStart(2, '0');

            // Walk up to 8 ancestors looking for links
            let container = node.parentElement;
            let links = [];
            for (let i = 0; i < 8; i++) {{
                if (!container || container.tagName === 'BODY') break;
                // Standard <a href>, Vue router-link, [data-url], [onclick]
                links = Array.from(container.querySelectorAll(
                    'a[href]:not([href="#"]):not([href^="javascript"]), ' +
                    'router-link[to], [data-url], [onclick]'
                ));
                if (links.length > 0 && links.length <= 20) break;
                container = container.parentElement;
            }}

            links.forEach(el => {{
                let href = el.getAttribute('href')
                    || el.getAttribute('to')
                    || el.getAttribute('data-url');
                if (!href) {{
                    const oc = el.getAttribute('onclick') || '';
                    const m2 = oc.match(/['"]([/][^'"]+)['"]/);
                    if (m2) href = m2[1];
                }}
                if (!href || href.includes('/page/') || href.includes('?page=')) return;

                const url = abs(href);
                if (!url || seen.has(url)) return;
                seen.add(url);

                // Title: prefer the container's first line of text
                let title = (el.innerText || '').trim();
                if (title.length < 3 && container) {{
                    title = (container.innerText || '').split('\\n')[0].trim();
                }}
                found.push({{ url, date_str: dateStr, text: title.substring(0, 80) }});
            }});
        }}

        // Holiday_weather special case: the page IS the article
        if (found.length === 0 && window.location.href.includes('Holiday_weather')) {{
            const bodyText = document.body.innerText;
            const m = bodyText.match(DATE_RE);
            if (m) {{
                found.push({{
                    url: window.location.href,
                    date_str: m[1] + '-' + m[2].padStart(2,'0') + '-' + m[3].padStart(2,'0'),
                    text: document.title,
                }});
            }}
        }}
        return found;
    }}""")

    items: dict[str, dict] = {}
    for item in results:
        ds = item.get("date_str", "")
        if len(ds) < 10:
            continue
        try:
            ly, lm = int(ds[:4]), int(ds[5:7])
        except ValueError:
            continue

        # BUG-FIX 3: Holiday_weather no longer bypasses the date filter.
        # They are included only if they fall in the target month.
        if (ly, lm) == (year, month):
            url = item["url"]
            if url not in items:
                items[url] = {**item, "source": source_name}

    return items


def download_pdf_robust(url: str, dest: Path, page: Page) -> bool:
    try:
        with page.context.expect_download(timeout=45_000) as dl_info:
            page.evaluate(f"window.open('{url}', '_blank')")
        dl_info.value.save_as(dest)
        return dest.exists() and dest.stat().st_size > 2_000
    except Exception as e:
        log.warning(f"  [PDF Download Failed] {url}: {e}")
        return False


def process_article(page: Page, item: dict, tmp_dir: Path, seq: int) -> Optional[Path]:
    safe_title = item["text"][:30].replace("/", "-")
    name = f"{seq:03d}_{item['date_str']}_{safe_title}.pdf"
    dest = tmp_dir / sanitize_filename(name)

    try:
        page.goto(item["url"], wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
        page.wait_for_timeout(2_000)

        # Try embedded PDF download link first
        pdf_links: list[str] = page.evaluate(
            "() => Array.from(document.querySelectorAll('a[href$=\".pdf\"], a[href*=\"download\"]'))"
            ".map(a => a.href)"
        )
        if pdf_links and download_pdf_robust(pdf_links[0], dest, page):
            return dest

        # Full-page screenshot → PDF
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_500)

        # BUG-FIX 5: Only remove known chrome elements, not all .navbar
        page.evaluate("""() => {
            ['header', 'nav', 'footer', '.site-header', '.breadcrumb',
             '#header', '#footer', '#nav', '.cookie-bar', '.back-to-top'
            ].forEach(s => document.querySelectorAll(s).forEach(el => el.remove()));
        }""")
        page.add_style_tag(content=(
            "@media print {"
            "  body { -webkit-print-color-adjust: exact !important;"
            "         print-color-adjust: exact !important; }"
            "}"
        ))
        page.pdf(path=str(dest), format="A4", print_background=True)

        if dest.exists() and dest.stat().st_size > 2_000:
            return dest
        log.warning(f"  Generated PDF too small, skipping: {dest.name}")
        return None

    except Exception as e:
        log.warning(f"  Failed processing {item['url']}: {e}")
        return None


def main(year: int, month: int) -> None:
    log.info(f"🚀 SMG Scraper — Target: {year}-{month:02d}")
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
            log.info(f"📋 Scanning source: {src['name']}")
            # BUG-FIX 2+4: Use scroll-interception instead of /page/N URL hacking.
            # Each source is fully scrolled; early-stop is handled inside
            # collect_articles_for_source via found_older flag, not at the per-link
            # level (which was too aggressive).
            src_items = collect_articles_for_source(page, src, year, month)
            before = len(all_items)
            all_items.update(src_items)
            log.info(f"  ✔ {src['name']}: {len(src_items)} articles "
                     f"(+{len(all_items) - before} new unique)")

        if not all_items:
            log.warning(f"❌ No matching articles found for {year}-{month:02d}.")
            browser.close()
            return

        sorted_items = sorted(all_items.values(), key=lambda x: x["date_str"])
        log.info(f"\n📦 Total unique articles: {len(sorted_items)}")

        writer = PdfWriter()
        for i, item in enumerate(sorted_items):
            log.info(
                f"\n⚙ ({i+1}/{len(sorted_items)}) "
                f"[{item['date_str']}] {item['text'][:50]}"
            )
            p = process_article(page, item, tmp_dir, i)
            if p:
                try:
                    writer.append(str(p))
                except Exception as e:
                    log.warning(f"  Could not append {p.name}: {e}")

        output_file = Path(f"SMG_Monthly_Report_{year}_{month:02d}.pdf")
        with output_file.open("wb") as f:
            writer.write(f)

        size_mb = output_file.stat().st_size / 1_048_576
        log.info(f"\n✅ Report generated: {output_file.name} ({size_mb:.2f} MB)")
        browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate monthly PDF report from SMG website"
    )
    parser.add_argument("--year",  type=int, default=get_target_month()[0])
    parser.add_argument("--month", type=int, default=get_target_month()[1])
    args = parser.parse_args()
    main(args.year, args.month)
