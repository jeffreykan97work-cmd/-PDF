from __future__ import annotations

import argparse
import logging
import re
import time
import hashlib
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from playwright.sync_api import Page, sync_playwright
from pypdf import PdfReader, PdfWriter

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
BASE_URL = "https://www.smg.gov.mo"
MAX_PDF_SIZE = 5 * 1024 * 1024  # 5 MB

# 只保留新聞和活動，排除可能包含月報嘅來源
SOURCES = [
    {"name": "news",            "url": f"{BASE_URL}/zh/news"},
    {"name": "activity",        "url": f"{BASE_URL}/zh/activity"},
    # {"name": "holiday_weather", "url": f"{BASE_URL}/zh/news/Holiday_weather"},
    # {"name": "chat_info",       "url": f"{BASE_URL}/zh/chat-info"},
    # {"name": "seasonal",        "url": f"{BASE_URL}/zh/seasonal"},
    # {"name": "climate",         "url": f"{BASE_URL}/zh/climate"},
]

NAV_TIMEOUT   = 60_000   # ms
RENDER_WAIT   = 5_000    # ms
MAX_PAGES     = 50       # safety cap

# ── Date & time regex ─────────────────────────────────────────────────────
DATE_RE = re.compile(
    r"(20\d{2})"                   # year
    r"[\s\-\/年\.]+"
    r"(1[0-2]|0?[1-9])"           # month
    r"[\s\-\/月\.]+"
    r"([12]\d|3[01]|0?[1-9])"     # day
)
TIME_RE = re.compile(r"([01]?\d|2[0-3]):([0-5]\d)")  # HH:MM


def get_target_month() -> tuple[int, int]:
    today = date.today()
    return (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)


def sanitize_filename(name: str, max_len: int = 100) -> str:
    name = re.sub(r"\s+", " ", name).strip()
    return re.sub(r'[\\/*?:"<>|]', "", name)[:max_len] or "Untitled"


def parse_datetime(raw: str) -> Optional[str]:
    date_match = DATE_RE.search(raw)
    if not date_match:
        return None
    y, m, d = date_match.group(1), date_match.group(2).zfill(2), date_match.group(3).zfill(2)
    time_match = TIME_RE.search(raw)
    if time_match:
        h, min = time_match.group(1).zfill(2), time_match.group(2).zfill(2)
        return f"{y}-{m}-{d}T{h}:{min}"
    else:
        return f"{y}-{m}-{d}T00:00"


# ── Core: extract article links with date & time ─────────────────────────
_EXTRACT_JS = """
() => {
    const DATE_RE = /(20\\d{2})[\\s\\-\\/年.]+(1[0-2]|0?[1-9])[\\s\\-\\/月.]+([12]\\d|3[01]|0?[1-9])/;
    const TIME_RE = /([01]?\\d|2[0-3]):([0-5]\\d)/;
    const found = [];
    const seen  = new Set();

    function abs(href) {
        if (!href) return null;
        if (href.startsWith('http')) return href;
        if (href.startsWith('/'))   return '""" + BASE_URL + """' + href;
        return '""" + BASE_URL + """/' + href;
    }

    function extractDateTime(text) {
        const dateMatch = text.match(DATE_RE);
        if (!dateMatch) return null;
        const y = dateMatch[1];
        const m = dateMatch[2].padStart(2, '0');
        const d = dateMatch[3].padStart(2, '0');
        const timeMatch = text.match(TIME_RE);
        let dt = y + '-' + m + '-' + d + 'T';
        if (timeMatch) {
            dt += timeMatch[1].padStart(2, '0') + ':' + timeMatch[2].padStart(2, '0');
        } else {
            dt += '00:00';
        }
        return dt;
    }

    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    let node;
    while (node = walker.nextNode()) {
        const text  = node.nodeValue.trim();
        const dateMatch = text.match(DATE_RE);
        if (!dateMatch) continue;

        let container = node.parentElement;
        let fullText = container ? container.innerText : text;
        const fullDate = extractDateTime(fullText) || extractDateTime(text);
        if (!fullDate) continue;

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

            found.push({ url, full_date: fullDate, text: title.substring(0, 80) });
        });
    }

    if (found.length === 0 && window.location.href.includes('Holiday_weather')) {
        const fullText = document.body.innerText;
        const fullDate = extractDateTime(fullText);
        if (fullDate) {
            found.push({
                url:      window.location.href,
                full_date: fullDate,
                text:     document.title,
            });
        }
    }
    return found;
}
"""

# ── Pagination info extraction ────────────────────────────────────────────
_PAGINATION_JS = """
() => {
    const bodyText = document.body.innerText;
    const m = bodyText.match(/共\\s*(\\d+)\\s*頁/) || bodyText.match(/of\\s+(\\d+)\\s+pages?/i);
    let maxPage = 1;
    if (m) maxPage = parseInt(m[1], 10);
    const links = Array.from(document.querySelectorAll('a[href*="page"]'));
    links.forEach(a => {
        const href = a.getAttribute('href');
        let m2 = href.match(/\\/page\\/(\\d+)/) || href.match(/[?&]page=(\\d+)/);
        if (m2) {
            const num = parseInt(m2[1], 10);
            if (num > maxPage) maxPage = num;
        }
    });
    return maxPage;
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


def get_max_page(page: Page) -> int:
    try:
        return max(1, int(page.evaluate(_PAGINATION_JS)))
    except Exception:
        return 1


def get_current_page(page: Page) -> int:
    try:
        selectors = [
            ".pagination .active", ".pagination .current",
            ".pager-current", ".active a", "li.active a", "span.current"
        ]
        for sel in selectors:
            elem = page.locator(sel)
            if elem.count():
                text = elem.inner_text().strip()
                if text.isdigit():
                    return int(text)
        return 1
    except Exception:
        return 1


def click_next_page(page: Page) -> bool:
    next_selectors = [
        "a:has-text('下一頁')", "a:has-text('下一页')",
        "a:has-text('Next')", "a:has-text('>')",
        "a.next", "li.next a", ".pagination .next a",
        ".pager-next a", "a[rel='next']"
    ]
    for sel in next_selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.is_visible() and loc.is_enabled():
                loc.click()
                return True
        except Exception:
            continue

    current = get_current_page(page)
    target = current + 1
    try:
        links = page.locator("a").all()
        for link in links:
            text = link.inner_text().strip()
            if text == str(target) or (text.isdigit() and int(text) == target):
                if link.is_visible() and link.is_enabled():
                    link.click()
                    return True
    except Exception:
        pass

    try:
        pagination = page.locator(".pagination, .pager, .page-nav, [class*='pagination'], [class*='pager']")
        if pagination.count():
            numbers = pagination.locator("a, span").all()
            min_num = current
            target_el = None
            for el in numbers:
                txt = el.inner_text().strip()
                if txt.isdigit():
                    num = int(txt)
                    if num > current and (target_el is None or num < min_num):
                        min_num = num
                        target_el = el
            if target_el and target_el.is_visible() and target_el.is_enabled():
                target_el.click()
                return True
    except Exception:
        pass
    return False


def get_articles_hash(page: Page) -> str:
    try:
        text = page.evaluate("""
            () => {
                const items = Array.from(document.querySelectorAll('.item, .news-item, .article, a'));
                return items.map(el => el.innerText).join('|');
            }
        """)
        return hashlib.md5(text.encode()).hexdigest()
    except Exception:
        return ""


# ── Collect source ──────────────────────────────────────────────────────
def collect_source(
    page: Page,
    src: dict,
    year: int,
    month: int,
) -> dict[str, dict]:
    all_items: dict[str, dict] = {}
    base_url = src["url"].rstrip("/")
    source_name = src["name"]

    log.info(f"  Loading page 1: {base_url}")
    if not navigate_and_wait(page, base_url):
        return {}

    max_page = get_max_page(page)
    log.info(f"  Detected max page from UI: {max_page}")

    page_num = 1
    empty_page_count = 0
    found_older = False

    while page_num <= MAX_PAGES:
        articles = extract_page_articles(page)
        if not articles:
            empty_page_count += 1
            log.info(f"  Page {page_num}: no articles extracted (empty count {empty_page_count})")
            if empty_page_count >= 2 and page_num > 1:
                log.info("  Two consecutive empty pages → stopping")
                break
        else:
            empty_page_count = 0
            added = 0
            for item in articles:
                full_date = item.get("full_date", "")
                if len(full_date) < 10:
                    continue
                try:
                    dt = datetime.fromisoformat(full_date)
                    ly, lm = dt.year, dt.month
                except ValueError:
                    continue

                if (ly, lm) < (year, month):
                    found_older = True
                elif (ly, lm) == (year, month):
                    url = item["url"]
                    if url not in all_items:
                        all_items[url] = {
                            "url": url,
                            "full_date": full_date,
                            "text": item.get("text", ""),
                            "source": source_name,
                        }
                        added += 1

            log.info(
                f"  Page {page_num}: {len(articles)} articles, "
                f"+{added} matched {year}-{month:02d}, "
                f"older_found={found_older}"
            )

            if found_older:
                break

        if page_num >= max_page:
            log.info(f"  Reached max_page {max_page}, stopping")
            break

        before_hash = get_articles_hash(page)
        clicked = click_next_page(page)
        if not clicked:
            log.info("  No 'next page' button or number found, stopping")
            break

        page.wait_for_timeout(2_000)
        try:
            page.wait_for_function(
                f"() => {{ const h = '{get_articles_hash(page)}'; return h !== '{before_hash}'; }}",
                timeout=15_000
            )
        except Exception:
            log.warning("  Content did not change after clicking 'next', stopping")
            break

        page_num += 1

    return all_items


# ── Article rendering ────────────────────────────────────────────────────
def process_article(page: Page, item: dict, tmp_dir: Path, seq: int) -> Optional[Path]:
    safe = item["text"][:30].replace("/", "-")
    dest = tmp_dir / sanitize_filename(f"{seq:03d}_{item['full_date'].replace(':', '-')}_{safe}.pdf")

    try:
        # 載入文章頁
        log.info(f"  Loading article: {item['url']}")
        page.goto(item["url"], wait_until="networkidle", timeout=NAV_TIMEOUT)
        page.wait_for_timeout(3_000)

        # 檢查是否為月報頁面 (跳過)
        title = page.title()
        body_text = page.inner_text("body")
        if "氣象觀測月報" in title or "氣象觀測月報" in body_text:
            log.warning(f"  Skipping monthly report page: {item['url']}")
            return None

        # 提取文章主體並打印（完全放棄下載 PDF）
        log.info("  Printing article content (scale=0.6)")
        page.evaluate("""
            () => {
                // 嘗試搵出文章主體
                const selectors = ['article', 'main', '.content', '.article-content', '.news-content', '.post-content', '.entry-content'];
                let content = null;
                for (const sel of selectors) {
                    const el = document.querySelector(sel);
                    if (el) { content = el; break; }
                }
                if (!content) {
                    // 如果搵唔到，就用 body，但移除導航等雜項
                    content = document.body;
                    const removeSelectors = ['header', 'nav', 'footer', 'aside', '.sidebar', '.cookie-bar', '.share-buttons', '.social-share', '.related-posts', '.advertisement', '.breadcrumb'];
                    removeSelectors.forEach(sel => {
                        document.querySelectorAll(sel).forEach(el => el.remove());
                    });
                }
                // 清空 body，放入文章內容
                document.body.innerHTML = '';
                const wrapper = document.createElement('div');
                wrapper.id = 'print-content';
                wrapper.style.cssText = 'margin:0 auto; padding:20px; max-width:900px; background:white; font-size:14px; line-height:1.6;';
                wrapper.appendChild(content.cloneNode(true));
                document.body.appendChild(wrapper);
                // 壓縮圖片尺寸
                document.querySelectorAll('img').forEach(img => {
                    img.style.maxWidth = '100%';
                    img.style.height = 'auto';
                });
                // 移除不需要的樣式
                document.querySelectorAll('*').forEach(el => {
                    if (el.style) {
                        el.style.removeProperty('position');
                        el.style.removeProperty('top');
                        el.style.removeProperty('left');
                        el.style.removeProperty('transform');
                    }
                });
            }
        """)

        # 打印成 PDF
        page.add_style_tag(content=(
            "@media print{body{-webkit-print-color-adjust:exact !important;"
            "print-color-adjust:exact !important}}"
        ))
        page.pdf(path=str(dest), format="A4", print_background=True, scale=0.6)

        if dest.exists() and dest.stat().st_size > 2_000:
            return dest
        log.warning(f"  PDF too small, skipping: {dest.name}")
        return None

    except Exception as e:
        log.warning(f"  Failed processing {item['url']}: {e}")
        return None


# ── Main ──────────────────────────────────────────────────────────────────
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

        sorted_items = sorted(all_items.values(), key=lambda x: x["full_date"])
        log.info(f"\n📦 Total unique articles to render: {len(sorted_items)}")

        writer = PdfWriter()
        writer.compress = True

        for i, item in enumerate(sorted_items, 1):
            log.info(f"\n⚙  ({i}/{len(sorted_items)}) [{item['full_date']}] {item['text'][:50]}")
            pdf_path = process_article(page, item, tmp_dir, i)
            if pdf_path:
                try:
                    writer.append(str(pdf_path))
                except Exception as e:
                    log.warning(f"  Could not append {pdf_path.name}: {e}")

        output = Path(f"SMG_Monthly_Report_{year}_{month:02d}.pdf")
        with output.open("wb") as fh:
            writer.write(fh)

        size = output.stat().st_size
        if size > MAX_PDF_SIZE:
            log.info(f"PDF size {size/1_048_576:.2f} MB exceeds 5MB, applying re-compression...")
            try:
                reader = PdfReader(output)
                writer2 = PdfWriter()
                writer2.compress = True
                for page_obj in reader.pages:
                    writer2.add_page(page_obj)
                with output.open("wb") as fh:
                    writer2.write(fh)
                new_size = output.stat().st_size
                log.info(f"Re-compressed size: {new_size/1_048_576:.2f} MB")
                if new_size > MAX_PDF_SIZE:
                    log.warning(f"Still exceeds 5MB ({new_size/1_048_576:.2f} MB).")
            except Exception as e:
                log.warning(f"Re-compression failed: {e}")

        mb = output.stat().st_size / 1_048_576
        log.info(f"\n✅ Done: {output.name}  ({mb:.2f} MB)")
        browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",  type=int, default=get_target_month()[0])
    parser.add_argument("--month", type=int, default=get_target_month()[1])
    args = parser.parse_args()
    main(args.year, args.month)
