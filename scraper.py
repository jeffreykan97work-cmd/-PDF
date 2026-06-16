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

from playwright.sync_api import Browser, Page, sync_playwright, TimeoutError as PWTimeout
from pypdf import PdfReader, PdfWriter

# ── Logging Setup ──────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ── Configuration (精準鎖定 6 個網址) ──────────────────────────────────────────
BASE_URL = "https://www.smg.gov.mo"
SOURCES: list[dict] = [
    {"name": "activity",        "url": f"{BASE_URL}/zh/activity"},
    {"name": "news",            "url": f"{BASE_URL}/zh/news"},
    {"name": "holiday_weather", "url": f"{BASE_URL}/zh/news/Holiday_weather"},
    {"name": "chat_info",       "url": f"{BASE_URL}/zh/chat-info"},
    {"name": "seasonal",        "url": f"{BASE_URL}/zh/seasonal"},
    {"name": "climate",         "url": f"{BASE_URL}/zh/climate"},
]
LANG_CODES = ["zh", "en", "pt"]
MAX_FINAL_BYTES = 50 * 1024 * 1024  # 50MB 確保夠位

MAX_SOURCE_PAGES = 80       
REQUEST_TIMEOUT = 90_000    

PDF_LINK_SELECTORS = ["a[href$='.pdf']", "a[href*='.pdf?']", "a[href*='/pdf/']", "a[href*='download']", "a[href*='attach']", "a[href*='file']"]

PRINT_CSS = """
@media print {
    header, nav, footer, .navbar, .site-header, .breadcrumb, .footer, .sidebar, .related-links { display: none !important; }
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

def get_target_month() -> tuple[int, int]:
    today = date.today()
    if today.month > 1: return today.year, today.month - 1
    return today.year - 1, 12

def sanitize_filename(name: str, max_len: int = 100) -> str:
    name = re.sub(r"\s+", " ", name).strip()
    return re.sub(r'[\\/*?:"<>|]', "", name)[:max_len] or "Untitled"

def switch_lang(url: str, target_lang: str) -> str:
    for lang in LANG_CODES:
        if f"/{lang}/" in url: return url.replace(f"/{lang}/", f"/{target_lang}/", 1)
    return url

def resolve_url(href: str) -> str:
    return href if href.startswith("http") else BASE_URL + href if href.startswith("/") else BASE_URL + "/" + href

def download_pdf(url: str, dest: Path, page: Page) -> bool:
    try:
        r = page.context.request.get(url, timeout=60000)
        if not r.ok:
            return False
        
        content = r.body()
        if not content.startswith(b"%PDF") and b"pdf" not in r.headers.get("content-type", "").lower() and not url.lower().endswith(".pdf"):
            return False
            
        dest.write_bytes(content)
        return dest.stat().st_size > 2000
    except Exception as exc:
        dest.unlink(missing_ok=True)
        return False

def extract_article_links(page: Page) -> list[dict]:
    results, seen_urls = [], set()
    
    page.wait_for_timeout(3000)
    page.evaluate("""() => {
        window.scrollTo(0, document.body.scrollHeight / 2);
        setTimeout(() => window.scrollTo(0, document.body.scrollHeight), 1500);
    }""")
    page.wait_for_timeout(3000)
    
    items_data = page.evaluate("""() => {
        const DATE_RE_1 = /(20\\d{2})[\\s\\-\\/年\\.]+(\\d{1,2})[\\s\\-\\/月\\.]+(\\d{1,2})/;
        const DATE_RE_2 = /(\\d{1,2})[\\s\\-\\/日\\.]+(\\d{1,2})[\\s\\-\\/月\\.]+(20\\d{2})/;
        const DATE_RE_3 = /(20\\d{2})[\\s\\-\\/年\\.]+(\\d{1,2})/; 
        
        const results = [];
        const links = Array.from(document.querySelectorAll('a[href], [onclick], [data-href], [data-url]'));
        
        links.forEach(el => {
            let href = el.getAttribute('href') || el.getAttribute('data-href') || el.getAttribute('data-url');
            if (!href && el.getAttribute('onclick')) {
                const m = el.getAttribute('onclick').match(/['"](.*?)['"]/);
                if (m) href = m[1];
            }
            if (!href || href === '#' || href.toLowerCase().includes('javascript:')) return;
            
            let node = el;
            let dateStr = null;
            let title = (el.innerText || '').trim();
            
            const urlDateMatch = href.match(/(20\\d{2})[\\-\\/](\\d{1,2})[\\-\\/](\\d{1,2})/);
            if (urlDateMatch) {
                dateStr = urlDateMatch[1] + '-' + urlDateMatch[2].padStart(2,'0') + '-' + urlDateMatch[3].padStart(2,'0');
            }
            
            for (let i = 0; i < 15; i++) {
                if (!node || node.tagName === 'BODY' || node.tagName === 'HTML') break;
                
                const text = (node.innerText || '').replace(/\\u200B/g, '').trim();
                
                if (!dateStr) {
                    let match = text.match(DATE_RE_1);
                    if (match) {
                        dateStr = match[1] + '-' + match[2].padStart(2,'0') + '-' + match[3].padStart(2,'0');
                    } else {
                        match = text.match(DATE_RE_2);
                        if (match) {
                            dateStr = match[3] + '-' + match[2].padStart(2,'0') + '-' + match[1].padStart(2,'0');
                        } else {
                            match = text.match(DATE_RE_3);
                            if (match) {
                                dateStr = match[1] + '-' + match[2].padStart(2,'0') + '-01'; 
                            }
                        }
                    }
                }
                
                if (dateStr) {
                    if (title.length < 4 || /閱讀|詳情|more|detail/i.test(title)) {
                        const heading = node.querySelector('h1, h2, h3, h4, .title, .subject, strong');
                        if (heading) {
                            title = heading.innerText.trim();
                        } else {
                            const lines = text.split('\\n').map(l => l.trim()).filter(l => l.length > 3 && !l.match(DATE_RE_1) && !l.match(DATE_RE_2));
                            if (lines.length > 0) title = lines[0];
                        }
                    }
                    break;
                }
                node = node.parentElement;
            }
            
            if (dateStr) {
                const hl = href.toLowerCase();
                if (hl.includes('/page/') || hl.includes('?page=')) return;
                results.push({ href: href, date_str: dateStr, text: title.substring(0, 150) });
            }
        });
        
        // Live 即時預報強制抓取邏輯
        if (results.length === 0 || window.location.href.includes('Holiday_weather')) {
            const bodyText = document.body.innerText;
            let m = bodyText.match(DATE_RE_1);
            let dateStr = null;
            if (m) {
                dateStr = m[1] + '-' + m[2].padStart(2,'0') + '-' + m[3].padStart(2,'0');
            } else {
                m = bodyText.match(DATE_RE_2);
                if (m) dateStr = m[3] + '-' + m[2].padStart(2,'0') + '-' + m[1].padStart(2,'0');
            }
            if (dateStr || window.location.href.includes('Holiday_weather')) {
                const title = document.querySelector('h1, h2, .title')?.innerText || document.title;
                if(!dateStr) {
                    const d = new Date();
                    dateStr = d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0');
                }
                results.push({ href: window.location.href, date_str: dateStr, text: "Live Bulletin: " + title.substring(0, 100) });
            }
        }
        
        return results;
    }""")

    for item in items_data:
        href = item['href']
        hl = href.lower()
        if hl.endswith(('.jpg', '.png', '.zip', '.css', '.js')): continue
        if any(x in hl for x in ['facebook.com', 'twitter.com', 'youtube.com', 'instagram.com']): continue
        
        full_url = resolve_url(href)
        norm_url = full_url
        for lang in LANG_CODES: norm_url = norm_url.replace(f"/{lang}/", "/zh/", 1)
        if norm_url in seen_urls: continue
        seen_urls.add(norm_url)
        
        zh_url = full_url
        for lang in LANG_CODES: zh_url = zh_url.replace(f"/{lang}/", "/zh/", 1)
        
        results.append({
            "url": zh_url,
            "text": item['text'] if item['text'] else "Article", 
            "date_str": item['date_str']
        })
        
    return results

def find_pdf_links_on_page(page: Page) -> list[str]:
    found, seen = [], set()
    for selector in PDF_LINK_SELECTORS:
        try:
            for a in page.query_selector_all(selector):
                href = a.get_attribute("href") or ""
                if href and resolve_url(href) not in seen:
                    seen.add(resolve_url(href))
                    found.append(resolve_url(href))
        except Exception: continue
    extra = page.evaluate("""() => {
        const urls = [];
        document.querySelectorAll('[onclick],[data-url],[data-href],[data-file]').forEach(el => {
            const raw = el.getAttribute('onclick') || el.getAttribute('data-url') || el.getAttribute('data-href') || el.getAttribute('data-file') || '';
            const m = raw.match(/https?:\\/\\/[^'"\\s]+\\.pdf[^'"\\s]*/i);
            if (m) urls.push(m[0]);
        }); return urls;
    }""") or []
    for u in extra:
        if u not in seen:
            seen.add(u)
            found.append(u)
    return found

def process_article(page: Page, item: dict, tmp_dir: Path, out_dir: Path, seq: int) -> Optional[Path]:
    collected, final_title = [], item["text"] or "Untitled"
    lang_pdf_map = {lang: [] for lang in LANG_CODES}
    
    for lang in LANG_CODES:
        try:
            page.goto(switch_lang(item["url"], lang), wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
            page.wait_for_timeout(3000)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            
            if not final_title or len(final_title) < 3 or "閱讀" in final_title or "詳情" in final_title or final_title == "Article":
                h = page.evaluate("() => document.querySelector('h1, h2, .news-detail-title, .title')?.innerText || ''")
                if h: final_title = h.strip()
            
            pdf_urls = find_pdf_links_on_page(page)
            if pdf_urls: lang_pdf_map[lang].extend(pdf_urls)
        except Exception as exc: log.warning(f"    [{lang}] Page load failed: {exc}")

    seen_pdf_urls = set()
    for lang in LANG_CODES:
        for url in lang_pdf_map[lang]:
            if url not in seen_pdf_urls:
                seen_pdf_urls.add(url)
                dest = tmp_dir / f"{seq:03d}_{lang}_{len(collected)}.pdf"
                log.info(f"    Downloading [{lang}]: {url[:80]}")
                if download_pdf(url, dest, page): collected.append((dest, LANG_CODES.index(lang)))

    if not collected:
        log.info(f"    No PDFs found — falling back to print-to-PDF")
        for lang in LANG_CODES:
            dest = tmp_dir / f"{seq:03d}_{lang}_print.pdf"
            try:
                page.goto(switch_lang(item["url"], lang), wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
                page.wait_for_timeout(2000)
                
                page.evaluate("""() => new Promise((resolve) => {
                    let totalHeight = 0;
                    let distance = 300;
                    let timer = setInterval(() => {
                        let scrollHeight = document.body.scrollHeight;
                        window.scrollBy(0, distance);
                        totalHeight += distance;
                        if(totalHeight >= scrollHeight){
                            clearInterval(timer);
                            window.scrollTo(0, 0);
                            setTimeout(resolve, 1500);
                        }
                    }, 150);
                })""")
                
                page.evaluate(WAIT_IMAGES_JS)
                page.evaluate("""() => { ['header','nav','footer','.site-header','.breadcrumb','.navbar'].forEach(s => document.querySelectorAll(s).forEach(el => el.remove())); }""")
                page.add_style_tag(content=PRINT_CSS)
                page.pdf(path=str(dest), format="A4", print_background=True, margin={"top": "1.5cm", "bottom": "1.5cm", "left": "1.5cm", "right": "1.5cm"})
                if dest.exists() and dest.stat().st_size > 2000: collected.append((dest, LANG_CODES.index(lang)))
            except Exception as exc: log.warning(f"    [{lang}] print-to-PDF failed: {exc}")

    if not collected: return None
    collected.sort(key=lambda x: x[1])
    article_pdf = out_dir / f"{seq:03d}_{item['date_str']}_{sanitize_filename(final_title)}.pdf"
    writer = PdfWriter()
    for pdf_path, _ in collected:
        try:
            for p in PdfReader(str(pdf_path)).pages: writer.add_page(p)
        except Exception: continue
    if not writer.pages: return None
    with article_pdf.open("wb") as f: writer.write(f)
    return article_pdf

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

def ensure_size_limit(src: Path, final_path: Path) -> Path:
    src_mb = src.stat().st_size / 1024 / 1024
    if src.stat().st_size <= MAX_FINAL_BYTES:
        shutil.copy(src, final_path)
        return final_path

    log.info(f"  {src_mb:.2f} MB > Limit — compressing…")
    tmp_gs = final_path.with_suffix(".gs.pdf")
    if compress_with_ghostscript(src, tmp_gs, "/ebook"):
        if tmp_gs.stat().st_size <= MAX_FINAL_BYTES:
            shutil.move(str(tmp_gs), final_path)
            return final_path
    
    shutil.copy(src, final_path)
    return final_path

def main(year: Optional[int], month: Optional[int]):
    if not year or not month:
        year, month = get_target_month()
        
    log.info(f"🚀 [SUPER-V6 CLI] Execution launched — target: {year}-{month:02d}")
    
    current_dir = Path(os.getcwd())
    tmp_dir = current_dir / f"smg_tmp_{year}_{month:02d}"
    tmp_dir.mkdir(exist_ok=True)
    
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1920, "height": 1080}, user_agent=user_agent)
        page, all_items = ctx.new_page(), {}
        
        for src in SOURCES:
            log.info(f"\n📋 Scanning: {src['name']}")
            
            try: 
                page.goto(src["url"], wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
                page.wait_for_timeout(4000)
            except Exception as e: 
                log.warning(f"  Failed to load category {src['name']}: {e}")
                continue 
            
            empty_pages_streak = 0
            for page_num in range(1, MAX_SOURCE_PAGES + 1):
                found = extract_article_links(page)
                
                if not found: 
                    log.info(f"  Page {page_num}: No links found. Reached end of list for {src['name']}.")
                    break

                added = 0
                for item in found:
                    try: 
                        ly, lm = int(item["date_str"][:4]), int(item["date_str"][5:7])
                    except Exception: 
                        continue
                    
                    is_live = "Live/" in item["text"]
                    if (ly == year and lm == month) or is_live:
                        if item["url"] not in all_items:
                            all_items[item["url"]] = item
                            added += 1

                log.info(f"  Page {page_num}: Processed {len(found)} links. Matched: {added}")
                
                # 模擬點擊下一頁
                try:
                    action = page.evaluate("""() => {
                        const links = Array.from(document.querySelectorAll('a, button, li, span'));
                        for (let el of links) {
                            const t = (el.innerText || '').trim();
                            const title = el.getAttribute('title') || '';
                            const isNext = t === '下一頁' || t === '下一页' || t === 'Next' || t === '>' || t === '»' || title.includes('下一頁');
                            
                            if (isNext && el.offsetParent !== null) {
                                const disabled = el.disabled || el.classList.contains('disabled');
                                if (!disabled) {
                                    el.click(); return 'clicked';
                                }
                            }
                        }
                        for (let el of links) {
                            const t = (el.innerText || '').trim();
                            if ((t.includes('載入更多') || t.includes('加载更多') || t.includes('Load More')) && el.offsetParent !== null) {
                                el.click(); return 'clicked';
                            }
                        }
                        return null;
                    }""")
                    
                    if action == 'clicked':
                        page.wait_for_timeout(4000) 
                    else:
                        if "?" not in src["url"] and "page" not in src["url"].lower():
                            next_url = f"{src['url'].rstrip('/')}/page/{page_num + 1}"
                            try:
                                page.goto(next_url, wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
                                page.wait_for_timeout(4000)
                            except:
                                break
                        else:
                            break
                except Exception as e:
                    break
        
        sorted_items = sorted(all_items.values(), key=lambda x: x["date_str"])
        if not sorted_items:
            log.warning(f"❌ No matching articles found for {year}-{month:02d}.")
            browser.close()
            return
        
        article_pdfs = []
        for idx, item in enumerate(sorted_items, 1):
            log.info(f"\n⚙ Processing ({idx}/{len(sorted_items)}) [{item['date_str']}] {item['text']}")
            pdf = process_article(page, item, tmp_dir, tmp_dir, idx)
            if pdf: article_pdfs.append(pdf)
        browser.close()
        
    if not article_pdfs:
        log.warning("❌ No PDFs generated.")
        return
        
    log.info(f"\n📎
