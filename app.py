from __future__ import annotations
import argparse
import logging
import os
import re
import shutil
import subprocess
import threading
import time
import sys
import webbrowser
from datetime import date
from pathlib import Path
from typing import Optional
from flask import Flask, Response, jsonify, render_template_string, request, send_file
from playwright.sync_api import Browser, Page, sync_playwright, TimeoutError as PWTimeout
from pypdf import PdfReader, PdfWriter

# ── PyInstaller Playwright Path Configuration ────────────────────────────────
if getattr(sys, 'frozen', False):
    bundle_dir = sys._MEIPASS
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = os.path.join(bundle_dir, 'ms-playwright')
else:
    bundle_dir = os.path.dirname(os.path.abspath(__file__))

# ── Logging and Memory Buffer Setup ──────────────────────────────────────────
app_log_buffer: list[str] = []

class WebLogHandler(logging.Handler):
    def emit(self, record):
        try:
            log_message = self.format(record)
            app_log_buffer.append(log_message)
        except Exception:
            self.handleError(record)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)
log.addHandler(WebLogHandler())

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
LANG_CODES = ["zh", "en", "pt"]
MAX_FINAL_BYTES = 10 * 1024 * 1024

MAX_SOURCE_PAGES = 80       # 終極掃描深度
REQUEST_TIMEOUT = 90_000    # 放寬超時限制

PDF_LINK_SELECTORS = ["a[href$='.pdf']", "a[href*='.pdf?']", "a[href*='/pdf/']", "a[href*='download']", "a[href*='attach']", "a[href*='file']"]
NO_CONTENT_MARKERS = ["no related content", "nenhum conteúdo relacionado", "nenhum conteudo relacionado", "404", "page not found"]

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

# ── State Control Variables ─────────────────────────
scraper_running_status: bool = False
scraper_execution_result: dict = {"success": False, "filename": "", "message": "Idle"}

# ── Helper Utility Functions ──────────────────────────────────────────────────
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

# ── Core Downloader Routines ────────────────────────────────────
def download_pdf(url: str, dest: Path, page: Page) -> bool:
    """使用 Playwright 的 Browser Context 進行下載，確保攜帶所有 Cookie"""
    try:
        r = page.context.request.get(url, timeout=60000)
        if not r.ok:
            log.warning(f"    [WARNING] Download rejected by server: {url}")
            return False
        
        content = r.body()
        if not content.startswith(b"%PDF") and b"pdf" not in r.headers.get("content-type", "").lower() and not url.lower().endswith(".pdf"):
            return False
            
        dest.write_bytes(content)
        return dest.stat().st_size > 2000
    except Exception as exc:
        log.warning(f"    [WARNING] Download failed for {url[:80]}: {exc}")
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
        const DATE_RE = /(20\\d{2})[\\s\\-\\/年\\.]+(\\d{1,2})[\\s\\-\\/月\\.]+(\\d{1,2})/;
        const results = [];
        const links = Array.from(document.querySelectorAll('a, [onclick], [data-href], [data-url]'));
        
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
            
            for (let i = 0; i < 15; i++) {
                if (!node || node.tagName === 'BODY' || node.tagName === 'HTML') break;
                
                const text = (node.innerText || '').replace(/\\u200B/g, '').trim();
                const match = text.match(DATE_RE);
                
                if (match) {
                    dateStr = match[1] + '-' + match[2].padStart(2,'0') + '-' + match[3].padStart(2,'0');
                    if (title.length < 4 || /閱讀|詳情|more|detail/i.test(title)) {
                        const heading = node.querySelector('h1, h2, h3, h4, .title, .subject, strong');
                        if (heading) {
                            title = heading.innerText.trim();
                        } else {
                            const lines = text.split('\\n').map(l => l.trim()).filter(l => l.length > 3 && !l.match(DATE_RE));
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
            
            if any(m in page.inner_text("body").lower() for m in NO_CONTENT_MARKERS): continue
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
                
                # 【修復重點】：深度滾動渲染機制，確保所有圖片及天氣圖表完全載入後才生成 PDF
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
                
                if any(m in page.inner_text("body").lower() for m in NO_CONTENT_MARKERS): continue
                page.evaluate(WAIT_IMAGES_JS)
                page.evaluate("""() => { ['header','nav','footer','.site-header','.breadcrumb','.navbar'].forEach(s => document.querySelectorAll(s).forEach(el => el.remove())); }""")
