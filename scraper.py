"""
SMG (澳門特別行政區政府地球物理氣象局) 每月新聞自動下載並整合成PDF
================================================================
目標月份邏輯：執行月份的上一個月（3月→找2月、6月→找5月、10月→找9月）

網站結構（SPA，JS渲染）：
  列表頁：https://www.smg.gov.mo/{lang}/subpage/{subpage_id}[/page/{N}]
  詳情頁：https://www.smg.gov.mo/{lang}/news-detail/{article_id}
  語言：zh（中文）、en（English）、pt（Português）

抓取策略：
  1. 抓所有 SOURCES 定義的列表頁（中文版），過濾目標月份文章
  2. 每篇文章依次下載 中文→英文→葡文 三個版本 PDF
  3. 英/葡文若顯示「無相關內容」則跳過
  4. 所有 PDF 依日期升序合併
  5. 超過 5MB 則壓縮

主要修復（相對舊版）：
  - 正確的分頁 URL：/page/N（而非 ?page=N）
  - 正確等待 SPA 渲染完成（networkidle + 等待列表元素出現）
  - 圖片完整等待（JS Promise + networkidle）
  - CSS background-image 強制列印
  - 更健壯的文章連結提取（多種 selector）
  - URL 去重，避免重複下載
  - 末頁/空頁偵測
"""

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from datetime import date
import os
import re
import logging
import subprocess
import shutil
from pypdf import PdfWriter, PdfReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── 來源定義 ──────────────────────────────────────────────────────────────────
# subpage_id 根據 SMG 網站實際 URL 整理：
#   73  = 消息/新聞 (News/Activity)
#   124 = 氣候 (Climate / Seasonal)
# fallback_urls：若主 URL 失效可嘗試的別名
SOURCES = [
    {
        "name": "news",
        "url": "https://www.smg.gov.mo/zh/subpage/73",
        "fallback_urls": [
            "https://www.smg.gov.mo/zh/news",
            "https://www.smg.gov.mo/zh/activity",
        ],
    },
    {
        "name": "climate",
        "url": "https://www.smg.gov.mo/zh/subpage/124",
        "fallback_urls": [
            "https://www.smg.gov.mo/zh/climate",
            "https://www.smg.gov.mo/zh/seasonal",
        ],
    },
    {
        "name": "holiday_weather",
        "url": "https://www.smg.gov.mo/zh/news/Holiday_weather",
        "fallback_urls": [],
    },
    {
        "name": "chat_info",
        "url": "https://www.smg.gov.mo/zh/chat-info",
        "fallback_urls": [],
    },
]

BASE_URL  = "https://www.smg.gov.mo"
MAX_BYTES = 5 * 1024 * 1024  # 5 MB
MAX_PAGES = 30               # 每個來源最多掃幾頁

LANGUAGES = [
    ("zh", "中文"),
    ("en", "English"),
    ("pt", "Português"),
]

NO_CONTENT_MARKERS = [
    "no related content",
    "nenhum conteúdo relacionado",
    "nenhum conteudo relacionado",
]

# 等待所有圖片的 JS Promise（最多額外等 5 秒）
WAIT_IMAGES_JS = """
() => new Promise(resolve => {
    const imgs = [...document.images].filter(i => !i.complete);
    if (!imgs.length) return resolve();
    let n = imgs.length;
    imgs.forEach(i => {
        i.onload  = () => { if (!--n) resolve(); };
        i.onerror = () => { if (!--n) resolve(); };
    });
    setTimeout(resolve, 5000);
})
"""

# 強制列印背景色/背景圖
PRINT_CSS = """
*, *::before, *::after {
    -webkit-print-color-adjust: exact !important;
    print-color-adjust: exact !important;
    color-adjust: exact !important;
}
"""


# ── 月份計算 ──────────────────────────────────────────────────────────────────
def get_target_month():
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


# ── URL 工具 ──────────────────────────────────────────────────────────────────
def to_lang_url(url: str, lang: str) -> str:
    """將 URL 的語言前綴替換（zh/en/pt）"""
    for lc in ("zh", "en", "pt"):
        if f"/{lc}/" in url:
            return url.replace(f"/{lc}/", f"/{lang}/", 1)
    return url


def build_page_url(base_url: str, page_num: int) -> str:
    """
    構建分頁 URL。
    SMG 分頁格式：
      第1頁：/subpage/{id}
      第N頁：/subpage/{id}/page/{N}
    """
    if page_num <= 1:
        return base_url
    return base_url.rstrip("/") + f"/page/{page_num}"


# ── Playwright 輔助 ───────────────────────────────────────────────────────────
def wait_for_spa(page, timeout: int = 20000):
    """等待 SPA 渲染完成：networkidle + 嘗試等列表元素"""
    try:
        page.wait_for_load_state("networkidle", timeout=timeout)
    except PWTimeout:
        log.debug("    networkidle 超時，繼續")
    # 額外嘗試等文章連結出現
    for sel in ("a[href*='news-detail']", ".list-item", "article", "li a"):
        try:
            page.wait_for_selector(sel, timeout=6000)
            break
        except PWTimeout:
            continue


def wait_for_images(page):
    """三層策略：networkidle → JS Promise → 兜底"""
    try:
        page.wait_for_load_state("networkidle", timeout=12000)
    except PWTimeout:
        pass
    try:
        page.evaluate(WAIT_IMAGES_JS)
    except Exception:
        pass


def hide_header(page):
    try:
        page.evaluate("""() => {
            ['header','nav','.navbar','.header','.site-header',
             '#header','#navbar','.top-bar','.navigation',
             '.breadcrumb','footer','.footer'].forEach(sel => {
                document.querySelectorAll(sel).forEach(el => el.style.display = 'none');
            });
        }""")
    except Exception:
        pass


def inject_print_css(page):
    try:
        page.add_style_tag(content=PRINT_CSS)
    except Exception:
        pass


def is_no_content_page(page) -> bool:
    try:
        text = page.inner_text("body").lower()
        return any(m in text for m in NO_CONTENT_MARKERS)
    except Exception:
        return False


# ── 提取文章連結 ──────────────────────────────────────────────────────────────
def extract_article_links(page) -> list:
    """
    從列表頁提取文章連結及日期。
    SMG 文章 URL 格式：/zh/news-detail/{id}
    """
    results   = []
    seen_urls = set()

    # 依優先順序嘗試選擇器
    links = []
    for sel in ("a[href*='news-detail']", "a[href*='/zh/']", "a[href]"):
        found = page.query_selector_all(sel)
        if found:
            links = found
            break

    for link in links:
        try:
            href = link.get_attribute("href") or ""
            if not href or href.startswith("#") or "javascript" in href:
                continue

            # 只收 news-detail 路徑（列表頁的文章連結）
            if "news-detail" not in href:
                continue

            full_url = href if href.startswith("http") else BASE_URL + href
            if "smg.gov.mo" not in full_url:
                continue
            if full_url in seen_urls:
                continue

            # 從容器抓日期文字
            try:
                container = link.evaluate_handle(
                    "el => el.closest('li, tr, article, .item, .list-item, "
                    ".news-item, div.row, div.card') || el.parentElement"
                )
                container_text = container.evaluate("el => el ? el.innerText : ''") if container else ""
            except Exception:
                container_text = ""

            link_text = link.inner_text().strip()
            combined  = (container_text + " " + link_text).strip()

            # 解析日期（多種格式）
            dm = re.search(
                r"(\d{4})\s*[-/年]\s*(\d{1,2})\s*(?:[-/月]\s*(\d{1,2}))?",
                combined
            )
            date_str = ""
            if dm:
                ly = int(dm.group(1))
                lm = int(dm.group(2))
                ld = int(dm.group(3)) if dm.group(3) else 1
                # 合理性檢查
                if 2000 <= ly <= 2100 and 1 <= lm <= 12 and 1 <= ld <= 31:
                    date_str = f"{ly:04d}-{lm:02d}-{ld:02d}"

            seen_urls.add(full_url)
            results.append({
                "url":      full_url,
                "text":     link_text[:80] or href,
                "date_str": date_str,
            })
        except Exception:
            continue

    return results


# ── 掃描列表頁 ────────────────────────────────────────────────────────────────
def collect_items_from_source(page, source: dict, year: int, month: int) -> list:
    """掃描一個來源的所有列表頁，收集目標月份文章連結。"""
    name      = source["name"]
    base_url  = source["url"]
    all_items = []
    page_num  = 1

    while page_num <= MAX_PAGES:
        list_url = build_page_url(base_url, page_num)
        log.info(f"  [{name}] 掃描第 {page_num} 頁：{list_url}")

        # 第1頁可嘗試 fallback
        urls_to_try = ([list_url] + source.get("fallback_urls", [])) if page_num == 1 else [list_url]
        loaded = False

        for try_url in urls_to_try:
            try:
                page.goto(try_url, wait_until="domcontentloaded", timeout=35000)
                wait_for_spa(page, timeout=20000)
                loaded = True
                break
            except Exception as e:
                log.warning(f"  [{name}] 載入失敗 {try_url}：{e}")

        if not loaded:
            log.warning(f"  [{name}] 第 {page_num} 頁無法載入，停止")
            break

        raw_links = extract_article_links(page)
        log.info(f"  [{name}] 第 {page_num} 頁：抓到 {len(raw_links)} 個文章連結")

        if not raw_links:
            log.info(f"  [{name}] 本頁無文章連結，停止翻頁")
            break

        stop_flag = False
        page_found = 0

        for item in raw_links:
            ds = item["date_str"]
            if not ds:
                continue  # 無日期跳過

            try:
                ly = int(ds[:4])
                lm = int(ds[5:7])
            except Exception:
                continue

            if ly == year and lm == month:
                all_items.append({**item, "source": name})
                page_found += 1
            elif (ly < year) or (ly == year and lm < month):
                # 已到達比目標更早的日期，停止翻頁
                stop_flag = True

        log.info(f"  [{name}] 第 {page_num} 頁：符合本月 {page_found} 篇")

        if stop_flag:
            log.info(f"  [{name}] 遇到更舊日期，停止翻頁")
            break

        # 找下一頁（按鈕或直接嘗試 /page/N）
        next_btn = page.query_selector(
            "a.next, a[rel='next'], .pagination .next a, li.next a, "
            "a:has-text('下一頁'), a:has-text('Next'), "
            "a:has-text('›'), a:has-text('»'), [aria-label='Next page']"
        )
        if not next_btn:
            # 直接試 /page/{N+1}
            next_url = build_page_url(base_url, page_num + 1)
            try:
                page.goto(next_url, wait_until="domcontentloaded", timeout=25000)
                wait_for_spa(page, timeout=12000)
                test = extract_article_links(page)
                if not test:
                    log.info(f"  [{name}] /page/{page_num+1} 無內容，停止")
                    break
                # 有內容，繼續下一輪（已 goto，直接跳到下一個 page_num）
                page_num += 1
                # 處理本頁（已載入）
                for item in test:
                    ds = item["date_str"]
                    if not ds:
                        continue
                    try:
                        ly = int(ds[:4])
                        lm = int(ds[5:7])
                    except Exception:
                        continue
                    if ly == year and lm == month:
                        all_items.append({**item, "source": name})
                    elif (ly < year) or (ly == year and lm < month):
                        stop_flag = True
                if stop_flag:
                    break
            except Exception:
                log.info(f"  [{name}] 無更多頁面，停止")
                break

        page_num += 1

    return all_items


# ── 下載文章（三語言）────────────────────────────────────────────────────────
def download_article_all_langs(page, item: dict, tmp_dir: str, idx: int) -> list:
    """下載一篇文章的中文、英文、葡文三個版本，各自存為 PDF。"""
    safe     = re.sub(r'[^\w\-]', '_', item.get("text", "article"))[:35]
    date_str = item.get("date_str", "0000-00-00")
    base_key = f"{date_str}_{idx:04d}_{item.get('source','src')}_{safe}"
    collected = []

    for lang_code, lang_label in LANGUAGES:
        lang_url = to_lang_url(item["url"], lang_code)
        pdf_path = os.path.join(tmp_dir, f"{base_key}_{lang_code}.pdf")

        log.info(f"    [{lang_label}] {lang_url}")
        try:
            # 1. 導航至文章頁
            page.goto(lang_url, wait_until="domcontentloaded", timeout=35000)

            # 2. 等待圖片及網絡靜止
            wait_for_images(page)

            # 3. 英/葡：確認是否「無相關內容」
            if lang_code != "zh" and is_no_content_page(page):
                log.info(f"    [{lang_label}] ⚠️  無相關內容，跳過")
                continue

            # 4. 隱藏導航欄
            hide_header(page)

            # 5. 強制列印背景
            inject_print_css(page)

            # 6. 輸出 PDF
            page.pdf(
                path=pdf_path,
                format="A4",
                print_background=True,
                margin={"top": "10mm", "bottom": "15mm", "left": "15mm", "right": "15mm"},
            )

            sz = os.path.getsize(pdf_path) if os.path.exists(pdf_path) else 0
            if sz > 500:
                collected.append(pdf_path)
                log.info(f"    [{lang_label}] ✓ {os.path.basename(pdf_path)} ({sz//1024} KB)")
            else:
                log.warning(f"    [{lang_label}] ⚠️  PDF 過小（{sz}B），跳過")

        except Exception as e:
            log.warning(f"    [{lang_label}] ✗ {e}")

    return collected


# ── PDF 合併 ──────────────────────────────────────────────────────────────────
def merge_pdfs(pdf_files: list, output_path: str) -> bool:
    if not pdf_files:
        log.warning("沒有PDF可合併")
        return False
    writer = PdfWriter()
    total  = 0
    for pdf_path in pdf_files:
        try:
            reader = PdfReader(pdf_path)
            for pg in reader.pages:
                writer.add_page(pg)
            total += len(reader.pages)
            log.info(f"  ✓ {os.path.basename(pdf_path)} ({len(reader.pages)} 頁)")
        except Exception as e:
            log.warning(f"  跳過損壞文件 {pdf_path}：{e}")
    with open(output_path, "wb") as f:
        writer.write(f)
    log.info(f"✅ 合併完成：{output_path}（共 {total} 頁）")
    return True


# ── PDF 壓縮 ──────────────────────────────────────────────────────────────────
def compress_pdf(input_path: str, output_path: str) -> bool:
    size_mb = os.path.getsize(input_path) / 1024 / 1024
    log.info(f"  原始大小：{size_mb:.2f} MB")

    if os.path.getsize(input_path) <= MAX_BYTES:
        shutil.copy(input_path, output_path)
        log.info("  大小已在5MB以下，無需壓縮")
        return True

    try:
        subprocess.run(["gs", "--version"], capture_output=True, check=True, timeout=5)
    except Exception:
        log.info("  安裝 ghostscript...")
        subprocess.run(["apt-get", "install", "-y", "-q", "ghostscript"],
                       capture_output=True, timeout=120)

    for setting in ["printer", "ebook"]:
        tmp_out = output_path + f".{setting}.tmp.pdf"
        cmd = [
            "gs", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4",
            f"-dPDFSETTINGS=/{setting}", "-dNOPAUSE", "-dQUIET", "-dBATCH",
            f"-sOutputFile={tmp_out}", input_path
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, timeout=180)
            if result.returncode == 0 and os.path.exists(tmp_out):
                csz = os.path.getsize(tmp_out)
                log.info(f"  [{setting}] → {csz/1024/1024:.2f} MB")
                if csz <= MAX_BYTES:
                    shutil.move(tmp_out, output_path)
                    log.info(f"✅ 壓縮成功（{setting}）")
                    return True
                os.remove(tmp_out)
        except Exception as e:
            log.warning(f"  gs [{setting}] 失敗：{e}")

    # 最後備用：/screen
    log.warning("  嘗試 /screen 壓縮...")
    tmp_screen = output_path + ".screen.tmp.pdf"
    try:
        result = subprocess.run([
            "gs", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4",
            "-dPDFSETTINGS=/screen", "-dNOPAUSE", "-dQUIET", "-dBATCH",
            f"-sOutputFile={tmp_screen}", input_path
        ], capture_output=True, timeout=180)
        if result.returncode == 0 and os.path.exists(tmp_screen):
            shutil.move(tmp_screen, output_path)
            log.info(f"  [screen] → {os.path.getsize(output_path)/1024/1024:.2f} MB")
            return True
    except Exception as e:
        log.warning(f"  gs [screen] 失敗：{e}")

    # pypdf 兜底
    try:
        src = output_path if os.path.exists(output_path) else input_path
        reader = PdfReader(src)
        writer = PdfWriter()
        for pg in reader.pages:
            pg.compress_content_streams()
            writer.add_page(pg)
        writer.compress_identical_objects(remove_identicals=True, remove_orphans=True)
        with open(output_path, "wb") as f:
            writer.write(f)
        log.info(f"  pypdf → {os.path.getsize(output_path)/1024/1024:.2f} MB")
        return True
    except Exception as e:
        log.warning(f"  pypdf 失敗：{e}")
        shutil.copy(input_path, output_path)
        return False


# ── 主函數 ────────────────────────────────────────────────────────────────────
def generate_monthly_report(target_year: int = None, target_month: int = None):
    if target_year is None or target_month is None:
        target_year, target_month = get_target_month()

    log.info(f"🗓️  目標月份：{target_year} 年 {target_month} 月")
    log.info("=" * 65)

    tmp_dir = f"smg_tmp_{target_year}_{target_month:02d}"
    os.makedirs(tmp_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="zh-TW",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = ctx.new_page()

        # 步驟一：收集所有文章連結
        all_items = []
        for source in SOURCES:
            log.info(f"\n📂 掃描來源：{source['name']}")
            try:
                items = collect_items_from_source(page, source, target_year, target_month)
                all_items.extend(items)
                log.info(f"  [{source['name']}] 本月符合：{len(items)} 篇")
            except Exception as e:
                log.error(f"  [{source['name']}] 錯誤：{e}", exc_info=True)

        # 去重（同 URL 可能在多來源出現）
        seen_urls = set()
        deduped   = []
        for it in all_items:
            if it["url"] not in seen_urls:
                seen_urls.add(it["url"])
                deduped.append(it)
        all_items = deduped

        # 日期升序
        all_items.sort(key=lambda x: x.get("date_str", ""))

        log.info(f"\n📊 共 {len(all_items)} 篇（去重後、日期升序）")
        for i, it in enumerate(all_items, 1):
            log.info(f"  {i:3d}. [{it.get('date_str','')}] [{it.get('source','')}] {it.get('text','')[:50]}")

        if not all_items:
            log.warning("⚠️  未找到任何符合的文章")
            browser.close()
            return None

        # 步驟二：逐篇下載三語言 PDF
        log.info(f"\n{'='*65}")
        log.info("📥 開始下載（每篇：中文 + 英文 + 葡文，無內容則跳過）")
        all_pdfs = []
        for idx, item in enumerate(all_items):
            log.info(f"\n  ── [{idx+1}/{len(all_items)}] [{item.get('date_str','')}] {item.get('text','')[:50]}")
            pdfs = download_article_all_langs(page, item, tmp_dir, idx)
            all_pdfs.extend(pdfs)

        browser.close()

    log.info(f"\n{'='*65}")
    log.info(f"📊 成功下載 {len(all_pdfs)} 個PDF")

    if not all_pdfs:
        log.warning("⚠️  沒有PDF，不生成報告")
        return None

    # 步驟三：合併
    merged_path = os.path.join(tmp_dir, "_merged_raw.pdf")
    if not merge_pdfs(all_pdfs, merged_path):
        return None

    # 步驟四：壓縮
    output   = f"SMG_Monthly_Report_{target_year}_{target_month:02d}.pdf"
    raw_size = os.path.getsize(merged_path)
    log.info(f"\n📦 合併後：{raw_size/1024/1024:.2f} MB（上限 5 MB）")

    if raw_size > MAX_BYTES:
        log.info("🗜️  壓縮中...")
        compress_pdf(merged_path, output)
    else:
        shutil.copy(merged_path, output)
        log.info("✅ 大小符合，直接輸出")

    final_size = os.path.getsize(output)
    log.info(f"\n🎉 完成！{output}  ({final_size/1024/1024:.2f} MB)")
    return output


# ── 入口 ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SMG 澳門氣象局每月新聞PDF生成器")
    parser.add_argument("--year",  type=int, default=None, help="指定年份（預設：上個月）")
    parser.add_argument("--month", type=int, default=None, help="指定月份（預設：上個月）")
    args = parser.parse_args()
    generate_monthly_report(target_year=args.year, target_month=args.month)
