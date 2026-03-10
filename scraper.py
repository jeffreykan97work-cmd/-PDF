"""
SMG (澳門氣象局) 每月消息自動下載並整合成PDF
邏輯：執行月份的上一個月（3月找2月、6月找5月、10月找9月）
修正：
  - seasonal / climate 改為列表模式（非截圖）
  - 文章按日期由新到舊排序後合併PDF
  - 移除標題列截圖（header capture）
"""

from playwright.sync_api import sync_playwright
from datetime import date
import os
import re
import logging
from pypdf import PdfWriter, PdfReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SOURCES = [
    {"name": "activity",        "url": "https://www.smg.gov.mo/zh/activity",            "type": "list"},
    {"name": "news",            "url": "https://www.smg.gov.mo/zh/news",                "type": "list"},
    {"name": "holiday_weather", "url": "https://www.smg.gov.mo/zh/news/Holiday_weather", "type": "list"},
    {"name": "chat_info",       "url": "https://www.smg.gov.mo/zh/chat-info",           "type": "list"},
    {"name": "seasonal",        "url": "https://www.smg.gov.mo/zh/seasonal",            "type": "list"},  # 改為list
    {"name": "climate",         "url": "https://www.smg.gov.mo/zh/climate",             "type": "list"},  # 改為list
]
BASE_URL = "https://www.smg.gov.mo"

# ── 月份計算 ──────────────────────────────────────────────────────────────────
def get_target_month():
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1

# ── PDF 合併 ──────────────────────────────────────────────────────────────────
def merge_pdfs(pdf_files: list, output_path: str) -> bool:
    if not pdf_files:
        log.warning("沒有PDF可合併")
        return False
    writer = PdfWriter()
    total = 0
    for pdf_path in pdf_files:
        try:
            reader = PdfReader(pdf_path)
            for pg in reader.pages:
                writer.add_page(pg)
            total += len(reader.pages)
            log.info(f"  已加入：{os.path.basename(pdf_path)} ({len(reader.pages)} 頁)")
        except Exception as e:
            log.warning(f"  跳過損壞文件 {pdf_path}：{e}")
    with open(output_path, "wb") as f:
        writer.write(f)
    log.info(f"✅ 合併完成：{output_path}（共 {total} 頁）")
    return True

# ── 隱藏標題列（header/navbar）──────────────────────────────────────────────
def hide_header(page):
    """隱藏網頁頂部導航欄，令PDF內容更乾淨"""
    try:
        page.evaluate("""
            () => {
                const selectors = [
                    'header', 'nav', '.navbar', '.header', '.site-header',
                    '#header', '#navbar', '.top-bar', '.navigation'
                ];
                selectors.forEach(sel => {
                    document.querySelectorAll(sel).forEach(el => {
                        el.style.display = 'none';
                    });
                });
            }
        """)
    except Exception:
        pass

# ── 列表頁爬蟲（返回帶日期的items，統一排序後再下載）────────────────────────
def collect_items_from_source(page, source: dict, year: int, month: int) -> list:
    """
    掃描所有分頁，收集符合目標月份的文章連結及日期。
    返回: [{"url": ..., "text": ..., "date_str": "YYYY-MM-DD", "source": ...}, ...]
    """
    name     = source["name"]
    base_url = source["url"]
    page_num = 1
    all_items = []

    while True:
        url = base_url if page_num == 1 else f"{base_url}?page={page_num}"
        log.info(f"  [{name}] 掃描第 {page_num} 頁：{url}")

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(2500)
        except Exception as e:
            log.warning(f"  [{name}] 載入失敗：{e}")
            break

        if page_num > 1 and page.url.rstrip("/") == base_url.rstrip("/"):
            log.info(f"  [{name}] 已到末頁，停止")
            break

        all_links = page.query_selector_all("a[href]")
        seen_urls = set()
        has_date  = False
        stop_flag = False
        page_items = []

        for link in all_links:
            try:
                parent = link.evaluate_handle(
                    "el => el.closest('li,tr,article,.item,.news-item,div.row') || el.parentElement"
                )
                container_text = parent.evaluate("el => el ? el.innerText : ''") if parent else ""
                link_text = link.inner_text().strip()
                combined  = container_text + " " + link_text
                href      = link.get_attribute("href") or ""

                if not href or href.startswith("#") or href.startswith("javascript"):
                    continue
                if "smg.gov.mo" not in href and not href.startswith("/"):
                    continue

                full_url = href if href.startswith("http") else BASE_URL + href
                if full_url in seen_urls:
                    continue

                # 支援 YYYY-MM-DD、YYYY-MM、YYYY/MM、YYYY年M月
                dm = re.search(r"(\d{4})[-/年]\s*(\d{1,2})(?:[-/]\s*(\d{1,2}))?", combined)
                if dm:
                    has_date = True
                    ly  = int(dm.group(1))
                    lm  = int(dm.group(2))
                    ld  = int(dm.group(3)) if dm.group(3) else 1
                    date_str = f"{ly:04d}-{lm:02d}-{ld:02d}"

                    if ly == year and lm == month:
                        seen_urls.add(full_url)
                        page_items.append({
                            "url":      full_url,
                            "text":     link_text[:80],
                            "date_str": date_str,
                            "source":   name,
                        })
                    elif (ly < year) or (ly == year and lm < month):
                        stop_flag = True

            except Exception:
                continue

        log.info(f"  [{name}] 第{page_num}頁找到 {len(page_items)} 個符合項目")
        all_items.extend(page_items)

        if stop_flag:
            log.info(f"  [{name}] 已超出目標月份，停止翻頁")
            break
        if not has_date:
            log.info(f"  [{name}] 無日期資訊，停止翻頁")
            break

        next_btn = page.query_selector(
            "a.next, a[rel='next'], .pagination .next a, li.next a, "
            "a:has-text('下一頁'), a:has-text('Next'), a:has-text('›'), a:has-text('»')"
        )
        if not next_btn:
            log.info(f"  [{name}] 無下一頁，停止")
            break

        page_num += 1
        if page_num > 20:
            log.warning(f"  [{name}] 達20頁上限，停止")
            break

    return all_items


def download_item_as_pdf(page, item: dict, tmp_dir: str, idx: int) -> str | None:
    """進入文章頁面，隱藏標題列，截圖成PDF。"""
    safe     = re.sub(r'[^\w\-]', '_', item["text"])[:40]
    # 檔名包含日期，方便debug
    pdf_path = os.path.join(tmp_dir, f"{item['date_str']}_{idx:04d}_{item['source']}_{safe}.pdf")
    log.info(f"  → [{item['date_str']}] {item['url']}")
    try:
        page.goto(item["url"], wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)
        hide_header(page)
        page.pdf(
            path=pdf_path, format="A4", print_background=True,
            margin={"top": "10mm", "bottom": "15mm", "left": "15mm", "right": "15mm"},
        )
        log.info(f"    ✓ {os.path.basename(pdf_path)}")
        return pdf_path
    except Exception as e:
        log.warning(f"    ✗ {e}")
        return None


# ── 主函數 ────────────────────────────────────────────────────────────────────
def generate_monthly_report(target_year: int = None, target_month: int = None):
    if target_year is None or target_month is None:
        target_year, target_month = get_target_month()

    log.info(f"🗓️  目標月份：{target_year} 年 {target_month} 月")
    log.info("=" * 60)

    tmp_dir = f"smg_tmp_{target_year}_{target_month:02d}"
    os.makedirs(tmp_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx  = browser.new_context(viewport={"width": 1280, "height": 900}, locale="zh-TW")
        page = ctx.new_page()

        # ── 第一步：收集所有來源的文章連結 ──────────────────────────────────
        all_items = []
        for source in SOURCES:
            log.info(f"\n📂 掃描來源：{source['name']}  ({source['url']})")
            try:
                items = collect_items_from_source(page, source, target_year, target_month)
                all_items.extend(items)
                log.info(f"  小計：{len(items)} 篇")
            except Exception as e:
                log.error(f"  來源 {source['name']} 錯誤：{e}")

        # ── 第二步：按日期由新到舊排序 ───────────────────────────────────────
        all_items.sort(key=lambda x: x["date_str"], reverse=True)
        log.info(f"\n📊 共找到 {len(all_items)} 篇文章，按日期排序後下載")

        # ── 第三步：逐篇下載成PDF ─────────────────────────────────────────────
        all_pdfs = []
        for idx, item in enumerate(all_items):
            pdf_path = download_item_as_pdf(page, item, tmp_dir, idx)
            if pdf_path:
                all_pdfs.append(pdf_path)

        browser.close()

    log.info(f"\n{'='*60}")
    log.info(f"📊 成功下載 {len(all_pdfs)} 個PDF")

    if not all_pdfs:
        log.warning("⚠️  未找到任何符合的消息，不生成PDF")
        return None

    # ── 第四步：合併（已按日期排序）──────────────────────────────────────────
    output = f"SMG_Monthly_Report_{target_year}_{target_month:02d}.pdf"
    merge_pdfs(all_pdfs, output)
    log.info(f"\n🎉 完成！最終報告：{output}")
    return output


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SMG 澳門氣象局每月消息PDF生成器")
    parser.add_argument("--year",  type=int, default=None, help="指定年份（預設：上個月）")
    parser.add_argument("--month", type=int, default=None, help="指定月份（預設：上個月）")
    args = parser.parse_args()
    generate_monthly_report(target_year=args.year, target_month=args.month)
