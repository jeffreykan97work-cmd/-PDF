"""
SMG (澳門氣象局) 每月消息自動下載並整合成PDF
邏輯：執行月份的上一個月（3月找2月、6月找5月、10月找9月）
"""

from playwright.sync_api import sync_playwright
from datetime import date
import os
import re
import logging
from pypdf import PdfWriter, PdfReader
import io

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SOURCES = [
    {"name": "activity",        "url": "https://www.smg.gov.mo/zh/activity",             "type": "list"},
    {"name": "news",            "url": "https://www.smg.gov.mo/zh/news",                 "type": "list"},
    {"name": "holiday_weather", "url": "https://www.smg.gov.mo/zh/news/Holiday_weather",  "type": "list"},
    {"name": "chat_info",       "url": "https://www.smg.gov.mo/zh/chat-info",            "type": "list"},
    {"name": "seasonal",        "url": "https://www.smg.gov.mo/zh/seasonal",             "type": "single"},
    {"name": "climate",         "url": "https://www.smg.gov.mo/zh/climate",              "type": "single"},
]
BASE_URL = "https://www.smg.gov.mo"


# ── 月份計算（純內建）────────────────────────────────────────────────────────
def get_target_month():
    """返回上個月的 (year, month)"""
    today = date.today()
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


# ── PDF 合併（用 pypdf，已在 requirements.txt 列明）──────────────────────────
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


# ── 列表頁爬蟲 ────────────────────────────────────────────────────────────────
def scrape_list_source(page, source: dict, year: int, month: int, tmp_dir: str) -> list:
    """
    遍歷分頁列表，找出符合目標月份的文章連結，進入後截圖成 PDF。
    """
    collected = []
    name     = source["name"]
    base_url = source["url"]
    page_num = 1

    while True:
        url = base_url if page_num == 1 else f"{base_url}?page={page_num}"
        log.info(f"  [{name}] 第 {page_num} 頁：{url}")

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(2500)
        except Exception as e:
            log.warning(f"  [{name}] 載入失敗：{e}")
            break

        # 分頁後若跳回首頁，代表已到末頁
        if page_num > 1 and page.url.rstrip("/") == base_url.rstrip("/"):
            log.info(f"  [{name}] 已到末頁，停止")
            break

        # ── 掃描所有連結 ────────────────────────────────────────────────────
        all_links  = page.query_selector_all("a[href]")
        items      = []
        seen_urls  = set()
        has_date   = False
        stop_flag  = False   # 遇到比目標更早的日期

        for link in all_links:
            try:
                # 取父元素文字（包含日期）
                parent = link.evaluate_handle(
                    "el => el.closest('li,tr,article,.item,.news-item,div.row') || el.parentElement"
                )
                container_text = parent.evaluate("el => el ? el.innerText : ''") if parent else ""
                link_text  = link.inner_text().strip()
                combined   = container_text + " " + link_text
                href       = link.get_attribute("href") or ""

                # 過濾無效連結
                if not href or href.startswith("#") or href.startswith("javascript"):
                    continue
                if "smg.gov.mo" not in href and not href.startswith("/"):
                    continue

                full_url = href if href.startswith("http") else BASE_URL + href
                if full_url in seen_urls:
                    continue

                # 比對日期：格式 YYYY/MM、YYYY-MM、YYYY年M月 等
                dm = re.search(r"(\d{4})[-/年]\s*(\d{1,2})", combined)
                if dm:
                    has_date = True
                    ly, lm   = int(dm.group(1)), int(dm.group(2))

                    if ly == year and lm == month:
                        seen_urls.add(full_url)
                        items.append({"url": full_url, "text": link_text[:80]})
                    elif (ly < year) or (ly == year and lm < month):
                        stop_flag = True   # 日期比目標早，這頁之後不用再找

            except Exception:
                continue

        log.info(f"  [{name}] 找到 {len(items)} 個符合 {year}/{month:02d} 的項目")

        # ── 進入每篇文章，截圖成 PDF ─────────────────────────────────────────
        for idx, item in enumerate(items):
            safe     = re.sub(r'[^\w\-]', '_', item["text"])[:50]
            pdf_path = os.path.join(tmp_dir, f"{name}_p{page_num}_{idx+1:03d}_{safe}.pdf")
            log.info(f"    → {item['url']}")
            try:
                page.goto(item["url"], wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(2000)
                page.pdf(
                    path=pdf_path, format="A4", print_background=True,
                    margin={"top": "15mm", "bottom": "15mm", "left": "15mm", "right": "15mm"},
                )
                collected.append(pdf_path)
                log.info(f"    ✓ {os.path.basename(pdf_path)}")
            except Exception as e:
                log.warning(f"    ✗ {e}")

        # ── 決定是否繼續翻頁 ─────────────────────────────────────────────────
        if stop_flag:
            log.info(f"  [{name}] 日期已超出目標月份，停止翻頁")
            break
        if not has_date:
            log.info(f"  [{name}] 此頁無日期資訊，停止翻頁")
            break

        # 找下一頁按鈕（支援多種樣式）
        next_btn = page.query_selector(
            "a.next, a[rel='next'], .pagination .next a, li.next a, "
            "a:has-text('下一頁'), a:has-text('Next'), a:has-text('›'), a:has-text('»')"
        )
        if not next_btn:
            log.info(f"  [{name}] 無下一頁按鈕，停止")
            break

        page_num += 1
        if page_num > 20:
            log.warning(f"  [{name}] 已達 20 頁上限，強制停止")
            break

    return collected


def scrape_single_source(page, source: dict, year: int, month: int, tmp_dir: str) -> list:
    """單頁來源：直接截圖整個頁面成 PDF。"""
    name     = source["name"]
    pdf_path = os.path.join(tmp_dir, f"{name}_main.pdf")
    log.info(f"  [{name}] 截圖：{source['url']}")
    try:
        page.goto(source["url"], wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)
        page.pdf(
            path=pdf_path, format="A4", print_background=True,
            margin={"top": "15mm", "bottom": "15mm", "left": "15mm", "right": "15mm"},
        )
        log.info(f"  ✓ {os.path.basename(pdf_path)}")
        return [pdf_path]
    except Exception as e:
        log.warning(f"  ✗ {e}")
        return []


# ── 主函數 ────────────────────────────────────────────────────────────────────
def generate_monthly_report(target_year: int = None, target_month: int = None):
    if target_year is None or target_month is None:
        target_year, target_month = get_target_month()

    log.info(f"🗓️  目標月份：{target_year} 年 {target_month} 月")
    log.info("=" * 60)

    tmp_dir = f"smg_tmp_{target_year}_{target_month:02d}"
    os.makedirs(tmp_dir, exist_ok=True)
    all_pdfs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx  = browser.new_context(viewport={"width": 1280, "height": 900}, locale="zh-TW")
        page = ctx.new_page()

        for source in SOURCES:
            log.info(f"\n📂 來源：{source['name']}  ({source['url']})")
            try:
                if source["type"] == "list":
                    pdfs = scrape_list_source(page, source, target_year, target_month, tmp_dir)
                else:
                    pdfs = scrape_single_source(page, source, target_year, target_month, tmp_dir)
                all_pdfs.extend(pdfs)
                log.info(f"  小計：{len(pdfs)} 個 PDF")
            except Exception as e:
                log.error(f"  來源 {source['name']} 錯誤：{e}")

        browser.close()

    log.info(f"\n{'='*60}")
    log.info(f"📊 共收集 {len(all_pdfs)} 個 PDF")

    if not all_pdfs:
        log.warning("⚠️  未找到任何符合的消息，不生成 PDF")
        return None

    output = f"SMG_Monthly_Report_{target_year}_{target_month:02d}.pdf"
    merge_pdfs(all_pdfs, output)
    log.info(f"\n🎉 完成！最終報告：{output}")
    return output


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SMG 澳門氣象局每月消息 PDF 生成器")
    parser.add_argument("--year",  type=int, default=None, help="指定年份（預設：上個月）")
    parser.add_argument("--month", type=int, default=None, help="指定月份（預設：上個月）")
    args = parser.parse_args()
    generate_monthly_report(target_year=args.year, target_month=args.month)
