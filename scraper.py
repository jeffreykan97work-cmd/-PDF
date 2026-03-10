"""
SMG (澳門氣象局) 每月消息自動下載並整合成PDF
邏輯：執行月份的上一個月（3月找2月、6月找5月、10月找9月）
"""

from playwright.sync_api import sync_playwright
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pypdf import PdfWriter, PdfReader
import os
import re
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── 目標網站設定 ─────────────────────────────────────────────────────────────
SOURCES = [
    {
        "name": "activity",
        "url": "https://www.smg.gov.mo/zh/activity",
        "type": "list",          # 有分頁列表
        "date_selector": None,   # 用通用邏輯
    },
    {
        "name": "news",
        "url": "https://www.smg.gov.mo/zh/news",
        "type": "list",
        "date_selector": None,
    },
    {
        "name": "holiday_weather",
        "url": "https://www.smg.gov.mo/zh/news/Holiday_weather",
        "type": "list",
        "date_selector": None,
    },
    {
        "name": "chat_info",
        "url": "https://www.smg.gov.mo/zh/chat-info",
        "type": "list",
        "date_selector": None,
    },
    {
        "name": "seasonal",
        "url": "https://www.smg.gov.mo/zh/seasonal",
        "type": "single",        # 單頁，直接截圖
        "date_selector": None,
    },
    {
        "name": "climate",
        "url": "https://www.smg.gov.mo/zh/climate",
        "type": "single",
        "date_selector": None,
    },
]

BASE_URL = "https://www.smg.gov.mo"

# ── 月份計算 ──────────────────────────────────────────────────────────────────
def get_target_month() -> tuple[int, int]:
    """返回目標年份和月份（執行月份的上一個月）"""
    today = date.today()
    target = today - relativedelta(months=1)
    return target.year, target.month

# ── 日期匹配 ──────────────────────────────────────────────────────────────────
MONTH_ZH = {
    1: "1月", 2: "2月", 3: "3月", 4: "4月",
    5: "5月", 6: "6月", 7: "7月", 8: "8月",
    9: "9月", 10: "10月", 11: "11月", 12: "12月",
}

def text_matches_month(text: str, year: int, month: int) -> bool:
    """檢查文字是否包含目標年月"""
    patterns = [
        rf"{year}[-/年]\s*0?{month}[-/月]",          # 2024-02 / 2024年2月
        rf"0?{month}[-/月]\s*{year}",                 # 02/2024
        rf"{year}\s*年\s*{month}\s*月",
        rf"{year}-{month:02d}",
    ]
    for p in patterns:
        if re.search(p, text):
            return True
    return False

# ── PDF 合併 ──────────────────────────────────────────────────────────────────
def merge_pdfs(pdf_files: list[str], output_path: str):
    """把多個PDF合併成一個"""
    writer = PdfWriter()
    for pdf in pdf_files:
        try:
            reader = PdfReader(pdf)
            for page in reader.pages:
                writer.add_page(page)
            log.info(f"  已加入：{os.path.basename(pdf)} ({len(reader.pages)} 頁)")
        except Exception as e:
            log.warning(f"  跳過損壞檔案 {pdf}：{e}")
    with open(output_path, "wb") as f:
        writer.write(f)
    log.info(f"✅ 合併完成：{output_path}（共 {len(writer.pages)} 頁）")

# ── 主要爬蟲邏輯 ──────────────────────────────────────────────────────────────
def scrape_list_source(page, source: dict, year: int, month: int, tmp_dir: str) -> list[str]:
    """
    處理有分頁列表的來源網站：
    1. 遍歷每一頁
    2. 找出日期符合目標月份的連結
    3. 進入每個連結並截圖成PDF
    """
    collected_pdfs = []
    source_name = source["name"]
    base_url = source["url"]
    page_num = 1
    found_older = False   # 當找到比目標月份更早的文章，可以考慮停止

    while not found_older:
        # 構建分頁URL（常見格式：?page=2 或 /page/2）
        if page_num == 1:
            list_url = base_url
        else:
            # 先嘗試 ?page=N
            list_url = f"{base_url}?page={page_num}"

        log.info(f"  [{source_name}] 正在掃描第 {page_num} 頁：{list_url}")
        try:
            page.goto(list_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(2000)
        except Exception as e:
            log.warning(f"  [{source_name}] 載入失敗：{e}")
            break

        # 檢查是否有效頁面（如果跳轉回首頁或404，停止）
        current_url = page.url
        if page_num > 1 and current_url == base_url:
            log.info(f"  [{source_name}] 已到達最後一頁，停止")
            break

        # ── 找出所有文章列表項目 ────────────────────────────────────────────
        # 嘗試多種常見的列表選擇器
        items = []
        selectors_to_try = [
            "article",
            ".news-item",
            ".list-item",
            ".item",
            "li.news",
            ".content-list li",
            ".article-list li",
            "table tr",
        ]

        # 先取得頁面所有包含連結的區塊文字
        # 用更通用的方法：找所有包含日期的連結
        all_links = page.query_selector_all("a[href]")
        
        page_has_target = False
        page_has_any_date = False
        stop_after_page = False

        for link in all_links:
            try:
                # 取得連結的周邊文字（包含父元素）
                parent = link.evaluate_handle("el => el.closest('li, tr, article, .item, .news-item, div.row') || el.parentElement")
                container_text = parent.evaluate("el => el ? el.innerText : ''") if parent else ""
                link_text = link.inner_text().strip()
                combined_text = container_text + " " + link_text
                href = link.get_attribute("href") or ""

                # 跳過導航連結、外部連結
                if not href or href.startswith("#") or href.startswith("javascript"):
                    continue
                if "smg.gov.mo" not in href and not href.startswith("/"):
                    continue

                # 檢查是否包含日期資訊
                date_match = re.search(r"(\d{4})[-/年]\s*(\d{1,2})", combined_text)
                if date_match:
                    page_has_any_date = True
                    link_year = int(date_match.group(1))
                    link_month = int(date_match.group(2))

                    if link_year == year and link_month == month:
                        page_has_target = True
                        full_url = href if href.startswith("http") else BASE_URL + href
                        items.append({"url": full_url, "text": link_text[:80]})
                    elif (link_year < year) or (link_year == year and link_month < month):
                        # 找到比目標更早的文章
                        stop_after_page = True
            except Exception:
                continue

        log.info(f"  [{source_name}] 第{page_num}頁：找到 {len(items)} 個符合 {year}/{month:02d} 的項目")

        # ── 進入每個符合的連結並存成PDF ────────────────────────────────────
        for idx, item in enumerate(items):
            article_url = item["url"]
            safe_title = re.sub(r'[^\w\-]', '_', item["text"])[:50]
            pdf_path = os.path.join(tmp_dir, f"{source_name}_p{page_num}_{idx+1:03d}_{safe_title}.pdf")

            log.info(f"    → 下載：{article_url}")
            try:
                page.goto(article_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(2000)
                page.pdf(
                    path=pdf_path,
                    format="A4",
                    margin={"top": "15mm", "bottom": "15mm", "left": "15mm", "right": "15mm"},
                    print_background=True,
                )
                collected_pdfs.append(pdf_path)
                log.info(f"    ✓ 已儲存：{os.path.basename(pdf_path)}")
            except Exception as e:
                log.warning(f"    ✗ 失敗：{e}")

        # 決定是否繼續下一頁
        if stop_after_page and not page_has_target:
            log.info(f"  [{source_name}] 內容已早於目標月份，停止分頁")
            break

        if not page_has_any_date:
            log.info(f"  [{source_name}] 此頁無日期資訊，停止分頁")
            break

        # 檢查是否有「下一頁」按鈕
        next_btn = page.query_selector("a.next, a[rel='next'], .pagination .next a, li.next a, a:has-text('下一頁'), a:has-text('Next')")
        if not next_btn:
            log.info(f"  [{source_name}] 無下一頁按鈕，停止")
            break

        page_num += 1
        if page_num > 20:  # 安全上限
            log.warning(f"  [{source_name}] 已達20頁上限，強制停止")
            break

    return collected_pdfs


def scrape_single_source(page, source: dict, year: int, month: int, tmp_dir: str) -> list[str]:
    """處理單頁來源：直接截圖整個頁面成PDF"""
    source_name = source["name"]
    url = source["url"]
    pdf_path = os.path.join(tmp_dir, f"{source_name}_main.pdf")

    log.info(f"  [{source_name}] 截圖單頁：{url}")
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)
        page.pdf(
            path=pdf_path,
            format="A4",
            margin={"top": "15mm", "bottom": "15mm", "left": "15mm", "right": "15mm"},
            print_background=True,
        )
        log.info(f"  ✓ 已儲存：{os.path.basename(pdf_path)}")
        return [pdf_path]
    except Exception as e:
        log.warning(f"  ✗ 失敗：{e}")
        return []


# ── 主函數 ────────────────────────────────────────────────────────────────────
def generate_monthly_report(target_year: int = None, target_month: int = None):
    """
    主函數：爬取所有來源、整合成單一PDF

    Args:
        target_year: 指定年份（None = 自動計算上個月）
        target_month: 指定月份（None = 自動計算上個月）
    """
    if target_year is None or target_month is None:
        target_year, target_month = get_target_month()

    log.info(f"🗓️  目標月份：{target_year} 年 {target_month} 月")
    log.info("=" * 60)

    # 建立臨時目錄
    tmp_dir = f"smg_tmp_{target_year}_{target_month:02d}"
    os.makedirs(tmp_dir, exist_ok=True)

    all_pdfs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="zh-TW",
        )
        page = context.new_page()

        for source in SOURCES:
            log.info(f"\n📂 處理來源：{source['name']} ({source['url']})")
            try:
                if source["type"] == "list":
                    pdfs = scrape_list_source(page, source, target_year, target_month, tmp_dir)
                else:
                    pdfs = scrape_single_source(page, source, target_year, target_month, tmp_dir)
                all_pdfs.extend(pdfs)
                log.info(f"  小計：{len(pdfs)} 個PDF")
            except Exception as e:
                log.error(f"  來源 {source['name']} 發生錯誤：{e}")

        browser.close()

    log.info(f"\n{'='*60}")
    log.info(f"📊 總共收集到 {len(all_pdfs)} 個PDF檔案")

    if not all_pdfs:
        log.warning("⚠️  未找到任何符合的消息，不生成PDF")
        return

    # 合併所有PDF
    output_filename = f"SMG_Monthly_Report_{target_year}_{target_month:02d}.pdf"
    merge_pdfs(all_pdfs, output_filename)

    # 清理臨時目錄（可選，若想保留個別PDF可以註釋以下代碼）
    # import shutil
    # shutil.rmtree(tmp_dir)
    # log.info(f"🗑️  已清理臨時目錄：{tmp_dir}")

    log.info(f"\n🎉 完成！最終報告：{output_filename}")
    return output_filename


# ── 執行入口 ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SMG 澳門氣象局每月消息PDF生成器")
    parser.add_argument("--year",  type=int, default=None, help="指定年份（預設：上個月）")
    parser.add_argument("--month", type=int, default=None, help="指定月份（預設：上個月）")
    args = parser.parse_args()

    generate_monthly_report(target_year=args.year, target_month=args.month)
