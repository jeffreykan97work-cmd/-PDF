"""
SMG (澳門氣象局) 每月消息自動下載並整合成PDF
邏輯：執行月份的上一個月（3月找2月、6月找5月、10月找9月）
- 日期由舊到新排序（升序）
- 最終PDF壓縮至5MB以下
"""

from playwright.sync_api import sync_playwright
from datetime import date
import os
import re
import logging
import subprocess
from pypdf import PdfWriter, PdfReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SOURCES = [
    {"name": "activity",        "url": "https://www.smg.gov.mo/zh/activity",            "type": "list"},
    {"name": "news",            "url": "https://www.smg.gov.mo/zh/news",                "type": "list"},
    {"name": "holiday_weather", "url": "https://www.smg.gov.mo/zh/news/Holiday_weather", "type": "list"},
    {"name": "chat_info",       "url": "https://www.smg.gov.mo/zh/chat-info",           "type": "list"},
    {"name": "seasonal",        "url": "https://www.smg.gov.mo/zh/seasonal",            "type": "list"},
    {"name": "climate",         "url": "https://www.smg.gov.mo/zh/climate",             "type": "list"},
]
BASE_URL  = "https://www.smg.gov.mo"
MAX_BYTES = 5 * 1024 * 1024   # 5 MB

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

# ── PDF 壓縮至 5MB 以下 ───────────────────────────────────────────────────────
def compress_pdf(input_path: str, output_path: str) -> bool:
    """
    用 Ghostscript 壓縮 PDF。
    壓縮級別逐步提升直至低於 MAX_BYTES：
      screen  → 72 dpi  (最小)
      ebook   → 150 dpi
      printer → 300 dpi (先試)
    """
    size_mb = os.path.getsize(input_path) / 1024 / 1024
    log.info(f"  原始大小：{size_mb:.2f} MB")

    if os.path.getsize(input_path) <= MAX_BYTES:
        log.info("  大小已在 5MB 以下，無需壓縮")
        import shutil
        shutil.copy(input_path, output_path)
        return True

    # 確認 ghostscript 可用（gs）
    try:
        subprocess.run(["gs", "--version"], capture_output=True, check=True, timeout=5)
    except Exception:
        # 嘗試安裝
        log.info("  正在安裝 ghostscript...")
        subprocess.run(["apt-get", "install", "-y", "-q", "ghostscript"],
                       capture_output=True, timeout=120)

    settings = ["printer", "ebook", "screen"]
    for setting in settings:
        tmp_out = output_path + f".{setting}.tmp.pdf"
        cmd = [
            "gs", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4",
            f"-dPDFSETTINGS=/{setting}",
            "-dNOPAUSE", "-dQUIET", "-dBATCH",
            f"-sOutputFile={tmp_out}", input_path
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, timeout=120)
            if result.returncode == 0 and os.path.exists(tmp_out):
                compressed_size = os.path.getsize(tmp_out)
                log.info(f"  [{setting}] 壓縮後：{compressed_size/1024/1024:.2f} MB")
                if compressed_size <= MAX_BYTES:
                    import shutil
                    shutil.move(tmp_out, output_path)
                    log.info(f"✅ 壓縮成功（{setting}）：{output_path}")
                    return True
                else:
                    os.remove(tmp_out)
        except Exception as e:
            log.warning(f"  gs [{setting}] 失敗：{e}")

    # 所有級別仍超過5MB，用pypdf內建壓縮作最後嘗試
    log.warning("  Ghostscript 無法壓到5MB以下，嘗試 pypdf 壓縮...")
    try:
        reader = PdfReader(input_path)
        writer = PdfWriter()
        for page in reader.pages:
            page.compress_content_streams()
            writer.add_page(page)
        writer.compress_identical_objects(remove_identicals=True, remove_orphans=True)
        with open(output_path, "wb") as f:
            writer.write(f)
        final_size = os.path.getsize(output_path)
        log.info(f"  pypdf 壓縮後：{final_size/1024/1024:.2f} MB")
        if final_size <= MAX_BYTES:
            log.info("✅ pypdf 壓縮成功")
        else:
            log.warning(f"⚠️  壓縮後仍有 {final_size/1024/1024:.2f} MB，已盡力壓縮")
        return True
    except Exception as e:
        log.warning(f"  pypdf 壓縮失敗：{e}")
        import shutil
        shutil.copy(input_path, output_path)
        return False

# ── 隱藏標題列 ────────────────────────────────────────────────────────────────
def hide_header(page):
    try:
        page.evaluate("""
            () => {
                ['header','nav','.navbar','.header','.site-header',
                 '#header','#navbar','.top-bar','.navigation'].forEach(sel => {
                    document.querySelectorAll(sel).forEach(el => el.style.display = 'none');
                });
            }
        """)
    except Exception:
        pass

# ── 掃描列表頁，收集文章連結 ──────────────────────────────────────────────────
def collect_items_from_source(page, source: dict, year: int, month: int) -> list:
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

        all_links  = page.query_selector_all("a[href]")
        seen_urls  = set()
        has_date   = False
        stop_flag  = False
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


def download_item_as_pdf(page, item: dict, tmp_dir: str, idx: int):
    safe     = re.sub(r'[^\w\-]', '_', item["text"])[:40]
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

        # 第一步：收集所有文章連結
        all_items = []
        for source in SOURCES:
            log.info(f"\n📂 掃描：{source['name']}  ({source['url']})")
            try:
                items = collect_items_from_source(page, source, target_year, target_month)
                all_items.extend(items)
                log.info(f"  小計：{len(items)} 篇")
            except Exception as e:
                log.error(f"  來源 {source['name']} 錯誤：{e}")

        # 第二步：日期由舊到新排序（升序）
        all_items.sort(key=lambda x: x["date_str"], reverse=False)
        log.info(f"\n📊 共 {len(all_items)} 篇，日期由舊到新排序")

        # 第三步：逐篇下載
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

    # 第四步：合併
    merged_path = os.path.join(tmp_dir, "_merged_raw.pdf")
    merge_pdfs(all_pdfs, merged_path)

    # 第五步：檢查大小，必要時壓縮
    output = f"SMG_Monthly_Report_{target_year}_{target_month:02d}.pdf"
    raw_size = os.path.getsize(merged_path)
    log.info(f"\n📦 合併後大小：{raw_size/1024/1024:.2f} MB（上限 5 MB）")

    if raw_size > MAX_BYTES:
        log.info("🗜️  超過5MB，開始壓縮...")
        compress_pdf(merged_path, output)
    else:
        import shutil
        shutil.copy(merged_path, output)
        log.info("✅ 大小符合，直接輸出")

    final_size = os.path.getsize(output)
    log.info(f"\n🎉 完成！{output}  ({final_size/1024/1024:.2f} MB)")
    return output


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SMG 澳門氣象局每月消息PDF生成器")
    parser.add_argument("--year",  type=int, default=None, help="指定年份（預設：上個月）")
    parser.add_argument("--month", type=int, default=None, help="指定月份（預設：上個月）")
    args = parser.parse_args()
    generate_monthly_report(target_year=args.year, target_month=args.month)
