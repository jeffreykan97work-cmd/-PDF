import os
import re
import shutil
import logging
from playwright.sync_api import sync_playwright
from pypdf import PdfWriter, PdfReader

# ... (保留原有的 SOURCES, WAIT_IMAGES_JS, PRINT_CSS 等配置)

def sanitize_filename(filename):
    """清理檔名中的非法字元，確保存檔成功"""
    return re.sub(r'[\\/*?:"<>|]', "_", filename)

# ... (保留 get_target_month, to_lang_url, build_page_url, clean_page, extract_article_links, collect_items_from_source)

def download_pdf_versions(page, item, tmp_dir, idx):
    """
    下載同一則消息的不同語言版本
    """
    downloaded = []
    base_name = f"temp_{idx:02d}" # 暫存檔名
    
    for lang, label in LANGUAGES:
        target_url = to_lang_url(item['url'], lang)
        output_path = os.path.join(tmp_dir, f"{base_name}_{lang}.pdf")
        
        try:
            page.goto(target_url, wait_until="networkidle", timeout=20000)
            if lang != "zh":
                body_text = page.inner_text("body").lower()
                if any(m in body_text for m in NO_CONTENT_MARKERS):
                    continue
            
            page.evaluate(WAIT_IMAGES_JS)
            clean_page(page)
            
            page.pdf(
                path=output_path,
                format="A4",
                print_background=True,
                margin={"top": "1cm", "bottom": "1cm", "left": "1cm", "right": "1cm"}
            )
            if os.path.getsize(output_path) > 1000:
                downloaded.append(output_path)
        except Exception as e:
            log.error(f"    - [{label}] 下載失敗: {e}")
            
    return downloaded

def main(year=None, month=None):
    if not year or not month:
        year, month = get_target_month()
        
    log.info(f"🚀 開始抓取 SMG 報告: {year}-{month:02d}")
    
    tmp_dir = f"smg_tmp_{year}_{month:02d}"
    # 專門存放「每一則已合併語言」的資料夾
    individual_dir = f"SMG_Individual_News_{year}_{month:02d}"
    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(individual_dir, exist_ok=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={'width': 1280, 'height': 1440})
        page = context.new_page()
        
        # 1. 收集
        all_items = []
        for src in SOURCES:
            all_items.extend(collect_items_from_source(page, src, year, month))
        
        # 2. 去重與排序 (按日期順序)
        unique_urls = set()
        deduplicated_items = []
        for item in all_items:
            if item['url'] not in unique_urls:
                unique_urls.add(item['url'])
                deduplicated_items.append(item)
        
        # 按日期由舊到新排序，確保序號與時間掛鉤
        deduplicated_items.sort(key=lambda x: x['date_str'])
        
        log.info(f"📊 共收集到 {len(deduplicated_items)} 篇不重複文章")
        
        # 3. 下載、合併單則消息並命名
        article_merged_files = [] # 紀錄每一則消息合併後的路徑
        
        for i, item in enumerate(deduplicated_items):
            seq = i + 1
            clean_title = sanitize_filename(item['text'])
            # 建立目標檔名：01_中文標題.pdf
            final_individual_name = f"{seq:02d}_{clean_title}.pdf"
            final_individual_path = os.path.join(individual_dir, final_individual_name)
            
            log.info(f"({seq}/{len(deduplicated_items)}) 處理: {item['date_str']} {item['text'][:20]}...")
            
            # 下載該消息的所有語言版本 (回傳 list of paths)
            lang_pdfs = download_pdf_versions(page, item, tmp_dir, i)
            
            if lang_pdfs:
                # --- 執行單則消息合併 (中+英+葡) ---
                item_writer = PdfWriter()
                for lp in lang_pdfs:
                    item_writer.append(lp)
                
                with open(final_individual_path, "wb") as f:
                    item_writer.write(f)
                
                article_merged_files.append(final_individual_path)
                log.info(f"    ✅ 已生成單則合併檔: {final_individual_name}")
            
        browser.close()

    # 4. 最終合併 (將所有 01, 02... 串接成月報)
    if not article_merged_files:
        log.warning("❌ 沒有找到任何文章，任務結束")
        return

    raw_merged = os.path.join(tmp_dir, "raw_merged.pdf")
    final_writer = PdfWriter()
    for p in article_merged_files:
        final_writer.append(p)
    
    with open(raw_merged, "wb") as f:
        final_writer.write(f)
        
    output_filename = f"SMG_Monthly_Report_{year}_{month:02d}.pdf"
    compress_pdf(raw_merged, output_filename)
    
    log.info(f"✅ 任務完成!")
    log.info(f"   - 總月報: {output_filename}")
    log.info(f"   - 單則消息目錄: {individual_dir}/")

if __name__ == "__main__":
    # ... (保留 argparse 部分)
