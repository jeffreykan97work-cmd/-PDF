import os
import re
import sys
import time
import shutil
import subprocess
import argparse
from datetime import datetime, date
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from pypdf import PdfWriter, PdfReader


# ==========================================
# 基本設定與常數
# ==========================================
BASE_DOMAIN = "https://www.smg.gov.mo"
LANGUAGES = ["zh", "pt", "en"]
LANG_NAMES = {
    "zh": "中文",
    "pt": "葡文 (Português)",
    "en": "英文 (English)"
}

# 預設掃描的列表路徑（支援 news-list 與 chat-info 等子頁面）
DEFAULT_CATEGORY_PATHS = [
    "news-list",
    "subpage/348/news-list",
]


def parse_flexible_date(text: str) -> date | None:
    """
    支援中、葡、英多種日期格式解析：
    - YYYY-MM-DD / YYYY/MM/DD / YYYY.MM.DD
    - YYYY年MM月DD日
    - DD-MM-YYYY / DD/MM/YYYY / DD.MM.YYYY
    """
    if not text:
        return None

    # 1. YYYY-MM-DD 或 YYYY/MM/DD 或 YYYY年MM月DD日
    m1 = re.search(r'(20\d{2})[-/.年\s]+(0?[1-9]|1[0-2])[-/.月\s]+(0?[1-9]|[12]\d|3[01])', text)
    if m1:
        try:
            return date(int(m1.group(1)), int(m1.group(2)), int(m1.group(3)))
        except ValueError:
            pass

    # 2. DD-MM-YYYY 或 DD/MM/YYYY (常見於葡文/英文介面)
    m2 = re.search(r'(0?[1-9]|[12]\d|3[01])[-/.](0?[1-9]|1[0-2])[-/.](20\d{2})', text)
    if m2:
        try:
            return date(int(m2.group(3)), int(m2.group(2)), int(m2.group(1)))
        except ValueError:
            pass

    return None


def normalize_article_key(url_or_path: str) -> str | None:
    """
    從網址或路徑中提取不帶語系前綴的標準化文章路徑作為唯一 Key。
    例如：
    https://www.smg.gov.mo/zh/news-detail/12345 -> news-detail/12345
    /pt/subpage/348/chat-info/67890 -> subpage/348/chat-info/67890
    """
    if not url_or_path:
        return None

    parsed = urlparse(url_or_path)
    path = parsed.path if parsed.path else url_or_path
    path = path.strip("/")

    # 移除開頭的 zh / pt / en 語系前綴
    path = re.sub(r'^(zh|pt|en)/', '', path, flags=re.IGNORECASE)

    # 只保留有效的文章內頁路徑 (news-detail 或 chat-info)
    if "news-detail" in path or "chat-info" in path:
        # 去除尾端多餘參數或斜線
        return path.strip("/")

    return None


def build_lang_url(canonical_key: str, lang: str) -> str:
    """根據標準化路徑與目標語系組裝完整 URL"""
    clean_key = canonical_key.strip("/")
    return f"{BASE_DOMAIN}/{lang}/{clean_key}"


def force_scroll_and_render(page, pause_sec: float = 0.6):
    """強制滾動頁面以觸發懶載入圖片與 SPA 動態區塊渲染"""
    try:
        page.evaluate(
            """
            async () => {
                await new Promise((resolve) => {
                    let totalHeight = 0;
                    const distance = 400;
                    const timer = setInterval(() => {
                        const scrollHeight = document.body.scrollHeight || document.documentElement.scrollHeight;
                        window.scrollBy(0, distance);
                        totalHeight += distance;
                        if (totalHeight >= scrollHeight || totalHeight > 15000) {
                            clearInterval(timer);
                            window.scrollTo(0, 0);
                            resolve();
                        }
                    }, 100);
                });
            }
            """
        )
        time.sleep(pause_sec)
    except Exception:
        pass


def extract_article_links_from_page(page) -> list[dict]:
    """
    從當前列表頁面提取所有文章連結與日期。
    同時掃描 <a> 標籤與內含日期的容器，支援 news-detail 與 chat-info。
    """
    results = []
    try:
        raw_items = page.evaluate(
            """
            () => {
                const items = [];
                const anchors = Array.from(document.querySelectorAll('a[href]'));
                for (const a of anchors) {
                    const href = a.getAttribute('href') || '';
                    if (href.includes('news-detail') || href.includes('chat-info')) {
                        // 往上找最近的列表項目容器以取得完整日期與標題文字
                        let container = a.closest('li, tr, .news-item, .list-item, .card, .row, div');
                        let text = a.innerText || '';
                        if (container && container.innerText) {
                            text = container.innerText;
                        }
                        items.push({
                            href: a.href || href,
                            text: text.replace(/\\s+/g, ' ').trim()
                        });
                    }
                }
                return items;
            }
            """
        )

        for item in raw_items:
            key = normalize_article_key(item.get("href", ""))
            if not key:
                continue
            item_date = parse_flexible_date(item.get("text", ""))
            results.append({
                "key": key,
                "href": item.get("href", ""),
                "text": item.get("text", ""),
                "date": item_date
            })
    except Exception as e:
        print(f"  [警告] 解析列表頁面連結時發生異常: {e}")

    return results


def click_next_page(page, next_page_num: int) -> bool:
    """
    模擬點擊分頁按鈕（支援頁碼點擊與下一頁箭頭），回傳是否成功翻頁。
    """
    try:
        # 先滾動到底部讓分頁列顯現
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.5)

        # 策略 1: 直接尋找對應頁碼按鈕 (例如 '2', '3'...)
        page_num_str = str(next_page_num)
        clicked = page.evaluate(
            """
            (targetPage) => {
                const selectors = [
                    '.pagination li a', '.pagination li button', '.pagination li',
                    '.el-pager li', '.page-item a', '.page-link', '[class*="page"] a', '[class*="page"] button',
                    '[class*="pagination"] span', '[class*="pagination"] a'
                ];
                for (const sel of selectors) {
                    const nodes = Array.from(document.querySelectorAll(sel));
                    for (const node of nodes) {
                        if ((node.innerText || '').trim() === targetPage) {
                            node.click();
                            return true;
                        }
                    }
                }
                // 策略 2: 找「下一頁 / Next / Seguinte / > / »」按鈕
                const nextSelectors = [
                    'button.btn-next:not([disabled])',
                    '.pagination-next:not(.disabled)',
                    'li.next:not(.disabled) a',
                    'a[aria-label*="Next"]:not(.disabled)',
                    'button[aria-label*="Next"]:not([disabled])'
                ];
                for (const sel of nextSelectors) {
                    const btn = document.querySelector(sel);
                    if (btn && !btn.disabled && !btn.classList.contains('disabled')) {
                        btn.click();
                        return true;
                    }
                }
                return false;
            }
            """,
            page_num_str
        )
        if clicked:
            time.sleep(1.8)
            force_scroll_and_render(page, pause_sec=0.5)
            return True
    except Exception as e:
        print(f"  [提示] 點擊翻頁 {next_page_num} 失敗: {e}")

    return False


def discover_articles_across_languages(
    browser,
    start_date: date,
    end_date: date,
    category_paths: list[str] = None,
    max_pages_per_cat: int = 30,
    log_fn=print
) -> list[dict]:
    """
    核心步驟 1：跨中、葡、英三語列表全量掃描，並取聯集（Union）。
    確保不會因為單一語系列表排序差異、漏登或早停而遺漏任何文章。
    """
    if category_paths is None:
        category_paths = DEFAULT_CATEGORY_PATHS

    discovered = {}  # key -> {"key": key, "date": date, "title_hint": str, "found_langs": set()}

    for cat_path in category_paths:
        for lang in LANGUAGES:
            context = browser.new_context(
                locale="zh-TW" if lang == "zh" else ("pt-PT" if lang == "pt" else "en-US"),
                viewport={"width": 1366, "height": 900}
            )
            page = context.new_page()
            base_list_url = f"{BASE_DOMAIN}/{lang}/{cat_path.strip('/')}"
            log_fn(f"\n[掃描目錄] 語系: {lang.upper()} | 網址: {base_list_url}")

            consecutive_old_pages = 0

            try:
                for page_num in range(1, max_pages_per_cat + 1):
                    if page_num == 1:
                        page.goto(base_list_url, wait_until="domcontentloaded", timeout=30000)
                        time.sleep(2.0)
                        force_scroll_and_render(page, pause_sec=0.6)
                    else:
                        # 優先嘗試頁面點擊翻頁，若無分頁按鈕則嘗試帶入 ?page= 參數
                        if not click_next_page(page, page_num):
                            paged_url = f"{base_list_url}?page={page_num}"
                            page.goto(paged_url, wait_until="domcontentloaded", timeout=25000)
                            time.sleep(1.8)
                            force_scroll_and_render(page, pause_sec=0.5)

                    items = extract_article_links_from_page(page)
                    if not items:
                        log_fn(f"  - 第 {page_num} 頁未偵測到文章連結，停止此目錄翻頁。")
                        break

                    in_range_count = 0
                    older_count = 0
                    dated_count = 0

                    for item in items:
                        key = item["key"]
                        art_date = item["date"]

                        if art_date:
                            dated_count += 1
                            if start_date <= art_date <= end_date:
                                in_range_count += 1
                                if key not in discovered:
                                    discovered[key] = {
                                        "key": key,
                                        "date": art_date,
                                        "title_hint": item["text"][:60],
                                        "found_langs": {lang}
                                    }
                                else:
                                    discovered[key]["found_langs"].add(lang)
                            elif art_date < start_date:
                                older_count += 1
                        else:
                            # 若列表上沒有顯示日期，但已經在既有 discovered 清單中，更新 found_langs
                            if key in discovered:
                                discovered[key]["found_langs"].add(lang)

                    log_fn(
                        f"  - 第 {page_num} 頁：共 {len(items)} 則連結，符合區間 {in_range_count} 則 "
                        f"(累計不重複文章：{len(discovered)} 則)"
                    )

                    # 嚴謹早停判斷：當本頁有明確日期的文章全部早於 start_date，且沒有任何符合區間的文章
                    if dated_count > 0 and in_range_count == 0 and older_count == dated_count:
                        consecutive_old_pages += 1
                        if consecutive_old_pages >= 2:
                            log_fn(f"  - 連續 {consecutive_old_pages} 頁文章皆早於 {start_date}，安全觸發早停。")
                            break
                    else:
                        consecutive_old_pages = 0

            except Exception as e:
                log_fn(f"  [錯誤] 掃描 {base_list_url} 時發生異常: {e}")
            finally:
                context.close()

    # 依日期由舊到新（或由新到舊）排序，同日期依 key 排序
    sorted_articles = sorted(
        discovered.values(),
        key=lambda x: (x["date"] or date.min, x["key"])
    )
    log_fn(f"\n[掃描完成] 在 {start_date} 至 {end_date} 區間內共找到 {len(sorted_articles)} 則不重複文章。")
    return sorted_articles


def verify_page_has_valid_article_content(page, lang: str) -> tuple[bool, date | None]:
    """
    驗證當前文章內頁是否已成功載入目標語系的正文內容，而非空白頁、404 或停留在列表頁。
    同時嘗試從內頁提取發佈日期作為雙重確認。
    """
    try:
        info = page.evaluate(
            """
            () => {
                const bodyText = (document.body ? document.body.innerText : '').trim();
                const currentUrl = window.location.href;
                // 尋找主要正文容器
                const mainSelectors = [
                    '.news-detail', '.chat-info', 'article', '.article-content',
                    '.detail-content', '.content-box', 'main'
                ];
                let contentLen = 0;
                for (const sel of mainSelectors) {
                    const el = document.querySelector(sel);
                    if (el && el.innerText) {
                        contentLen = Math.max(contentLen, el.innerText.trim().length);
                    }
                }
                if (contentLen === 0) {
                    contentLen = bodyText.length;
                }
                return {
                    url: currentUrl,
                    bodyText: bodyText.slice(0, 2000),
                    contentLen: contentLen
                };
            }
            """
        )

        url_now = info.get("url", "")
        body_text = info.get("bodyText", "")
        content_len = info.get("contentLen", 0)

        # 若被重導向回首頁或列表頁，視為無效
        if "news-detail" not in url_now and "chat-info" not in url_now:
            return False, None

        # 檢查常見的 404 / 無內容字樣
        error_keywords = ["404", "Page Not Found", "找不到頁面", "Página não encontrada"]
        for kw in error_keywords:
            if kw in body_text and content_len < 300:
                return False, None

        # 確認 URL 中的語系與目標語系一致 (避免 SPA 快取跳回其他語系)
        if f"/{lang}/" not in url_now:
            return False, None

        # 確認正文有實質內容
        if content_len < 80:
            return False, None

        extracted_date = parse_flexible_date(body_text)
        return True, extracted_date
    except Exception:
        return False, None


def try_switch_language_in_page(page, lang: str) -> bool:
    """
    當直接透過 URL 進入某語系被 SPA 重導向時，嘗試在頁面頂部點擊語系切換按鈕。
    """
    target_labels = {
        "zh": ["繁", "中", "繁體", "中文"],
        "pt": ["PT", "Português", "POR", "葡"],
        "en": ["EN", "English", "ENG", "英"]
    }[lang]

    try:
        switched = page.evaluate(
            """
            (labels) => {
                const elements = Array.from(document.querySelectorAll('a, button, span, li'));
                for (const el of elements) {
                    const txt = (el.innerText || '').trim();
                    if (labels.includes(txt)) {
                        el.click();
                        return true;
                    }
                }
                return false;
            }
            """,
            target_labels
        )
        if switched:
            time.sleep(2.0)
            return True
    except Exception:
        pass
    return False


def capture_single_language_pdf(
    browser,
    canonical_key: str,
    lang: str,
    output_pdf_path: str,
    max_retries: int = 3,
    log_fn=print
) -> bool:
    """
    針對單一文章的特定語系（zh / pt / en）進行獨立無痕環境載入、驗證與 PDF 截圖。
    具備多次重試與語系切換雙重保底機制，確保葡文與中文版不會因載入延遲而遺漏。
    """
    target_url = build_lang_url(canonical_key, lang)

    for attempt in range(1, max_retries + 1):
        # 每次重試皆使用全新無痕 Context，徹底隔離 localStorage / Cookie 語系干擾
        context = browser.new_context(
            locale="zh-TW" if lang == "zh" else ("pt-PT" if lang == "pt" else "en-US"),
            viewport={"width": 1280, "height": 1024}
        )
        page = context.new_page()

        try:
            wait_time = 2.0 + (attempt - 1) * 1.5
            page.goto(target_url, wait_until="domcontentloaded", timeout=35000)
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except PlaywrightTimeoutError:
                pass

            time.sleep(wait_time)

            # 若 URL 被 SPA 自動跳轉到其他語系，強制切換或重新導向
            if f"/{lang}/" not in page.url:
                try_switch_language_in_page(page, lang)
                if f"/{lang}/" not in page.url:
                    page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
                    time.sleep(wait_time)

            force_scroll_and_render(page, pause_sec=0.8)

            is_valid, _ = verify_page_has_valid_article_content(page, lang)
            if not is_valid:
                log_fn(f"    [重試 {attempt}/{max_retries}] {lang.upper()} 內容尚未完整渲染 ({target_url})，延長等待...")
                time.sleep(2.5)
                force_scroll_and_render(page, pause_sec=1.0)
                is_valid, _ = verify_page_has_valid_article_content(page, lang)

            if is_valid:
                # 注入列印優化樣式：展開隱藏區塊、避免圖片跨頁截斷、隱藏懸浮遮擋物
                page.add_style_tag(
                    content="""
                    @media print {
                        header, .fixed-top, .back-to-top, .cookie-banner { position: static !important; }
                        body, html { height: auto !important; overflow: visible !important; }
                    }
                    """
                )
                page.emulate_media(media="screen")
                page.pdf(
                    path=output_pdf_path,
                    format="A4",
                    print_background=True,
                    margin={"top": "10mm", "bottom": "10mm", "left": "10mm", "right": "10mm"}
                )

                if os.path.exists(output_pdf_path) and os.path.getsize(output_pdf_path) > 1024:
                    return True

        except Exception as e:
            log_fn(f"    [異常 {attempt}/{max_retries}] 擷取 {lang.upper()} ({target_url}) 失敗: {e}")
        finally:
            context.close()

    return False


def merge_pdfs(pdf_paths: list[str], output_path: str) -> bool:
    """合併多個 PDF 檔案為單一 PDF"""
    writer = PdfWriter()
    valid_count = 0
    for p in pdf_paths:
        if os.path.exists(p) and os.path.getsize(p) > 0:
            try:
                reader = PdfReader(p)
                for page in reader.pages:
                    writer.add_page(page)
                valid_count += 1
            except Exception as e:
                print(f"  [警告] 讀取 PDF 分片失敗 ({p}): {e}")

    if valid_count == 0:
        return False

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "wb") as f:
        writer.write(f)
    return True


def compress_pdf_with_ghostscript(input_pdf: str, output_pdf: str, target_max_mb: float = 5.0) -> str:
    """
    若系統安裝有 Ghostscript (gs / gswin64c)，將 PDF 壓縮至目標大小以下。
    若無安裝則直接回傳原檔案。
    """
    if not os.path.exists(input_pdf):
        return input_pdf

    size_mb = os.path.getsize(input_pdf) / (1024 * 1024)
    if size_mb <= target_max_mb:
        return input_pdf

    gs_cmd = shutil.which("gs") or shutil.which("gswin64c") or shutil.which("gswin32c")
    if not gs_cmd:
        print("  [提示] 未偵測到 Ghostscript，略過外部壓縮。")
        return input_pdf

    for setting in ["/ebook", "/screen"]:
        try:
            cmd = [
                gs_cmd,
                "-sDEVICE=pdfwrite",
                "-dCompatibilityLevel=1.4",
                f"-dPDFSETTINGS={setting}",
                "-dNOPAUSE",
                "-dQUIET",
                "-dBATCH",
                f"-sOutputFile={output_pdf}",
                input_pdf
            ]
            subprocess.run(cmd, check=True, timeout=180)
            if os.path.exists(output_pdf):
                new_size = os.path.getsize(output_pdf) / (1024 * 1024)
                print(f"  [壓縮完成] {setting} 模式：{size_mb:.2f} MB -> {new_size:.2f} MB")
                if new_size <= target_max_mb:
                    return output_pdf
        except Exception as e:
            print(f"  [警告] Ghostscript 壓縮失敗 ({setting}): {e}")

    return output_pdf if os.path.exists(output_pdf) else input_pdf


def run_smg_pdf_scraper(
    start_date: date,
    end_date: date,
    output_dir: str = "output_pdfs",
    headless: bool = True,
    log_fn=print
) -> dict:
    """
    主執行流程：
    1. 跨中、葡、英三語掃描目錄，確保選取範圍內所有文章無遺漏。
    2. 針對每篇文章逐一擷取 zh、pt、en 三個語系的 PDF。
    3. 若有單一語系初次擷取失敗，啟動二次延長等待補抓（Second-Pass Recovery）。
    4. 輸出每則新聞的三語合一 PDF，以及整份區間總合月報 PDF。
    """
    os.makedirs(output_dir, exist_ok=True)
    temp_dir = os.path.join(output_dir, "_temp_parts")
    os.makedirs(temp_dir, exist_ok=True)

    summary = {
        "total_articles": 0,
        "fully_complete_articles": 0,
        "missing_lang_records": [],
        "merged_report_path": None,
        "article_pdfs": []
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
        )

        # 第一階段：全語系目錄掃描並歸戶
        articles = discover_articles_across_languages(
            browser=browser,
            start_date=start_date,
            end_date=end_date,
            log_fn=log_fn
        )

        summary["total_articles"] = len(articles)
        all_article_merged_pdfs = []

        # 第二階段：逐篇擷取中、葡、英三語版本 PDF
        for idx, art in enumerate(articles, start=1):
            key = art["key"]
            art_date_str = art["date"].strftime("%Y-%m-%d") if art["date"] else "unknown-date"
            safe_key_id = re.sub(r'[^a-zA-Z0-9_-]', '_', key)
            log_fn(f"\n[{idx}/{len(articles)}] 處理文章: {art_date_str} | {key}")

            lang_pdf_map = {}
            missing_langs = []

            # 第一輪擷取：依序擷取 zh -> pt -> en
            for lang in LANGUAGES:
                part_path = os.path.join(temp_dir, f"{idx:03d}_{art_date_str}_{safe_key_id}_{lang}.pdf")
                log_fn(f"  -> 正在擷取 [{LANG_NAMES[lang]}] 版本: {build_lang_url(key, lang)}")
                ok = capture_single_language_pdf(
                    browser=browser,
                    canonical_key=key,
                    lang=lang,
                    output_pdf_path=part_path,
                    max_retries=3,
                    log_fn=log_fn
                )
                if ok:
                    lang_pdf_map[lang] = part_path
                    log_fn(f"     [成功] {lang.upper()} 版本已產生。")
                else:
                    missing_langs.append(lang)
                    log_fn(f"     [缺失] {lang.upper()} 版本初次擷取未成功，列入二次補抓清單。")

            # 第二輪二次補抓（Second-Pass Recovery）：針對缺失的語系延長等待再次嘗試
            if missing_langs:
                log_fn(f"  [二次補抓] 針對缺失語系 {missing_langs} 啟動深度重試...")
                still_missing = []
                for lang in missing_langs:
                    part_path = os.path.join(temp_dir, f"{idx:03d}_{art_date_str}_{safe_key_id}_{lang}.pdf")
                    ok = capture_single_language_pdf(
                        browser=browser,
                        canonical_key=key,
                        lang=lang,
                        output_pdf_path=part_path,
                        max_retries=4,
                        log_fn=log_fn
                    )
                    if ok:
                        lang_pdf_map[lang] = part_path
                        log_fn(f"     [補抓成功] {lang.upper()} 版本已成功補齊！")
                    else:
                        still_missing.append(lang)
                        log_fn(f"     [確認無內容] {lang.upper()} 經 7 次驗證仍無有效內容（可能官網未發佈該語系）。")
                missing_langs = still_missing

            if not missing_langs:
                summary["fully_complete_articles"] += 1
            else:
                summary["missing_lang_records"].append({
                    "index": idx,
                    "date": art_date_str,
                    "key": key,
                    "missing_langs": missing_langs
                })

            # 依照固定順序 (中文 -> 葡文 -> 英文) 合併該篇文章的 PDF
            ordered_parts = [lang_pdf_map[l] for l in LANGUAGES if l in lang_pdf_map]
            if ordered_parts:
                article_merged_path = os.path.join(
                    output_dir,
                    f"{idx:03d}_{art_date_str}_{safe_key_id}_zh_pt_en.pdf"
                )
                if merge_pdfs(ordered_parts, article_merged_path):
                    all_article_merged_pdfs.append(article_merged_path)
                    summary["article_pdfs"].append(article_merged_path)

        browser.close()

    # 第三階段：將所有單篇文章（已含中葡英）合併為總表 PDF 並壓縮
    if all_article_merged_pdfs:
        raw_total_pdf = os.path.join(
            output_dir,
            f"SMG_Report_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_RAW.pdf"
        )
        final_total_pdf = os.path.join(
            output_dir,
            f"SMG_Report_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.pdf"
        )
        if merge_pdfs(all_article_merged_pdfs, raw_total_pdf):
            compressed_path = compress_pdf_with_ghostscript(raw_total_pdf, final_total_pdf, target_max_mb=5.0)
            if compressed_path != final_total_pdf and os.path.exists(raw_total_pdf):
                shutil.copyfile(raw_total_pdf, final_total_pdf)
            summary["merged_report_path"] = final_total_pdf
            log_fn(f"\n[總表完成] 合併後總報告已儲存至: {final_total_pdf}")

    # 輸出完整性檢核報告
    log_fn("\n" + "=" * 55)
    log_fn(f"【中葡英三語 PDF 完整性檢核報告 ({start_date} ~ {end_date})】")
    log_fn(f"- 總文章數：{summary['total_articles']} 則")
    log_fn(f"- 三語完全齊全：{summary['fully_complete_articles']} 則")
    if summary["missing_lang_records"]:
        log_fn(f"- 官網缺少特定語系版本：{len(summary['missing_lang_records'])} 則")
        for rec in summary["missing_lang_records"]:
            log_fn(f"  * [{rec['date']}] {rec['key']} -> 缺少: {', '.join(rec['missing_langs'])}")
    else:
        log_fn("- 所有文章皆 100% 包含中、葡、英三語版本！")
    log_fn("=" * 55)

    return summary


def main():
    parser = argparse.ArgumentParser(description="SMG 氣象局新聞/資訊 中葡英三語完整 PDF 擷取工具")
    parser.add_argument("--start", type=str, help="起始日期 (YYYY-MM-DD)，預設為當月1號", default=None)
    parser.add_argument("--end", type=str, help="結束日期 (YYYY-MM-DD)，預設為今天", default=None)
    parser.add_argument("--out", type=str, help="輸出資料夾路徑", default="output_pdfs")
    parser.add_argument("--headed", action="store_true", help="顯示瀏覽器視窗以供除錯")
    args = parser.parse_args()

    today = date.today()
    start_dt = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else date(today.year, today.month, 1)
    end_dt = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else today

    run_smg_pdf_scraper(
        start_date=start_dt,
        end_date=end_dt,
        output_dir=args.out,
        headless=not args.headed
    )


if __name__ == "__main__":
    main()
