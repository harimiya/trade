"""
nsearch_scraper.py
─────────────────────────────────────────────────────────────────────────────
nsearch.jp 専用スクレイパー（Playwright 必須）

nsearch.jp は完全 JS レンダリングのため requests では内容が取得できない。
GitHub Actions では playwright install chromium で動作する。

モード:
  --mode history  : 過去5年分を全ページ取得（初回のみ実行）
  --mode daily    : 直近7日分のみ取得（毎日の差分スキャン）
  --mode range    : --from / --to で日付範囲を指定

出力:
  data/nsearch_YYYY.json   (history モード: 年別)
  data/nsearch_delta.json  (daily モード: 最新差分)
─────────────────────────────────────────────────────────────────────────────
"""

import argparse
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

JST = timezone(timedelta(hours=9))
ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

BASE_URL = "https://nsearch.jp/nyusatsu_ankens"
PER_PAGE = 100

# ─────────────────────────────────────────────────
# URL ビルダー
# ─────────────────────────────────────────────────

def build_url(
    page: int = 1,
    date_from: str = "",
    date_to: str = "",
    amount_from: float = 1_000_000_000,
) -> str:
    params = [
        "fulltext_target_fields_cd=0",
        "include_sanka_gyosha=true",
        f"per_page={PER_PAGE}",
        f"rakusatsu_kakaku_from={int(amount_from)}.0",
        "sort=rakusatsu_date_desc",
        f"page={page}",
    ]
    if date_from:
        params.append(f"rakusatsu_date_from={date_from}")
    if date_to:
        params.append(f"rakusatsu_date_to={date_to}")
    return BASE_URL + "?" + "&".join(params)


# ─────────────────────────────────────────────────
# HTML パーサー（Next.js __NEXT_DATA__ 解析）
# ─────────────────────────────────────────────────

def parse_page(html: str, source_url: str) -> tuple[list[dict], int]:
    """
    Returns (items, total_pages)
    items: [{winner, bid_date, project_name, client, amount, url}, ...]
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    items: list[dict] = []
    total_pages = 1

    # ── 方法1: __NEXT_DATA__ JSON を解析 ──
    next_data_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if next_data_tag and next_data_tag.string:
        try:
            nd = json.loads(next_data_tag.string)
            props = nd.get("props", {}).get("pageProps", {})

            # ページ総数を取得
            total_count = (
                props.get("totalCount")
                or props.get("total_count")
                or props.get("meta", {}).get("total", 0)
            )
            if total_count:
                total_pages = max(1, -(-int(total_count) // PER_PAGE))  # ceil div

            # 案件リストを取得（キー名はサイト実装に依存）
            ankens = (
                props.get("ankens")
                or props.get("nyusatsuAnkens")
                or props.get("data", {}).get("ankens", [])
                or []
            )

            # ネストしたケースも探索
            if not ankens:
                for key, val in props.items():
                    if isinstance(val, list) and len(val) > 0:
                        if isinstance(val[0], dict) and any(
                            k in val[0] for k in ("anken_name", "winner", "rakusatsu_sha", "project_name")
                        ):
                            ankens = val
                            break

            for a in ankens:
                winner = (
                    a.get("rakusatsu_gyosha_name")
                    or a.get("rakusatsu_sha")
                    or a.get("winner_name")
                    or a.get("winner", "")
                ).strip()

                bid_date = str(
                    a.get("rakusatsu_date")
                    or a.get("nyusatsu_date")
                    or a.get("bid_date", "")
                )[:10]

                project = (
                    a.get("anken_name")
                    or a.get("project_name")
                    or a.get("title", "")
                ).strip()

                client = (
                    a.get("hacchusha_name")
                    or a.get("hacchusha")
                    or a.get("client", "")
                ).strip()

                amount = str(
                    a.get("rakusatsu_kakaku")
                    or a.get("amount", "")
                )

                detail_url = a.get("url") or a.get("detail_url") or source_url

                if winner and project:
                    items.append({
                        "source": "nsearch",
                        "winner": winner,
                        "bid_date": bid_date,
                        "project_name": project,
                        "client": client,
                        "amount": amount,
                        "url": detail_url,
                    })

            if items:
                return items, total_pages

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.debug(f"__NEXT_DATA__ parse failed: {e}")

    # ── 方法2: テーブル/リスト HTML を直接パース ──
    # 案件行を探す
    for row in soup.find_all(["tr", "li", "div"], class_=re.compile(
        r"anken|nyusatsu|result|item|row|card|tender", re.I
    )):
        text = row.get_text("\n", strip=True)
        lines = [l for l in text.split("\n") if l.strip()]

        winner = client = bid_date = project = amount = ""
        for i, line in enumerate(lines):
            nxt = lines[i + 1] if i + 1 < len(lines) else ""
            if re.search(r"落札者|落札業者|受注者|落札企業", line):
                winner = nxt
            elif re.search(r"工事名|案件名|件名|工事件名", line):
                project = nxt
            elif re.search(r"落札日|入札日|公告日", line):
                bid_date = nxt[:10]
            elif re.search(r"発注者|発注機関|発注官庁", line):
                client = nxt
            elif re.search(r"落札金額|落札価格|契約金額", line):
                amount = nxt

        if winner and project:
            items.append({
                "source": "nsearch",
                "winner": winner,
                "bid_date": bid_date,
                "project_name": project,
                "client": client,
                "amount": amount,
                "url": source_url,
            })

    # ページネーション情報を取得
    pg_links = soup.find_all("a", href=re.compile(r"page=(\d+)"))
    if pg_links:
        max_pg = max(
            int(re.search(r"page=(\d+)", a["href"]).group(1))
            for a in pg_links if re.search(r"page=(\d+)", a.get("href", ""))
        )
        total_pages = max(total_pages, max_pg)

    return items, total_pages


# ─────────────────────────────────────────────────
# Playwright スクレイパー本体
# ─────────────────────────────────────────────────

def scrape_with_playwright(
    date_from: str = "",
    date_to: str = "",
    max_pages: int = 9999,
    page_wait_ms: int = 3000,
    page_interval_sec: float = 1.5,
) -> list[dict]:
    """
    Playwright で nsearch.jp を指定期間スクレイピング。
    date_from / date_to : "YYYY-MM-DD" 形式
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError

    all_items: list[dict] = []
    total_pages_detected = 1

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="ja-JP",
            extra_http_headers={"Accept-Language": "ja,en-US;q=0.9"},
        )
        page = ctx.new_page()

        for pg in range(1, min(max_pages, total_pages_detected) + 1):
            url = build_url(pg, date_from, date_to)
            logger.info(f"[nsearch] page {pg}/{total_pages_detected}  {url}")

            try:
                page.goto(url, wait_until="networkidle", timeout=30_000)
                # JS レンダリング完了を待つ
                page.wait_for_timeout(page_wait_ms)

                html = page.content()
                items, tp = parse_page(html, url)

                # 総ページ数の更新
                if pg == 1:
                    total_pages_detected = min(tp, max_pages)
                    logger.info(f"[nsearch] Total pages detected: {total_pages_detected}")

                all_items.extend(items)
                logger.info(f"[nsearch] page {pg}: {len(items)} items (cumulative: {len(all_items)})")

                if len(items) == 0 and pg > 1:
                    logger.info("[nsearch] Empty page, stopping")
                    break

            except PwTimeoutError:
                logger.warning(f"[nsearch] Timeout on page {pg}, retrying once")
                try:
                    page.wait_for_timeout(5000)
                    html = page.content()
                    items, _ = parse_page(html, url)
                    all_items.extend(items)
                except Exception as e2:
                    logger.error(f"[nsearch] Retry failed page {pg}: {e2}")

            except Exception as e:
                logger.error(f"[nsearch] Error on page {pg}: {e}")
                break

            time.sleep(page_interval_sec)

        browser.close()

    return all_items


# ─────────────────────────────────────────────────
# モード別エントリーポイント
# ─────────────────────────────────────────────────

def run_history(years: int = 5) -> None:
    """過去 N 年分を年単位で取得し data/nsearch_YYYY.json に保存"""
    today = datetime.now(JST)
    all_saved = 0

    for y in range(years):
        year_end = today.replace(
            year=today.year - y,
            month=12 if y > 0 else today.month,
            day=31 if y > 0 else today.day,
        )
        year_start = year_end.replace(
            year=year_end.year if y == 0 else year_end.year,
            month=1,
            day=1,
        )
        # 1年前のデータ
        date_to   = year_end.strftime("%Y-%m-%d")
        date_from = year_start.strftime("%Y-%m-%d")
        year_label = year_end.year

        out_file = DATA_DIR / f"nsearch_{year_label}.json"

        # 既にファイルがある場合はスキップ（増分取得のみ行う）
        if out_file.exists() and y > 0:
            logger.info(f"[nsearch history] {year_label} already exists, skipping")
            continue

        logger.info(f"[nsearch history] Scraping {year_label}: {date_from} ~ {date_to}")
        items = scrape_with_playwright(date_from=date_from, date_to=date_to)
        logger.info(f"[nsearch history] {year_label}: {len(items)} items")

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        all_saved += len(items)

    logger.info(f"[nsearch history] Complete: {all_saved} items saved")


def run_daily(days: int = 7) -> None:
    """直近 N 日分を取得し data/nsearch_delta.json に保存"""
    today = datetime.now(JST)
    date_to   = today.strftime("%Y-%m-%d")
    date_from = (today - timedelta(days=days)).strftime("%Y-%m-%d")

    logger.info(f"[nsearch daily] {date_from} ~ {date_to}")
    items = scrape_with_playwright(date_from=date_from, date_to=date_to, max_pages=20)

    out_file = DATA_DIR / "nsearch_delta.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    logger.info(f"[nsearch daily] {len(items)} items → {out_file}")


def run_range(date_from: str, date_to: str) -> None:
    """指定日付範囲をスクレイピング"""
    items = scrape_with_playwright(date_from=date_from, date_to=date_to)
    out_file = DATA_DIR / f"nsearch_{date_from}_{date_to}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    logger.info(f"[nsearch range] {len(items)} items → {out_file}")


def load_all_history() -> list[dict]:
    """保存済みの nsearch_*.json を全件読み込んで返す"""
    items = []
    for f in sorted(DATA_DIR.glob("nsearch_*.json")):
        try:
            with open(f, encoding="utf-8") as fp:
                data = json.load(fp)
                items.extend(data)
                logger.info(f"Loaded {len(data)} items from {f.name}")
        except Exception as e:
            logger.error(f"Failed to load {f}: {e}")
    return items


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="nsearch.jp scraper")
    parser.add_argument("--mode", choices=["history", "daily", "range"],
                        default="daily")
    parser.add_argument("--years", type=int, default=5,
                        help="history モード: 遡る年数")
    parser.add_argument("--days",  type=int, default=7,
                        help="daily モード: 遡る日数")
    parser.add_argument("--from",  dest="date_from", default="",
                        help="range モード: 開始日 YYYY-MM-DD")
    parser.add_argument("--to",    dest="date_to",   default="",
                        help="range モード: 終了日 YYYY-MM-DD")
    args = parser.parse_args()

    if args.mode == "history":
        run_history(args.years)
    elif args.mode == "daily":
        run_daily(args.days)
    elif args.mode == "range":
        if not args.date_from or not args.date_to:
            parser.error("--from と --to を指定してください")
        run_range(args.date_from, args.date_to)
