"""
nsearch_scraper.py
nsearch.jp 専用スクレイパー（Playwright 使用）

モード:
  --mode history  過去5年を年別取得 → data/nsearch_YYYY.json
  --mode daily    直近7日 → data/nsearch_delta.json
"""

import argparse
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)
JST      = timezone(timedelta(hours=9))
ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

BASE_URL = "https://nsearch.jp/nyusatsu_ankens"
PER_PAGE = 100


def _build_url(page: int = 1, date_from: str = "", date_to: str = "") -> str:
    params = [
        "fulltext_target_fields_cd=0",
        "include_sanka_gyosha=true",
        f"per_page={PER_PAGE}",
        "rakusatsu_kakaku_from=1000000000.0",
        "sort=rakusatsu_date_desc",
        f"page={page}",
    ]
    if date_from:
        params.append(f"rakusatsu_date_from={date_from}")
    if date_to:
        params.append(f"rakusatsu_date_to={date_to}")
    return BASE_URL + "?" + "&".join(params)


def _parse_html(html: str, url: str) -> tuple[list[dict], int]:
    """__NEXT_DATA__ または HTMLテーブルから案件を抽出"""
    from bs4 import BeautifulSoup

    soup  = BeautifulSoup(html, "html.parser")
    items: list[dict] = []
    total_pages = 1

    # ── Next.js ページデータ ──
    tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if tag and tag.string:
        try:
            nd   = json.loads(tag.string)
            props = nd.get("props", {}).get("pageProps", {})
            total = (props.get("totalCount") or
                     props.get("total_count") or
                     props.get("meta", {}).get("total", 0))
            if total:
                total_pages = max(1, -(-int(total) // PER_PAGE))

            ankens = (props.get("ankens") or
                      props.get("nyusatsuAnkens") or
                      props.get("data", {}).get("ankens", []) or [])
            if not ankens:
                for v in props.values():
                    if isinstance(v, list) and len(v) > 0:
                        if isinstance(v[0], dict) and any(
                            k in v[0] for k in ("anken_name","winner","rakusatsu_sha")
                        ):
                            ankens = v
                            break

            for a in ankens:
                winner  = (a.get("rakusatsu_gyosha_name") or a.get("rakusatsu_sha") or "").strip()
                bid_date = str(a.get("rakusatsu_date") or a.get("nyusatsu_date") or "")[:10]
                project  = (a.get("anken_name") or a.get("title") or "").strip()
                client   = (a.get("hacchusha_name") or a.get("hacchusha") or "").strip()
                amount   = str(a.get("rakusatsu_kakaku") or "")
                if winner and project:
                    items.append({"source": "nsearch", "winner": winner, "bid_date": bid_date,
                                  "project_name": project, "client": client, "amount": amount, "url": url})
            if items:
                return items, total_pages
        except Exception:
            pass

    # ── HTMLフォールバック ──
    for row in soup.find_all(class_=re.compile(r"anken|nyusatsu|result|item|card|tender", re.I)):
        text  = row.get_text("\n", strip=True)
        lines = [l for l in text.split("\n") if l.strip()]
        winner = client = bid_date = project = amount = ""
        for i, line in enumerate(lines):
            nxt = lines[i+1] if i+1 < len(lines) else ""
            if re.search(r"落札者|落札業者|受注者", line):   winner   = nxt
            elif re.search(r"工事名|案件名|件名",   line):   project  = nxt
            elif re.search(r"落札日|入札日",        line):   bid_date = nxt[:10]
            elif re.search(r"発注者|発注機関",       line):   client   = nxt
            elif re.search(r"落札金額|落札価格",     line):   amount   = nxt
        if winner and project:
            items.append({"source": "nsearch", "winner": winner, "bid_date": bid_date,
                          "project_name": project, "client": client, "amount": amount, "url": url})
    return items, total_pages


def scrape(date_from: str = "", date_to: str = "", max_pages: int = 9999) -> list[dict]:
    """Playwright で nsearch.jp をスクレイピング"""
    from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

    all_items: list[dict] = []
    detected_pages = 1

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx  = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="ja-JP",
        )
        page = ctx.new_page()

        for pg in range(1, min(max_pages, detected_pages) + 1):
            url = _build_url(pg, date_from, date_to)
            logger.info(f"[nsearch] page {pg}/{detected_pages}  {url[:80]}")
            try:
                page.goto(url, wait_until="networkidle", timeout=30_000)
                page.wait_for_timeout(2500)
                html  = page.content()
                items, tp = _parse_html(html, url)
                if pg == 1:
                    detected_pages = min(tp, max_pages)
                    logger.info(f"[nsearch] total pages: {detected_pages}")
                all_items.extend(items)
                if not items and pg > 1:
                    break
            except PwTimeout:
                logger.warning(f"[nsearch] timeout page {pg}")
            except Exception as e:
                logger.error(f"[nsearch] error page {pg}: {e}")
                break
            time.sleep(1.5)

        browser.close()

    logger.info(f"[nsearch] collected {len(all_items)} items")
    return all_items


def run_daily(days: int = 7) -> None:
    now = datetime.now(JST)
    date_to   = now.strftime("%Y-%m-%d")
    date_from = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    items = scrape(date_from=date_from, date_to=date_to, max_pages=20)
    out   = DATA_DIR / "nsearch_delta.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    logger.info(f"[nsearch daily] {len(items)} items → {out}")


def run_history(years: int = 5) -> None:
    now = datetime.now(JST)
    for y in range(years):
        yr    = now.year - y
        df    = f"{yr}-01-01"
        dt    = f"{yr}-12-31" if y > 0 else now.strftime("%Y-%m-%d")
        out_f = DATA_DIR / f"nsearch_{yr}.json"
        if out_f.exists() and y > 0:
            logger.info(f"[nsearch history] {yr} already exists, skip")
            continue
        logger.info(f"[nsearch history] {yr}: {df} ~ {dt}")
        items = scrape(date_from=df, date_to=dt)
        with open(out_f, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        logger.info(f"[nsearch history] {yr}: {len(items)} items saved")


def load_all_history() -> list[dict]:
    items = []
    for f in sorted(DATA_DIR.glob("nsearch_2*.json")):
        try:
            data = json.load(open(f, encoding="utf-8"))
            items.extend(data)
        except Exception:
            pass
    return items


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",  choices=["history", "daily"], default="daily")
    parser.add_argument("--years", type=int, default=5)
    parser.add_argument("--days",  type=int, default=7)
    args = parser.parse_args()
    if args.mode == "history":
        run_history(args.years)
    else:
        run_daily(args.days)
