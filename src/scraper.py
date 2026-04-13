"""
scraper.py
─────────────────────────────────────────────────────────────────────────────
入札落札シグナル 検知スクレイパー

Source A : nikoukei.co.jp  全 67 ページ
Source B : nsearch.jp       直近 N 日分（Playwright）

検知したシグナルは Discord Webhook で通知する。
（DISCORD_WEBHOOK_URL を GitHub Secrets に登録しておくこと）
─────────────────────────────────────────────────────────────────────────────
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ── ローカルモジュール ──────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from company_mapper  import CompanyMapper
from nsearch_scraper import run_daily as nsearch_run_daily
from discord_notify  import notify_signals, notify_summary, notify_error

# ─────────────────────────────────────────────────────────────────────────────
# 定数・パス
# ─────────────────────────────────────────────────────────────────────────────
JST      = timezone(timedelta(hours=9))
ROOT_DIR = Path(__file__).parent.parent
SIG_DIR  = ROOT_DIR / "signals"
DATA_DIR = ROOT_DIR / "data"
STATE_F  = DATA_DIR / "seen_ids.json"

SIG_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

NIKOUKEI_BASE      = "https://www.nikoukei.co.jp"
NIKOUKEI_LIST      = f"{NIKOUKEI_BASE}/bid_result"
NIKOUKEI_MAX_PAGES = 67

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": UA, "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"}
SKIP_VALS = frozenset(["HP会員（無料）で金額表示", "さらに詳しい内容は無料IDでご確認ください。"])


# ─────────────────────────────────────────────────────────────────────────────
# 状態管理・重複除去
# ─────────────────────────────────────────────────────────────────────────────

def _dedup_key(winner: str, bid_date: str, project: str) -> str:
    return hashlib.md5(f"{winner}|{bid_date}|{project[:20]}".encode()).hexdigest()


def load_seen_ids() -> set:
    if STATE_F.exists():
        with open(STATE_F, encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def save_seen_ids(ids: set) -> None:
    with open(STATE_F, "w", encoding="utf-8") as f:
        json.dump(list(ids)[-20000:], f, ensure_ascii=False)


def load_past_dedup_keys() -> set:
    """過去シグナルファイルから重複チェック用キーを収集"""
    keys: set = set()
    for sf in SIG_DIR.glob("*.json"):
        try:
            for s in json.load(open(sf, encoding="utf-8")):
                keys.add(_dedup_key(
                    s.get("winner", ""),
                    s.get("bid_date", ""),
                    s.get("project_name", ""),
                ))
        except Exception:
            pass
    return keys


# ─────────────────────────────────────────────────────────────────────────────
# Source A: nikoukei.co.jp
# ─────────────────────────────────────────────────────────────────────────────

def _http_get(url: str, session: requests.Session, retries: int = 3) -> Optional[requests.Response]:
    for n in range(retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            logger.warning(f"  HTTP attempt {n+1}/{retries}: {e}")
            time.sleep(2 * (n + 1))
    return None


def _nik_list_page(page: int, session: requests.Session) -> list[dict]:
    resp = _http_get(f"{NIKOUKEI_LIST}?page={page}", session)
    if not resp:
        return []

    soup  = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    rows  = table.find_all("tr") if table else []
    seen_hrefs: set = set()
    items = []

    for row in rows:
        link = row.find("a", href=re.compile(r"/bid_result/detail/\d+"))
        if not link:
            continue
        href = link["href"]
        if href in seen_hrefs:
            continue
        seen_hrefs.add(href)

        m = re.search(r"/bid_result/detail/(\d+)", href)
        if not m:
            continue

        cells = [c.get_text(strip=True) for c in row.find_all("td")]
        items.append({
            "id":      f"nik_{m.group(1)}",
            "url":     NIKOUKEI_BASE + href,
            "source":  "nikoukei",
            "client":          cells[0] if len(cells) > 0 else "",
            "bid_date_preview":cells[1] if len(cells) > 1 else "",
            "winner_preview":  cells[3] if len(cells) > 3 else "",
        })
    return items


def _nik_detail(url: str, session: requests.Session) -> Optional[dict]:
    resp = _http_get(url, session)
    if not resp:
        return None

    lines = [l.strip() for l in
             BeautifulSoup(resp.text, "html.parser").get_text("\n").split("\n")
             if l.strip()]

    info: dict = {"url": url, "source": "nikoukei"}
    LABELS = {"発注者名": "client", "入札日": "bid_date",
               "工事件名": "project_name", "落札者": "winner", "発表日": "publish_date"}

    for i, line in enumerate(lines):
        if line in LABELS and i + 1 < len(lines) and lines[i+1] not in SKIP_VALS:
            info[LABELS[line]] = lines[i+1]

    return info if "winner" in info else None


def scrape_nikoukei(
    session: requests.Session,
    seen_ids: set,
    max_pages: int = NIKOUKEI_MAX_PAGES,
) -> tuple[list[dict], set]:

    raw: list[dict] = []
    new_ids: set    = set()
    zero_streak     = 0

    for page in range(1, max_pages + 1):
        logger.info(f"[nikoukei] page {page}/{max_pages}")
        items = _nik_list_page(page, session)
        if not items:
            logger.info(f"[nikoukei] empty page {page} — stop")
            break

        new_this = 0
        for item in items:
            if item["id"] in seen_ids:
                continue
            new_ids.add(item["id"])
            new_this += 1

            time.sleep(1.0)
            detail = _nik_detail(item["url"], session)
            if detail:
                if "bid_date" not in detail:
                    detail["bid_date"] = item["bid_date_preview"]
                if "winner" not in detail:
                    detail["winner"]   = item["winner_preview"]
                if "client" not in detail:
                    detail["client"]   = item["client"]
                raw.append(detail)

        logger.info(f"[nikoukei] page {page}: {new_this} new")
        zero_streak = 0 if new_this > 0 else zero_streak + 1
        if zero_streak >= 3:
            logger.info("[nikoukei] 3 consecutive empty pages — stop early")
            break

        time.sleep(2.0)

    return raw, new_ids


# ─────────────────────────────────────────────────────────────────────────────
# Source B: nsearch.jp  （nsearch_scraper.py 経由）
# ─────────────────────────────────────────────────────────────────────────────

def scrape_nsearch(days: int = 7) -> list[dict]:
    delta_file = DATA_DIR / "nsearch_delta.json"
    try:
        nsearch_run_daily(days=days)
        if delta_file.exists():
            items = json.load(open(delta_file, encoding="utf-8"))
            logger.info(f"[nsearch] {len(items)} items loaded")
            return items
    except Exception as e:
        msg = f"nsearch scrape failed: {e}"
        logger.error(msg)
        notify_error(msg, traceback.format_exc())
    return []


# ─────────────────────────────────────────────────────────────────────────────
# シグナル生成
# ─────────────────────────────────────────────────────────────────────────────

def build_signals(
    raw: list[dict],
    mapper: CompanyMapper,
    past_keys: set,
) -> tuple[list[dict], set]:

    signals: list[dict] = []
    new_keys: set = set()

    for item in raw:
        winner  = item.get("winner",       "").strip()
        project = item.get("project_name", "").strip()
        bid_date = item.get("bid_date",    "").strip()

        if not winner or not project:
            continue

        dk = _dedup_key(winner, bid_date, project)
        if dk in past_keys or dk in new_keys:
            logger.debug(f"dedup skip: {winner} / {project[:30]}")
            continue
        new_keys.add(dk)

        tickers = mapper.get_tickers(winner)
        if not tickers:
            logger.debug(f"no ticker: {winner}")
            continue

        now = datetime.now(JST).isoformat()
        for t in tickers:
            signals.append({
                "detected_at":  now,
                "source":       item.get("source", ""),
                "bid_date":     bid_date,
                "publish_date": item.get("publish_date", bid_date),
                "client":       item.get("client", ""),
                "project_name": project,
                "winner":       winner,
                "ticker_role":  t["role"],
                "company":      t["company"],
                "parent":       t["parent"],
                "ticker":       t["ticker"],
                "amount":       item.get("amount", ""),
                "source_url":   item.get("url", ""),
                "action":       "BUY",
                "timing":       "翌営業日 寄り付き成行",
                "exit_rule":    "+20%利確 / -15%ロスカット / 最大60日",
            })
            logger.info(
                f"🔔 [{t['role']}] {winner} → {t['company']} ({t['ticker']}) "
                f"| {project[:40]}"
            )

    return signals, new_keys


# ─────────────────────────────────────────────────────────────────────────────
# シグナル保存
# ─────────────────────────────────────────────────────────────────────────────

def save_signals(signals: list[dict]) -> None:
    if not signals:
        return
    today = datetime.now(JST).strftime("%Y-%m-%d")
    out   = SIG_DIR / f"{today}.json"
    existing = json.load(open(out, encoding="utf-8")) if out.exists() else []
    with open(out, "w", encoding="utf-8") as f:
        json.dump(existing + signals, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(signals)} signals → {out}")


# ─────────────────────────────────────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────────────────────────────────────

def run(nikoukei_pages: int = NIKOUKEI_MAX_PAGES, nsearch_days: int = 7) -> list[dict]:
    mapper    = CompanyMapper()
    seen_ids  = load_seen_ids()
    past_keys = load_past_dedup_keys()
    session   = requests.Session()

    all_raw: list[dict] = []
    new_ids:  set       = set()

    # ── Source A ──
    logger.info("=== [A] nikoukei.co.jp ===")
    try:
        nik_raw, nik_ids = scrape_nikoukei(session, seen_ids, nikoukei_pages)
        all_raw.extend(nik_raw)
        new_ids.update(nik_ids)
        logger.info(f"[nikoukei] {len(nik_raw)} raw")
    except Exception as e:
        msg = f"nikoukei scrape failed: {e}"
        logger.error(msg)
        notify_error(msg, traceback.format_exc())

    # ── Source B ──
    logger.info("=== [B] nsearch.jp ===")
    nsr_raw = scrape_nsearch(nsearch_days)
    all_raw.extend(nsr_raw)

    # ── シグナル生成 ──
    signals, new_keys = build_signals(all_raw, mapper, past_keys)

    # ── 保存・通知 ──
    save_signals(signals)
    seen_ids.update(new_ids)
    save_seen_ids(seen_ids)

    # Discord 通知
    if signals:
        notify_signals(signals)
    notify_summary(signals, nikoukei_pages, len(nsr_raw))

    return signals


# ─────────────────────────────────────────────────────────────────────────────
# CLI エントリーポイント
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="入札落札シグナル スクレイパー")
    parser.add_argument("--nikoukei-pages", type=int, default=NIKOUKEI_MAX_PAGES,
                        help=f"nikoukei スキャンページ数（デフォルト: {NIKOUKEI_MAX_PAGES}）")
    parser.add_argument("--nsearch-days",   type=int, default=7,
                        help="nsearch 直近日数（デフォルト: 7）")
    parser.add_argument("--quick", action="store_true",
                        help="クイックテスト（各3ページ / 2日）")
    args = parser.parse_args()

    if args.quick:
        args.nikoukei_pages = 3
        args.nsearch_days   = 2

    logger.info("=" * 60)
    logger.info("  入札落札シグナル スクレイパー START")
    logger.info(f"  nikoukei: {args.nikoukei_pages}ページ / nsearch: {args.nsearch_days}日分")
    logger.info("=" * 60)

    try:
        signals = run(args.nikoukei_pages, args.nsearch_days)
    except Exception as e:
        notify_error(f"スクレイパーが予期しないエラーで停止しました\n`{e}`", traceback.format_exc())
        raise

    if signals:
        print(f"\n✅ {len(signals)} シグナル検知（Discord に通知済み）:")
        for s in signals:
            role = "自社" if s["ticker_role"] == "self" else "親会社"
            print(f"  [{role}] {s['bid_date']} | {s['winner']} → "
                  f"{s['company']} ({s['ticker']}) | {s['project_name'][:40]}")
    else:
        print("✅ 新規シグナルなし（Discord にサマリー通知済み）")

    logger.info("  入札落札シグナル スクレイパー END")
