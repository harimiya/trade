"""
scraper.py  ─  入札落札シグナル スクレイパー

ソース:
  1. nikoukei.co.jp/bid_result/  … 1〜3ページ
  2. nsearch.jp                  … 直近3日分（Playwright）

戦略A:
  エントリー : 翌営業日 寄り付き 成行買い
  利確       : +20%
  ロスカット : -15%
  最大保有   : 60営業日（強制決済）
"""

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

sys.path.insert(0, str(Path(__file__).parent))
from company_mapper import CompanyMapper
from discord_notify import notify_signals, notify_summary, notify_error

# ── 定数 ───────────────────────────────────────────────────────────────────
JST      = timezone(timedelta(hours=9))
ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data"
SIG_DIR  = ROOT_DIR / "signals"
STATE_F  = DATA_DIR / "seen_ids.json"

DATA_DIR.mkdir(exist_ok=True)
SIG_DIR.mkdir(exist_ok=True)

NIKOUKEI_BASE  = "https://www.nikoukei.co.jp"
NIKOUKEI_LIST  = f"{NIKOUKEI_BASE}/bid_result"
NIKOUKEI_PAGES = 3   # ← 3ページまで

NSEARCH_BASE   = "https://nsearch.jp/nyusatsu_ankens"
NSEARCH_DAYS   = 3   # ← 直近3日分
NSEARCH_PER_PAGE = 100

SKIP_VALS = frozenset([
    "HP会員（無料）で金額表示",
    "さらに詳しい内容は無料IDでご確認ください。",
])
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": UA, "Accept-Language": "ja,en-US;q=0.9"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
# 状態管理・重複除去
# ══════════════════════════════════════════════════════════════════

def _dedup_key(winner: str, bid_date: str, project: str) -> str:
    raw = f"{winner}|{bid_date}|{project[:20]}"
    return hashlib.md5(raw.encode()).hexdigest()


def load_seen_ids() -> set:
    if STATE_F.exists():
        with open(STATE_F, encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def save_seen_ids(ids: set) -> None:
    with open(STATE_F, "w", encoding="utf-8") as f:
        json.dump(list(ids)[-20000:], f, ensure_ascii=False)


def load_past_dedup_keys() -> set:
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


# ══════════════════════════════════════════════════════════════════
# HTTP ユーティリティ
# ══════════════════════════════════════════════════════════════════

def _get(url: str, session: requests.Session,
         retries: int = 3) -> Optional[requests.Response]:
    for n in range(retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            logger.warning(f"  HTTP {n+1}/{retries}: {e}")
            time.sleep(2 * (n + 1))
    return None


# ══════════════════════════════════════════════════════════════════
# Source 1: nikoukei.co.jp（3ページ）
# ══════════════════════════════════════════════════════════════════

def _nik_list_page(page: int, session: requests.Session) -> list[dict]:
    """一覧ページ1枚から入札案件のURLリストを取得"""
    resp = _get(f"{NIKOUKEI_LIST}?page={page}", session)
    if not resp:
        return []

    soup  = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    rows  = table.find_all("tr") if table else []
    items = []
    seen: set = set()

    for row in rows:
        link = row.find("a", href=re.compile(r"/bid_result/detail/\d+"))
        if not link:
            continue
        href = link["href"]
        if href in seen:
            continue
        seen.add(href)

        m = re.search(r"/bid_result/detail/(\d+)", href)
        if not m:
            continue

        cells = [c.get_text(strip=True) for c in row.find_all("td")]
        items.append({
            "id":               f"nik_{m.group(1)}",
            "url":              NIKOUKEI_BASE + href,
            "source":           "nikoukei",
            "client":           cells[0] if len(cells) > 0 else "",
            "bid_date_preview": cells[1] if len(cells) > 1 else "",
            "winner_preview":   cells[3] if len(cells) > 3 else "",
        })

    return items


def _nik_detail(url: str, session: requests.Session) -> Optional[dict]:
    """詳細ページから落札者・発注者・工事件名・入札日を取得"""
    resp = _get(url, session)
    if not resp:
        return None

    lines = [
        l.strip()
        for l in BeautifulSoup(resp.text, "html.parser").get_text("\n").split("\n")
        if l.strip()
    ]
    info: dict = {"url": url, "source": "nikoukei"}
    LABELS = {
        "発注者名": "client",
        "入札日":   "bid_date",
        "工事件名": "project_name",
        "落札者":   "winner",
        "発表日":   "publish_date",
    }
    for i, line in enumerate(lines):
        if line in LABELS and i + 1 < len(lines) and lines[i + 1] not in SKIP_VALS:
            info[LABELS[line]] = lines[i + 1]

    return info if "winner" in info else None


def scrape_nikoukei(session: requests.Session,
                    seen_ids: set) -> tuple[list[dict], set]:
    """nikoukei を3ページスキャン。新規案件の詳細を取得して返す。"""
    raw: list[dict] = []
    new_ids: set    = set()

    for page in range(1, NIKOUKEI_PAGES + 1):
        logger.info(f"[nikoukei] page {page}/{NIKOUKEI_PAGES}")
        items = _nik_list_page(page, session)

        if not items:
            logger.info(f"[nikoukei] page {page}: no items")
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
                # プレビュー情報で補完
                detail.setdefault("bid_date", item["bid_date_preview"])
                detail.setdefault("winner",   item["winner_preview"])
                detail.setdefault("client",   item["client"])
                raw.append(detail)

        logger.info(f"[nikoukei] page {page}: {new_this} new items")
        time.sleep(2.0)

    return raw, new_ids


# ══════════════════════════════════════════════════════════════════
# Source 2: nsearch.jp（直近3日 / Playwright）
# ══════════════════════════════════════════════════════════════════

def _nsearch_parse(html: str, url: str) -> tuple[list[dict], int]:
    """nsearch.jp のHTMLから案件リストとページ数を抽出"""
    from bs4 import BeautifulSoup

    soup       = BeautifulSoup(html, "html.parser")
    items: list[dict] = []
    total_pages = 1

    # ── Next.js __NEXT_DATA__ ──
    tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if tag and tag.string:
        try:
            nd    = json.loads(tag.string)
            props = nd.get("props", {}).get("pageProps", {})
            total = (props.get("totalCount") or
                     props.get("total_count") or
                     props.get("meta", {}).get("total", 0))
            if total:
                total_pages = max(1, -(-int(total) // NSEARCH_PER_PAGE))

            ankens = (props.get("ankens") or
                      props.get("nyusatsuAnkens") or
                      props.get("data", {}).get("ankens", []) or [])
            if not ankens:
                for v in props.values():
                    if (isinstance(v, list) and len(v) > 0 and
                            isinstance(v[0], dict) and
                            any(k in v[0] for k in ("anken_name", "winner", "rakusatsu_sha"))):
                        ankens = v
                        break

            for a in ankens:
                winner   = (a.get("rakusatsu_gyosha_name") or a.get("rakusatsu_sha") or "").strip()
                bid_date = str(a.get("rakusatsu_date") or a.get("nyusatsu_date") or "")[:10]
                project  = (a.get("anken_name") or a.get("title") or "").strip()
                client   = (a.get("hacchusha_name") or a.get("hacchusha") or "").strip()
                amount   = str(a.get("rakusatsu_kakaku") or "")
                if winner and project:
                    items.append({
                        "source": "nsearch", "winner": winner, "bid_date": bid_date,
                        "project_name": project, "client": client,
                        "amount": amount, "url": url,
                    })
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
            nxt = lines[i + 1] if i + 1 < len(lines) else ""
            if re.search(r"落札者|落札業者|受注者", line):   winner   = nxt
            elif re.search(r"工事名|案件名|件名",   line):   project  = nxt
            elif re.search(r"落札日|入札日",        line):   bid_date = nxt[:10]
            elif re.search(r"発注者|発注機関",       line):   client   = nxt
            elif re.search(r"落札金額|落札価格",     line):   amount   = nxt
        if winner and project:
            items.append({
                "source": "nsearch", "winner": winner, "bid_date": bid_date,
                "project_name": project, "client": client, "amount": amount, "url": url,
            })

    return items, total_pages


def scrape_nsearch() -> list[dict]:
    """
    Playwright で nsearch.jp を直近3日分スクレイピング。
    Playwright 未インストール時は requests でフォールバック。
    """
    now       = datetime.now(JST)
    date_to   = now.strftime("%Y-%m-%d")
    date_from = (now - timedelta(days=NSEARCH_DAYS)).strftime("%Y-%m-%d")

    def _build_url(page: int) -> str:
        return (
            f"{NSEARCH_BASE}"
            f"?fulltext_target_fields_cd=0"
            f"&include_sanka_gyosha=true"
            f"&per_page={NSEARCH_PER_PAGE}"
            f"&rakusatsu_date_from={date_from}"
            f"&rakusatsu_date_to={date_to}"
            f"&sort=rakusatsu_date_desc"
            f"&page={page}"
        )

    all_items: list[dict] = []

    # ── Playwright ──
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
            ctx  = browser.new_context(
                user_agent=UA, locale="ja-JP",
            )
            page = ctx.new_page()
            detected_pages = 1

            for pg in range(1, detected_pages + 1):
                url = _build_url(pg)
                logger.info(f"[nsearch/playwright] page {pg}/{detected_pages}")
                try:
                    page.goto(url, wait_until="networkidle", timeout=30_000)
                    page.wait_for_timeout(2500)
                    html  = page.content()
                    items, tp = _nsearch_parse(html, url)
                    if pg == 1:
                        detected_pages = tp
                    all_items.extend(items)
                    if not items and pg > 1:
                        break
                except PwTimeout:
                    logger.warning(f"[nsearch] timeout page {pg}")
                    break
                except Exception as e:
                    logger.error(f"[nsearch] error page {pg}: {e}")
                    break
                time.sleep(1.5)

            browser.close()

        logger.info(f"[nsearch] {len(all_items)} items (Playwright)")
        return all_items

    except ImportError:
        logger.info("[nsearch] Playwright not available — using requests fallback")

    # ── requests フォールバック ──
    session = requests.Session()
    detected_pages = 1
    for pg in range(1, detected_pages + 1):
        url  = _build_url(pg)
        resp = session.get(url, headers=HEADERS, timeout=25)
        if resp.status_code != 200:
            break
        items, tp = _nsearch_parse(resp.text, url)
        if pg == 1:
            detected_pages = tp
        all_items.extend(items)
        if not items and pg > 1:
            break
        time.sleep(1.5)

    logger.info(f"[nsearch] {len(all_items)} items (requests)")
    return all_items


# ══════════════════════════════════════════════════════════════════
# シグナル生成（戦略A）
# ══════════════════════════════════════════════════════════════════

def build_signals(
    raw:       list[dict],
    mapper:    CompanyMapper,
    past_keys: set,
) -> tuple[list[dict], set]:
    """入札情報リスト → 戦略Aシグナルリスト（重複除去済み）"""
    signals:  list[dict] = []
    new_keys: set        = set()

    for item in raw:
        winner   = item.get("winner",       "").strip()
        project  = item.get("project_name", "").strip()
        bid_date = item.get("bid_date",     "").strip()

        if not winner or not project:
            continue

        dk = _dedup_key(winner, bid_date, project)
        if dk in past_keys or dk in new_keys:
            logger.debug(f"dedup skip: {winner} / {project[:30]}")
            continue
        new_keys.add(dk)

        tickers = mapper.get_tickers(winner)
        if not tickers:
            logger.debug(f"no ticker mapping: {winner}")
            continue

        now = datetime.now(JST).isoformat()
        for t in tickers:
            signals.append({
                "detected_at":   now,
                "source":        item.get("source", ""),
                "bid_date":      bid_date,
                "publish_date":  item.get("publish_date", bid_date),
                "client":        item.get("client", ""),
                "project_name":  project,
                "winner":        winner,
                "ticker_role":   t["role"],
                "company":       t["company"],
                "parent":        t["parent"],
                "ticker":        t["ticker"],
                "amount":        item.get("amount", ""),
                "source_url":    item.get("url", ""),
                # 戦略Aパラメータ
                "strategy":      "A",
                "action":        "BUY",
                "timing":        "翌営業日 寄り付き成行",
                "tp_pct":        20,
                "sl_pct":        -15,
                "max_hold_days": 60,
                "exit_rule":     "+20%利確 / -15%ロスカット / 最大60営業日",
            })
            logger.info(
                f"🔔 [{t['role']}] {winner} → {t['company']} "
                f"({t['ticker']}) | {project[:45]}"
            )

    return signals, new_keys


def save_signals(signals: list[dict]) -> None:
    if not signals:
        return
    today = datetime.now(JST).strftime("%Y-%m-%d")
    out   = SIG_DIR / f"{today}.json"
    existing = []
    if out.exists():
        with open(out, encoding="utf-8") as f:
            existing = json.load(f)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(existing + signals, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(signals)} signals → {out}")


# ══════════════════════════════════════════════════════════════════
# メイン
# ══════════════════════════════════════════════════════════════════

def run() -> list[dict]:
    mapper    = CompanyMapper()
    seen_ids  = load_seen_ids()
    past_keys = load_past_dedup_keys()
    session   = requests.Session()

    all_raw: list[dict] = []
    all_new_ids: set    = set()

    # ── Source 1: nikoukei（3ページ）──────────────────────────────
    logger.info("=== [1] nikoukei.co.jp（3ページ）===")
    try:
        nik_raw, nik_ids = scrape_nikoukei(session, seen_ids)
        all_raw.extend(nik_raw)
        all_new_ids.update(nik_ids)
        logger.info(f"[nikoukei] {len(nik_raw)} raw items")
    except Exception as e:
        notify_error(f"nikoukei エラー: {e}", traceback.format_exc())

    # ── Source 2: nsearch（直近3日）────────────────────────────────
    logger.info("=== [2] nsearch.jp（直近3日）===")
    try:
        nsr_raw = scrape_nsearch()
        all_raw.extend(nsr_raw)
        logger.info(f"[nsearch] {len(nsr_raw)} raw items")
    except Exception as e:
        notify_error(f"nsearch エラー: {e}", traceback.format_exc())
        nsr_raw = []

    # ── シグナル生成 ───────────────────────────────────────────────
    signals, new_keys = build_signals(all_raw, mapper, past_keys)

    # ── 保存 ──────────────────────────────────────────────────────
    save_signals(signals)
    seen_ids.update(all_new_ids)
    save_seen_ids(seen_ids)

    # ── Discord 通知 ───────────────────────────────────────────────
    if signals:
        notify_signals(signals)
    notify_summary(signals,
                   nikoukei_pages=NIKOUKEI_PAGES,
                   nsearch_items=len(nsr_raw) if "nsr_raw" in dir() else 0)

    return signals


if __name__ == "__main__":
    logger.info("=" * 55)
    logger.info("  入札落札シグナル スクレイパー 【戦略A】 START")
    logger.info(f"  nikoukei: {NIKOUKEI_PAGES}ページ / nsearch: 直近{NSEARCH_DAYS}日")
    logger.info("=" * 55)

    try:
        signals = run()
    except Exception as e:
        notify_error(f"スクレイパー致命的エラー: {e}", traceback.format_exc())
        raise

    if signals:
        print(f"\n✅ {len(signals)} シグナル（Discord 通知済み）")
        for s in signals:
            role = "自社" if s["ticker_role"] == "self" else "親会社"
            print(f"  [{role}] {s['bid_date']} | {s['winner']} → "
                  f"{s['company']} ({s['ticker']}) | {s['project_name'][:40]}")
    else:
        print("✅ 新規シグナルなし（Discord にサマリー通知済み）")

    logger.info("  入札落札シグナル スクレイパー END")
