"""
scraper.py  ─  入札落札シグナル スクレイパー（全20ソース / 直近3日）

ソース:
  1.  nikoukei    日本工業経済新聞社      nikoukei.co.jp/bid_result/
  2.  nsearch      エヌサーチ              nsearch.jp（Playwright）
  3.  p_portal     調達ポータル            p-portal.go.jp（差分CSV）
  4.  meti         経済産業省              meti.go.jp
  5.  kkj          官公需情報ポータル       kkj.go.jp
  6.  mlit_kanto   国交省関東整備局         ktr.mlit.go.jp
  7.  mod          防衛省・防衛装備庁       mod.go.jp
  8.  mof          財務省                  mof.go.jp
  9.  mlit_kyu     国交省九州整備局         qsr.mlit.go.jp
  10. mlit_chu     国交省中国整備局         cgr.mlit.go.jp
  11. nexco_e      NEXCO東日本             e-nexco.co.jp
  12. nexco_c      NEXCO中日本             c-nexco.co.jp
  13. nexco_w      NEXCO西日本             w-nexco.co.jp
  14. tokyo        東京都電子調達           e-procurement.metro.tokyo.lg.jp
  15. osaka        大阪府電子調達           pref.osaka.lg.jp
  16. ipa          IPA情報処理推進機構      ipa.go.jp
  17. jrtt         鉄道・運輸機構(JRTT)    jrtt.go.jp
  18. ur           UR都市機構              ur-net.go.jp
  19. water        水資源機構              water.go.jp
  20. mlit_tohoku  国交省東北整備局         thr.mlit.go.jp

取得範囲: 直近3日分 / nikoukei は3ページまで
戦略A: 翌営業日寄り付き成行 / +20%利確 / -15%ロスカット / 最大60営業日
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

# ── 定数 ──────────────────────────────────────────────────────────────────
JST      = timezone(timedelta(hours=9))
ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data"
SIG_DIR  = ROOT_DIR / "signals"
STATE_F  = DATA_DIR / "seen_ids.json"

DATA_DIR.mkdir(exist_ok=True)
SIG_DIR.mkdir(exist_ok=True)

NIKOUKEI_BASE  = "https://www.nikoukei.co.jp"
NIKOUKEI_LIST  = f"{NIKOUKEI_BASE}/bid_result"
NIKOUKEI_PAGES = 3

NSEARCH_BASE   = "https://nsearch.jp/nyusatsu_ankens"
NSEARCH_DAYS   = 3
NSEARCH_PER    = 100

RECENT_DAYS    = 3   # 他ソース共通: 直近N日以内のみ対象

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
HEADERS  = {"User-Agent": UA, "Accept-Language": "ja,en-US;q=0.9"}
SKIP_SET = frozenset([
    "HP会員（無料）で金額表示",
    "さらに詳しい内容は無料IDでご確認ください。",
])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
# ユーティリティ
# ══════════════════════════════════════════════════════════════════

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


def _to_abs_url(href: str, base: str) -> str:
    """
    ★ バグ修正箇所
    href が既に https:// / http:// で始まる場合はそのまま返す。
    相対パスのみ base を前置する。
    これにより 'www.nikoukei.co.jphttps://...' という壊れたURLを防ぐ。
    """
    href = (href or "").strip()
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return base.rstrip("/") + href
    return base.rstrip("/") + "/" + href


def _get(url: str, session: requests.Session,
         retries: int = 3) -> Optional[requests.Response]:
    for n in range(retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            logger.warning(f"  HTTP {n+1}/{retries}: {e}")
            time.sleep(2 * (n + 1))
    return None


def _is_recent(date_str: str, days: int = RECENT_DAYS) -> bool:
    """date_str が直近 N 日以内なら True（日付不明は True）"""
    if not date_str:
        return True
    try:
        ds = date_str.replace("/", "-")[:10]
        dt = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=JST)
        return (datetime.now(JST) - dt).days <= days
    except ValueError:
        return True


def _parse_table(html: str, source: str, url: str,
                 winner_col: int = 2, project_col: int = 1,
                 date_col: int = 0, client_col: int = -1) -> list[dict]:
    """汎用テーブルパーサー（直近3日分のみ返す）"""
    soup  = BeautifulSoup(html, "html.parser")
    items = []
    for table in soup.find_all("table"):
        for row in table.find_all("tr")[1:]:
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if len(cells) <= max(winner_col, project_col):
                continue
            winner  = cells[winner_col]  if winner_col  < len(cells) else ""
            project = cells[project_col] if project_col < len(cells) else ""
            date    = cells[date_col]    if date_col    < len(cells) else ""
            client  = cells[client_col]  if 0 <= client_col < len(cells) else ""
            if winner and project and len(winner) >= 2 and _is_recent(date):
                items.append({
                    "source": source, "winner": winner,
                    "bid_date": date[:10], "project_name": project,
                    "client": client, "amount": "", "url": url,
                })
    return items


def _scrape_url(session: requests.Session, url: str, source: str,
                winner_col: int = 2, project_col: int = 1,
                date_col: int = 0, client_col: int = -1) -> list[dict]:
    resp = _get(url, session)
    if not resp:
        logger.info(f"[{source}] 0 items (no response)")
        return []
    items = _parse_table(resp.text, source, url,
                         winner_col=winner_col, project_col=project_col,
                         date_col=date_col, client_col=client_col)
    logger.info(f"[{source}] {len(items)} items")
    return items


# ══════════════════════════════════════════════════════════════════
# Source 1: nikoukei.co.jp（3ページ / URLバグ修正済み）
# ══════════════════════════════════════════════════════════════════

def scrape_nikoukei(session: requests.Session,
                    seen_ids: set) -> tuple[list[dict], set]:
    raw: list[dict] = []
    new_ids: set    = set()

    for page in range(1, NIKOUKEI_PAGES + 1):
        logger.info(f"[nikoukei] page {page}/{NIKOUKEI_PAGES}")
        resp = _get(f"{NIKOUKEI_LIST}?page={page}", session)
        if not resp:
            break

        soup  = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table:
            break

        new_this = 0
        for row in table.find_all("tr")[1:]:
            link = row.find("a", href=re.compile(r"/bid_result/detail/\d+"))
            if not link:
                continue

            href = link.get("href", "")

            # ★ 修正: フルURL・相対パス両対応
            detail_url = _to_abs_url(href, NIKOUKEI_BASE)

            m = re.search(r"/bid_result/detail/(\d+)", href)
            if not m:
                continue

            bid_id = f"nik_{m.group(1)}"
            if bid_id in seen_ids:
                continue
            new_ids.add(bid_id)

            # テーブルセルからプレビュー情報を取得
            cells = [c.get_text(strip=True) for c in row.find_all("td")]
            bid_date_preview = cells[1] if len(cells) > 1 else ""
            winner_preview   = cells[3] if len(cells) > 3 else ""
            client_preview   = cells[0] if len(cells) > 0 else ""

            # 日付チェック（直近3日以内のみ）
            if not _is_recent(bid_date_preview):
                continue

            new_this += 1
            time.sleep(1.0)

            # 詳細ページを取得
            detail_resp = _get(detail_url, session)
            if not detail_resp:
                # 取得失敗時はプレビュー情報だけで登録
                if winner_preview:
                    raw.append({
                        "source":       "nikoukei",
                        "winner":       winner_preview,
                        "bid_date":     bid_date_preview,
                        "project_name": link.get_text(strip=True),
                        "client":       client_preview,
                        "amount":       "",
                        "url":          detail_url,
                    })
                continue

            lines = [
                l.strip()
                for l in BeautifulSoup(
                    detail_resp.text, "html.parser"
                ).get_text("\n").split("\n")
                if l.strip()
            ]
            info: dict = {
                "source":   "nikoukei",
                "url":      detail_url,
                "bid_date": bid_date_preview,
                "winner":   winner_preview,
                "client":   client_preview,
            }
            LABELS = {
                "発注者名": "client",
                "入札日":   "bid_date",
                "工事件名": "project_name",
                "落札者":   "winner",
                "発表日":   "publish_date",
            }
            for i, line in enumerate(lines):
                if line in LABELS and i + 1 < len(lines) and lines[i+1] not in SKIP_SET:
                    info[LABELS[line]] = lines[i+1]

            if info.get("winner"):
                raw.append(info)

        logger.info(f"[nikoukei] page {page}: {new_this} new")
        time.sleep(2.0)

    logger.info(f"[nikoukei] total {len(raw)} items")
    return raw, new_ids


# ══════════════════════════════════════════════════════════════════
# Source 2: nsearch.jp（直近3日 / Playwright + requests）
# ══════════════════════════════════════════════════════════════════

def scrape_nsearch() -> list[dict]:
    now       = datetime.now(JST)
    date_to   = now.strftime("%Y-%m-%d")
    date_from = (now - timedelta(days=NSEARCH_DAYS)).strftime("%Y-%m-%d")

    def _url(page: int) -> str:
        return (
            f"{NSEARCH_BASE}"
            f"?fulltext_target_fields_cd=0"
            f"&include_sanka_gyosha=true"
            f"&per_page={NSEARCH_PER}"
            f"&rakusatsu_date_from={date_from}"
            f"&rakusatsu_date_to={date_to}"
            f"&sort=rakusatsu_date_desc"
            f"&page={page}"
        )

    def _parse(html: str, url: str) -> tuple[list[dict], int]:
        soup  = BeautifulSoup(html, "html.parser")
        items: list[dict] = []
        total_pages = 1

        # __NEXT_DATA__ から取得
        tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if tag and tag.string:
            try:
                nd    = json.loads(tag.string)
                props = nd.get("props", {}).get("pageProps", {})
                total = (props.get("totalCount") or
                         props.get("total_count") or
                         props.get("meta", {}).get("total", 0))
                if total:
                    total_pages = max(1, -(-int(total) // NSEARCH_PER))

                ankens = props.get("ankens") or props.get("nyusatsuAnkens") or []
                if not ankens:
                    for v in props.values():
                        if (isinstance(v, list) and len(v) > 0 and
                                isinstance(v[0], dict) and
                                any(k in v[0] for k in ("anken_name", "rakusatsu_sha"))):
                            ankens = v
                            break

                for a in ankens:
                    winner   = (a.get("rakusatsu_gyosha_name") or
                                a.get("rakusatsu_sha") or "").strip()
                    bid_date = str(a.get("rakusatsu_date") or
                                   a.get("nyusatsu_date") or "")[:10]
                    project  = (a.get("anken_name") or a.get("title") or "").strip()
                    client   = (a.get("hacchusha_name") or "").strip()
                    amount   = str(a.get("rakusatsu_kakaku") or "")
                    if winner and project:
                        items.append({
                            "source": "nsearch", "winner": winner,
                            "bid_date": bid_date, "project_name": project,
                            "client": client, "amount": amount, "url": url,
                        })
                if items:
                    return items, total_pages
            except Exception:
                pass

        # HTML フォールバック
        for row in soup.find_all(class_=re.compile(
                r"anken|nyusatsu|result|item|card|tender", re.I)):
            text  = row.get_text("\n", strip=True)
            lines = [l for l in text.split("\n") if l.strip()]
            winner = client = bid_date = project = amount = ""
            for i, line in enumerate(lines):
                nxt = lines[i+1] if i+1 < len(lines) else ""
                if re.search(r"落札者|落札業者",  line): winner   = nxt
                elif re.search(r"工事名|案件名",   line): project  = nxt
                elif re.search(r"落札日|入札日",   line): bid_date = nxt[:10]
                elif re.search(r"発注者",          line): client   = nxt
                elif re.search(r"落札金額",        line): amount   = nxt
            if winner and project:
                items.append({
                    "source": "nsearch", "winner": winner, "bid_date": bid_date,
                    "project_name": project, "client": client, "amount": amount,
                    "url": url,
                })
        return items, total_pages

    all_items: list[dict] = []

    # Playwright 優先
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

        with sync_playwright() as pw:
            browser  = pw.chromium.launch(headless=True, args=["--no-sandbox"])
            ctx      = browser.new_context(user_agent=UA, locale="ja-JP")
            page_obj = ctx.new_page()
            detected = 1

            for pg in range(1, detected + 1):
                logger.info(f"[nsearch/playwright] page {pg}/{detected}")
                try:
                    page_obj.goto(_url(pg), wait_until="networkidle", timeout=30_000)
                    page_obj.wait_for_timeout(2500)
                    items, tp = _parse(page_obj.content(), _url(pg))
                    if pg == 1:
                        detected = tp
                    all_items.extend(items)
                    if not items and pg > 1:
                        break
                except PwTimeout:
                    logger.warning(f"[nsearch] timeout page {pg}")
                    break
                except Exception as e:
                    logger.error(f"[nsearch] page {pg}: {e}")
                    break
                time.sleep(1.5)

            browser.close()

        logger.info(f"[nsearch] {len(all_items)} items (Playwright)")
        return all_items

    except ImportError:
        logger.info("[nsearch] Playwright unavailable — requests fallback")

    # requests フォールバック
    sess     = requests.Session()
    detected = 1
    for pg in range(1, detected + 1):
        resp = sess.get(_url(pg), headers=HEADERS, timeout=25)
        if resp.status_code != 200:
            break
        items, tp = _parse(resp.text, _url(pg))
        if pg == 1:
            detected = tp
        all_items.extend(items)
        if not items and pg > 1:
            break
        time.sleep(1.5)

    logger.info(f"[nsearch] {len(all_items)} items (requests)")
    return all_items


# ══════════════════════════════════════════════════════════════════
# Sources 3〜20: 各官公庁・機関
# ══════════════════════════════════════════════════════════════════

def scrape_pportal(session: requests.Session) -> list[dict]:
    """調達ポータル — 直近3日分の差分CSVをzip DL"""
    import io, zipfile, csv as _csv

    items = []
    base  = "https://www.p-portal.go.jp/pps-web-biz/UAB02/OAB0201/"
    now   = datetime.now(JST)
    for i in range(RECENT_DAYS + 1):
        d     = now - timedelta(days=i)
        fname = f"successful_bid_record_info_diff_{d.strftime('%Y%m%d')}.zip"
        try:
            resp = session.get(base + fname, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                continue
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                for name in zf.namelist():
                    if not name.endswith(".csv"):
                        continue
                    text   = zf.read(name).decode("utf-8-sig", errors="replace")
                    reader = _csv.DictReader(io.StringIO(text))
                    for row in reader:
                        winner  = (row.get("落札者名","") or row.get("受注者名","")).strip()
                        project = (row.get("入札件名","") or row.get("調達件名","")).strip()
                        date    = row.get("落札日","")[:10]
                        client  = row.get("調達機関名","").strip()
                        amount  = row.get("落札金額","").strip()
                        if winner and project and _is_recent(date):
                            items.append({
                                "source":"p_portal","winner":winner,
                                "bid_date":date,"project_name":project,
                                "client":client,"amount":amount,"url":base+fname,
                            })
        except Exception as e:
            logger.debug(f"[p_portal] {fname}: {e}")

    logger.info(f"[p_portal] {len(items)} items")
    return items


def scrape_meti(session: requests.Session) -> list[dict]:
    reiwa = datetime.now(JST).year - 2018
    items = []
    for url in [
        f"https://www.meti.go.jp/information_2/publicoffer/R_{reiwa:02d}_bid_news_list.html",
        f"https://www.meti.go.jp/information_2/publicoffer/R_{reiwa-1:02d}_bid_news_list.html",
    ]:
        items.extend(_scrape_url(session, url, "meti",
                                 winner_col=2, project_col=1, date_col=0))
        time.sleep(1.0)
    return items


def scrape_kkj(session: requests.Session) -> list[dict]:
    return _scrape_url(session, "https://kkj.go.jp/s/", "kkj",
                       winner_col=3, project_col=1, date_col=0, client_col=2)


def scrape_mod(session: requests.Session) -> list[dict]:
    items = []
    for url in [
        "https://www.mod.go.jp/atla/data/info/ny_honbu/ippan.html",
        "https://www.mod.go.jp/asdf/4dep/kouhyou.html",
    ]:
        items.extend(_scrape_url(session, url, "mod",
                                 winner_col=2, project_col=1, date_col=0))
        time.sleep(1.0)
    return items


def scrape_mof(session: requests.Session) -> list[dict]:
    return _scrape_url(
        session,
        "https://www.mof.go.jp/application-contact/procurement/buppinn/index.htm",
        "mof", winner_col=2, project_col=1, date_col=0,
    )


def scrape_mlit(session: requests.Session, url: str, source: str) -> list[dict]:
    return _scrape_url(session, url, source,
                       winner_col=3, project_col=1, date_col=0, client_col=2)


def scrape_nexco(session: requests.Session, url: str, source: str) -> list[dict]:
    return _scrape_url(session, url, source,
                       winner_col=3, project_col=1, date_col=0)


def scrape_tokyo(session: requests.Session) -> list[dict]:
    return _scrape_url(
        session,
        "https://www.e-procurement.metro.tokyo.lg.jp/indexPbi.jsp",
        "tokyo", winner_col=3, project_col=2, date_col=1, client_col=0,
    )


def scrape_osaka(session: requests.Session) -> list[dict]:
    return _scrape_url(
        session,
        "https://www.pref.osaka.lg.jp/o040100/keiyaku_2/e-nyuusatsu/e-kekka.html",
        "osaka", winner_col=3, project_col=2, date_col=1,
    )


def scrape_ipa(session: requests.Session) -> list[dict]:
    yr = datetime.now(JST).year
    return _scrape_url(
        session,
        f"https://www.ipa.go.jp/choutatsu/nyusatsu/{yr}/index.html",
        "ipa", winner_col=2, project_col=1, date_col=0,
    )


def scrape_jrtt(session: requests.Session) -> list[dict]:
    return _scrape_url(
        session,
        "https://www.jrtt.go.jp/procurement/tender-notice.html",
        "jrtt", winner_col=2, project_col=1, date_col=0,
    )


def scrape_ur(session: requests.Session) -> list[dict]:
    return _scrape_url(
        session,
        "https://www.ur-net.go.jp/order/information/",
        "ur", winner_col=2, project_col=1, date_col=0,
    )


def scrape_water(session: requests.Session) -> list[dict]:
    return _scrape_url(
        session,
        "https://www.water.go.jp/honsya/honsya/jigyou/choutatsu/",
        "water", winner_col=2, project_col=1, date_col=0,
    )


# ══════════════════════════════════════════════════════════════════
# シグナル生成（戦略A）
# ══════════════════════════════════════════════════════════════════

def build_signals(raw: list[dict], mapper: CompanyMapper,
                  past_keys: set) -> tuple[list[dict], set]:
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
            continue
        new_keys.add(dk)

        tickers = mapper.get_tickers(winner)
        if not tickers:
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
# メイン（全20ソース）
# ══════════════════════════════════════════════════════════════════

def run() -> list[dict]:
    mapper    = CompanyMapper()
    seen_ids  = load_seen_ids()
    past_keys = load_past_dedup_keys()
    session   = requests.Session()

    all_raw: list[dict] = []
    all_new_ids: set    = set()
    nsr_count           = 0

    # ── 全20ソース定義 ─────────────────────────────────────────────
    def run_source(label: str, func, is_nikoukei: bool = False):
        nonlocal nsr_count
        logger.info(f"=== {label} ===")
        try:
            result = func()
            if is_nikoukei:
                raw_items, ids = result
                all_new_ids.update(ids)
            else:
                raw_items = result
            all_raw.extend(raw_items)
            if label.startswith("[2]"):
                nsr_count = len(raw_items)
        except Exception as e:
            logger.warning(f"{label} エラー（スキップ）: {e}")

    run_source("[1]  nikoukei",
               lambda: scrape_nikoukei(session, seen_ids), is_nikoukei=True)
    run_source("[2]  nsearch",
               scrape_nsearch)
    run_source("[3]  調達ポータル",
               lambda: scrape_pportal(session))
    run_source("[4]  経産省",
               lambda: scrape_meti(session))
    run_source("[5]  官公需",
               lambda: scrape_kkj(session))
    run_source("[6]  国交省関東",
               lambda: scrape_mlit(session,
                   "https://www.ktr.mlit.go.jp/nyuusatu/nyuusatu00004729.html",
                   "mlit_kanto"))
    run_source("[7]  防衛省",
               lambda: scrape_mod(session))
    run_source("[8]  財務省",
               lambda: scrape_mof(session))
    run_source("[9]  国交省九州",
               lambda: scrape_mlit(session,
                   "https://www.qsr.mlit.go.jp/nyusatu_joho/keiyaku/nyusatu_data/",
                   "mlit_kyu"))
    run_source("[10] 国交省中国",
               lambda: scrape_mlit(session,
                   "https://www.cgr.mlit.go.jp/order/nyusatsu/index.html",
                   "mlit_chu"))
    run_source("[11] NEXCO東日本",
               lambda: scrape_nexco(session,
                   "https://www.e-nexco.co.jp/bids/public_notice/search_service",
                   "nexco_e"))
    run_source("[12] NEXCO中日本",
               lambda: scrape_nexco(session,
                   "https://contract.c-nexco.co.jp/auction_info/search",
                   "nexco_c"))
    run_source("[13] NEXCO西日本",
               lambda: scrape_nexco(session,
                   "https://corp.w-nexco.co.jp/procurement/library/",
                   "nexco_w"))
    run_source("[14] 東京都",
               lambda: scrape_tokyo(session))
    run_source("[15] 大阪府",
               lambda: scrape_osaka(session))
    run_source("[16] IPA",
               lambda: scrape_ipa(session))
    run_source("[17] JRTT",
               lambda: scrape_jrtt(session))
    run_source("[18] UR都市機構",
               lambda: scrape_ur(session))
    run_source("[19] 水資源機構",
               lambda: scrape_water(session))
    run_source("[20] 国交省東北",
               lambda: scrape_mlit(session,
                   "https://www.thr.mlit.go.jp/nyusatsu/kekka/",
                   "mlit_tohoku"))

    # ── シグナル生成・保存・通知 ───────────────────────────────────
    logger.info(f"=== 合計 {len(all_raw)} raw items → シグナル生成 ===")
    signals, _ = build_signals(all_raw, mapper, past_keys)
    save_signals(signals)
    seen_ids.update(all_new_ids)
    save_seen_ids(seen_ids)

    if signals:
        notify_signals(signals)
    notify_summary(signals,
                   nikoukei_pages=NIKOUKEI_PAGES,
                   nsearch_items=nsr_count)
    return signals


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("  入札落札シグナル スクレイパー（全20ソース / 直近3日）")
    logger.info("=" * 60)
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
