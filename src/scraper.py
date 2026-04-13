"""
scraper.py
════════════════════════════════════════════════════════════════════════════
公共入札落札 シグナル生成スクレイパー  【戦略A専用】

戦略A（ベースライン）:
  エントリー  : 検知翌営業日 寄り付き 成行買い
  利確        : +20%
  ロスカット  : -15%
  最大保有    : 60営業日（強制決済）

データソース（全20）:
  1.  nikoukei    日本工業経済新聞社      nikoukei.co.jp/bid_result
  2.  nsearch      エヌサーチ              nsearch.jp（Playwright）
  3.  p_portal     調達ポータル            p-portal.go.jp（年次CSV zip）
  4.  meti         経済産業省              meti.go.jp（HTMLテーブル）
  5.  kkj          官公需情報ポータル       kkj.go.jp（フォームPOST）
  6.  mlit_kanto   国交省関東整備局         ktr.mlit.go.jp（月次Excel）
  7.  mod          防衛省・防衛装備庁       mod.go.jp（HTML一覧）
  8.  mof          財務省                  mof.go.jp（年度別HTML）
  9.  mlit_kyu     国交省九州整備局         qsr.mlit.go.jp（月次Excel）
  10. mlit_chu     国交省中国整備局         cgr.mlit.go.jp（月次CSV）
  11. nexco_e      NEXCO東日本             e-nexco.co.jp（RSS+HTML）
  12. nexco_c      NEXCO中日本             c-nexco.co.jp（日立系PPI HTML）
  13. nexco_w      NEXCO西日本             w-nexco.co.jp（日立系PPI HTML）
  14. tokyo        東京都電子調達           e-procurement.metro.tokyo.lg.jp
  15. osaka        大阪府電子調達           pref.osaka.lg.jp（月別HTML）
  16. ipa          IPA情報処理推進機構      ipa.go.jp（年度別HTML）
  17. jrtt         鉄道・運輸機構(JRTT)    jrtt.go.jp（PPI HTML）
  18. ur           UR都市機構              ur-net.go.jp（HTML）
  19. water        水資源機構              water.go.jp（HTML）
  20. mlit_tohoku  国交省東北整備局         thr.mlit.go.jp（月次Excel）

重複除去: (落札者+入札日+案件名先頭20字) MD5 → 正規化一致 → Levenshtein≤3
════════════════════════════════════════════════════════════════════════════
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

# ── ローカルモジュール ──
sys.path.insert(0, str(Path(__file__).parent))
from company_mapper  import CompanyMapper
from discord_notify  import notify_signals, notify_summary, notify_error

# ── 定数 ──────────────────────────────────────────────────────────────────
JST       = timezone(timedelta(hours=9))
ROOT_DIR  = Path(__file__).parent.parent
DATA_DIR  = ROOT_DIR / "data"
SIG_DIR   = ROOT_DIR / "signals"
STATE_F   = DATA_DIR / "seen_ids.json"

DATA_DIR.mkdir(exist_ok=True)
SIG_DIR.mkdir(exist_ok=True)

NIKOUKEI_BASE     = "https://www.nikoukei.co.jp"
NIKOUKEI_LIST     = f"{NIKOUKEI_BASE}/bid_result"
NIKOUKEI_MAX_PAGES = 67

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
HEADERS  = {"User-Agent": UA, "Accept-Language": "ja,en-US;q=0.9"}
SKIP_SET = frozenset(["HP会員（無料）で金額表示", "さらに詳しい内容は無料IDでご確認ください。"])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════════
# 状態管理・重複除去
# ════════════════════════════════════════════════════════════════════════════

def _dedup_key(winner: str, bid_date: str, project: str) -> str:
    return hashlib.md5(f"{winner}|{bid_date}|{project[:20]}".encode()).hexdigest()


def _normalize_name(s: str) -> str:
    s = s.strip()
    s = s.translate(str.maketrans(
        "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
        "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ０１２３４５６７８９",
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"))
    for x in ["株式会社","（株）","(株)","合同会社","有限会社","ＪＶ","JV","共同企業体","グループ"]:
        s = s.replace(x, "")
    return s.replace("　","").replace(" ","")


def _normalize_project(p: str) -> str:
    p = re.sub(r'（[^）]*億[^）]*）', '', p)
    p = re.sub(r'\([^)]*億[^)]*\)', '', p)
    return p.strip()[:30]


def _levenshtein(s1: str, s2: str) -> int:
    if abs(len(s1)-len(s2)) > 5: return 99
    m, n = len(s1), len(s2)
    dp = list(range(n+1))
    for i in range(1, m+1):
        prev = dp[:]
        dp[0] = i
        for j in range(1, n+1):
            dp[j] = min(prev[j]+1, dp[j-1]+1,
                        prev[j-1]+(0 if s1[i-1]==s2[j-1] else 1))
    return dp[n]


def load_seen_ids() -> set:
    if STATE_F.exists():
        with open(STATE_F, encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def save_seen_ids(ids: set) -> None:
    with open(STATE_F, "w", encoding="utf-8") as f:
        json.dump(list(ids)[-20000:], f, ensure_ascii=False)


def load_past_dedup_keys() -> tuple[set, list]:
    """既存シグナルファイルから重複チェック用セットを構築"""
    keys:  set  = set()
    saved: list = []  # [(winner_norm, project_norm), ...]
    for sf in SIG_DIR.glob("*.json"):
        try:
            for s in json.load(open(sf, encoding="utf-8")):
                w = _normalize_name(s.get("winner",""))
                p = _normalize_project(s.get("project_name",""))
                keys.add(_dedup_key(
                    s.get("winner",""), s.get("bid_date",""), s.get("project_name","")))
                saved.append((w, p))
        except Exception:
            pass
    return keys, saved


def is_duplicate(winner: str, bid_date: str, project: str,
                 hash_set: set, norm_set: set, saved: list) -> bool:
    """3段階重複チェック。重複なら True"""
    # Step1: MD5ハッシュ
    h = _dedup_key(winner, bid_date, project)
    if h in hash_set:
        return True

    # Step2: 正規化一致
    wn = _normalize_name(winner)
    pn = _normalize_project(project)
    nk = f"{wn}|{pn}"
    if nk in norm_set:
        return True

    # Step3: Levenshtein類似度
    for sw, sp in saved:
        if sw == wn and _levenshtein(pn, sp) <= 3:
            return True

    return False


# ════════════════════════════════════════════════════════════════════════════
# HTTP ユーティリティ
# ════════════════════════════════════════════════════════════════════════════

def _get(url: str, session: requests.Session, retries: int = 3) -> Optional[requests.Response]:
    for n in range(retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            logger.warning(f"  HTTP {n+1}/{retries}: {e}")
            time.sleep(2 * (n+1))
    return None


# ════════════════════════════════════════════════════════════════════════════
# Source 1: nikoukei.co.jp（全67ページ）
# ════════════════════════════════════════════════════════════════════════════

def _nik_list(page: int, session: requests.Session) -> list[dict]:
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
            "id":      f"nik_{m.group(1)}",
            "url":     NIKOUKEI_BASE + href,
            "source":  "nikoukei",
            "client":          cells[0] if len(cells) > 0 else "",
            "bid_date_preview":cells[1] if len(cells) > 1 else "",
            "winner_preview":  cells[3] if len(cells) > 3 else "",
        })
    return items


def _nik_detail(url: str, session: requests.Session) -> Optional[dict]:
    resp = _get(url, session)
    if not resp:
        return None
    lines = [l.strip() for l in
             BeautifulSoup(resp.text, "html.parser").get_text("\n").split("\n")
             if l.strip()]
    info: dict = {"url": url, "source": "nikoukei"}
    LABELS = {"発注者名":"client","入札日":"bid_date",
               "工事件名":"project_name","落札者":"winner","発表日":"publish_date"}
    for i, line in enumerate(lines):
        if line in LABELS and i+1 < len(lines) and lines[i+1] not in SKIP_SET:
            info[LABELS[line]] = lines[i+1]
    return info if "winner" in info else None


def scrape_nikoukei(session: requests.Session, seen_ids: set,
                    max_pages: int = NIKOUKEI_MAX_PAGES) -> tuple[list[dict], set]:
    raw: list[dict] = []
    new_ids: set    = set()
    zero_streak     = 0

    for page in range(1, max_pages+1):
        logger.info(f"[nikoukei] page {page}/{max_pages}")
        items = _nik_list(page, session)
        if not items:
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
                if "bid_date" not in detail: detail["bid_date"] = item["bid_date_preview"]
                if "winner"   not in detail: detail["winner"]   = item["winner_preview"]
                if "client"   not in detail: detail["client"]   = item["client"]
                raw.append(detail)

        logger.info(f"[nikoukei] page {page}: {new_this} new")
        zero_streak = 0 if new_this > 0 else zero_streak + 1
        if zero_streak >= 3:
            logger.info("[nikoukei] 3 empty pages — stop early")
            break
        time.sleep(2.0)

    return raw, new_ids


# ════════════════════════════════════════════════════════════════════════════
# Source 2: nsearch.jp（Playwright デイリー差分）
# ════════════════════════════════════════════════════════════════════════════

def scrape_nsearch_daily(days: int = 7) -> list[dict]:
    try:
        from nsearch_scraper import run_daily
        run_daily(days=days)
        delta = DATA_DIR / "nsearch_delta.json"
        if delta.exists():
            items = json.load(open(delta, encoding="utf-8"))
            logger.info(f"[nsearch] {len(items)} items loaded")
            return items
    except Exception as e:
        logger.error(f"[nsearch] scrape failed: {e}")
        notify_error(f"nsearch スクレイプ失敗: {e}", traceback.format_exc())
    return []


# ════════════════════════════════════════════════════════════════════════════
# Sources 3〜20: 各官公庁・機関のスクレイピング関数
# ════════════════════════════════════════════════════════════════════════════
# 注意: 各サイトのHTML構造はアップデートで変わる場合があります。
#       実際の運用前に各URLにアクセスして構造を確認してください。
# ════════════════════════════════════════════════════════════════════════════

def _parse_generic_table(html: str, source: str, url: str,
                          winner_col: int = 2, project_col: int = 1,
                          date_col: int = 0, client_col: int = -1) -> list[dict]:
    """汎用テーブルパーサー（列インデックス指定）"""
    soup  = BeautifulSoup(html, "html.parser")
    items = []
    for table in soup.find_all("table"):
        for row in table.find_all("tr")[1:]:   # ヘッダ行スキップ
            cells = [c.get_text(strip=True) for c in row.find_all(["td","th"])]
            if len(cells) <= max(winner_col, project_col):
                continue
            winner  = cells[winner_col]  if winner_col  < len(cells) else ""
            project = cells[project_col] if project_col < len(cells) else ""
            date    = cells[date_col]    if date_col    < len(cells) else ""
            client  = cells[client_col]  if client_col >= 0 and client_col < len(cells) else ""
            if winner and project and len(winner) >= 2:
                items.append({"source": source, "winner": winner, "bid_date": date[:10],
                               "project_name": project, "client": client, "amount": "", "url": url})
    return items


def scrape_pportal(session: requests.Session) -> list[dict]:
    """
    調達ポータル (p-portal.go.jp)
    年次CSVを直接DLして落札者を抽出。
    CSV列: 調達機関名,入札件名,落札者名,落札金額,入札日,...
    """
    import io, zipfile
    items = []
    today = datetime.now(JST)
    years = [today.year, today.year - 1]  # 当年度＋前年度
    year_map = {2025: "2025", 2024: "2024", 2023: "2023"}

    for yr in years:
        y_str = year_map.get(yr)
        if not y_str:
            continue
        zip_url = (f"https://www.p-portal.go.jp/pps-web-biz/UAB02/OAB0201/"
                   f"successful_bid_record_info_all_{y_str}.zip")
        try:
            resp = session.get(zip_url, headers=HEADERS, timeout=60)
            if resp.status_code != 200:
                continue
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                for name in zf.namelist():
                    if not name.endswith(".csv"):
                        continue
                    csv_bytes = zf.read(name)
                    # UTF-8 BOM 付きCSV
                    text = csv_bytes.decode("utf-8-sig", errors="replace")
                    import csv, io as sio
                    reader = csv.DictReader(sio.StringIO(text))
                    for row in reader:
                        winner  = (row.get("落札者名","") or row.get("受注者名","")).strip()
                        project = (row.get("入札件名","") or row.get("調達件名","")).strip()
                        date    = (row.get("落札日","") or row.get("契約日",""))[:10]
                        client  = row.get("調達機関名","").strip()
                        amount  = row.get("落札金額","").strip()
                        if winner and project:
                            items.append({"source":"p_portal","winner":winner,
                                          "bid_date":date,"project_name":project,
                                          "client":client,"amount":amount,
                                          "url":zip_url})
        except Exception as e:
            logger.warning(f"[p_portal] {yr}: {e}")

    logger.info(f"[p_portal] {len(items)} items")
    return items


def _scrape_html_source(session: requests.Session, urls: list[str],
                         source: str, **kwargs) -> list[dict]:
    """複数URLを順に取得して汎用テーブルパーサーを適用"""
    items = []
    for url in urls:
        resp = _get(url, session)
        if resp:
            items.extend(_parse_generic_table(resp.text, source, url, **kwargs))
        time.sleep(1.0)
    logger.info(f"[{source}] {len(items)} items")
    return items


def scrape_meti(session: requests.Session) -> list[dict]:
    """経済産業省 meti.go.jp — 年度別HTMLテーブル"""
    today = datetime.now(JST)
    reiwa = today.year - 2018  # 令和換算
    urls  = [
        f"https://www.meti.go.jp/information_2/publicoffer/R_{reiwa:02d}_bid_news_list.html",
        f"https://www.meti.go.jp/information_2/publicoffer/R_{reiwa-1:02d}_bid_news_list.html",
    ]
    # meti は「落札者」「件名」「落札金額」のテーブル構造（列0=日付,1=件名,2=落札者）
    return _scrape_html_source(session, urls, "meti",
                                winner_col=2, project_col=1, date_col=0, client_col=-1)


def scrape_kkj(session: requests.Session) -> list[dict]:
    """
    官公需情報ポータル kkj.go.jp
    キーワード検索APIに近い形でHTMLを取得
    """
    base = "https://kkj.go.jp/s/"
    items = []
    try:
        # 全件検索（工事・役務）
        resp = _get(base, session)
        if resp:
            items.extend(_parse_generic_table(resp.text, "kkj", base,
                                               winner_col=3, project_col=1,
                                               date_col=0, client_col=2))
    except Exception as e:
        logger.warning(f"[kkj] {e}")
    logger.info(f"[kkj] {len(items)} items")
    return items


def scrape_mlit_excel(session: requests.Session, url_list: list[str],
                       source: str) -> list[dict]:
    """
    国交省各整備局の月次Excel入札結果
    列構成(想定): A=工事番号 B=件名 C=落札者 D=落札金額 E=入札日
    """
    try:
        import openpyxl
    except ImportError:
        logger.warning(f"[{source}] openpyxl not installed, skip")
        return []

    items = []
    for url in url_list:
        resp = session.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            continue
        try:
            import io
            wb = openpyxl.load_workbook(io.BytesIO(resp.content), data_only=True)
            ws = wb.active
            for row in ws.iter_rows(min_row=2, values_only=True):
                if not row or len(row) < 3:
                    continue
                project = str(row[1] or "").strip()
                winner  = str(row[2] or "").strip()
                amount  = str(row[3] or "") if len(row) > 3 else ""
                date    = str(row[4] or "")[:10] if len(row) > 4 else ""
                if winner and project and len(winner) >= 2:
                    items.append({"source": source, "winner": winner, "bid_date": date,
                                  "project_name": project, "client": "", "amount": amount,
                                  "url": url})
        except Exception as e:
            logger.warning(f"[{source}] Excel parse error: {e}")
        time.sleep(1.0)

    logger.info(f"[{source}] {len(items)} items from Excel")
    return items


def scrape_mod(session: requests.Session) -> list[dict]:
    """防衛省・防衛装備庁 mod.go.jp — 落札結果HTML一覧"""
    urls = [
        "https://www.mod.go.jp/atla/data/info/ny_honbu/ippan.html",
        "https://www.mod.go.jp/asdf/4dep/kouhyou.html",
    ]
    return _scrape_html_source(session, urls, "mod",
                                winner_col=2, project_col=1, date_col=0)


def scrape_mof(session: requests.Session) -> list[dict]:
    """財務省 mof.go.jp — 年度別落札情報HTML"""
    urls = [
        "https://www.mof.go.jp/application-contact/procurement/buppinn/index.htm",
    ]
    return _scrape_html_source(session, urls, "mof",
                                winner_col=2, project_col=1, date_col=0)


def scrape_nexco(session: requests.Session, base_url: str, source: str) -> list[dict]:
    """NEXCO各社 — 入札情報公開システム（日立システムズ運営）"""
    resp = _get(base_url, session)
    if not resp:
        return []
    items = _parse_generic_table(resp.text, source, base_url,
                                  winner_col=3, project_col=1, date_col=0, client_col=-1)
    logger.info(f"[{source}] {len(items)} items")
    return items


def scrape_tokyo(session: requests.Session) -> list[dict]:
    """東京都電子調達システム"""
    url = "https://www.e-procurement.metro.tokyo.lg.jp/indexPbi.jsp"
    resp = _get(url, session)
    if not resp:
        return []
    items = _parse_generic_table(resp.text, "tokyo", url,
                                  winner_col=3, project_col=2, date_col=1, client_col=0)
    logger.info(f"[tokyo] {len(items)} items")
    return items


def scrape_osaka(session: requests.Session) -> list[dict]:
    """大阪府電子調達システム"""
    url = "https://www.pref.osaka.lg.jp/o040100/keiyaku_2/e-nyuusatsu/e-kekka.html"
    resp = _get(url, session)
    if not resp:
        return []
    items = _parse_generic_table(resp.text, "osaka", url,
                                  winner_col=3, project_col=2, date_col=1)
    logger.info(f"[osaka] {len(items)} items")
    return items


def scrape_ipa(session: requests.Session) -> list[dict]:
    """IPA 情報処理推進機構"""
    today = datetime.now(JST)
    yr    = today.year
    url   = f"https://www.ipa.go.jp/choutatsu/nyusatsu/{yr}/index.html"
    resp  = _get(url, session)
    if not resp:
        return []
    items = _parse_generic_table(resp.text, "ipa", url,
                                  winner_col=2, project_col=1, date_col=0)
    logger.info(f"[ipa] {len(items)} items")
    return items


def scrape_jrtt(session: requests.Session) -> list[dict]:
    """JRTT 鉄道・運輸機構"""
    url  = "https://www.jrtt.go.jp/procurement/tender-notice.html"
    resp = _get(url, session)
    if not resp:
        return []
    items = _parse_generic_table(resp.text, "jrtt", url,
                                  winner_col=2, project_col=1, date_col=0)
    logger.info(f"[jrtt] {len(items)} items")
    return items


def scrape_ur(session: requests.Session) -> list[dict]:
    """UR都市機構"""
    url  = "https://www.ur-net.go.jp/order/information/"
    resp = _get(url, session)
    if not resp:
        return []
    items = _parse_generic_table(resp.text, "ur", url,
                                  winner_col=2, project_col=1, date_col=0)
    logger.info(f"[ur] {len(items)} items")
    return items


def scrape_water(session: requests.Session) -> list[dict]:
    """水資源機構"""
    url  = "https://www.water.go.jp/honsya/honsya/jigyou/choutatsu/"
    resp = _get(url, session)
    if not resp:
        return []
    items = _parse_generic_table(resp.text, "water", url,
                                  winner_col=2, project_col=1, date_col=0)
    logger.info(f"[water] {len(items)} items")
    return items


# ════════════════════════════════════════════════════════════════════════════
# シグナル生成（戦略A）
# ════════════════════════════════════════════════════════════════════════════

def build_signals_stratA(
    raw:         list[dict],
    mapper:      CompanyMapper,
    hash_set:    set,
    norm_set:    set,
    saved:       list,
) -> tuple[list[dict], set, set, list]:
    """
    生の入札情報 → 戦略Aシグナル（+20%TP / -15%SL / 最大60営業日）
    Returns: (signals, new_hash_keys, new_norm_keys, new_saved)
    """
    signals:    list[dict] = []
    new_hashes: set        = set()
    new_norms:  set        = set()
    new_saved:  list       = []

    for item in raw:
        winner  = item.get("winner", "").strip()
        project = item.get("project_name", "").strip()
        bid_date = item.get("bid_date", "").strip()

        if not winner or not project:
            continue

        if is_duplicate(winner, bid_date, project, hash_set | new_hashes,
                        norm_set | new_norms, saved + new_saved):
            logger.debug(f"dedup: {winner} / {project[:30]}")
            continue

        # 重複セットに追加
        h  = _dedup_key(winner, bid_date, project)
        wn = _normalize_name(winner)
        pn = _normalize_project(project)
        new_hashes.add(h)
        new_norms.add(f"{wn}|{pn}")
        new_saved.append((wn, pn))

        tickers = mapper.get_tickers(winner)
        if not tickers:
            logger.debug(f"no ticker: {winner}")
            continue

        now = datetime.now(JST).isoformat()
        for t in tickers:
            sig = {
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
                # 戦略A固定パラメータ
                "strategy":     "A",
                "action":       "BUY",
                "timing":       "翌営業日 寄り付き成行",
                "tp_pct":       20,
                "sl_pct":       -15,
                "max_hold_days": 60,
                "exit_rule":    "+20%利確 / -15%ロスカット / 最大60営業日",
            }
            signals.append(sig)
            logger.info(
                f"🔔 [{t['role']}] {winner} → {t['company']} ({t['ticker']}) "
                f"| {project[:40]}"
            )

    return signals, new_hashes, new_norms, new_saved


# ════════════════════════════════════════════════════════════════════════════
# シグナル保存
# ════════════════════════════════════════════════════════════════════════════

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


# ════════════════════════════════════════════════════════════════════════════
# メイン処理
# ════════════════════════════════════════════════════════════════════════════

def run(nikoukei_pages: int = NIKOUKEI_MAX_PAGES,
        nsearch_days:   int = 7,
        enable_all_srcs: bool = True) -> list[dict]:
    """
    全ソースをスキャンしてシグナルを生成・保存・Discord通知する。
    """
    mapper   = CompanyMapper()
    seen_ids = load_seen_ids()
    hash_set, saved = load_past_dedup_keys()
    norm_set: set = {f"{w}|{p}" for w, p in saved}
    session  = requests.Session()

    all_raw: list[dict] = []
    new_ids: set        = set()

    # ── Source 1: nikoukei ──────────────────────────────────────────
    logger.info("=== [1] nikoukei.co.jp ===")
    try:
        r, ids = scrape_nikoukei(session, seen_ids, nikoukei_pages)
        all_raw.extend(r)
        new_ids.update(ids)
    except Exception as e:
        notify_error(f"nikoukei エラー: {e}", traceback.format_exc())

    # ── Source 2: nsearch ───────────────────────────────────────────
    logger.info("=== [2] nsearch.jp ===")
    nsr_items = scrape_nsearch_daily(nsearch_days)
    all_raw.extend(nsr_items)

    if enable_all_srcs:
        # ── Sources 3〜20 ────────────────────────────────────────────
        src_jobs = [
            ("[3] 調達ポータル",       lambda: scrape_pportal(session)),
            ("[4] 経産省",            lambda: scrape_meti(session)),
            ("[5] 官公需",            lambda: scrape_kkj(session)),
            ("[6] 国交省関東",         lambda: scrape_mlit_excel(session, [], "mlit_kanto")),
            ("[7] 防衛省",            lambda: scrape_mod(session)),
            ("[8] 財務省",            lambda: scrape_mof(session)),
            ("[9] 国交省九州",         lambda: scrape_mlit_excel(session, [], "mlit_kyu")),
            ("[10] 国交省中国",        lambda: scrape_mlit_excel(session, [], "mlit_chu")),
            ("[11] NEXCO東日本",       lambda: scrape_nexco(session,
                "https://www.e-nexco.co.jp/bids/public_notice/search_service", "nexco_e")),
            ("[12] NEXCO中日本",       lambda: scrape_nexco(session,
                "https://contract.c-nexco.co.jp/auction_info/search", "nexco_c")),
            ("[13] NEXCO西日本",       lambda: scrape_nexco(session,
                "https://corp.w-nexco.co.jp/procurement/library/", "nexco_w")),
            ("[14] 東京都",            lambda: scrape_tokyo(session)),
            ("[15] 大阪府",            lambda: scrape_osaka(session)),
            ("[16] IPA",              lambda: scrape_ipa(session)),
            ("[17] JRTT",             lambda: scrape_jrtt(session)),
            ("[18] UR都市機構",         lambda: scrape_ur(session)),
            ("[19] 水資源機構",         lambda: scrape_water(session)),
            ("[20] 国交省東北",         lambda: scrape_mlit_excel(session, [], "mlit_tohoku")),
        ]
        for label, func in src_jobs:
            logger.info(f"=== {label} ===")
            try:
                items = func()
                all_raw.extend(items)
            except Exception as e:
                logger.warning(f"{label} error: {e}")

    # ── シグナル生成 ─────────────────────────────────────────────────
    signals, nh, nn, ns = build_signals_stratA(
        all_raw, mapper, hash_set, norm_set, saved
    )

    # 状態保存
    seen_ids.update(new_ids)
    save_seen_ids(seen_ids)
    save_signals(signals)

    # Discord通知
    if signals:
        notify_signals(signals)
    notify_summary(signals, nikoukei_pages, len(nsr_items))

    return signals


# ════════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="入札落札シグナル スクレイパー（戦略A）")
    parser.add_argument("--nikoukei-pages", type=int, default=NIKOUKEI_MAX_PAGES)
    parser.add_argument("--nsearch-days",   type=int, default=7)
    parser.add_argument("--quick",          action="store_true",
                        help="クイックテスト（nikoukei 3ページ, nsearch 2日）")
    parser.add_argument("--nikoukei-only",  action="store_true",
                        help="nikoukei と nsearch のみ（他ソースをスキップ）")
    args = parser.parse_args()

    if args.quick:
        args.nikoukei_pages = 3
        args.nsearch_days   = 2

    logger.info("=" * 60)
    logger.info("  入札落札シグナル スクレイパー 【戦略A専用】 START")
    logger.info(f"  nikoukei: {args.nikoukei_pages}p / nsearch: {args.nsearch_days}日")
    logger.info("=" * 60)

    try:
        signals = run(
            nikoukei_pages   = args.nikoukei_pages,
            nsearch_days     = args.nsearch_days,
            enable_all_srcs  = not args.nikoukei_only,
        )
    except Exception as e:
        notify_error(f"スクレイパー致命的エラー: {e}", traceback.format_exc())
        raise

    if signals:
        print(f"\n✅ {len(signals)} シグナル（Discord 通知済み）:")
        for s in signals:
            role = "自社" if s["ticker_role"] == "self" else "親会社"
            print(f"  [{role}] {s['bid_date']} | {s['winner']} → "
                  f"{s['company']} ({s['ticker']}) | {s['project_name'][:40]}")
    else:
        print("✅ 新規シグナルなし（Discord にサマリー通知済み）")

    logger.info("  入札落札シグナル スクレイパー END")
