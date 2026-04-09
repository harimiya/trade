"""
日本株 値上がり初動ピックアップ → Discord通知スクリプト
============================================================
対象シグナル（古河電工 2026/2/9 パターン）:
  1. 前日終値比 +5%以上 の急騰
  2. 出来高が20日平均の 3倍以上
  3. 時価総額 1000億円以上
  4. （任意）直近に決算・上方修正・増配ニュース

データソース:
  - yfinance (株価・出来高・時価総額)
  - JPX公式CSV (東証プライム全銘柄リスト)

実行:
  python japan_stock_alert.py          # 即時実行
  python japan_stock_alert.py --dry    # Discord送信なし（テスト）
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
import yfinance as yf

# ─────────────────────────────────────────────
# 設定
# ─────────────────────────────────────────────
DISCORD_WEBHOOK_URL = os.environ.get(
    "DISCORD_WEBHOOK_URL",
    "https://discord.com/api/webhooks/1490293520085418125/b3PUB4-Y5guGheOKFzI8LIwHlY7q8upaEzZzFI5knJkWlAkw_JXgoEzSs9HMMCh9v8nr",
)

# スクリーニング条件
MIN_MARKET_CAP_JPY   = 100_000_000_000   # 時価総額 1000億円以上
MIN_PRICE_CHANGE_PCT = 5.0               # 前日比 +5%以上
MIN_VOLUME_RATIO     = 3.0               # 20日平均出来高の3倍以上
LOOKBACK_DAYS        = 30                # 出来高平均算出期間

# yfinance リクエスト間隔（レート制限対策）
FETCH_SLEEP_SEC = 0.3

JST = timezone(timedelta(hours=9))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# 銘柄リスト取得（東証プライム）
# ─────────────────────────────────────────────
def fetch_prime_tickers() -> list[str]:
    """JPX公開CSVから東証プライム全銘柄コードを取得"""
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    try:
        df = pd.read_excel(url, header=0)
        # 列名を正規化
        df.columns = df.columns.str.strip()
        # 市場区分列を探す
        market_col = [c for c in df.columns if "市場・商品区分" in c or "市場区分" in c]
        code_col   = [c for c in df.columns if "コード" in c]
        if not market_col or not code_col:
            raise ValueError(f"列名不一致: {df.columns.tolist()}")
        df_prime = df[df[market_col[0]].str.contains("プライム", na=False)]
        codes = df_prime[code_col[0]].astype(str).str.zfill(4).tolist()
        log.info(f"東証プライム銘柄数: {len(codes)}")
        return [f"{c}.T" for c in codes]
    except Exception as e:
        log.warning(f"JPX CSVの取得失敗 ({e})。フォールバック銘柄リストを使用")
        # フォールバック：代表的な大型株
        fallback = [
            "7203","6758","9984","6861","8306","9432","7974","6098",
            "8316","9433","4063","8058","6501","6902","7267","8031",
            "5801","5803","5802","4568","9020","6594","7741","8001",
        ]
        return [f"{c}.T" for c in fallback]


# ─────────────────────────────────────────────
# 個別銘柄スクリーニング
# ─────────────────────────────────────────────
def screen_ticker(ticker: str) -> dict | None:
    """
    Returns screening result dict if the stock passes all filters, else None.
    """
    try:
        tk = yf.Ticker(ticker)
        hist = tk.history(period=f"{LOOKBACK_DAYS + 5}d")
        if hist.empty or len(hist) < 5:
            return None

        # ── 直近終値と前日終値 ──
        last_close = float(hist["Close"].iloc[-1])
        prev_close = float(hist["Close"].iloc[-2])
        if prev_close == 0:
            return None
        price_chg_pct = (last_close - prev_close) / prev_close * 100

        if price_chg_pct < MIN_PRICE_CHANGE_PCT:
            return None

        # ── 出来高比率 ──
        last_vol  = float(hist["Volume"].iloc[-1])
        avg_vol   = float(hist["Volume"].iloc[-LOOKBACK_DAYS:-1].mean())
        if avg_vol == 0:
            return None
        vol_ratio = last_vol / avg_vol

        if vol_ratio < MIN_VOLUME_RATIO:
            return None

        # ── 時価総額 ──
        info = tk.fast_info
        mkt_cap = getattr(info, "market_cap", None)
        if mkt_cap is None or mkt_cap < MIN_MARKET_CAP_JPY:
            return None

        # ── 銘柄名 ──
        short_name = getattr(info, "currency", "JPY")
        try:
            short_name = tk.info.get("shortName") or tk.info.get("longName") or ticker
        except Exception:
            short_name = ticker

        return {
            "ticker":       ticker,
            "name":         short_name,
            "last_close":   last_close,
            "price_chg":    round(price_chg_pct, 2),
            "vol_ratio":    round(vol_ratio, 1),
            "market_cap":   mkt_cap,
        }

    except Exception as e:
        log.debug(f"{ticker}: スキップ ({e})")
        return None


# ─────────────────────────────────────────────
# Discord 通知
# ─────────────────────────────────────────────
def format_market_cap(cap: float) -> str:
    if cap >= 1_000_000_000_000:
        return f"{cap/1_000_000_000_000:.1f}兆円"
    return f"{cap/100_000_000:.0f}億円"


def build_discord_payload(results: list[dict], run_date: str) -> dict:
    if not results:
        description = "本日の対象銘柄はありませんでした。"
        color = 0x95a5a6
    else:
        lines = []
        for r in results:
            mc = format_market_cap(r["market_cap"])
            lines.append(
                f"**{r['name']}** (`{r['ticker']}`)\n"
                f"　  前日比: **+{r['price_chg']}%**　"
                f"出来高: **{r['vol_ratio']}倍**　"
                f"時価総額: {mc}\n"
                f"　終値: \{r['last_close']:,.0f}"
            )
        description = "\n\n".join(lines)
        color = 0xe74c3c  # 赤（急騰）

    return {
        "embeds": [
            {
                "title": f"  日本株 急騰初動ピックアップ｜{run_date}",
                "description": description,
                "color": color,
                "footer": {
                    "text": (
                        f"条件: 前日比+{MIN_PRICE_CHANGE_PCT}%以上 / "
                        f"出来高{MIN_VOLUME_RATIO}倍以上 / "
                        f"時価総額{MIN_MARKET_CAP_JPY//100_000_000}億円以上"
                    )
                },
                "timestamp": datetime.now(JST).isoformat(),
            }
        ]
    }


def send_discord(payload: dict) -> bool:
    resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
    if resp.status_code in (200, 204):
        log.info("Discord通知成功")
        return True
    log.error(f"Discord通知失敗: {resp.status_code} {resp.text}")
    return False


# ─────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry", action="store_true", help="Discord送信しない（テスト）")
    args = parser.parse_args()

    now_jst = datetime.now(JST)
    run_date = now_jst.strftime("%Y/%m/%d")
    log.info(f"=== 日本株スクリーニング開始 {run_date} ===")

    tickers = fetch_prime_tickers()
    results = []

    for i, ticker in enumerate(tickers):
        result = screen_ticker(ticker)
        if result:
            log.info(
                f"? HIT: {result['name']} ({ticker}) "
                f"+{result['price_chg']}% 出来高{result['vol_ratio']}倍"
            )
            results.append(result)
        if i % 50 == 0:
            log.info(f"進捗: {i}/{len(tickers)} 完了")
        time.sleep(FETCH_SLEEP_SEC)

    # 価格変化率の降順でソート
    results.sort(key=lambda x: x["price_chg"], reverse=True)
    log.info(f"=== スクリーニング完了: {len(results)}銘柄ヒット ===")

    payload = build_discord_payload(results, run_date)

    if args.dry:
        import json
        print("\n[DRY RUN] Discord送信内容:")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        send_discord(payload)


if __name__ == "__main__":
    main()
