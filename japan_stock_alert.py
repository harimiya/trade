"""
日本株 値上がり初動ピックアップ → Discord通知スクリプト
============================================================
対象シグナル（古河電工 2026/2/9 パターン）:
  1. 前日終値比 +5%以上 の急騰
  2. 出来高が20日平均の 3倍以上
  3. 時価総額 1000億円以上
  4. 直近3年間の高値を更新（ブレイクアウト確認）
  5. （任意）直近に決算・上方修正・増配ニュース

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
_WEBHOOK_FALLBACK = (
    "https://discord.com/api/webhooks/1490293520085418125/"
    "b3PUB4-Y5guGheOKFzI8LIwHlY7q8upaEzZzFI5knJkWlAkw_JXgoEzSs9HMMCh9v8nr"
)
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip() or _WEBHOOK_FALLBACK

if not DISCORD_WEBHOOK_URL.startswith("https://"):
    raise RuntimeError(f"DISCORD_WEBHOOK_URL が無効です: '{DISCORD_WEBHOOK_URL}'")

# スクリーニング条件
MIN_MARKET_CAP_JPY   = 100_000_000_000   # 時価総額 1000億円以上
MIN_PRICE_CHANGE_PCT = 5.0               # 前日比 +5%以上
MIN_VOLUME_RATIO     = 3.0               # 20日平均出来高の3倍以上
LOOKBACK_DAYS        = 30                # 出来高平均算出期間
HIGH_LOOKBACK_YEARS  = 3                 # 高値更新チェック期間（年）

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
# 高値更新チェック
# ─────────────────────────────────────────────
def check_high_breakouts(hist_3y: pd.DataFrame, last_close: float) -> dict:
    """
    直近1年・2年・3年の高値（当日を除く）を last_close が更新しているか判定。
    戻り値例: {"1y": True, "2y": False, "3y": False}
    """
    today_idx = len(hist_3y) - 1  # 最新行インデックス
    result = {}
    for label, years in [("1y", 1), ("2y", 2), ("3y", 3)]:
        cutoff = hist_3y.index[-1] - pd.DateOffset(years=years)
        window = hist_3y.iloc[:today_idx]          # 当日を除く過去全期間
        window = window[window.index >= cutoff]    # 指定年数分に絞る
        if window.empty:
            result[label] = False
            continue
        prev_high = float(window["High"].max())
        result[label] = last_close > prev_high
    return result


# ─────────────────────────────────────────────
# 個別銘柄スクリーニング
# ─────────────────────────────────────────────
def screen_ticker(ticker: str) -> dict | None:
    """
    Returns screening result dict if the stock passes all filters, else None.
    高値更新フラグ(1y/2y/3y)はフィルター条件ではなく情報として付与。
    """
    try:
        tk = yf.Ticker(ticker)

        # 3年分まとめて取得（出来高計算 + 高値更新チェック兼用）
        hist = tk.history(period="3y")
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

        # ── 出来高比率（直近20営業日平均） ──
        last_vol = float(hist["Volume"].iloc[-1])
        avg_vol  = float(hist["Volume"].iloc[-(LOOKBACK_DAYS + 1):-1].mean())
        if avg_vol == 0:
            return None
        vol_ratio = last_vol / avg_vol

        if vol_ratio < MIN_VOLUME_RATIO:
            return None

        # ── 時価総額 ──
        info    = tk.fast_info
        mkt_cap = getattr(info, "market_cap", None)
        if mkt_cap is None or mkt_cap < MIN_MARKET_CAP_JPY:
            return None

        # ── 高値更新チェック（1年・2年・3年） ──
        breakouts = check_high_breakouts(hist, last_close)

        # ── 銘柄名 ──
        try:
            short_name = tk.info.get("shortName") or tk.info.get("longName") or ticker
        except Exception:
            short_name = ticker

        return {
            "ticker":     ticker,
            "name":       short_name,
            "last_close": last_close,
            "price_chg":  round(price_chg_pct, 2),
            "vol_ratio":  round(vol_ratio, 1),
            "market_cap": mkt_cap,
            "new_high_1y": breakouts["1y"],
            "new_high_2y": breakouts["2y"],
            "new_high_3y": breakouts["3y"],
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

            # ── 高値更新バッジ ──
            badges = []
            if r.get("new_high_3y"):
                badges.append("  3年高値更新")
            elif r.get("new_high_2y"):
                badges.append("  2年高値更新")
            elif r.get("new_high_1y"):
                badges.append("  1年高値更新")
            badge_str = "　".join(badges) if badges else "  高値更新なし"

            lines.append(
                f"**{r['name']}** (`{r['ticker']}`)\n"
                f"　{badge_str}\n"
                f"　  前日比: **+{r['price_chg']}%**　"
                f"出来高: **{r['vol_ratio']}倍**　"
                f"時価総額: {mc}\n"
                f"　終値: \{r['last_close']:,.0f}"
            )
        description = "\n\n".join(lines)

        # 3年高値更新があれば金色、2年なら橙、1年なら黄、なしは赤
        if any(r.get("new_high_3y") for r in results):
            color = 0xf1c40f   # 金
        elif any(r.get("new_high_2y") for r in results):
            color = 0xe67e22   # 橙
        elif any(r.get("new_high_1y") for r in results):
            color = 0xf39c12   # 黄橙
        else:
            color = 0xe74c3c   # 赤

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
                        f"時価総額{MIN_MARKET_CAP_JPY//100_000_000}億円以上 / "
                        f" =3年高値更新  =2年  =1年"
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
            highs = " ".join([
                k for k, v in [("3y高値更新", result["new_high_3y"]),
                                ("2y高値更新", result["new_high_2y"]),
                                ("1y高値更新", result["new_high_1y"])] if v
            ]) or "高値更新なし"
            log.info(
                f"? HIT: {result['name']} ({ticker}) "
                f"+{result['price_chg']}% 出来高{result['vol_ratio']}倍 [{highs}]"
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
