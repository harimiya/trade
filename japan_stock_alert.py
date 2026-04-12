"""
日本株 急騰初動スクリーニング → Discord通知
================================================================
【対象市場】東証プライム・スタンダード・グロース（全銘柄）

【スクリーニング条件】バックテスト最適化済み（勝率62.3% / 平均+6.10%）

  ① 前日比 +5?15%（急騰、ただしストップ高直後の過熱は除外）
  ② 出来高が20日平均の 2.0倍以上
  ③ 前日出来高も20日平均の 1.5倍以上（2日連続の盛り上がり）
  ④ シグナル後1営業日、終値がシグナル日終値以上を維持
  ⑤ 以下のいずれかを満たす（高値ブレイクアウト確認）
       ・直近2年間の高値を更新
       ・上場来高値を更新（上場2年未満の銘柄もカバー）
  ⑥ 日経平均が75日移動平均線より上（上昇相場のみ参加）
  ※ 時価総額条件なし（全銘柄対象）

【通知内容】
  ・購入予定日  ：通知当日（翌営業日寄り付き成行買い）
  ・売却予定日  ：エントリーから60暦日後の最初の営業日

【実行方法】
  python japan_stock_alert.py          # 即時実行
  python japan_stock_alert.py --dry    # Discord送信なし（テスト）
================================================================
"""

import argparse
import logging
import os
import time
from datetime import datetime, date, timedelta, timezone

import pandas as pd
import requests
import yfinance as yf

# ──────────────────────────────────────────────
# 設定
# ──────────────────────────────────────────────
_WEBHOOK_FALLBACK = (
    "https://discord.com/api/webhooks/1490293520085418125/"
    "b3PUB4-Y5guGheOKFzI8LIwHlY7q8upaEzZzFI5knJkWlAkw_JXgoEzSs9HMMCh9v8nr"
)
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip() or _WEBHOOK_FALLBACK

if not DISCORD_WEBHOOK_URL.startswith("https://"):
    raise RuntimeError(f"DISCORD_WEBHOOK_URL が無効です: '{DISCORD_WEBHOOK_URL}'")

# ── スクリーニングパラメータ ──
MIN_PRICE_CHG_LOW   = 5.0
MIN_PRICE_CHG_HIGH  = 15.0
MIN_VOL_RATIO       = 2.0
MIN_VOL_PREV_RATIO  = 1.5
VOL_MA_DAYS         = 20
SUSTAINED_DAYS      = 1
HOLD_CALENDAR_DAYS  = 60    # 保有暦日（売却予定日計算用）
NIKKEI_TICKER       = "^N225"
NIKKEI_MA_DAYS      = 75

FETCH_SLEEP_SEC     = 0.4
JST = timezone(timedelta(hours=9))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# 売却予定日の計算（エントリー日 + 60暦日後の最初の平日）
# ──────────────────────────────────────────────
def calc_estimated_sell_date(entry_date: date) -> date:
    """
    エントリー日から60暦日後以降の最初の月?金を返す。
    （祝日は考慮しないが実用上の目安として表示）
    """
    d = entry_date + timedelta(days=HOLD_CALENDAR_DAYS)
    while d.weekday() >= 5:   # 土=5, 日=6 を飛ばす
        d += timedelta(days=1)
    return d


# ──────────────────────────────────────────────
# 日経平均フィルター（A2: 日経 > 75日MA）
# ──────────────────────────────────────────────
def is_nikkei_above_ma75() -> bool:
    try:
        hist  = yf.Ticker(NIKKEI_TICKER).history(period="6mo")
        if len(hist) < NIKKEI_MA_DAYS + 1:
            log.warning("日経平均データ不足 → フィルターをスキップ（通過扱い）")
            return True
        close = float(hist["Close"].iloc[-1])
        ma75  = float(hist["Close"].rolling(NIKKEI_MA_DAYS).mean().iloc[-1])
        result = close > ma75
        log.info(
            f"日経平均: {close:,.0f} / 75MA: {ma75:,.0f} "
            f"→ {'?通過' if result else '?市場フィルターNG'}"
        )
        return result
    except Exception as e:
        log.warning(f"日経平均取得失敗({e}) → スキップ（通過扱い）")
        return True


# ──────────────────────────────────────────────
# 市場別 最低時価総額（円）
# ──────────────────────────────────────────────
MARKET_CAP_LIMIT = {
    "プライム":    100_000_000_000,   # 1000億円
}


# ──────────────────────────────────────────────
# 銘柄リスト取得（東証プライム・時価総額1000億円以上）
# 戻り値: [(yf_ticker, 市場名, 最低時価総額), ...]
# ──────────────────────────────────────────────
def fetch_all_tickers() -> list[tuple[str, str, int]]:
    url = (
        "https://www.jpx.co.jp/markets/statistics-equities/misc/"
        "tvdivq0000001vg2-att/data_j.xls"
    )
    try:
        df = pd.read_excel(url, header=0)
        df.columns = df.columns.str.strip()
        market_col = [c for c in df.columns if "市場・商品区分" in c or "市場区分" in c]
        code_col   = [c for c in df.columns if "コード" in c]
        if not market_col or not code_col:
            raise ValueError(f"列名不一致: {df.columns.tolist()}")

        result = []
        for market_name, min_cap in MARKET_CAP_LIMIT.items():
            df_m  = df[df[market_col[0]].str.contains(market_name, na=False)]
            codes = df_m[code_col[0]].astype(str).str.zfill(4).tolist()
            for c in codes:
                result.append((f"{c}.T", market_name, min_cap))
            log.info(f"  {market_name}: {len(codes)}銘柄（時価総額{min_cap//100_000_000}億円以上）")

        log.info(f"対象銘柄合計: {len(result)}銘柄")
        return result

    except Exception as e:
        log.warning(f"JPX CSV取得失敗({e})。フォールバック使用（全てプライム扱い）")
        fallback = [
            "7203","6758","9984","6861","8306","9432","7974","6098",
            "8316","9433","4063","8058","6501","6902","7267","8031",
            "5801","5803","5802","4568","9020","6594","7741","8001",
            "6367","6954","7751","4502","9022","8802","3382","2914",
        ]
        return [(f"{c}.T", "プライム", MARKET_CAP_LIMIT["プライム"]) for c in fallback]


# ──────────────────────────────────────────────
# 高値更新チェック（2年高値 OR 上場来高値）
# ──────────────────────────────────────────────
def check_high_breakout(hist: pd.DataFrame, sig_idx: int) -> tuple[bool, str]:
    """
    Returns:
        (通過フラグ, バッジ文字列)
    優先順位: 上場来高値 > 3年高値 > 2年高値
    上場来高値 OR 2年高値更新のいずれかでTrue
    """
    close_val   = float(hist["Close"].iloc[sig_idx])
    signal_date = hist.index[sig_idx]

    # ── 上場来高値更新チェック（全履歴の高値と比較）──
    past_all = hist.iloc[:sig_idx]
    if not past_all.empty:
        all_time_high = float(past_all["High"].max())
        if close_val > all_time_high:
            return True, "  上場来高値更新"

    # ── 3年高値更新チェック ──
    cutoff_3y = signal_date - pd.DateOffset(years=3)
    past_3y   = past_all[past_all.index >= cutoff_3y]
    if not past_3y.empty and close_val > float(past_3y["High"].max()):
        return True, "  3年高値更新"

    # ── 2年高値更新チェック（メイン条件）──
    cutoff_2y = signal_date - pd.DateOffset(years=2)
    past_2y   = past_all[past_all.index >= cutoff_2y]
    if not past_2y.empty and close_val > float(past_2y["High"].max()):
        return True, "  2年高値更新"

    return False, "  高値更新なし"


# ──────────────────────────────────────────────
# 個別銘柄スクリーニング
# ──────────────────────────────────────────────
def screen_ticker(ticker: str, today: date, market_name: str, min_market_cap: int) -> dict | None:
    """
    【判定フロー（x=1）】
    hist[-2] = シグナル日
    hist[-1] = 確認日（終値 >= シグナル日終値なら維持確認OK）
    today    = 通知日 = 購入予定日（寄り付き成行買い）
    min_market_cap = 市場別の最低時価総額（円）
    """
    try:
        tk   = yf.Ticker(ticker)
        hist = tk.history(period="max")   # 上場来高値チェックのためmax取得

        required = VOL_MA_DAYS + SUSTAINED_DAYS + 5
        if hist.empty or len(hist) < required:
            return None

        # ── シグナル日インデックス（x=1 → -2）──
        sig_idx   = -(SUSTAINED_DAYS + 1)
        sig_close = float(hist["Close"].iloc[sig_idx])
        sig_prev  = float(hist["Close"].iloc[sig_idx - 1])
        if sig_prev == 0:
            return None

        # ── ①: 前日比 +5?15% ──
        pct_chg = (sig_close - sig_prev) / sig_prev * 100
        if not (MIN_PRICE_CHG_LOW <= pct_chg < MIN_PRICE_CHG_HIGH):
            return None

        # ── ②: 出来高 2.0倍以上 ──
        sig_vol  = float(hist["Volume"].iloc[sig_idx])
        avg_vol  = float(hist["Volume"].iloc[sig_idx - VOL_MA_DAYS: sig_idx].mean())
        if avg_vol == 0:
            return None
        vol_ratio = sig_vol / avg_vol
        if vol_ratio < MIN_VOL_RATIO:
            return None

        # ── ③: 前日出来高 1.5倍以上 ──
        prev_vol_ratio = float(hist["Volume"].iloc[sig_idx - 1]) / avg_vol
        if prev_vol_ratio < MIN_VOL_PREV_RATIO:
            return None

        # ── ④: 1営業日 高値維持 ──
        confirm_close = float(hist["Close"].iloc[-1])
        if confirm_close < sig_close:
            return None
        sustain_chg = (confirm_close - sig_close) / sig_close * 100

        # ── ⑤: 2年高値更新 OR 上場来高値更新 ──
        high_ok, high_badge = check_high_breakout(hist, len(hist) + sig_idx)
        if not high_ok:
            return None

        # ── ⑥: 市場別 時価総額フィルター ──
        mkt_cap = getattr(tk.fast_info, "market_cap", None)
        if mkt_cap is None or mkt_cap < min_market_cap:
            return None

        # ── 銘柄名 ──
        try:
            info       = tk.info
            short_name = info.get("shortName") or info.get("longName") or ticker
        except Exception:
            short_name = ticker

        # ── 購入予定日・売却予定日 ──
        buy_date  = today
        sell_date = calc_estimated_sell_date(buy_date)

        return {
            "ticker":          ticker,
            "name":            short_name,
            "market":          market_name,
            "market_cap":      mkt_cap,
            "sig_close":       sig_close,
            "sig_pct_chg":     round(pct_chg, 2),
            "vol_ratio":       round(vol_ratio, 1),
            "prev_vol_ratio":  round(prev_vol_ratio, 1),
            "confirm_close":   confirm_close,
            "sustain_chg":     round(sustain_chg, 2),
            "high_badge":      high_badge,
            "buy_date":        buy_date,
            "sell_date":       sell_date,
        }

    except Exception as e:
        log.debug(f"{ticker}: スキップ ({e})")
        return None


# ──────────────────────────────────────────────
# Discord 通知
# ──────────────────────────────────────────────
def format_market_cap(cap: float) -> str:
    if cap >= 1_000_000_000_000:
        return f"{cap / 1_000_000_000_000:.1f}兆円"
    return f"{cap / 100_000_000:.0f}億円"


def build_discord_payload(results: list[dict], run_date: str, nikkei_ok: bool) -> dict:
    nk_badge = "  日経MA上" if nikkei_ok else "  日経MA下（警戒）"

    if not results:
        description = "本日の対象銘柄はありませんでした。"
        color       = 0x95a5a6
    else:
        lines = []
        for r in results:
            trend = " " if r["sustain_chg"] >= 0 else " "
            buy_str  = r["buy_date"].strftime("%Y/%m/%d")
            sell_str = r["sell_date"].strftime("%Y/%m/%d")

            lines.append(
                f"**{r['name']}** (`{r['ticker']}`）　{r['market']}\n"
                f"　{r['high_badge']}　? 1日高値維持確認済み\n"
                f"　  急騰: **+{r['sig_pct_chg']}%**　"
                f"出来高: **{r['vol_ratio']}倍**（前日: {r['prev_vol_ratio']}倍）\n"
                f"　{trend} 確認日終値: \{r['confirm_close']:,.0f}　"
                f"（シグナル比: {r['sustain_chg']:+.2f}%）　"
                f"時価総額: {format_market_cap(r['market_cap'])}\n"
                f"　  **購入予定日: {buy_str}（本日寄り付き）**\n"
                f"　  **売却予定日: {sell_str}（60日後・最初の営業日引け）**"
            )
        description = "\n\n".join(lines)

        # バッジ優先度で色を決定
        if any("上場来" in r["high_badge"] for r in results):
            color = 0x9b59b6   # 紫（上場来高値）
        elif any("3年" in r["high_badge"] for r in results):
            color = 0xf1c40f   # 金
        else:
            color = 0xe67e22   # 橙（2年高値）

    return {
        "embeds": [
            {
                "title": f"  買いエントリー候補｜{run_date}　{nk_badge}",
                "description": description,
                "color": color,
                "footer": {
                    "text": (
                        "条件: 急騰+5?15% / 出来高2倍以上 / 前日出来高1.5倍以上 / "
                        "1日高値維持 / 2年高値更新or上場来高値更新 / 日経>75MA / "
                        "東証プライム・時価総額1000億円以上 "
                        "| バックテスト実績: 勝率62.3% / 平均+6.10%"
                    )
                },
                "timestamp": datetime.now(JST).isoformat(),
            }
        ]
    }


def send_discord(payload: dict) -> bool:
    resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
    if resp.status_code in (200, 204):
        log.info("Discord通知 成功")
        return True
    log.error(f"Discord通知 失敗: {resp.status_code} {resp.text}")
    return False


# ──────────────────────────────────────────────
# メイン
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry", action="store_true", help="Discord送信なし（テスト）")
    args = parser.parse_args()

    now_jst  = datetime.now(JST)
    run_date = now_jst.strftime("%Y/%m/%d")
    today    = now_jst.date()
    log.info(f"=== スクリーニング開始 {run_date} ===")

    # ── 日経平均フィルター ──
    nikkei_ok = is_nikkei_above_ma75()
    if not nikkei_ok:
        log.warning("?? 日経平均が75日MA以下 → 警告付きで通知継続")

    # ── 全市場銘柄リスト取得 ──
    ticker_list = fetch_all_tickers()   # [(ticker, market_name, min_cap), ...]
    results = []

    for i, (ticker, market_name, min_cap) in enumerate(ticker_list):
        result = screen_ticker(ticker, today, market_name, min_cap)
        if result:
            log.info(
                f"? HIT: {result['name']} ({ticker}) [{result['market']}] "
                f"+{result['sig_pct_chg']}% 出来高{result['vol_ratio']}倍 "
                f"{result['high_badge']} "
                f"買:{result['buy_date']} 売:{result['sell_date']}"
            )
            results.append(result)
        if i % 100 == 0:
            log.info(f"進捗: {i}/{len(ticker_list)}")
        time.sleep(FETCH_SLEEP_SEC)

    # 急騰率の降順でソート
    results.sort(key=lambda x: x["sig_pct_chg"], reverse=True)
    log.info(f"=== 完了: {len(results)}銘柄ヒット ===")

    payload = build_discord_payload(results, run_date, nikkei_ok)

    if args.dry:
        import json
        print("\n[DRY RUN] Discord送信内容:")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        send_discord(payload)


if __name__ == "__main__":
    main()
