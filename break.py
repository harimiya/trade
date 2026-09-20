import os
import requests
import yfinance as yf
import pandas as pd

# 1. 監視対象の指数・ETF・銘柄リスト
TARGET_TICKERS = {
    # 主要株価指数・セクター指数
    "^SOX": "フィラデルフィア半導体株指数 (SOX)",
    "^GSPC": "S&P 500",
    "^IXIC": "NASDAQ Composite",
    "^DJI": "ダウ工業株30種平均",
    "^NDX": "NASDAQ 100",
    "^N225": "日経平均株価",
    "^RUT": "ラッセル2000 (小型株)",
    
    # コモディティ・コモディティETF
    "GLD": "SPDR Gold Shares (金ETF)",
    "SLV": "iShares Silver Trust (銀ETF)",
    "USO": "United States Oil Fund (原油ETF)",
    "UNG": "United States Natural Gas Fund (天然ガスETF)",
    
    # セクター別ETF (US)
    "SMH": "VanEck Semiconductor ETF",
    "XLK": "テクノロジー・セクター ETF",
    "XLF": "金融セクター ETF",
    "XLE": "エネルギー・セクター ETF",
    "XLV": "ヘルスケア・セクター ETF",
    
    # 国別・地域別ETF
    "EEM": "iShares MSCI Emerging Markets (新興国)",
    "EFA": "iShares MSCI EAFE (先進国除米)"
}

# 6か月間（約126営業日）の取引日数を指定
LOOKBACK_DAYS = 126

def check_breakout(ticker_symbol: str, name: str):
    """
    指定したティッカーの過去データから直近6か月間の高値ブレイクアウト／安値ブレイクアウトを検出する
    """
    try:
        # 過去6か月＋余剰期間（180日分の日足データ）を取得
        df = yf.download(ticker_symbol, period="180d", interval="1d", progress=False)
        if df.empty or len(df) < LOOKBACK_DAYS + 1:
            return None

        # マルチインデックスの整形 (yfinanceの仕様対応)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # 直前（昨日まで）の過去6か月（126営業日）における最高値と最安値を算出
        past_high_max = df['High'].iloc[-(LOOKBACK_DAYS + 1):-1].max()
        past_low_min = df['Low'].iloc[-(LOOKBACK_DAYS + 1):-1].min()
        
        # 最新（当日）のデータ
        latest_close = df['Close'].iloc[-1]
        latest_date = df.index[-1].strftime('%Y-%m-%d')

        # 上値ブレイクアウト（高値更新）判定
        if latest_close >= past_high_max:
            pct_change = ((latest_close - past_high_max) / past_high_max) * 100
            return {
                "symbol": ticker_symbol,
                "name": name,
                "type": "HIGH_BREAKOUT",  # 高値ブレイク
                "date": latest_date,
                "close": round(float(latest_close), 2),
                "threshold": round(float(past_high_max), 2),
                "pct": round(float(pct_change), 2)
            }

        # 下値ブレイクアウト（安値更新）判定
        elif latest_close <= past_low_min:
            pct_change = ((latest_close - past_low_min) / past_low_min) * 100
            return {
                "symbol": ticker_symbol,
                "name": name,
                "type": "LOW_BREAKOUT",   # 安値ブレイク
                "date": latest_date,
                "close": round(float(latest_close), 2),
                "threshold": round(float(past_low_min), 2),
                "pct": round(float(pct_change), 2)
            }

    except Exception as e:
        print(f"Error fetching data for {ticker_symbol}: {e}")
    return None

def send_notification(alerts):
    """
    Webhook (Discord / Slack) へ結果を通知する
    """
    webhook_url = os.getenv("WEBHOOK_URL")
    if not webhook_url:
        print("WEBHOOK_URL is not set. Skipping notification.")
        return

    if not alerts:
        message = "📊 **【毎朝市場ブレイクアウトチェック】**\n本日、直近6か月間の高値・安値をブレイクした指数・ETFはありません。"
    else:
        message = "🚨 **【6か月高値・安値ブレイクアウト検出】**\n以下の指数・ETFでブレイクアウトが発生しました：\n\n"
        for item in alerts:
            if item["type"] == "HIGH_BREAKOUT":
                message += (
                    f"🚀 **[高値更新] {item['name']}** (`{item['symbol']}`)\n"
                    f"  - 日付: {item['date']}\n"
                    f"  - 終値: {item['close']} (6か月高値: {item['threshold']} / 変化率: +{item['pct']}%)\n\n"
                )
            else:
                message += (
                    f"⚠️ **[安値更新] {item['name']}** (`{item['symbol']}`)\n"
                    f"  - 日付: {item['date']}\n"
                    f"  - 終値: {item['close']} (6か月安値: {item['threshold']} / 変化率: {item['pct']}%)\n\n"
                )

    # Discord / Slack 互換送信フォーマット
    payload = {"content": message, "text": message}
    requests.post(webhook_url, json=payload)

def main():
    detected_alerts = []
    print("6か月間の高値・安値ブレイクアウトの検証を開始...")
    for symbol, name in TARGET_TICKERS.items():
        result = check_breakout(symbol, name)
        if result:
            print(f"[{result['type']}] {name} ({symbol})")
            detected_alerts.append(result)
            
    send_notification(detected_alerts)

if __name__ == "__main__":
    main()
