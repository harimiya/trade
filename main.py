import os
import requests
import yfinance as yf
import pandas as pd

# 1. 監視対象の指数・ETF・銘柄リスト（ティッカーシンボル）
# 上場している主要指数・ETFなどを網羅的に設定します
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

# 2. 上値ブレイクアウト判定ロジック
LOOKBACK_DAYS = 20  # 過去何日間の最高値を基準にするか (例: 20日ブレイクアウト / 60日等に変更可能)

def check_breakout(ticker_symbol: str, name: str):
    """
    指定したティッカーの過去データから直近の上値ブレイクアウトを検出する
    """
    try:
        # 直近のデータを取得 (余裕を持って60日分)
        df = yf.download(ticker_symbol, period="60d", interval="1d", progress=False)
        if df.empty or len(df) < LOOKBACK_DAYS + 1:
            return None

        # マルチインデックスの整形 (yfinanceの仕様変更対応)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # 直前（昨日まで）の過去N日間の最高値を計算
        past_high_max = df['High'].iloc[-(LOOKBACK_DAYS + 1):-1].max()
        
        # 最新（当日）の終値・高値
        latest_close = df['Close'].iloc[-1]
        latest_high = df['High'].iloc[-1]
        latest_date = df.index[-1].strftime('%Y-%m-%d')

        # 上値離れ（Breakout）判定: 当日終値が過去N日間の高値を上回ったか
        if latest_close >= past_high_max:
            pct_change = ((latest_close - past_high_max) / past_high_max) * 100
            return {
                "symbol": ticker_symbol,
                "name": name,
                "date": latest_date,
                "close": round(float(latest_close), 2),
                "past_max": round(float(past_high_max), 2),
                "pct": round(float(pct_change), 2)
            }
    except Exception as e:
        print(f"Error fetching data for {ticker_symbol}: {e}")
    return None

def send_notification(alerts):
    """
    Webhook (Discord または Slack) へ結果を通知する
    """
    webhook_url = os.getenv("WEBHOOK_URL")
    if not webhook_url:
        print("WEBHOOK_URL is not set. Skipping notification.")
        return

    if not alerts:
        message = "📊 **【毎朝市場ブレイクアウトチェック】**\n本日、上値離れ（新高値ブレイクアウト）を検出した指数・ETFはありません。"
    else:
        message = "🚀 **【上値離れ（ブレイクアウト）サイン検出】**\n以下の指数・ETFが過去20日間の上値をブレイクしました：\n\n"
        for item in alerts:
            message += (
                f"・**{item['name']}** (`{item['symbol']}`)\n"
                f"  - 日付: {item['date']}\n"
                f"  - 終値: {item['close']} (過去最高値ブレイク: +{item['pct']}%)\n"
            )

    # Discord / Slack 互換送信フォーマット
    payload = {"content": message, "text": message}
    requests.post(webhook_url, json=payload)

def main():
    detected_alerts = []
    print("市場データのスクレイピングおよびブレイクアウト判定を開始...")
    for symbol, name in TARGET_TICKERS.items():
        result = check_breakout(symbol, name)
        if result:
            print(f"[Breakout Target Detected] {name} ({symbol})")
            detected_alerts.append(result)
            
    send_notification(detected_alerts)

if __name__ == "__main__":
    main()
