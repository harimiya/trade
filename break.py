import os
import requests
import yfinance as yf
import pandas as pd

# 1. 監視対象：日本の東証上場ETF・国内指標・為替（約70銘柄）
# コード末尾に ".T" を付けることで東証上場銘柄を取得可能です。
TARGET_TICKERS = {
    # --- 日本主要株価指数・サイズ別 ---
    "^N225": "日経平均株価 (日経225)",
    "1321.T": "NEXT FUNDS 日経225連動型上場投信",
    "1306.T": "NEXT FUNDS TOPIX連動型上場投信",
    "1655.T": "iShares TOPIX Small Cap (小型株)",
    "2516.T": "東証グロース250ETF (旧マザーズ)",
    "1570.T": "NF日経平均レバレッジ・インデックス",

    # --- 日本株 東証17業種（東証セクター別ETF） ---
    "1617.T": "NF・TOPIX-17 食品",
    "1618.T": "NF・TOPIX-17 エネルギー資源",
    "1619.T": "NF・TOPIX-17 建設・資材",
    "1620.T": "NF・TOPIX-17 素材・化学",
    "1621.T": "NF・TOPIX-17 医薬品",
    "1622.T": "NF・TOPIX-17 自動車・輸送機",
    "1623.T": "NF・TOPIX-17 鉄鋼・非鉄",
    "1624.T": "NF・TOPIX-17 機械",
    "1625.T": "NF・TOPIX-17 電機・精密 (半導体関連含む)",
    "1626.T": "NF・TOPIX-17 情報通信・サービスその他",
    "1627.T": "NF・TOPIX-17 電力・ガス",
    "1628.T": "NF・TOPIX-17 運輸・物流",
    "1629.T": "NF・TOPIX-17 商社・卸売",
    "1630.T": "NF・TOPIX-17 小売",
    "1631.T": "NF・TOPIX-17 銀行",
    "1632.T": "NF・TOPIX-17 金融(除く銀行)",
    "1633.T": "NF・TOPIX-17 不動産",

    # --- 国内高配当・テーマ株ETF ---
    "1489.T": "NF・日本高配当70 ETF",
    "1577.T": "NF・野村日本株高配当70 ETF",
    "2244.T": "GX US Tech Top20 (米Tech巨頭・東証上場)",
    "2243.T": "GX 半導体株 ETF (東証上場)",
    "2644.T": "GX グローバル半導体株 ETF (日本株関連)",

    # --- 東証上場・海外株価指数連動ETF ---
    "2243.T": "GX 半導体(SOX指数連動) ETF",
    "1545.T": "NEXT FUNDS NASDAQ100連動型上場投信",
    "1655.T": "iShares S&P 500 米国株 ETF",
    "2559.T": "MAXIS 全世界株式(オール・カントリー) ETF",
    "1678.T": "NEXT FUNDS インド株式(Nifty50)連動型",
    "2530.T": "MAXIS 新興国株式(MSCIエマージング) ETF",
    "1309.T": "NEXT FUNDS 中国株(上海50)連動型",
    "1386.T": "UBS ETF 欧州株 (MSCI EMU)",

    # --- 東証上場・コモディティ（貴金属・原油等） ---
    "1540.T": "純金信託 (純金上場投資信託)",
    "1541.T": "純プラチナ信託",
    "1542.T": "純銀信託",
    "1671.T": "WTI原油価格連動型上場投信",
    "1689.T": "WisdomTree 天然ガス上場投資信託",
    "1687.T": "WisdomTree 農業再生上場投資信託",

    # --- 東証上場・J-REIT (不動産投信) ---
    "1343.T": "NEXT FUNDS 東証REIT指数連動型上場投信",

    # --- 海外主要指数（比較用） ---
    "^SOX": "【原指標】フィラデルフィア半導体株指数 (SOX)",
    "^GSPC": "【原指標】S&P 500",
    "^IXIC": "【原指標】NASDAQ Composite",
    
    # --- 為替・ドル円（日本市場への影響大） ---
    "USDJPY=X": "米ドル / 日本円",
    "EURJPY=X": "ユーロ / 日本円",
    "GBPJPY=X": "英ポンド / 日本円"
}

# 6か月間（約126営業日）の取引日数を指定
LOOKBACK_DAYS = 126

def check_breakout(ticker_symbol: str, name: str):
    """
    指定したティッカーの過去データから直近6か月間の高値・安値ブレイクアウトを検出する
    """
    try:
        df = yf.download(ticker_symbol, period="180d", interval="1d", progress=False)
        if df.empty or len(df) < LOOKBACK_DAYS + 1:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # 直前（昨日まで）の過去126営業日の最高値・最安値
        past_high_max = df['High'].iloc[-(LOOKBACK_DAYS + 1):-1].max()
        past_low_min = df['Low'].iloc[-(LOOKBACK_DAYS + 1):-1].min()
        
        latest_close = df['Close'].iloc[-1]
        latest_date = df.index[-1].strftime('%Y-%m-%d')

        # 高値ブレイク（上値離れ）
        if latest_close >= past_high_max:
            pct_change = ((latest_close - past_high_max) / past_high_max) * 100
            return {
                "symbol": ticker_symbol,
                "name": name,
                "type": "HIGH_BREAKOUT",
                "date": latest_date,
                "close": round(float(latest_close), 2),
                "threshold": round(float(past_high_max), 2),
                "pct": round(float(pct_change), 2)
            }

        # 安値ブレイク（下値離れ）
        elif latest_close <= past_low_min:
            pct_change = ((latest_close - past_low_min) / past_low_min) * 100
            return {
                "symbol": ticker_symbol,
                "name": name,
                "type": "LOW_BREAKOUT",
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
    Webhook (Discord / Slack) へ結果を分割通知する
    """
    webhook_url = os.getenv("WEBHOOK_URL")
    if not webhook_url:
        print("WEBHOOK_URL is not set. Skipping notification.")
        return

    if not alerts:
        message = "📊 **【毎朝市場ブレイクアウトチェック】**\n本日、直近6か月間の高値・安値をブレイクした日本のETF・指数はありませんでした。"
        requests.post(webhook_url, json={"content": message, "text": message})
        return

    high_alerts = [a for a in alerts if a["type"] == "HIGH_BREAKOUT"]
    low_alerts = [a for a in alerts if a["type"] == "LOW_BREAKOUT"]

    messages = []
    
    if high_alerts:
        msg = f"🚀 **【日本市場・ETF 6か月高値ブレイクアウト ({len(high_alerts)}件)】**\n"
        for item in high_alerts:
            line = f"・**{item['name']}** (`{item['symbol']}`)\n  └ 終値: {item['close']} (過去高値: {item['threshold']} / +{item['pct']}%)\n"
            if len(msg) + len(line) > 1800:
                messages.append(msg)
                msg = ""
            msg += line
        messages.append(msg)

    if low_alerts:
        msg = f"⚠️ **【日本市場・ETF 6か月安値ブレイクアウト ({len(low_alerts)}件)】**\n"
        for item in low_alerts:
            line = f"・**{item['name']}** (`{item['symbol']}`)\n  └ 終値: {item['close']} (過去安値: {item['threshold']} / {item['pct']}%)\n"
            if len(msg) + len(line) > 1800:
                messages.append(msg)
                msg = ""
            msg += line
        messages.append(msg)

    for m in messages:
        requests.post(webhook_url, json={"content": m, "text": m})

def main():
    detected_alerts = []
    print(f"全 {len(TARGET_TICKERS)} 銘柄（日本市場メイン）の6か月高値・安値ブレイクアウト検証を開始...")
    for symbol, name in TARGET_TICKERS.items():
        result = check_breakout(symbol, name)
        if result:
            print(f"[{result['type']}] {name} ({symbol})")
            detected_alerts.append(result)
            
    send_notification(detected_alerts)

if __name__ == "__main__":
    main()
