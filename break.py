import os
import requests
import yfinance as yf
import pandas as pd

# 1. 監視対象の指数・ETF・銘柄リスト（約80銘柄）
TARGET_TICKERS = {
    # --- 米国主要株価指数・サイズ別 ---
    "^GSPC": "S&P 500",
    "^IXIC": "NASDAQ Composite",
    "^NDX": "NASDAQ 100",
    "^DJI": "ダウ工業株30種平均",
    "^RUT": "ラッセル2000 (小型株)",
    "^MID": "S&P 400 (中型株)",
    
    # --- 半導体・ハイテク・先端技術 ---
    "^SOX": "フィラデルフィア半導体株指数 (SOX)",
    "SMH": "VanEck Semiconductor ETF",
    "SOXX": "iShares Semiconductor ETF",
    "BOTZ": "Global X Robotics & AI ETF",
    "IGV": "iShares Expanded Tech-Software ETF",
    "CIBR": "First Trust Cybersecurity ETF",
    "SKYY": "First Trust Cloud Computing ETF",
    "QTUM": "Defiance Quantum ETF (量子コンピュータ)",
    
    # --- 米国セクター（全11セクター） ---
    "XLK": "テクノロジー (XLK)",
    "XLC": "通信サービス (XLC)",
    "XLY": "一般消費財 (XLY)",
    "XLP": "生活必需品 (XLP)",
    "XLE": "エネルギー (XLE)",
    "XLF": "金融 (XLF)",
    "XLV": "ヘルスケア (XLV)",
    "XLI": "資本財・サービス (XLI)",
    "XLB": "素材 (XLB)",
    "XLRE": "不動産 (XLRE)",
    "XLU": "公益事業 (XLU)",

    # --- テーマ別・成長セクター ---
    "ARKK": "ARK Innovation ETF",
    "XBI": "SPDR S&P Biotech ETF (バイオ)",
    "TAN": "Invesco Solar ETF (クリーンエネルギー)",
    "LIT": "Global X Lithium & Battery ETF",
    "PPA": "Invesco Aerospace & Defense ETF (防衛)",
    "JETS": "U.S. Global Jets ETF (航空)",
    "MJ": "ETFMG Alternative Harvest (大麻)",
    
    # --- 暗号資産・関連ETF ---
    "IBIT": "iShares Bitcoin Trust (ビットコイン現物)",
    "ETHA": "iShares Ethereum Trust (イーサリアム現物)",
    "WGMI": "Valkyrie Bitcoin Miners ETF (マイニング株)",
    "COIN": "Coinbase Global",
    
    # --- 日本株・アジア ---
    "^N225": "日経平均株価",
    "1321.T": "NEXT FUNDS 日経225連動型上場投信",
    "1306.T": "NEXT FUNDS TOPIX連動型上場投信",
    "1570.T": "NF日経平均レバレッジ",
    "2516.T": "東証グロース250ETF",
    "^HSI": "香港ハンセン指数",
    "000001.SS": "上海総合指数",
    "^EWT": "iShares MSCI Taiwan ETF",
    "^EWY": "iShares MSCI South Korea ETF",
    "INDA": "iShares MSCI India ETF",
    
    # --- 欧州・その他新興国 ---
    "^GDAXI": "ドイツ DAX",
    "^FTSE": "イギリス FTSE100",
    "^FCHI": "フランス CAC40",
    "EEM": "iShares MSCI Emerging Markets (新興国全体)",
    "EWZ": "iShares MSCI Brazil ETF",
    
    # --- コモディティ（貴金属・エネルギー・農産物） ---
    "GLD": "SPDR Gold Shares (金ETF)",
    "SLV": "iShares Silver Trust (銀ETF)",
    "PPLT": "Aberdeen Physical Platinum ETF (プラチナ)",
    "CPER": "United States Copper Index Fund (銅)",
    "USO": "United States Oil Fund (原油)",
    "UNG": "United States Natural Gas Fund (天然ガス)",
    "DBA": "Invesco DB Agriculture Fund (農産物総合)",
    "WEAT": "Teucrium Wheat Fund (小麦)",
    
    # --- 債券・金利・市場の恐怖指数 ---
    "^VIX": "CBOE VIX指数 (恐怖指数)",
    "TLT": "iShares 20+ Year Treasury Bond ETF (超長期国債)",
    "IEF": "iShares 7-10 Year Treasury Bond ETF (中期国債)",
    "HYG": "iShares iBoxx High Yield Corporate Bond (ハイイールド債)",
    "LQD": "iShares iBoxx Investment Grade Corporate Bond (社債)",
    
    # --- 為替（主要通貨ペア） ---
    "USDJPY=X": "米ドル / 日本円",
    "EURUSD=X": "ユーロ / 米ドル",
    "GBPUSD=X": "英ポンド / 米ドル",
    "AUDUSD=X": "豪ドル / 米ドル"
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
    ※ 文字数制限（2000文字）を考慮して分割送信を行います
    """
    webhook_url = os.getenv("WEBHOOK_URL")
    if not webhook_url:
        print("WEBHOOK_URL is not set. Skipping notification.")
        return

    if not alerts:
        message = "📊 **【毎朝市場ブレイクアウトチェック】**\n本日、直近6か月間の高値・安値をブレイクした銘柄はありませんでした。"
        requests.post(webhook_url, json={"content": message, "text": message})
        return

    # 高値更新と安値更新に分別
    high_alerts = [a for a in alerts if a["type"] == "HIGH_BREAKOUT"]
    low_alerts = [a for a in alerts if a["type"] == "LOW_BREAKOUT"]

    messages = []
    
    if high_alerts:
        msg = f"🚀 **【6か月高値ブレイクアウト ({len(high_alerts)}件)】**\n"
        for item in high_alerts:
            line = f"・**{item['name']}** (`{item['symbol']}`)\n  └ 終値: {item['close']} (過去高値: {item['threshold']} / +{item['pct']}%)\n"
            if len(msg) + len(line) > 1800:
                messages.append(msg)
                msg = ""
            msg += line
        messages.append(msg)

    if low_alerts:
        msg = f"⚠️ **【6か月安値ブレイクアウト ({len(low_alerts)}件)】**\n"
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
    print(f"全 {len(TARGET_TICKERS)} 銘柄の6か月高値・安値ブレイクアウト検証を開始...")
    for symbol, name in TARGET_TICKERS.items():
        result = check_breakout(symbol, name)
        if result:
            print(f"[{result['type']}] {name} ({symbol})")
            detected_alerts.append(result)
            
    send_notification(detected_alerts)

if __name__ == "__main__":
    main()
