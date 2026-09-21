import os
import requests
import yfinance as yf
import pandas as pd

# 1. 監視対象：全世界・米国・日本・東証ETF・コモディティ・債券・為替（全約120銘柄）
TARGET_TICKERS = {
    # ==========================================
    # 1. 米国主要指数 & 先端テクノロジー・セクター
    # ==========================================
    "^GSPC": "S&P 500",
    "^IXIC": "NASDAQ Composite",
    "^NDX": "NASDAQ 100",
    "^DJI": "ダウ工業株30種平均",
    "^RUT": "ラッセル2000 (米国小型株)",
    "^MID": "S&P 400 (米国中型株)",
    "^SOX": "フィラデルフィア半導体株指数 (SOX)",
    "SMH": "VanEck Semiconductor ETF",
    "SOXX": "iShares Semiconductor ETF",
    "BOTZ": "Global X Robotics & AI ETF",
    "IGV": "iShares Expanded Tech-Software ETF",
    "CIBR": "First Trust Cybersecurity ETF",
    "SKYY": "First Trust Cloud Computing ETF",
    "QTUM": "Defiance Quantum ETF (量子コンピュータ)",
    
    # 米国11セクターETF
    "XLK": "米国テクノロジー (XLK)",
    "XLC": "米国通信サービス (XLC)",
    "XLY": "米国一般消費財 (XLY)",
    "XLP": "米国生活必需品 (XLP)",
    "XLE": "米国エネルギー (XLE)",
    "XLF": "米国金融 (XLF)",
    "XLV": "米国ヘルスケア (XLV)",
    "XLI": "米国資本財・サービス (XLI)",
    "XLB": "米国素材 (XLB)",
    "XLRE": "米国不動産 (XLRE)",
    "XLU": "米国公益事業 (XLU)",

    # テーマ別・アクティブETF
    "ARKK": "ARK Innovation ETF",
    "XBI": "SPDR S&P Biotech ETF (バイオ)",
    "TAN": "Invesco Solar ETF (クリーンエネルギー)",
    "LIT": "Global X Lithium & Battery ETF",
    "PPA": "Invesco Aerospace & Defense ETF (防衛)",
    "JETS": "U.S. Global Jets ETF (航空)",
    "MJ": "ETFMG Alternative Harvest (大麻)",

    # ==========================================
    # 2. 日本市場（指数・東証セクター・テーマETF）
    # ==========================================
    "^N225": "日経平均株価",
    "1321.T": "NEXT FUNDS 日経225連動型上場投信",
    "1306.T": "NEXT FUNDS TOPIX連動型上場投信",
    "1655.T": "iShares TOPIX Small Cap (日本小型株)",
    "2516.T": "東証グロース250ETF (旧マザーズ)",
    "1570.T": "NF日経平均レバレッジ・インデックス",

    # 東証17業種（セクター別ETF）
    "1617.T": "東証17 食品",
    "1618.T": "東証17 エネルギー資源",
    "1619.T": "東証17 建設・資材",
    "1620.T": "東証17 素材・化学",
    "1621.T": "東証17 医薬品",
    "1622.T": "東証17 自動車・輸送機",
    "1623.T": "東証17 鉄鋼・非鉄",
    "1624.T": "東証17 機械",
    "1625.T": "東証17 電機・精密 (半導体関連含む)",
    "1626.T": "東証17 情報通信・サービスその他",
    "1627.T": "東証17 電力・ガス",
    "1628.T": "東証17 運輸・物流",
    "1629.T": "東証17 商社・卸売",
    "1630.T": "東証17 小売",
    "1631.T": "東証17 銀行",
    "1632.T": "東証17 金融(除く銀行)",
    "1633.T": "東証17 不動産",

    # 国内テーマ・日本から買える人気指標連動ETF
    "1489.T": "NF・日本高配当70 ETF",
    "1577.T": "NF・野村日本株高配当70 ETF",
    "2244.T": "GX US Tech Top20 (東証上場)",
    "2243.T": "GX 半導体(SOX指数連動) ETF (東証上場)",
    "2644.T": "GX グローバル半導体株 ETF (東証上場)",
    "1545.T": "NEXT FUNDS NASDAQ100連動型 (東証上場)",
    "2559.T": "MAXIS 全世界株式(オール・カントリー) ETF",
    "1678.T": "NEXT FUNDS インド株式(Nifty50)連動型",
    "2530.T": "MAXIS 新興国株式(MSCIエマージング) ETF",
    "1309.T": "NEXT FUNDS 中国株(上海50)連動型",
    "1386.T": "UBS ETF 欧州株 (MSCI EMU)",
    "1343.T": "NEXT FUNDS 東証REIT指数連動型",

    # ==========================================
    # 3. グローバル地域・新興国市場
    # ==========================================
    "^GDAXI": "ドイツ DAX",
    "^FTSE": "イギリス FTSE100",
    "^FCHI": "フランス CAC40",
    "^HSI": "香港ハンセン指数",
    "000001.SS": "上海総合指数",
    "EEM": "iShares MSCI Emerging Markets (新興国全体)",
    "EFA": "iShares MSCI EAFE (先進国除米)",
    "INDA": "iShares MSCI India ETF (インド株)",
    "^EWT": "iShares MSCI Taiwan ETF (台湾株)",
    "^EWY": "iShares MSCI South Korea ETF (韓国株)",
    "EWZ": "iShares MSCI Brazil ETF (ブラジル株)",

    # ==========================================
    # 4. コモディティ（貴金属・エネルギー・農産物）
    # ==========================================
    "GLD": "SPDR Gold Shares (金ETF・米国)",
    "1540.T": "純金信託 (純金上場投資信託・東証)",
    "SLV": "iShares Silver Trust (銀ETF)",
    "1542.T": "純銀信託 (東証)",
    "PPLT": "Aberdeen Physical Platinum ETF (プラチナ)",
    "1541.T": "純プラチナ信託 (東証)",
    "CPER": "United States Copper Index Fund (銅)",
    "USO": "United States Oil Fund (原油ETF)",
    "1671.T": "WTI原油価格連動型上場投信 (東証)",
    "UNG": "United States Natural Gas Fund (天然ガスETF)",
    "1689.T": "WisdomTree 天然ガス上場投資信託 (東証)",
    "DBA": "Invesco DB Agriculture Fund (農産物総合)",
    "WEAT": "Teucrium Wheat Fund (小麦)",
    "1687.T": "WisdomTree 農業再生上場投資信託 (東証)",

    # ==========================================
    # 5. 債券・マクロ・リスク指標 & 暗号資産
    # ==========================================
    "^VIX": "CBOE VIX指数 (恐怖指数)",
    "TLT": "iShares 20+ Year Treasury Bond ETF (米国超長期国債)",
    "IEF": "iShares 7-10 Year Treasury Bond ETF (米国中期国債)",
    "HYG": "iShares High Yield Corporate Bond (ハイイールド債)",
    "LQD": "iShares Investment Grade Corporate Bond (社債)",
    "IBIT": "iShares Bitcoin Trust (ビットコイン現物)",
    "ETHA": "iShares Ethereum Trust (イーサリアム現物)",
    "WGMI": "Valkyrie Bitcoin Miners ETF (マイニング株)",
    "COIN": "Coinbase Global",

    # ==========================================
    # 6. 為替（主要通貨ペア）
    # ==========================================
    "USDJPY=X": "米ドル / 日本円",
    "EURUSD=X": "ユーロ / 米ドル",
    "EURJPY=X": "ユーロ / 日本円",
    "GBPJPY=X": "英ポンド / 日本円",
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
    """
    webhook_url = os.getenv("WEBHOOK_URL")
    if not webhook_url:
        print("WEBHOOK_URL is not set. Skipping notification.")
        return

    if not alerts:
        message = "📊 **【毎朝市場ブレイクアウトチェック】**\n本日、直近6か月間の高値・安値をブレイクした指数・ETFはありませんでした。"
        requests.post(webhook_url, json={"content": message, "text": message})
        return

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
    print(f"全 {len(TARGET_TICKERS)} 銘柄（グローバル統合版）の6か月高値・安値ブレイクアウト検証を開始...")
    for symbol, name in TARGET_TICKERS.items():
        result = check_breakout(symbol, name)
        if result:
            print(f"[{result['type']}] {name} ({symbol})")
            detected_alerts.append(result)
            
    send_notification(detected_alerts)

if __name__ == "__main__":
    main()
