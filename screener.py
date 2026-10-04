import os
import requests
import json
import pandas as pd
import yfinance as yf
import ta
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 1. 日本株全銘柄コードの取得 ---
def get_jquants_or_jp_symbols():
    """
    東証全銘柄のシンボルリストを取得（例として主要・全銘柄対応のCSV取得ソース）
    """
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    try:
        df = pd.read_excel(url)
        # コード列を取得し、yfinance形式 ("XXXX.T") に変換
        codes = df['コード'].astype(str).str.zfill(4) + ".T"
        return codes.tolist()
    except Exception as e:
        print(f"銘柄リスト取得エラー: {e}")
        # フォールバック用の代表銘柄リスト
        return ["7203.T", "6758.T", "9984.T", "8306.T", "6861.T"]

# --- 2. 各指標の条件判定（マネックス紹介指標） ---
def analyze_stock(ticker_symbol):
    try:
        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(period="6m")
        
        if len(df) < 50: # 計算に十分な期間がない場合スキップ
            return None

        # --- トレンド系 ---
        # 移動平均線 (SMA)
        df['SMA20'] = ta.trend.sma_indicator(df['Close'], window=20)
        df['SMA50'] = ta.trend.sma_indicator(df['Close'], window=50)
        # MACD
        macd = ta.trend.MACD(df['Close'])
        df['MACD'] = macd.macd()
        df['MACD_Signal'] = macd.macd_signal()
        # ボリンジャーバンド
        bb = ta.volatility.BollingerBands(df['Close'], window=20, window_dev=2)
        df['BB_Upper'] = bb.bollinger_hband()
        df['BB_Lower'] = bb.bollinger_lband()
        # パラボリック SAR
        df['PSAR'] = ta.trend.psar_down(df['High'], df['Low'], df['Close'])

        # --- オシレーター系 ---
        # RSI (14日)
        df['RSI'] = ta.momentum.rsi(df['Close'], window=14)
        # ストキャスティクス (%K, %D)
        stoch = ta.momentum.StochasticOscillator(df['High'], df['Low'], df['Close'])
        df['Stoch_K'] = stoch.stoch()
        df['Stoch_D'] = stoch.stoch_signal()
        # 移動平均乖離率 (20日)
        df['SMA_Dev'] = ((df['Close'] - df['SMA20']) / df['SMA20']) * 100

        # 直近2日分のデータでクロス・シグナル判定
        latest = df.iloc[-1]
        prev = df.iloc[-2]

        signals = []

        # シグナル条件判定
        # 1. 移動平均 GC (20日線が50日線を上抜け)
        if prev['SMA20'] <= prev['SMA50'] and latest['SMA20'] > latest['SMA50']:
            signals.append("移動平均GC")

        # 2. MACD GC
        if prev['MACD'] <= prev['MACD_Signal'] and latest['MACD'] > latest['MACD_Signal']:
            signals.append("MACD-GC")

        # 3. ボリンジャーバンドブレイク (+2σ上抜け)
        if prev['Close'] <= prev['BB_Upper'] and latest['Close'] > latest['BB_Upper']:
            signals.append("ボリバン+2σ上抜け")

        # 4. RSI売られすぎからの回復 (30以下から上抜け)
        if prev['RSI'] <= 30 and latest['RSI'] > 30:
            signals.append("RSI売られすぎ脱出(買いシグナル)")

        # 5. ストキャスティクス GC (20以下でのクロス)
        if prev['Stoch_K'] <= prev['Stoch_D'] and latest['Stoch_K'] > latest['Stoch_D'] and latest['Stoch_K'] < 30:
            signals.append("ストキャス低水準GC")

        # 6. 移動平均乖離率 (マイナス10%以下：買われ過ぎ/売られ過ぎ逆張り指標)
        if latest['SMA_Dev'] <= -10.0:
            signals.append("移動平均乖離-10%以上")

        if signals:
            stock_name = ticker.info.get('shortName', ticker_symbol)
            return {
                "code": ticker_symbol.replace(".T", ""),
                "name": stock_name,
                "price": round(latest['Close'], 1),
                "signals": signals
            }
    except Exception:
        return None
    return None

# --- 3. Discord Webhook送信 ---
def send_discord_notification(results, webhook_url):
    if not results:
        payload = {"content": "【日本株スクリーニング】本日のシグナル該当銘柄はありませんでした。"}
        requests.post(webhook_url, json=payload)
        return

    # メッセージ長制限(2000文字)対策のため件数を制限・分割
    summary_text = f"**【株テクニカル指標スクリーニング BOT】** ({datetime.now().strftime('%Y-%m-%d')})\n"
    summary_text += f"該当銘柄数: **{len(results)}件**\n\n"

    fields = []
    for r in results[:15]:  # 上位15件を表示
        fields.append({
            "name": f"{r['name']} ({r['code']}) - ¥{r['price']:,}",
            "value": " / ".join(r['signals']),
            "inline": False
        })

    embed = {
        "title": "🎯 該当銘柄一覧",
        "color": 3447003, # Blue
        "fields": fields
    }

    payload = {
        "content": summary_text,
        "embeds": [embed]
    }

    requests.post(webhook_url, json=payload)

# --- メイン実行処理 ---
def main():
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        print("エラー: DISCORD_WEBHOOK_URL が設定されていません。")
        return

    print("東証銘柄リストを取得中...")
    symbols = get_jquants_or_jp_symbols()
    
    print(f"スクリーニングを開始します... (対象: {len(symbols)} 銘柄)")
    matched_results = []

    # マルチスレッドによる高速化 (GitHub ActionsのCPUコア数に合わせて調整)
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(analyze_stock, sym): sym for sym in symbols}
        for future in as_completed(futures):
            res = future.result()
            if res:
                matched_results.append(res)

    print(f"スクリーニング完了: {len(matched_results)}件検出")
    send_discord_notification(matched_results, webhook_url)

if __name__ == "__main__":
    main()
