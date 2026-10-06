import pandas as pd
import numpy as np
import yfinance as yf
import requests
import os
import time

# --- Discord通知関数 ---
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

def send_discord_notification(message):
    if not DISCORD_WEBHOOK_URL:
        print(message)
        return
    payload = {"content": message}
    requests.post(DISCORD_WEBHOOK_URL, json=payload)

# --- テクニカル指標算出 ---
def analyze_ticker(ticker_code):
    symbol = f"{ticker_code}.T"
    df = yf.download(symbol, period="1y", interval="1d", progress=False)
    
    if len(df) < 120:
        return None
    
    # 1. ボリンジャーバンド (20日)
    sma20 = df['Close'].rolling(20).mean()
    std20 = df['Close'].rolling(20).std()
    upper2sigma = sma20 + (2 * std20)
    upper1sigma = sma20 + (1 * std20)
    lower2sigma = sma20 - (2 * std20)
    
    bandwidth = (upper2sigma - lower2sigma) / sma20
    is_squeeze = bandwidth.iloc[-1] <= bandwidth.tail(120).quantile(0.20)
    
    if not is_squeeze:
        return None

    # 2. 一目均衡表 (雲)
    high9 = df['High'].rolling(9).max()
    low9 = df['Low'].rolling(9).min()
    tenkan = (high9 + low9) / 2
    
    high26 = df['High'].rolling(26).max()
    low26 = df['Low'].rolling(26).min()
    kijun = (high26 + low26) / 2
    
    senkou_a = ((tenkan + kijun) / 2).shift(26)
    high52 = df['High'].rolling(52).max()
    low52 = df['Low'].rolling(52).min()
    senkou_b = ((high52 + low52) / 2).shift(26)
    
    cloud_top = np.maximum(senkou_a, senkou_b)
    cloud_bottom = np.minimum(senkou_a, senkou_b)

    # 3. 直近6ヶ月（120日）の2番底判定
    recent_df = df.tail(120)
    lows = recent_df['Low']
    
    # 前半と後半で最安値を検出（簡略化モデル）
    l1_idx = lows.iloc[:-30].idxmin()
    l1_val = lows.loc[l1_idx]
    
    l2_df = lows.loc[l1_idx:].tail(30)
    if len(l2_df) < 5:
        return None
    l2_val = l2_df.min()
    
    # 2番底が1番底より高く、かつ離れすぎていない
    has_double_bottom = (l2_val >= l1_val) and (l2_val <= l1_val * 1.12)
    if not has_double_bottom:
        return None

    # 4. 雲の位置関係ステータス
    close_now = df['Close'].iloc[-1]
    close_prev = df['Close'].iloc[-2]
    c_top_now = cloud_top.iloc[-1]
    c_bot_now = cloud_bottom.iloc[-1]
    
    status = None
    if close_now > c_top_now and close_prev <= c_top_now:
        status = "【雲上抜け直後】🚀"
    elif c_bot_now <= close_now <= c_top_now:
        status = "【雲侵入中】⚡"
    elif close_now < c_bot_now and close_now >= c_bot_now * 0.98:
        status = "【雲直前（接近）】👀"
        
    if not status:
        return None

    return {
        "code": ticker_code,
        "close": float(close_now),
        "status": status,
        "l1": float(l1_val),
        "l2": float(l2_val)
    }

# --- 実行用サンプル（銘柄リストをループ処理） ---
if __name__ == "__main__":
    # 本来は全東証銘柄（約4,000銘柄）のコードリストを読み込む
    target_tickers = ["3110", "7203", "6758", "9984"] # テスト用
    
    results = []
    for code in target_tickers:
        try:
            res = analyze_ticker(code)
            if res:
                results.append(res)
            time.sleep(0.5) # レートリミット回避
        except Exception as e:
            continue
            
    if results:
        msg = "【日足スクリーニング検知】\n"
        for r in results:
            msg += f"銘柄コード: {r['code']} | 株価: {r['close']}円 | 状態: {r['status']}\n(1番底: {r['l1']:.1f} / 2番底: {r['l2']:.1f})\n\n"
        send_discord_notification(msg)
