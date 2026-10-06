import os
import time
import io
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from scipy.signal import find_peaks

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# ==========================================
# 1. JPX銘柄リスト取得
# ==========================================
def get_jpx_stock_list():
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        df_jpx = pd.read_excel(io.BytesIO(response.content))
        target_markets = ['プライム（内国株式）', 'スタンダード（内国株式）', 'グロース（内国株式）']
        filtered_df = df_jpx[df_jpx['市場・商品区分'].isin(target_markets)]
        tickers = [str(code).zfill(4) for code in filtered_df['コード'] if len(str(code).zfill(4)) == 4 and str(code).isdigit()]
        print(f"JPXから {len(tickers)} 銘柄を取得しました。")
        return tickers
    except Exception as e:
        print(f"JPX銘柄リスト取得失敗: {e}")
        # テスト用フォールバック
        return ["3110", "7203", "6758", "9984", "6501"]

# ==========================================
# 2. 日証金 貸借取引残高（株不足判定）
# ==========================================
def check_japan_net_short(ticker_code):
    """
    日証金のWeb API/データから該当銘柄が株不足（売り残 > 買い残 または 逆日歩発生）か確認する関数
    ※ 簡易判定: 貸借取引の差引残高がマイナス（売り越し）であることを確認
    """
    # 実際の実装では日証金サイトから当日データをスクレイピング/CSV解析
    # ここではテスト用にTrue（通過）としつつ、日東紡(3110)等の株不足条件を判定枠として保持
    return True

# ==========================================
# 3. チャート分析＆スクリーニング（日東紡パターン対応）
# ==========================================
def analyze_ticker(ticker_code):
    symbol = f"{ticker_code}.T"
    
    # yfinanceデータ取得
    df = yf.download(symbol, period="1y", interval="1d", progress=False)
    if df.empty or len(df) < 120:
        return None

    # MultiIndexの解消（yfinance最新仕様対策）
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    close = df['Close']
    high = df['High']
    low = df['Low']

    # --- A. ボリンジャーバンド (20日) ---
    sma20 = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    upper1sigma = sma20 + std20
    upper2sigma = sma20 + (2 * std20)
    lower2sigma = sma20 - (2 * std20)
    
    bandwidth = (upper2sigma - lower2sigma) / sma20
    
    # 過去10日以内にスクイーズ（過去120日の中で下位30%以下の狭さ）
    recent_bw = bandwidth.tail(10)
    hist_threshold = bandwidth.tail(120).quantile(0.30)
    was_squeezed = (recent_bw <= hist_threshold).any()
    
    # 直近の価格がSMA20以上（バンドウォーク〜上昇開始トレンド）
    is_uptrend = close.iloc[-1] >= sma20.iloc[-1]
    
    if not (was_squeezed and is_uptrend):
        return None

    # --- B. 2番底（ダブルボトム）判定 ---
    low_vals = low.tail(120).values
    # 谷（ボトム）を検出
    peaks, _ = find_peaks(-low_vals, distance=12, prominence=np.nanstd(low_vals) * 0.2)
    
    if len(peaks) < 2:
        return None
        
    l1_idx, l2_idx = peaks[-2], peaks[-1]
    l1_val, l2_val = low_vals[l1_idx], low_vals[l2_idx]
    
    # 2番底が1番底と同等〜切り上がり（1番底の1.18倍以内）
    has_double_bottom = (l2_idx > l1_idx) and (l2_val >= l1_val * 0.95) and (l2_val <= l1_val * 1.18)
    if not has_double_bottom:
        return None

    # --- C. 一目均衡表（雲）判定 ---
    high9, low9 = high.rolling(9).max(), low.rolling(9).min()
    tenkan = (high9 + low9) / 2
    
    high26, low26 = high.rolling(26).max(), low.rolling(26).min()
    kijun = (high26 + low26) / 2
    
    senkou_a = ((tenkan + kijun) / 2).shift(26)
    high52, low52 = high.rolling(52).max(), low.rolling(52).min()
    senkou_b = ((high52 + low52) / 2).shift(26)
    
    cloud_top = np.maximum(senkou_a, senkou_b)
    cloud_bottom = np.minimum(senkou_a, senkou_b)

    c_now = float(close.iloc[-1])
    c_prev = float(close.iloc[-2])
    top_now = float(cloud_top.iloc[-1])
    bot_now = float(cloud_bottom.iloc[-1])
    
    status = None
    if c_now > top_now and c_prev <= top_now:
        status = "【雲上抜け直後】🚀"
    elif bot_now <= c_now <= top_now:
        status = "【雲侵入中】⚡"
    elif c_now < bot_now and c_now >= bot_now * 0.97:
        status = "【雲直前（接近）】👀"

    if not status:
        return None

    # --- D. 日証金 株不足チェック ---
    if not check_japan_net_short(ticker_code):
        return None

    return {
        "code": ticker_code,
        "close": c_now,
        "status": status,
        "l1": float(l1_val),
        "l2": float(l2_val)
    }

# ==========================================
# 4. 実行 & 通知
# ==========================================
def send_discord_notification(message):
    if not DISCORD_WEBHOOK_URL:
        print("\n--- Discord Notification ---")
        print(message)
        return
    chunks = [message[i:i+1900] for i in range(0, len(message), 1900)]
    for chunk in chunks:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": chunk})
        time.sleep(1)

if __name__ == "__main__":
    tickers = get_jpx_stock_list()
    
    results = []
    print(f"全 {len(tickers)} 銘柄の分析を開始します...")
    
    # 進捗確認用のカウンタ
    processed_count = 0
    
    for code in tickers:
        processed_count += 1
        if processed_count % 100 == 0:
            print(f"進捗: {processed_count}/{len(tickers)} 銘柄完了...")
            
        try:
            res = analyze_ticker(code)
            if res:
                results.append(res)
                print(f"★ 該当検知: {code} - {res['status']}")
            time.sleep(0.05)
        except Exception as e:
            # エラー内容を出力して原因特定できるようにする
            print(f"銘柄 {code} でエラーが発生しました: {e}")
            continue

    print(f"処理完了。検出数: {len(results)} 件")

    if results:
        msg = f"【日足チャートスクリーニング検知】（該当: {len(results)}件）\n"
        msg += "----------------------------------------\n"
        for r in results:
            msg += f"■ 銘柄コード: {r['code']}\n"
            msg += f" 株価: {r['close']:,.1f}円 | 状態: {r['status']}\n"
            msg += f" (1番底: {r['l1']:,.1f}円 / 2番底: {r['l2']:,.1f}円)\n\n"
        send_discord_notification(msg)
    else:
        send_discord_notification("【定期スクリーニング完了】\n本日の条件に該当する銘柄はありませんでした。")
