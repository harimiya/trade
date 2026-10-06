import os
import time
import io
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from scipy.signal import find_peaks

# ==========================================
# 設定＆環境変数
# ==========================================
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# ==========================================
# 1. JPX公式銘柄リスト取得（プライム・スタンダード・グロース）
# ==========================================
def get_jpx_stock_list():
    """JPX公式Excelから東証プライム・スタンダード・グロースの4桁銘柄コードを取得"""
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        response = requests.get(url, headers=headers)
        df_jpx = pd.read_excel(io.BytesIO(response.content))
        
        # 市場・商品区分でフィルタリング
        target_markets = ['プライム（内国株式）', 'スタンダード（内国株式）', 'グロース（内国株式）']
        filtered_df = df_jpx[df_jpx['市場・商品区分'].isin(target_markets)]
        
        # 4桁コードのみ抽出 (ETFや5桁文字コードを除外)
        tickers = []
        for code in filtered_df['コード']:
            code_str = str(code).zfill(4)
            if len(code_str) == 4 and code_str.isdigit():
                tickers.append(code_str)
                
        print(f"JPXから {len(tickers)} 銘柄を取得しました。")
        return tickers
    except Exception as e:
        print(f"銘柄リスト取得エラー: {e}")
        # フォールバック（主要銘柄等）
        return ["3110", "7203", "6758", "9984", "6501"]

# ==========================================
# 2. テクニカル分析＆スクリーニング判定
# ==========================================
def analyze_ticker(ticker_code):
    symbol = f"{ticker_code}.T"
    
    # 過去1年分のデータ取得 (250営業日)
    df = yf.download(symbol, period="1y", interval="1d", progress=False)
    if len(df) < 120:
        return None

    # MultiIndexの解除（yfinanceの仕様対策）
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    close = df['Close']
    high = df['High']
    low = df['Low']

    # --- A. ボリンジャーバンド (20日) ---
    sma20 = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    upper1sigma = sma20 + (1 * std20)
    upper2sigma = sma20 + (2 * std20)
    lower2sigma = sma20 - (2 * std20)
    
    bandwidth = (upper2sigma - lower2sigma) / sma20
    
    # 【判定1】直近10営業日以内にスクイーズ（過去半年の下位25%の狭さ）があったか？
    recent_bw = bandwidth.tail(10)
    hist_bw_threshold = bandwidth.tail(120).quantile(0.25)
    was_squeezed = (recent_bw <= hist_bw_threshold).any()
    
    # 【判定2】本日の株価が+1σ〜+2σ付近（バンドウォーク初期）に位置しているか？
    is_bandwalk = close.iloc[-1] >= upper1sigma.iloc[-1]
    
    if not (was_squeezed and is_bandwalk):
        return None

    # --- B. 2番底（ダブルボトム）検出 ---
    # 過去120日のローカルミニマム（谷）を探す
    low_vals = low.tail(120).values
    # 10日以上離れたProminence(目立ち度)のある谷を抽出
    peaks, _ = find_peaks(-low_vals, distance=10, prominence=low_vals.std() * 0.3)
    
    if len(peaks) < 2:
        return None
        
    l1_idx, l2_idx = peaks[-2], peaks[-1]
    l1_val, l2_val = low_vals[l1_idx], low_vals[l2_idx]
    
    # 2番底の条件：
    # 1. 2番底(L2)は1番底(L1)よりも時系列で後に来ている
    # 2. L2 >= L1 * 0.98 (1番底と同等か切り上がっている)
    # 3. L2 <= L1 * 1.15 (1番底から離れすぎていない)
    has_double_bottom = (l2_idx > l1_idx) and (l2_val >= l1_val * 0.98) and (l2_val <= l1_val * 1.15)
    
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
    # 1. 雲上抜け直後
    if c_now > top_now and c_prev <= top_now:
        status = "【雲上抜け直後】🚀"
    # 2. 雲侵入中（日東紡 9/30のパターン）
    elif bot_now <= c_now <= top_now:
        status = "【雲侵入中】⚡"
    # 3. 雲直前（雲下限の3%以内に接近）
    elif c_now < bot_now and c_now >= bot_now * 0.97:
        status = "【雲直前（接近）】👀"

    if not status:
        return None

    return {
        "code": ticker_code,
        "close": c_now,
        "status": status,
        "l1": float(l1_val),
        "l2": float(l2_val)
    }

# ==========================================
# 3. メイン処理 & Discord通知
# ==========================================
def send_discord_notification(message):
    if not DISCORD_WEBHOOK_URL:
        print("Webhook URLが未設定です。以下を出力します：")
        print(message)
        return
    
    # Discordの文字数制限(2000文字)対策のため分割送信
    chunks = [message[i:i+1900] for i in range(0, len(message), 1900)]
    for chunk in chunks:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": chunk})
        time.sleep(1)

if __name__ == "__main__":
    tickers = get_jpx_stock_list()
    
    results = []
    print(f"全 {len(tickers)} 銘柄のスクリーニングを開始します...")
    
    for idx, code in enumerate(tickers):
        try:
            res = analyze_ticker(code)
            if res:
                results.append(res)
                print(f"検出: {code} - {res['status']}")
            
            # API負荷軽減のためわずかにウェイト
            time.sleep(0.1)
        except Exception as e:
            continue

    # 通知メッセージの生成
    if results:
        msg = f"【日足チャートスクリーニング検知】（ヒット: {len(results)}件）\n"
        msg += "----------------------------------------\n"
        for r in results:
            msg += f"■ 銘柄コード: {r['code']}\n"
            msg += f" 株価: {r['close']:,.1f}円 | ステータス: {r['status']}\n"
            msg += f" (1番底: {r['l1']:,.1f}円 / 2番底: {r['l2']:,.1f}円)\n\n"
        send_discord_notification(msg)
    else:
        send_discord_notification("【定期スクリーニング実行結果】\n本日の条件に該当する銘柄はありませんでした。")
