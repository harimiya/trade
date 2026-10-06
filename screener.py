import os
import time
import io
import re
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from scipy.signal import find_peaks
from bs4 import BeautifulSoup

# ==========================================
# 1. 環境変数設定（Discord Webhook）
# ==========================================
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# ==========================================
# 2. JPX公式一覧ページから最新の全銘柄を取得
# ==========================================
def get_jpx_stock_list():
    """JPXの公式一覧ページから動的に最新のExcelリンクを取得して東証銘柄（プライム・スタンダード・グロース）コードを取得"""
    page_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
    base_url = "https://www.jpx.co.jp"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        # 1. 銘柄一覧HTMLページからExcel(.xls)の最新URLを検索
        res_page = requests.get(page_url, headers=headers, timeout=15)
        res_page.raise_for_status()
        
        soup = BeautifulSoup(res_page.text, 'html.parser')
        xls_link = None
        for a in soup.find_all('a', href=True):
            if 'data_j.xls' in a['href'] or a['href'].endswith('.xls'):
                xls_link = a['href']
                break
                
        if not xls_link:
            raise ValueError("銘柄一覧Excelのダウンロードリンクが見つかりませんでした。")
            
        full_excel_url = base_url + xls_link if xls_link.startswith('/') else xls_link
        print(f"【JPX】最新の銘柄一覧Excel URLを取得しました: {full_excel_url}")
        
        # 2. Excelファイルをダウンロードして解析
        response = requests.get(full_excel_url, headers=headers, timeout=20)
        response.raise_for_status()
        
        df_jpx = pd.read_excel(io.BytesIO(response.content))
        
        # 対象市場（プライム・スタンダード・グロース）で絞り込み
        target_markets = ['プライム（内国株式）', 'スタンダード（内国株式）', 'グロース（内国株式）']
        filtered_df = df_jpx[df_jpx['市場・商品区分'].isin(target_markets)]
        
        tickers = []
        for code in filtered_df['コード']:
            code_str = str(code).zfill(4)
            if len(code_str) == 4 and code_str.isdigit():
                tickers.append(code_str)
                
        print(f"【成功】JPXより {len(tickers)} 銘柄を取得しました。")
        return tickers

    except Exception as e:
        print(f"【警告】JPX銘柄リストの自動取得に失敗しました: {e}")
        # フォールバック（自動取得失敗時の予備銘柄リスト）
        return ["3110", "6146", "7203", "6758", "9984", "6501", "6857", "8035", "7011"]

# ==========================================
# 3. テクニカル指標・チャートパターン判定
# ==========================================
def analyze_df(ticker_code, df):
    """単一銘柄のDataFrameを受け取り、条件（スクイーズ＋2番底＋一目均衡表の雲）を判定"""
    if df is None or df.empty or len(df) < 120:
        return None

    # MultiIndexの解除（yfinanceのバージョン差異対策）
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    try:
        close = df['Close'].dropna()
        high = df['High'].dropna()
        low = df['Low'].dropna()
        
        if len(close) < 120:
            return None

        # --- A. ボリンジャーバンド (20日) ---
        sma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        upper1sigma = sma20 + std20
        upper2sigma = sma20 + (2 * std20)
        lower2sigma = sma20 - (2 * std20)
        bandwidth = (upper2sigma - lower2sigma) / sma20
        
        # 過去10日以内にスクイーズ（過去120日の中で下位30%以下の狭さ）が発生していたか
        recent_bw = bandwidth.tail(10)
        hist_threshold = bandwidth.tail(120).quantile(0.30)
        was_squeezed = (recent_bw <= hist_threshold).any()
        
        # 直近の株価がSMA20以上（上昇トレンド傾向）
        is_uptrend = close.iloc[-1] >= sma20.iloc[-1]
        
        if not (was_squeezed and is_uptrend):
            return None

        # --- B. 2番底（ダブルボトム）判定 ---
        low_vals = low.tail(120).values
        peaks, _ = find_peaks(-low_vals, distance=10, prominence=np.nanstd(low_vals) * 0.2)
        if len(peaks) < 2:
            return None
            
        l1_idx, l2_idx = peaks[-2], peaks[-1]
        l1_val, l2_val = low_vals[l1_idx], low_vals[l2_idx]
        
        # 2番底の切り上がり（1番底の0.95倍〜1.18倍以内）
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

        return {
            "code": ticker_code,
            "close": c_now,
            "status": status,
            "l1": float(l1_val),
            "l2": float(l2_val)
        }
    except Exception:
        return None

# ==========================================
# 4. Discord通知送信
# ==========================================
def send_discord_notification(message):
    if not DISCORD_WEBHOOK_URL:
        print("\n--- Discord Webhook未設定（ログ出力） ---")
        print(message)
        return
    # Discordの2000文字制限対策
    chunks = [message[i:i+1900] for i in range(0, len(message), 1900)]
    for chunk in chunks:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": chunk})
        time.sleep(1)

# ==========================================
# 5. メイン実行部（バッチ処理）
# ==========================================
if __name__ == "__main__":
    tickers = get_jpx_stock_list()
    total = len(tickers)
    results = []

    # 100銘柄ごとにまとめてダウンロード（高速化＆APIブロック対策）
    BATCH_SIZE = 100
    ticker_symbols = [f"{code}.T" for code in tickers]
    
    print(f"=== 全銘柄スクリーニング開始（対象: {total} 銘柄） ===")

    for i in range(0, total, BATCH_SIZE):
        batch_codes = tickers[i:i + BATCH_SIZE]
        batch_symbols = ticker_symbols[i:i + BATCH_SIZE]
        
        print(f"進捗: {i+1} ~ {min(i+BATCH_SIZE, total)} / {total} 銘柄のデータを一括処理中...")
        
        try:
            # yfinanceでバッチ一括取得
            data = yf.download(batch_symbols, period="1y", interval="1d", group_by='ticker', progress=False)
            
            for code in batch_codes:
                symbol = f"{code}.T"
                try:
                    if len(batch_codes) == 1:
                        df_single = data
                    else:
                        if symbol in data:
                            df_single = data[symbol].dropna(how='all')
                        else:
                            continue

                    res = analyze_df(code, df_single)
                    if res:
                        results.append(res)
                        print(f"  ★[ヒット] 銘柄コード: {code} | 状態: {res['status']}")
                except Exception:
                    continue
        except Exception as e:
            print(f"  バッチ取得エラー ({i}~): {e}")
            
        time.sleep(1) # API制限回避用ウェイト

    print(f"\n=== 全処理完了。該当件数: {len(results)} 件 ===")

    # Discord通知処理
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
