import os
import time
import io
import json
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from scipy.signal import find_peaks
from bs4 import BeautifulSoup
import matplotlib
matplotlib.use('Agg') # ヘッドレス環境用
import matplotlib.pyplot as plt
import mplfinance as mpf

# ==========================================
# 1. 環境変数設定
# ==========================================
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# ==========================================
# 2. 一目均衡表チャート画像の自前生成 (mplfinance)
# ==========================================
def generate_ichimoku_chart(ticker_code, df):
    """yfinanceのデータから一目均衡表チャート画像を生成"""
    image_path = f"chart_{ticker_code}.png"
    
    try:
        # 直近120日分のデータを使用
        df_chart = df.tail(120).copy()
        
        # 一目均衡表の計算
        high = df_chart['High']
        low = df_chart['Low']
        
        high9, low9 = high.rolling(9).max(), low.rolling(9).min()
        tenkan = (high9 + low9) / 2
        
        high26, low26 = high.rolling(26).max(), low.rolling(26).min()
        kijun = (high26 + low26) / 2
        
        senkou_a = ((tenkan + kijun) / 2).shift(26)
        high52, low52 = high.rolling(52).max(), low.rolling(52).min()
        senkou_b = ((high52 + low52) / 2).shift(26)
        
        # mplfinance用の追加ライン設定
        addplots = [
            mpf.make_addplot(tenkan, color='blue', width=0.8),
            mpf.make_addplot(kijun, color='red', width=0.8),
            mpf.make_addplot(senkou_a, color='green', width=0.6),
            mpf.make_addplot(senkou_b, color='purple', width=0.6),
        ]
        
        # 配色設定（日本風：陽線=赤、陰線=緑）
        mc = mpf.make_marketcolors(up='red', down='green', edge='inherit', wick='inherit', volume='inherit')
        s = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', y_on_right=True)
        
        # チャート作成
        fig, axes = mpf.plot(
            df_chart,
            type='candle',
            volume=True,
            addplot=addplots,
            style=s,
            title=f"Ticker: {ticker_code} (Daily / Ichimoku)",
            figsize=(9, 6),
            fill_between=dict(y1=senkou_a.values, y2=senkou_b.values, color='gray', alpha=0.2),
            returnfig=True
        )
        
        fig.savefig(image_path, bbox_inches='tight', dpi=100)
        plt.close(fig)
        return image_path

    except Exception as e:
        print(f"[{ticker_code}] チャート画像生成失敗: {e}")
        return None

# ==========================================
# 3. Discordへの画像付き送信 (payload_json方式)
# ==========================================
def send_discord_notification_with_image(message, image_path=None):
    if not DISCORD_WEBHOOK_URL:
        print("\n--- Notification Log ---")
        print(message)
        return

    try:
        if image_path and os.path.exists(image_path):
            payload = {"content": message}
            with open(image_path, "rb") as f:
                files = {
                    "payload_json": (None, json.dumps(payload), "application/json"),
                    "file": (os.path.basename(image_path), f, "image/png")
                }
                res = requests.post(DISCORD_WEBHOOK_URL, files=files)
            
            if res.status_code in [200, 204]:
                os.remove(image_path)
            else:
                print(f"Discord送信エラー ({res.status_code}): {res.text}")
        else:
            requests.post(DISCORD_WEBHOOK_URL, json={"content": message})
    except Exception as e:
        print(f"送信エラー: {e}")

# ==========================================
# 4. JPX銘柄取得 & 分析ロジック
# ==========================================
def get_jpx_stock_list():
    page_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
    base_url = "https://www.jpx.co.jp"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        res_page = requests.get(page_url, headers=headers, timeout=15)
        soup = BeautifulSoup(res_page.text, 'html.parser')
        xls_link = None
        for a in soup.find_all('a', href=True):
            if 'data_j.xls' in a['href'] or a['href'].endswith('.xls'):
                xls_link = a['href']
                break
                
        full_excel_url = base_url + xls_link if xls_link.startswith('/') else xls_link
        response = requests.get(full_excel_url, headers=headers, timeout=20)
        df_jpx = pd.read_excel(io.BytesIO(response.content))
        
        target_markets = ['プライム（内国株式）', 'スタンダード（内国株式）', 'グロース（内国株式）']
        filtered_df = df_jpx[df_jpx['市場・商品区分'].isin(target_markets)]
        
        tickers = [str(code).zfill(4) for code in filtered_df['コード'] if len(str(code).zfill(4)) == 4 and str(code).isdigit()]
        print(f"【JPX】 {len(tickers)} 銘柄を取得しました。")
        return tickers
    except Exception as e:
        print(f"【JPX取得エラー】: {e}")
        return ["3110", "6146", "9401", "7203", "6758"]

def analyze_df(ticker_code, df):
    if df is None or df.empty or len(df) < 120:
        return None, None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    try:
        close = df['Close'].dropna()
        high = df['High'].dropna()
        low = df['Low'].dropna()

        if len(close) < 120:
            return None, None

        # ==========================================
        # 1. 一目均衡表の計算
        # ==========================================
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
        top_prev = float(cloud_top.iloc[-2])

        tenkan_now = float(tenkan.iloc[-1])
        kijun_now = float(kijun.iloc[-1])

        # ==========================================
        # 2. 条件判定（日東紡パターンの核に絞り込み）
        # ==========================================
        
        # 条件1: 転換線が基準線以上（上昇トレンドの芽がある）
        if tenkan_now < kijun_now:
            return None, None

        # 条件2: 雲の位置関係（以下のいずれかに該当）
        # A) 雲を上抜けた直後（前日は雲以下、当日雲上）
        # B) 雲のなかに侵入中、かつ直近で下値固め（安値から回復）している
        status = None
        if c_now > top_now and c_prev <= top_prev:
            status = "【雲上抜けブレイク】🚀"
        elif bot_now <= c_now <= top_now:
            status = "【底練り＆雲侵入初動】⚡"
        
        if not status:
            return None, None

        # 条件3: 過去60日間に大きな下落を経験している（高値からの調整後の底練り判定）
        max_60 = float(high.tail(60).max())
        min_60 = float(low.tail(60).min())
        # 高値から20%以上の調整を経てからの戻り局面であること
        if (max_60 - min_60) / max_60 < 0.20:
            return None, None

        # 2番底の算出（通知用データ）
        low_vals = low.tail(120).values
        peaks, _ = find_peaks(-low_vals, distance=10, prominence=np.nanstd(low_vals) * 0.2)
        l1_val = low_vals[peaks[-2]] if len(peaks) >= 2 else low_vals[-1]
        l2_val = low_vals[peaks[-1]] if len(peaks) >= 2 else low_vals[-1]

        result_dict = {
            "code": ticker_code,
            "close": c_now,
            "status": status,
            "l1": float(l1_val),
            "l2": float(l2_val)
        }
        return result_dict, df

    except Exception:
        return None, None
# ==========================================
# 5. メイン処理
# ==========================================
if __name__ == "__main__":
    tickers = get_jpx_stock_list()
    total = len(tickers)
    results = []

    BATCH_SIZE = 100
    ticker_symbols = [f"{code}.T" for code in tickers]
    
    print(f"=== スクリーニング開始（対象: {total} 銘柄） ===")

    for i in range(0, total, BATCH_SIZE):
        batch_codes = tickers[i:i + BATCH_SIZE]
        batch_symbols = ticker_symbols[i:i + BATCH_SIZE]
        
        try:
            data = yf.download(batch_symbols, period="1y", interval="1d", group_by='ticker', progress=False)
            for code in batch_codes:
                symbol = f"{code}.T"
                try:
                    df_single = data if len(batch_codes) == 1 else data.get(symbol, pd.DataFrame()).dropna(how='all')
                    res, df_matched = analyze_df(code, df_single)
                    if res:
                        results.append((res, df_matched))
                        print(f"  ★[ヒット] 銘柄コード: {code} | 状態: {res['status']}")
                except Exception:
                    continue
        except Exception as e:
            print(f"  バッチエラー: {e}")
        time.sleep(1)

    print(f"\n=== スクリーニング完了。検知件数: {len(results)} 件 ===")

    # 検知された銘柄ごとに自前で一目均衡表画像を生成して通知
    if results:
        for r, df_matched in results:
            msg = (f"【日足チャートスクリーニング検知】\n"
                   f"■ 銘柄コード: {r['code']}\n"
                   f" 株価: {r['close']:,.1f}円 | 状態: {r['status']}\n"
                   f" (1番底: {r['l1']:,.1f}円 / 2番底: {r['l2']:,.1f}円)")
            
            # 自前で一目均衡表チャート画像を生成
            img_path = generate_ichimoku_chart(r['code'], df_matched)
            
            # Discordへ画像添付送信
            send_discord_notification_with_image(msg, img_path)
            time.sleep(1)
    else:
        send_discord_notification_with_image("【定期スクリーニング完了】\n本日の条件に該当する銘柄はありませんでした。")
