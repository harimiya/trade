import os
import time
import io
import json
import math
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from scipy.signal import find_peaks
from bs4 import BeautifulSoup
from PIL import Image

import matplotlib
matplotlib.use('Agg') # ヘッドレス環境用
import matplotlib.pyplot as plt
import mplfinance as mpf

# ==========================================
# 1. 環境変数設定（Discord Webhook）
# ==========================================
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# ==========================================
# 2. JPX公式一覧ページから最新の全銘柄を取得
# ==========================================
def get_jpx_stock_list():
    page_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
    base_url = "https://www.jpx.co.jp"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
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
        
        response = requests.get(full_excel_url, headers=headers, timeout=20)
        response.raise_for_status()
        
        df_jpx = pd.read_excel(io.BytesIO(response.content))
        
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
        return ["3110", "6146", "7203", "6758", "9984", "6501", "6857", "8035", "7011"]

# ==========================================
# 3. テクニカル指標・チャートパターン判定（出来高フィルター追加）
# ==========================================
def analyze_df(ticker_code, df):
    if df is None or df.empty or len(df) < 120:
        return None, None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    try:
        close = df['Close'].dropna()
        high = df['High'].dropna()
        low = df['Low'].dropna()
        volume = df['Volume'].dropna()

        if len(close) < 120:
            return None, None

        # 一目均衡表の計算
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

        # 条件1: 転換線が基準線以上（上昇トレンドの芽）
        if tenkan_now < kijun_now:
            return None, None

        # 条件2: 雲の状況（雲上抜けブレイク または 底練り＆雲侵入初動）
        status = None
        if c_now > top_now and c_prev <= top_prev:
            status = "【雲上抜けブレイク】🚀"
        elif bot_now <= c_now <= top_now:
            status = "【底練り＆雲侵入初動】⚡"
        
        if not status:
            return None, None

        # 条件3: 過去60日間に高値から20%以上の調整（下落）を経ていること
        max_60 = float(high.tail(60).max())
        min_60 = float(low.tail(60).min())
        if (max_60 - min_60) / max_60 < 0.20:
            return None, None

        # 条件4: 出来高の増加（当日の出来高が過去30日平均の1.2倍以上）
        vol_sma30 = float(volume.tail(30).iloc[:-1].mean())
        vol_now = float(volume.iloc[-1])
        if vol_sma30 == 0 or vol_now < vol_sma30 * 1.2:
            return None, None

        # 2番底の算出（通知テキスト用）
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
# 4. 個別チャート画像生成 & タイル（グリッド）合成
# ==========================================
def create_chart_image_object(ticker_code, res_dict, df):
    temp_path = f"temp_{ticker_code}.png"
    try:
        df_chart = df.tail(120).copy()
        high, low = df_chart['High'], df_chart['Low']
        
        high9, low9 = high.rolling(9).max(), low.rolling(9).min()
        tenkan = (high9 + low9) / 2
        high26, low26 = high.rolling(26).max(), high.rolling(26).min()
        kijun = (high26 + low26) / 2
        senkou_a = ((tenkan + kijun) / 2).shift(26)
        high52, low52 = high.rolling(52).max(), high.rolling(52).min()
        senkou_b = ((high52 + low52) / 2).shift(26)
        
        addplots = [
            mpf.make_addplot(tenkan, color='blue', width=0.8),
            mpf.make_addplot(kijun, color='red', width=0.8),
            mpf.make_addplot(senkou_a, color='green', width=0.6),
            mpf.make_addplot(senkou_b, color='purple', width=0.6),
        ]
        
        mc = mpf.make_marketcolors(up='red', down='green', edge='inherit', wick='inherit', volume='inherit')
        s = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', y_on_right=True)
        
        title_str = f"{ticker_code} | {res_dict['status']}\nClose: {res_dict['close']:,.1f}円"
        
        fig, axes = mpf.plot(
            df_chart,
            type='candle',
            volume=True,
            addplot=addplots,
            style=s,
            title=title_str,
            figsize=(5, 3.5),
            fill_between=dict(y1=senkou_a.values, y2=senkou_b.values, color='gray', alpha=0.2),
            returnfig=True
        )
        
        fig.savefig(temp_path, bbox_inches='tight', dpi=100)
        plt.close(fig)
        
        img = Image.open(temp_path).copy()
        img.load()
        os.remove(temp_path)
        return img
    except Exception as e:
        print(f"[{ticker_code}] 個別チャート生成失敗: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return None

def generate_tile_chart(results):
    valid_images = []
    for r, df_matched in results:
        img = create_chart_image_object(r['code'], r, df_matched)
        if img:
            valid_images.append((r['code'], img))
            
    if not valid_images:
        return None
        
    count = len(valid_images)
    cols = 3 if count >= 3 else count
    rows = math.ceil(count / cols)
    
    w, h = valid_images[0][1].size
    padding = 10
    
    tile_width = cols * w + (cols + 1) * padding
    tile_height = rows * h + (rows + 1) * padding
    
    canvas = Image.new("RGB", (tile_width, tile_height), (245, 247, 250))
    
    for idx, (code, img) in enumerate(valid_images):
        r_idx = idx // cols
        c_idx = idx % cols
        
        x = padding + c_idx * (w + padding)
        y = padding + r_idx * (h + padding)
        
        canvas.paste(img, (x, y))
        
    output_path = "screened_tile_result.png"
    canvas.save(output_path, quality=95)
    return output_path

# ==========================================
# 5. Discord通知送信
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
# 6. メイン実行部（バッチ処理）
# ==========================================
if __name__ == "__main__":
    tickers = get_jpx_stock_list()
    total = len(tickers)
    results = []

    BATCH_SIZE = 100
    ticker_symbols = [f"{code}.T" for code in tickers]
    
    print(f"=== 全銘柄スクリーニング開始（対象: {total} 銘柄） ===")

    for i in range(0, total, BATCH_SIZE):
        batch_codes = tickers[i:i + BATCH_SIZE]
        batch_symbols = ticker_symbols[i:i + BATCH_SIZE]
        
        print(f"進捗: {i+1} ~ {min(i+BATCH_SIZE, total)} / {total} 銘柄を処理中...")
        
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

    print(f"\n=== 全処理完了。検知件数: {len(results)} 件 ===")

    # 結果の送信処理（文字数制限対策済み）
    if results:
        summary_msg = f"【日足チャートスクリーニング検知】（合計: {len(results)}件）\n"
        
        # 2000文字制限に引っかからないよう、最大30件までテキスト列記し、超過分は省略
        display_results = results[:30]
        for r, _ in display_results:
            summary_msg += f"・`{r['code']}` : {r['status']} ({r['close']:,.1f}円)\n"
            
        if len(results) > 30:
            summary_msg += f"\n...他 {len(results) - 30} 件の銘柄が検知されました。"
            
        if len(summary_msg) > 1900:
            summary_msg = summary_msg[:1900] + "\n...(文字数制限のため一部省略)"

        print("ヒットした全銘柄のタイル画像を生成中...")
        tile_img_path = generate_tile_chart(results)
        
        send_discord_notification_with_image(summary_msg, tile_img_path)
    else:
        send_discord_notification_with_image("【定期スクリーニング完了】\n本日の条件に該当する銘柄はありませんでした。")
