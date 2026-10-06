import os
import time
import io
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from scipy.signal import find_peaks
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ==========================================
# 1. 環境変数設定（Discord Webhook）
# ==========================================
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# ==========================================
# 2. 株探の「一目均衡表」チャート画像の撮影
# ==========================================
def capture_kabutan_chart(ticker_code):
    """株探のチャートページを開き、一目均衡表を選択した状態の画像キャプチャを取得"""
    url = f"https://kabutan.jp/stock/chart/?code={ticker_code}"
    image_path = f"chart_{ticker_code}.png"
    
    try:
        with sync_playwright() as p:
            # ヘッドレ スブラウザ起動
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1280, "height": 960})
            
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            
            # 「一目均衡表」のラジオボタンをクリック（一目均衡表の値: 'ichimoku' やテキスト指定）
            # 株探のラジオボタン要素を指定
            ichimoku_radio = page.locator("input[type='radio'][value='ichimoku']").first
            if ichimoku_radio.is_visible():
                ichimoku_radio.click()
            else:
                # テキストラベルから選択を試みる
                page.locator("label", has_text="一目均衡表").click()
                
            time.sleep(1.5) # チャート再描画の待機
            
            # チャート表示部分要素の切り出し（画面全体またはチャート領域）
            # #stock_chart または .chart_box などの領域を取得
            chart_element = page.locator("#stock_chart").first
            if chart_element.is_visible():
                chart_element.screenshot(path=image_path)
            else:
                page.screenshot(path=image_path, full_page=False)
                
            browser.close()
            return image_path
    except Exception as e:
        print(f"[{ticker_code}] 画像キャプチャ失敗: {e}")
        return None

# ==========================================
# 3. Discordへの画像付きメッセージ送信
# ==========================================
def send_discord_notification_with_image(message, image_path=None):
    """Discord Webhookにテキストおよび画像を添付して送信"""
    if not DISCORD_WEBHOOK_URL:
        print("\n--- Discord Notification ---")
        print(message)
        return

    payload = {"content": message}
    
    if image_path and os.path.exists(image_path):
        with open(image_path, "rb") as f:
            files = {
                "file": (os.path.basename(image_path), f, "image/png")
            }
            requests.post(DISCORD_WEBHOOK_URL, data=payload, files=files)
        # 送信後に一時画像を削除
        try:
            os.remove(image_path)
        except Exception:
            pass
    else:
        requests.post(DISCORD_WEBHOOK_URL, json=payload)

# ==========================================
# 4. JPX銘柄取得 & 分析ロジック（前述と同様）
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
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    try:
        close, high, low = df['Close'].dropna(), df['High'].dropna(), df['Low'].dropna()
        if len(close) < 120:
            return None

        # ボリンジャーバンド
        sma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        bandwidth = ((sma20 + (2 * std20)) - (sma20 - (2 * std20))) / sma20
        was_squeezed = (bandwidth.tail(10) <= bandwidth.tail(120).quantile(0.30)).any()
        if not (was_squeezed and close.iloc[-1] >= sma20.iloc[-1]):
            return None

        # 2番底（ダブルボトム）
        low_vals = low.tail(120).values
        peaks, _ = find_peaks(-low_vals, distance=10, prominence=np.nanstd(low_vals) * 0.2)
        if len(peaks) < 2:
            return None
        l1_val, l2_val = low_vals[peaks[-2]], low_vals[peaks[-1]]
        if not (peaks[-1] > peaks[-2] and l1_val * 0.95 <= l2_val <= l1_val * 1.18):
            return None

        # 一目均衡表（雲）
        high9, low9 = high.rolling(9).max(), low.rolling(9).min()
        tenkan = (high9 + low9) / 2
        high26, low26 = high.rolling(26).max(), low.rolling(26).min()
        kijun = (high26 + low26) / 2
        senkou_a = ((tenkan + kijun) / 2).shift(26)
        high52, low52 = high.rolling(52).max(), low.rolling(52).min()
        senkou_b = ((high52 + low52) / 2).shift(26)
        
        cloud_top = np.maximum(senkou_a, senkou_b)
        cloud_bottom = np.minimum(senkou_a, senkou_b)

        c_now, c_prev = float(close.iloc[-1]), float(close.iloc[-2])
        top_now, bot_now = float(cloud_top.iloc[-1]), float(cloud_bottom.iloc[-1])
        
        status = None
        if c_now > top_now and c_prev <= top_now:
            status = "【雲上抜け直後】🚀"
        elif bot_now <= c_now <= top_now:
            status = "【雲侵入中】⚡"
        elif c_now < bot_now and c_now >= bot_now * 0.97:
            status = "【雲直前（接近）】👀"

        if not status:
            return None

        return {"code": ticker_code, "close": c_now, "status": status, "l1": float(l1_val), "l2": float(l2_val)}
    except Exception:
        return None

# ==========================================
# 5. メイン処理（検知銘柄ごとにキャプチャを取得して通知）
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
                    res = analyze_df(code, df_single)
                    if res:
                        results.append(res)
                        print(f"  ★[ヒット] 銘柄コード: {code} | 状態: {res['status']}")
                except Exception:
                    continue
        except Exception as e:
            print(f"  バッチエラー: {e}")
        time.sleep(1)

    print(f"\n=== スクリーニング完了。検知件数: {len(results)} 件 ===")

    # 検知された銘柄ごとに株探チャートを撮影して通知
    if results:
        for r in results:
            msg = (f"【日足チャートスクリーニング検知】\n"
                   f"■ 銘柄コード: {r['code']}\n"
                   f" 株価: {r['close']:,.1f}円 | 状態: {r['status']}\n"
                   f" (1番底: {r['l1']:,.1f}円 / 2番底: {r['l2']:,.1f}円)")
            
            # キャプチャ撮影
            img_path = capture_kabutan_chart(r['code'])
            
            # 画像付き送信
            send_discord_notification_with_image(msg, img_path)
            time.sleep(1)
    else:
        send_discord_notification_with_image("【定期スクリーニング完了】\n本日の条件に該当する銘柄はありませんでした。")
