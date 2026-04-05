import pandas as pd
import yfinance as yf
import numpy as np
from datetime import datetime, timedelta

def get_data():
    print("Fetching data from Yahoo Finance...")
    # 米国セクターETF (代表例)
    us_tickers = ['XLK', 'XLV', 'XLF', 'XLY', 'XLP', 'XLI', 'XLU', 'XLE', 'XLB', 'XLRE']
    # 日本市場 (日経225先物代わりの1321、またはETF)
    jp_tickers = ['1321.T']
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=60) # 余裕を持って60日分

    # threads=False にすることで SQLite のロック競合 (database is locked) を回避します
    try:
        p_u = yf.download(us_tickers, start=start_date, end=end_date, interval='1d', threads=False)['Adj Close']
        p_j = yf.download(jp_tickers, start=start_date, end=end_date, interval='1d', threads=False)['Adj Close']
        return p_u, p_j
    except Exception as e:
        print(f"Download Error: {e}")
        return pd.DataFrame(), pd.DataFrame()

def compute_signal(p_u, p_j):
    # パラメータ設定
    L = 20
    
    # 騰落率の計算
    ret_u = p_u.pct_change().dropna()
    ret_j = p_j.pct_change().dropna()

    # --- データ存在チェック ---
    if ret_u.empty or ret_j.empty:
        print("Error: One of the dataframes is empty after pct_change.")
        return None, None, None, None

    if len(ret_u) < L:
        print(f"Error: Not enough data points. Need {L}, but got {len(ret_u)}.")
        return None, None, None, None
    # -----------------------

    # 米国市場の直近Z-score
    # iloc[-1] を参照する前にデータがあることを上記で確認済み
    u_mean = ret_u.tail(L).mean()
    u_std = ret_u.tail(L).std(ddof=0).replace(0, 1e-8)
    z_U_today = ((ret_u.iloc[-1] - u_mean) / u_std).values

    # 各セクターの直近リターン
    us_ret_today = ret_u.iloc[-1].values
    
    # 日本市場（1321.T）の翌日リターンを予測する信号（ここでは単純平均などの例）
    # 実際はここでUSのリードを日本に適用するロジックを記述
    signal_value = np.mean(z_U_today) 

    # 結果をまとめる（簡易版）
    sig_df = pd.DataFrame({"Signal": [signal_value]}, index=[datetime.now().date()])
    
    return sig_df, signal_value, us_ret_today, z_U_today

def main():
    p_u, p_j = get_data()
    
    if p_u.empty or p_j.empty:
        print("No data downloaded. Exiting script.")
        return

    sig_df, f_t, us_ret, us_a = compute_signal(p_u, p_j)

    if sig_df is None:
        print("Signal calculation failed due to insufficient data.")
        return

    print("--- Strategy Results ---")
    print(f"Date: {sig_df.index[0]}")
    print(f"Signal Value: {f_t:.4f}")
    
    # ここで注文指示やファイル保存などの処理を行う
    # 例: sig_df.to_csv("latest_signal.csv")

if __name__ == "__main__":
    main()
