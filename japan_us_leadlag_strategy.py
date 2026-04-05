import pandas as pd
import yfinance as yf
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os

def get_data():
    print("Fetching data from Yahoo Finance...")
    # 米国セクターETF
    us_tickers = ['XLK', 'XLV', 'XLF', 'XLY', 'XLP', 'XLI', 'XLU', 'XLE', 'XLB', 'XLRE']
    # 日本市場 (日経225連動型上場投資信託)
    jp_tickers = ['1321.T']
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90) # 余裕を持って90日分

    # threads=False で SQLite のロック競合を回避
    try:
        p_u = yf.download(us_tickers, start=start_date, end=end_date, interval='1d', threads=False)
        p_j = yf.download(jp_tickers, start=start_date, end=end_date, interval='1d', threads=False)
        
        # マルチインデックス対策（Adj Closeを取得）
        if 'Adj Close' in p_u.columns:
            p_u = p_u['Adj Close']
        if 'Adj Close' in p_j.columns:
            p_j = p_j['Adj Close']
            
        return p_u, p_j
    except Exception as e:
        print(f"Download Error: {e}")
        return pd.DataFrame(), pd.DataFrame()

def compute_signal(p_u, p_j):
    L = 20 # 移動平均の窓期間
    
    # リターンの計算
    ret_u = p_u.pct_change().dropna()
    ret_j = p_j.pct_change().dropna()

    # --- データ存在チェック (IndexError対策) ---
    if ret_u.empty or ret_j.empty:
        print("Error: DataFrame is empty after pct_change.")
        return None, None, None
    
    if len(ret_u) < L:
        print(f"Error: Not enough data points. Need {L}, but got {len(ret_u)}.")
        return None, None, None
    # ----------------------------------------

    # 最新のUSセクターリターン
    latest_ret_u = ret_u.iloc[-1]
    
    # 各セクターのZ-score計算
    u_mean = ret_u.tail(L).mean()
    u_std = ret_u.tail(L).std(ddof=0).replace(0, 1e-8)
    z_scores = (latest_ret_u - u_mean) / u_std

    # 日本市場へのシグナル（例：US全セクターの平均Z-score）
    signal_value = z_scores.mean()
    
    return signal_value, z_scores, ret_u

def main():
    p_u, p_j = get_data()
    
    if p_u.empty or p_j.empty:
        print("No data available. Skipping execution.")
        return

    signal_value, z_scores, ret_u = compute_signal(p_u, p_j)

    if signal_value is None:
        return

    print(f"--- Strategy Results ---")
    print(f"Signal Value: {signal_value:.4f}")

    # --- グラフの作成と保存 (GitHub Actionsのエラー対策) ---
    try:
        plt.figure(figsize=(10, 6))
        z_scores.plot(kind='bar', color='skyblue')
        plt.axhline(0, color='black', linewidth=0.8)
        plt.title(f"US Sector Z-Scores (Lead-Lag Signal: {signal_value:.4f})")
        plt.ylabel("Z-Score")
        plt.xlabel("Sectors")
        plt.tight_layout()
        
        # YAMLが探しているファイル名で保存
        filename = "leadlag_signal_dashboard.png"
        plt.savefig(filename)
        print(f"Successfully saved {filename}")
        
    except Exception as e:
        print(f"Plotting Error: {e}")

if __name__ == "__main__":
    main()
