import os
import requests
import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.linalg import eigh
import tempfile

# --- 設定 ---
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL')

# セクター定義
TICKERS_U = ['XLB', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY', 'XLC']
TICKERS_J = ['1617.T', '1618.T', '1619.T', '1620.T', '1621.T', '1622.T', '1623.T', '1624.T', '1625.T', '1626.T', '1627.T']
LABELS_E = ['Materials', 'Energy', 'Financials', 'Industrials', 'Tech', 'Staples', 'RealEstate', 'Utilities', 'HealthCare', 'Discretionary', 'Comm.Svcs']

def calculate_signal():
    data_u = yf.download(TICKERS_U, period='65d')['Close'].dropna()
    data_j = yf.download(TICKERS_J, period='65d')['Close'].dropna()
    ret_u = data_u.pct_change().dropna()
    ret_j = data_j.pct_change().dropna()
    common_idx = ret_u.index.intersection(ret_j.index)
    Z_u = ret_u.loc[common_idx].values
    Z_j = ret_j.loc[common_idx].values
    T = len(common_idx)
    C_uu = (Z_u.T @ Z_u) / T
    C_jj = (Z_j.T @ Z_j) / T
    C_ju = (Z_j.T @ Z_u) / T
    lam = 0.9
    C_reg = (1-lam)*(C_ju @ C_ju.T) + lam * C_jj
    # 下位互換性を保ちつつ、確実に動く書き方に変更します
    vals, vecs = eigh(C_reg, subset_by_index=[C_reg.shape[0]-3, C_reg.shape[0]-1])
    V_j = vecs[:, ::-1]
    B = V_j @ V_j.T @ C_ju @ np.linalg.inv(C_uu + 1e-6 * np.eye(len(TICKERS_U)))
    latest_ret_u = ret_u.iloc[-1].values
    pred_signal = B @ latest_ret_u
    return pred_signal

def create_plot(signals):
    plt.figure(figsize=(10, 6))
    df_plot = pd.DataFrame({'Sector': LABELS_E, 'Signal': signals})
    df_plot = df_plot.sort_values('Signal')
    colors = ['red' if x > 0 else 'blue' for x in df_plot['Signal']]
    plt.barh(df_plot['Sector'], df_plot['Signal'], color=colors)
    plt.title('Predicted Japan Sector Returns')
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    temp_file = os.path.join(tempfile.gettempdir(), 'signal.png')
    plt.savefig(temp_file)
    plt.close()
    return temp_file

if __name__ == "__main__":
    try:
        if not DISCORD_WEBHOOK_URL:
            raise ValueError("DISCORD_WEBHOOK_URL is not set")
        
        signals = calculate_signal()
        img_path = create_plot(signals)
        
        with open(img_path, 'rb') as f:
            files = {'file': ('signal.png', f, 'image/png')}
            payload = {'content': '📊 **本日の日米リードラグ・予測シグナル**'}
            requests.post(DISCORD_WEBHOOK_URL, data=payload, files=files)
        print("Success")
    except Exception as e:
        print(f"Error: {e}")
        exit(1)
