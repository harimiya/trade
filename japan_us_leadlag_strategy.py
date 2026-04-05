import warnings
warnings.filterwarnings('ignore')

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import platform
import os
import numpy as np
import pandas as pd
import yfinance as yf
import matplotlib.patches as mpatches

# ── フォント設定 (Linux/GitHub Actions環境への対応) ──
def setup_font():
    system = platform.system()
    if system == 'Darwin':
        font_name = 'Hiragino Sans'
    elif system == 'Windows':
        font_name = 'MS Gothic'
    else:
        # GitHub Actions (Linux) では日本語フォントがないため標準フォントを使用
        font_name = 'DejaVu Sans' 
    
    matplotlib.rcParams['font.family'] = font_name
    matplotlib.rcParams['axes.unicode_minus'] = False

setup_font()

# ── ティッカー定義 ──
US_TICKERS = ['XLB','XLC','XLE','XLF','XLI','XLK','XLP','XLRE','XLU','XLV','XLY']
JP_TICKERS = ['1617.T','1618.T','1619.T','1620.T','1621.T','1622.T','1623.T','1624.T','1625.T','1626.T','1627.T','1628.T','1629.T','1630.T','1631.T','1632.T','1633.T']
JP_LABELS = {'1617.T':'食品','1618.T':'エネルギー','1619.T':'建設資材','1620.T':'素材化学','1621.T':'医薬品','1622.T':'自動車','1623.T':'鉄鋼非鉄','1624.T':'機械','1625.T':'電機精密','1626.T':'情通サビ','1627.T':'電力ガス','1628.T':'運輸物流','1629.T':'商社','1630.T':'小売','1631.T':'銀行','1632.T':'その他金融','1633.T':'不動産'}

US_CYCLICAL = ['XLB','XLE','XLF','XLRE']
US_DEFENSIVE = ['XLK','XLP','XLU','XLV']
JP_CYCLICAL = ['1618.T','1625.T','1629.T','1631.T']
JP_DEFENSIVE = ['1617.T','1621.T','1627.T','1630.T']

# ── データ取得 ──
def fetch_data():
    print("Fetching data from Yahoo Finance...")
    # threads=False でデータベースロック競合を回避
    p_u = yf.download(US_TICKERS, period="6mo", interval="1d", threads=False)
    p_j = yf.download(JP_TICKERS, period="6mo", interval="1d", threads=False)
    
    # 最近のyfinance仕様に合わせて Adj Close を取得
    p_u = p_u['Adj Close'] if 'Adj Close' in p_u.columns else p_u['Close']
    p_j = p_j['Adj Close'] if 'Adj Close' in p_j.columns else p_j['Close']
    
    return p_u.ffill().dropna(), p_j.ffill().dropna()

# ── 事前空間構築 ──
def build_prior_subspace(u_list, j_list):
    N_U, N_J = len(u_list), len(j_list)
    N = N_U + N_J
    v1 = np.ones(N) / np.sqrt(N)
    v2_raw = np.concatenate([np.ones(N_U)/N_U, -np.ones(N_J)/N_J])
    v2 = (v2_raw - v2_raw.dot(v1)*v1)
    v2 /= np.linalg.norm(v2)
    
    sign_vec = np.zeros(N)
    for i, t in enumerate(u_list):
        if t in US_CYCLICAL: sign_vec[i] = 1.0
        elif t in US_DEFENSIVE: sign_vec[i] = -1.0
    for j, t in enumerate(j_list):
        if t in JP_CYCLICAL: sign_vec[N_U+j] = 1.0
        elif t in JP_DEFENSIVE: sign_vec[N_U+j] = -1.0
    v3 = (sign_vec - sign_vec.dot(v1)*v1 - sign_vec.dot(v2)*v2)
    v3 /= np.linalg.norm(v3)
    return np.column_stack([v1, v2, v3])

# ── シグナル計算 ──
def compute_signal(p_u, p_j):
    L, K, lam = 60, 3, 0.9
    ret_u, ret_j = p_u.pct_change().dropna(), p_j.pct_change().dropna()
    
    if len(ret_u) < L + 1 or len(ret_j) < L + 1:
        print("Data length insufficient.")
        return None, None, None, None

    common_idx = ret_u.index.intersection(ret_j.index)
    z_joint = pd.concat([ret_u.loc[common_idx], ret_j.loc[common_idx]], axis=1)
    z_win = z_joint.tail(L)
    C_t = np.corrcoef(z_win.T)
    
    V0 = build_prior_subspace(ret_u.columns, ret_j.columns)
    C_reg = (1 - lam) * C_t + lam * (V0 @ V0.T)
    
    vals, vecs = np.linalg.eigh(C_reg)
    Vt_K = vecs[:, np.argsort(vals)[::-1][:K]]
    
    V_U, V_J = Vt_K[:len(ret_u.columns), :], Vt_K[len(ret_u.columns):, :]
    
    u_latest = ret_u.iloc[-1]
    u_z = (u_latest - ret_u.tail(L).mean()) / ret_u.tail(L).std().replace(0, 1e-8)
    
    f_t = V_U.T @ u_z.values
    z_hat_J = V_J @ f_t
    
    sig_df = pd.DataFrame({'label': [JP_LABELS.get(t, t) for t in ret_j.columns], 'signal': z_hat_J}, index=ret_j.columns)
    return sig_df, f_t, u_latest, ret_u.columns

# ── 可視化 ──
def plot_dashboard(sig_df, f_t, u_ret, u_cols):
    fig = plt.figure(figsize=(15, 12), facecolor='#0d1117')
    gs = fig.add_gridspec(3, 2, hspace=0.4, wspace=0.3)
    
    # US Returns
    ax1 = fig.add_subplot(gs[0, :])
    u_vals = [u_ret[t]*100 for t in u_cols]
    ax1.bar(u_cols, u_vals, color=['#2ea043' if v >= 0 else '#da3633' for v in u_vals])
    ax1.set_title("[US] Sector Daily Returns (%)", color='white')
    
    # JP Signals
    ax2 = fig.add_subplot(gs[1, :])
    sig_sorted = sig_df.sort_values('signal', ascending=False)
    ax2.bar(sig_sorted['label'], sig_sorted['signal'], color=['#2ea043' if v >= 0 else '#da3633' for v in sig_sorted['signal']])
    ax2.set_title("[JP] Prediction Signals (Z-score)", color='white')
    
    # Factors
    ax3 = fig.add_subplot(gs[2, 0])
    ax3.bar(['Global', 'Country', 'Cyclical'], f_t, color='#58a6ff')
    ax3.set_title("Common Factor Scores (f_t)", color='white')

    # Recommendation Text
    ax4 = fig.add_subplot(gs[2, 1])
    ax4.axis('off')
    buy = sig_sorted.head(3).index.tolist()
    sell = sig_sorted.tail(3).index.tolist()
    text = f"【BUY】\n{', '.join(buy)}\n\n【SELL】\n{', '.join(sell)}"
    ax4.text(0.1, 0.5, text, color='white', fontsize=14, fontweight='bold', va='center')

    for ax in [ax1, ax2, ax3]:
        ax.set_facecolor('#161b22')
        ax.tick_params(colors='white')
        for spine in ax.spines.values(): spine.set_edgecolor('#30363d')

    plt.savefig('leadlag_signal_dashboard.png', facecolor='#0d1117')
    print("Dashboard image saved successfully.")

def main():
    try:
        p_u, p_j = fetch_data()
        if p_u.empty or p_j.empty:
            print("No data fetched.")
            return
            
        sig_df, f_t, u_ret, u_cols = compute_signal(p_u, p_j)
        if sig_df is not None:
            plot_dashboard(sig_df, f_t, u_ret, u_cols)
        else:
            print("Could not compute signal (Insufficient data points).")
    except Exception as e:
        print(f"Main Execution Error: {e}")

if __name__ == "__main__":
    main()
