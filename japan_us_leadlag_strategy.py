import warnings
warnings.filterwarnings('ignore')
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import yfinance as yf
import os

# --- 元のコードの定義をそのまま使用 ---
US_TICKERS = ['XLB','XLC','XLE','XLF','XLI','XLK','XLP','XLRE','XLU','XLV','XLY']
JP_TICKERS = ['1617.T','1618.T','1619.T','1620.T','1621.T','1622.T','1623.T','1624.T','1625.T','1626.T','1627.T','1628.T','1629.T','1630.T','1631.T','1632.T','1633.T']
JP_LABELS = {'1617.T':'食品','1618.T':'エネルギー資源','1619.T':'建設・資材','1620.T':'素材・化学','1621.T':'医薬品','1622.T':'自動車・輸送機','1623.T':'鉄鋼・非鉄','1624.T':'機械','1625.T':'電機・精密','1626.T':'情報通信・サービス','1627.T':'電力・ガス','1628.T':'運輸・物流','1629.T':'商社・卸売','1630.T':'小売','1631.T':'銀行','1632.T':'金融(除く銀行)','1633.T':'不動産'}
US_CYCLICAL, US_DEFENSIVE = ['XLB','XLE','XLF','XLRE'], ['XLK','XLP','XLU','XLV']
JP_CYCLICAL, JP_DEFENSIVE = ['1618.T','1625.T','1629.T','1631.T'], ['1617.T','1621.T','1627.T','1630.T']

def fetch_data():
    # 価格取得の修正
    u_raw = yf.download(US_TICKERS, period="2y", auto_adjust=True, threads=False)
    j_raw = yf.download(JP_TICKERS, period="2y", auto_adjust=True, threads=False)
    p_u = u_raw['Close'] if 'Close' in u_raw.columns else u_raw
    p_j = j_raw['Close'] if 'Close' in j_raw.columns else j_raw
    return p_u.ffill().dropna(), p_j.ffill().dropna()

# --- 元のコードの build_prior_subspace / build_C0 を完全再現 ---
def build_prior_subspace(u_list, j_list):
    N_U, N_J = len(u_list), len(j_list)
    N = N_U + N_J
    v1 = np.ones(N) / np.sqrt(N)
    v2_raw = np.concatenate([np.ones(N_U)/N_U, -np.ones(N_J)/N_J])
    v2_raw -= v2_raw.dot(v1) * v1
    v2 = v2_raw / np.linalg.norm(v2_raw)
    sign_vec = np.zeros(N)
    for i, t in enumerate(u_list):
        if t in US_CYCLICAL: sign_vec[i] = 1.0
        elif t in US_DEFENSIVE: sign_vec[i] = -1.0
    for j, t in enumerate(j_list):
        if t in JP_CYCLICAL: sign_vec[N_U+j] = 1.0
        elif t in JP_DEFENSIVE: sign_vec[N_U+j] = -1.0
    v3_raw = sign_vec.copy()
    v3_raw -= v3_raw.dot(v1) * v1
    v3_raw -= v3_raw.dot(v2) * v2
    v3 = v3_raw / np.linalg.norm(v3_raw)
    return np.column_stack([v1, v2, v3])

def build_C0(V0, C_full):
    D0 = np.diag(V0.T @ C_full @ V0)
    C_raw = V0 @ np.diag(D0) @ V0.T
    D_inv_sqrt = np.diag(1.0 / np.sqrt(np.maximum(np.diag(C_raw), 1e-12)))
    C0 = D_inv_sqrt @ C_raw @ D_inv_sqrt
    np.fill_diagonal(C0, 1.0)
    return C0

def compute_signal(p_u, p_j):
    L, K, lam = 60, 3, 0.9
    cc_u, cc_j = p_u.pct_change().dropna(), p_j.pct_change().dropna()
    common_idx = cc_u.index.intersection(cc_j.index)
    
    # 共通期間でC_fullとC_tを計算
    cc_joint = pd.concat([cc_u.loc[common_idx], cc_j.loc[common_idx]], axis=1)
    Z_full = ((cc_joint - cc_joint.mean()) / cc_joint.std(ddof=0).replace(0, 1e-8)).values
    C_full = np.corrcoef(Z_full.T)
    
    window = cc_joint.iloc[-L:]
    C_t = np.corrcoef(window.T)
    
    V0 = build_prior_subspace(cc_u.columns, cc_j.columns)
    C0 = build_C0(V0, C_full)
    C_reg = (1 - lam) * C_t + lam * C0
    
    vals, vecs = np.linalg.eigh(C_reg)
    Vt_K = vecs[:, np.argsort(vals)[::-1][:K]]
    V_U, V_J = Vt_K[:len(cc_u.columns), :], Vt_K[len(cc_u.columns):, :]
    
    # 【重要】米国当日の標準化（過去L日間の統計を使用）
    u_latest = cc_u.iloc[-1]
    u_hist = cc_u.iloc[-L-1:-1]
    z_U = ((u_latest - u_hist.mean()) / u_hist.std(ddof=0).replace(0, 1e-8)).values
    
    f_t = V_U.T @ z_U
    z_hat_J = V_J @ f_t
    
    sig_df = pd.DataFrame({'ticker': cc_j.columns, 'label': [JP_LABELS[t] for t in cc_j.columns], 'signal': z_hat_J}).sort_values('signal', ascending=False)
    return sig_df, f_t, u_latest.to_dict()

def main():
    p_u, p_j = fetch_data()
    sig_df, f_t, u_ret = compute_signal(p_u, p_j)
    
    # グラフ描画（元コードのスタイルを尊重）
    plt.style.use('dark_background')
    fig = plt.figure(figsize=(15, 12), facecolor='#0d1117')
    gs = fig.add_gridspec(3, 1, hspace=0.5)
    
    # 日本予測（メイン）
    ax = fig.add_subplot(gs[1, 0])
    colors = ['#2ea043' if x > 0 else '#da3633' for x in sig_df['signal']]
    ax.bar(sig_df['label'], sig_df['signal'], color=colors)
    ax.set_title("[JP] Prediction Signals", color='white', fontsize=14)
    plt.xticks(rotation=30, ha='right')
    
    plt.savefig('leadlag_signal_dashboard.png', facecolor='#0d1117', bbox_inches='tight')
    
    # Discordメッセージ
    buy, sell = sig_df.head(3), sig_df.tail(3)
    msg = "🚀 **日米リードラグ投資戦略**\n\n【BUY】\n" + "\n".join([f"・{r.label} ({r.ticker})" for _,r in buy.iterrows()])
    msg += "\n\n【SELL】\n" + "\n".join([f"・{r.label} ({r.ticker})" for _,r in sell.iterrows()])
    with open("discord_msg.txt", "w", encoding="utf-8") as f: f.write(msg)

if __name__ == "__main__":
    main()
