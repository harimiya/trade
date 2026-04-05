import warnings
warnings.filterwarnings('ignore')

import matplotlib
matplotlib.use('Agg') # GitHub Actions用
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import yfinance as yf
import os

# ── 設定 ──────────────────────────────────────────────────────────
US_TICKERS = ['XLB','XLC','XLE','XLF','XLI','XLK','XLP','XLRE','XLU','XLV','XLY']
US_LABELS = {'XLB':'Materials','XLC':'Comm Svcs','XLE':'Energy','XLF':'Financials','XLI':'Industrials','XLK':'IT','XLP':'Staples','XLRE':'Real Estate','XLU':'Utilities','XLV':'Health Care','XLY':'Discr'}

JP_TICKERS = ['1617.T','1618.T','1619.T','1620.T','1621.T','1622.T','1623.T','1624.T','1625.T','1626.T','1627.T','1628.T','1629.T','1630.T','1631.T','1632.T','1633.T']
JP_LABELS_EN = {'1617.T':'Food','1618.T':'Energy','1619.T':'Const','1620.T':'Chem','1621.T':'Pharma','1622.T':'Auto','1623.T':'Steel','1624.T':'Machin','1625.T':'Electric','1626.T':'IT/Svcs','1627.T':'Gas/Elec','1628.T':'Trans','1629.T':'Trading','1630.T':'Retail','1631.T':'Bank','1632.T':'Finance','1633.T':'RealEstate'}
JP_LABELS_JP = {'1617.T':'食品','1618.T':'エネルギー資源','1619.T':'建設・資材','1620.T':'素材・化学','1621.T':'医薬品','1622.T':'自動車・輸送機','1623.T':'鉄鋼・非鉄','1624.T':'機械','1625.T':'電機精密','1626.T':'情報通信・サービス','1627.T':'電力・ガス','1628.T':'運輸・物流','1629.T':'商社・卸売','1630.T':'小売','1631.T':'銀行','1632.T':'金融(除く銀行)','1633.T':'不動産'}

US_CYCLICAL, US_DEFENSIVE = ['XLB','XLE','XLF','XLRE'], ['XLK','XLP','XLU','XLV']
JP_CYCLICAL, JP_DEFENSIVE = ['1618.T','1625.T','1629.T','1631.T'], ['1617.T','1621.T','1627.T','1630.T']

# ── データ取得 ────────────────────────────────────────────────────
def fetch_data():
    # Price/Adj Closeの仕様変更に耐える取得方法
    u_raw = yf.download(US_TICKERS, period="1y", auto_adjust=True, threads=False)
    j_raw = yf.download(JP_TICKERS, period="1y", auto_adjust=True, threads=False)
    # マルチインデックスでも単一インデックスでも'Close'を取得
    p_u = u_raw['Close'] if 'Close' in u_raw.columns else u_raw
    p_j = j_raw['Close'] if 'Close' in j_raw.columns else j_raw
    return p_u.ffill().dropna(), p_j.ffill().dropna()

# ── 部分空間正則化PCAロジック (論文準拠) ──────────────────────────
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

def compute_signal(p_u, p_j):
    L, K, lam = 60, 3, 0.9
    ret_u, ret_j = p_u.pct_change().dropna(), p_j.pct_change().dropna()
    common_idx = ret_u.index.intersection(ret_j.index)
    if len(common_idx) < L: return None
    
    z_joint = pd.concat([ret_u.loc[common_idx], ret_j.loc[common_idx]], axis=1).tail(L)
    C_t = np.corrcoef(z_joint.T)
    V0 = build_prior_subspace(ret_u.columns, ret_j.columns)
    C_reg = (1 - lam) * C_t + lam * (V0 @ V0.T)
    
    vals, vecs = np.linalg.eigh(C_reg)
    Vt_K = vecs[:, np.argsort(vals)[::-1][:K]]
    V_U, V_J = Vt_K[:len(ret_u.columns), :], Vt_K[len(ret_u.columns):, :]
    
    # 米国最新リターンの標準化
    u_latest = ret_u.iloc[-1]
    u_mu, u_std = ret_u.tail(L).mean(), ret_u.tail(L).std().replace(0, 1e-8)
    z_U = (u_latest - u_mu) / u_std
    
    f_t = V_U.T @ z_U.values
    z_hat_J = V_J @ f_t
    
    sig_df = pd.DataFrame({
        'ticker': ret_j.columns,
        'label_en': [JP_LABELS_EN[t] for t in ret_j.columns],
        'label_jp': [JP_LABELS_JP[t] for t in ret_j.columns],
        'signal': z_hat_J
    }).sort_values('signal', ascending=False)
    
    return sig_df, f_t, u_latest

# ── 可視化 ────────────────────────────────────────────────────────
def plot_dashboard(sig_df, f_t, u_latest):
    plt.style.use('dark_background')
    fig = plt.figure(figsize=(18, 14), facecolor='#0d1117')
    gs = fig.add_gridspec(3, 2, hspace=0.4, wspace=0.3)
    
    # [1] US Returns
    ax1 = fig.add_subplot(gs[0, :])
    u_ret_pct = u_latest * 100
    ax1.bar(u_ret_pct.index, u_ret_pct.values, color=['#2ea043' if x > 0 else '#da3633' for x in u_ret_pct])
    ax1.set_title("[US] Sector Daily Returns (%)", color='white', fontsize=15)
    
    # [2] JP Signal (英語ラベルで文字化け回避)
    ax2 = fig.add_subplot(gs[1, :])
    ax2.bar(sig_df['label_en'], sig_df['signal'], color=['#2ea043' if x > 0 else '#da3633' for x in sig_df['signal']])
    ax2.set_title("[JP] Prediction Signals (PCA SUB Lead-Lag)", color='white', fontsize=15)
    plt.xticks(rotation=30)
    
    # [3] Factor Scores
    ax3 = fig.add_subplot(gs[2, 0])
    ax3.bar(['Global', 'Spread', 'Cyclical'], f_t, color='#58a6ff')
    ax3.set_title("Common Factor Scores (f_t)", color='white')

    # [4] Text Summary
    ax4 = fig.add_subplot(gs[2, 1])
    ax4.axis('off')
    buy, sell = sig_df.head(3), sig_df.tail(3)
    res_txt = "🚀 RECOMMENDATION\n\n【BUY】\n" + "\n".join([f"・{r.label_jp} ({r.ticker})" for _,r in buy.iterrows()])
    res_txt += "\n\n【SELL】\n" + "\n".join([f"・{r.label_jp} ({r.ticker})" for _,r in sell.iterrows()])
    ax4.text(0.1, 0.5, res_txt, color='white', fontsize=14, fontweight='bold', va='center')

    for ax in [ax1, ax2, ax3]:
        ax.set_facecolor('#161b22')
        ax.spines[:].set_edgecolor('#30363d')

    plt.savefig('leadlag_signal_dashboard.png', facecolor='#0d1117')
    
    # Discord用のメッセージ作成
    with open("discord_msg.txt", "w", encoding="utf-8") as f:
        f.write(res_txt)

if __name__ == "__main__":
    p_u, p_j = fetch_data()
    res = compute_signal(p_u, p_j)
    if res:
        plot_dashboard(*res)
