import warnings
warnings.filterwarnings('ignore')

import matplotlib
matplotlib.use('Agg') # GitHub Actions環境で必須
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import yfinance as yf
import os

# ── フォント設定 (UbuntuのNoto Sans CJK JPを直接指定) ──
font_path = '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc'
if os.path.exists(font_path):
    prop = fm.FontProperties(fname=font_path)
    plt.rcParams['font.family'] = prop.get_name()
else:
    plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['axes.unicode_minus'] = False

# ── ティッカー & ラベル定義 ──
US_TICKERS = ['XLB','XLC','XLE','XLF','XLI','XLK','XLP','XLRE','XLU','XLV','XLY']
US_LABELS = {'XLB':'Materials','XLC':'Comm Svcs','XLE':'Energy','XLF':'Financials','XLI':'Industrials','XLK':'IT','XLP':'Staples','XLRE':'Real Estate','XLU':'Utilities','XLV':'Health Care','XLY':'Discr'}

JP_TICKERS = ['1617.T','1618.T','1619.T','1620.T','1621.T','1622.T','1623.T','1624.T','1625.T','1626.T','1627.T','1628.T','1629.T','1630.T','1631.T','1632.T','1633.T']
JP_LABELS = {
    '1617.T': '食品', '1618.T': 'エネルギー資源', '1619.T': '建設・資材', '1620.T': '素材・化学',
    '1621.T': '医薬品', '1622.T': '自動車・輸送機', '1623.T': '鉄鋼・非鉄', '1624.T': '機械',
    '1625.T': '電機・精密', '1626.T': '情報通信・サービス', '1627.T': '電力・ガス', '1628.T': '運輸・物流',
    '1629.T': '商社・卸売', '1630.T': '小売', '1631.T': '銀行', '1632.T': '金融(除く銀行)', '1633.T': '不動産'
}

US_CYCLICAL, US_DEFENSIVE = ['XLB','XLE','XLF','XLRE'], ['XLK','XLP','XLU','XLV']
JP_CYCLICAL, JP_DEFENSIVE = ['1618.T','1625.T','1629.T','1631.T'], ['1617.T','1621.T','1627.T','1630.T']

# ── データ取得 ──
def fetch_data():
    # 2019年から取得することでC_fullの計算を安定させる
    u_raw = yf.download(US_TICKERS, start='2019-01-01', auto_adjust=True, threads=False)
    j_raw = yf.download(JP_TICKERS, start='2019-01-01', auto_adjust=True, threads=False)
    p_u = u_raw['Close'] if 'Close' in u_raw.columns else u_raw
    p_j = j_raw['Close'] if 'Close' in j_raw.columns else j_raw
    return p_u.ffill().dropna(), p_j.ffill().dropna()

# ── 部分空間構築 (論文ロジック) ──
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

# ── シグナル計算 (元のコードを完全再現) ──
def compute_signal(p_u, p_j):
    L, K, lam = 60, 3, 0.9
    cc_u, cc_j = p_u.pct_change().dropna(), p_j.pct_change().dropna()
    common_idx = cc_u.index.intersection(cc_j.index)
    
    # 全期間の相関 (C_full)
    cc_joint = pd.concat([cc_u.loc[common_idx], cc_j.loc[common_idx]], axis=1)
    Z_full = ((cc_joint - cc_joint.mean()) / cc_joint.std(ddof=0).replace(0, 1e-8)).values
    C_full = np.corrcoef(Z_full.T)
    
    # 直近L日の相関 (C_t)
    window = cc_joint.iloc[-L:]
    C_t = np.corrcoef(window.T)
    
    # 正則化
    V0 = build_prior_subspace(cc_u.columns, cc_j.columns)
    C0 = build_C0(V0, C_full)
    C_reg = (1 - lam) * C_t + lam * C0
    
    vals, vecs = np.linalg.eigh(C_reg)
    Vt_K = vecs[:, np.argsort(vals)[::-1][:K]]
    V_U, V_J = Vt_K[:len(cc_u.columns), :], Vt_K[len(cc_u.columns):, :]
    
    # 米国当日の標準化 (過去L日の統計を使用)
    u_latest = cc_u.iloc[-1]
    u_hist = cc_u.iloc[-L-1:-1]
    z_U = ((u_latest - u_hist.mean()) / u_hist.std(ddof=0).replace(0, 1e-8)).values
    
    f_t = V_U.T @ z_U
    z_hat_J = V_J @ f_t
    
    sig_df = pd.DataFrame({
        'ticker': cc_j.columns, 
        'label': [JP_LABELS[t] for t in cc_j.columns], 
        'signal': z_hat_J
    }).sort_values('signal', ascending=False)
    
    return sig_df, f_t, u_latest

# ── メイン処理 & 可視化 ──
def main():
    p_u, p_j = fetch_data()
    sig_df, f_t, u_ret = compute_signal(p_u, p_j)
    
    plt.style.use('dark_background')
    fig = plt.figure(figsize=(15, 12), facecolor='#0d1117')
    gs = fig.add_gridspec(3, 1, hspace=0.4)
    
    # 米国リターン
    ax0 = fig.add_subplot(gs[0, 0])
    u_pct = u_ret * 100
    ax0.bar(u_pct.index, u_pct.values, color=['#2ea043' if x > 0 else '#da3633' for x in u_pct])
    ax0.set_title("[US] 当日セクターリターン (%)", fontsize=14)
    
    # 日本予測
    ax1 = fig.add_subplot(gs[1, 0])
    colors = ['#2ea043' if x > 0 else '#da3633' for x in sig_df['signal']]
    ax1.bar(sig_df['label'], sig_df['signal'], color=colors)
    ax1.set_title("[JP] 翌日予測シグナル (部分空間正則化PCA)", fontsize=14)
    plt.xticks(rotation=30, ha='right')
    
    # ファクタースコア
    ax2 = fig.add_subplot(gs[2, 0])
    ax2.bar(['Global', 'Spread', 'Cyclical'], f_t, color='#58a6ff')
    ax2.set_title("共通ファクタースコア (f_t)", fontsize=14)

    plt.savefig('leadlag_signal_dashboard.png', facecolor='#0d1117', bbox_inches='tight')
    
    # Discord用テキスト
    buy, sell = sig_df.head(3), sig_df.tail(3)
    msg = "🚀 **日米リードラグ投資戦略**\n\n【BUY】\n" + "\n".join([f"・{r.label} ({r.ticker}) Sig:{r.signal:+.3f}" for _,r in buy.iterrows()])
    msg += "\n\n【SELL】\n" + "\n".join([f"・{r.label} ({r.ticker}) Sig:{r.signal:+.3f}" for _,r in sell.iterrows()])
    with open("discord_msg.txt", "w", encoding="utf-8") as f:
        f.write(msg)

if __name__ == "__main__":
    main()
