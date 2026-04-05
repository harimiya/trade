import warnings
warnings.filterwarnings('ignore')

import matplotlib
matplotlib.use('Agg') # GUIなし環境用
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime

# ── 設定 ──────────────────────────────────────────────────────────
# グラフ用：日本語が使えないためローマ字ラベルを使用
JP_LABELS_ROMAJI = {
    '1617.T': 'Food', '1618.T': 'Energy', '1619.T': 'Construction', '1620.T': 'Chemical',
    '1621.T': 'Pharma', '1622.T': 'Auto', '1623.T': 'Steel', '1624.T': 'Machinery',
    '1625.T': 'Electric', '1626.T': 'IT/Service', '1627.T': 'Gas/Electric', '1628.T': 'Transport',
    '1629.T': 'Trading', '1630.T': 'Retail', '1631.T': 'Bank', '1632.T': 'Finance', '1633.T': 'RealEstate'
}

# 通知用：日本語ラベル
JP_LABELS_JP = {
    '1617.T': '食品', '1618.T': 'エネルギー', '1619.T': '建設資材', '1620.T': '素材化学',
    '1621.T': '医薬品', '1622.T': '自動車', '1623.T': '鉄鋼非鉄', '1624.T': '機械',
    '1625.T': '電機精密', '1626.T': '情通サビ', '1627.T': '電力ガス', '1628.T': '運輸物流',
    '1629.T': '商社', '1630.T': '小売', '1631.T': '銀行', '1632.T': 'その他金融', '1633.T': '不動産'
}

US_TICKERS = ['XLB','XLC','XLE','XLF','XLI','XLK','XLP','XLRE','XLU','XLV','XLY']
JP_TICKERS = list(JP_LABELS_JP.keys())

# ── データ取得 ────────────────────────────────────────────────────
def fetch_data():
    p_u = yf.download(US_TICKERS, period="6mo", interval="1d", threads=False)['Adj Close']
    p_j = yf.download(JP_TICKERS, period="6mo", interval="1d", threads=False)['Adj Close']
    return p_u.ffill().dropna(), p_j.ffill().dropna()

# ── シグナル計算 ──────────────────────────────────────────────────
def compute_signal(p_u, p_j):
    L, K, lam = 60, 3, 0.9
    ret_u, ret_j = p_u.pct_change().dropna(), p_j.pct_change().dropna()
    
    if len(ret_u) < L + 1: return None
    
    common_idx = ret_u.index.intersection(ret_j.index)
    z_u = (ret_u.loc[common_idx] - ret_u.tail(L).mean()) / ret_u.tail(L).std().replace(0, 1e-8)
    z_j = (ret_j.loc[common_idx] - ret_j.tail(L).mean()) / ret_j.tail(L).std().replace(0, 1e-8)
    
    # 簡易的なリード・ラグ相関ロジック（USの直近リターンをJPに投影）
    u_latest = z_u.iloc[-1]
    # 相関行列の計算とシグナル生成
    corr_matrix = np.corrcoef(z_u.tail(L).T, z_j.tail(L).T)[:len(US_TICKERS), len(US_TICKERS):]
    signals = corr_matrix.T @ u_latest.values
    
    sig_df = pd.DataFrame({
        'ticker': JP_TICKERS,
        'label_jp': [JP_LABELS_JP[t] for t in JP_TICKERS],
        'label_en': [JP_LABELS_ROMAJI[t] for t in JP_TICKERS],
        'signal': signals
    }).sort_values('signal', ascending=False)
    
    return sig_df, u_latest

# ── 可視化 ────────────────────────────────────────────────────────
def plot_dashboard(sig_df, u_latest):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), facecolor='#0d1117')
    
    # US Sectors (Latest Z-score)
    ax1.bar(u_latest.index, u_latest.values, color='#58a6ff')
    ax1.set_title("Latest US Sector Z-Scores", color='white')
    
    # JP Prediction (Sorted)
    colors = ['#2ea043' if x > 0 else '#da3633' for x in sig_df['signal']]
    ax2.bar(sig_df['label_en'], sig_df['signal'], color=colors)
    ax2.set_title("JP Sector Prediction Signals", color='white')
    plt.xticks(rotation=45, ha='right')

    for ax in [ax1, ax2]:
        ax.set_facecolor('#161b22')
        ax.tick_params(colors='white')
        ax.title.set_color('white')
        for spine in ax.spines.values(): spine.set_edgecolor('#30363d')
    
    plt.tight_layout()
    plt.savefig('leadlag_signal_dashboard.png', facecolor='#0d1117')

def main():
    p_u, p_j = fetch_data()
    result = compute_signal(p_u, p_j)
    if result:
        sig_df, u_latest = result
        plot_dashboard(sig_df, u_latest)
        
        # Discord用のテキストファイルを作成
        buy = sig_df.head(3)
        sell = sig_df.tail(3)
        with open("discord_msg.txt", "w") as f:
            f.write("🚀 **日米リードラグ シグナル報告**\n\n")
            f.write("📈 **BUY (買い推奨)**\n")
            for _, r in buy.iterrows():
                f.write(f"・{r['ticker']} {r['label_jp']} (Sig: {r['signal']:.3f})\n")
            f.write("\n📉 **SELL (売り推奨)**\n")
            for _, r in sell.iterrows():
                f.write(f"・{r['ticker']} {r['label_jp']} (Sig: {r['signal']:.3f})\n")

if __name__ == "__main__":
    main()
