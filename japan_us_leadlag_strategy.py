import warnings
warnings.filterwarnings('ignore')

import matplotlib
matplotlib.use('Agg') # GUIがないGitHub Actions環境で必須
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yfinance as yf
import os

# ── 設定 (文字化け回避のため英語ラベルのみ使用) ──────────────────
JP_LABELS_MAP = {
    '1617.T': 'Food', '1618.T': 'Energy', '1619.T': 'Construction', '1620.T': 'Chemical',
    '1621.T': 'Pharma', '1622.T': 'Auto', '1623.T': 'Steel', '1624.T': 'Machinery',
    '1625.T': 'Electric', '1626.T': 'IT-Service', '1627.T': 'Gas-Electric', '1628.T': 'Transport',
    '1629.T': 'Trading', '1630.T': 'Retail', '1631.T': 'Bank', '1632.T': 'Finance', '1633.T': 'RealEstate'
}

# 通知用日本語名
JP_NAME_JP = {
    '1617.T': '食品', '1618.T': 'エネルギー', '1619.T': '建設資材', '1620.T': '素材化学',
    '1621.T': '医薬品', '1622.T': '自動車', '1623.T': '鉄鋼非鉄', '1624.T': '機械',
    '1625.T': '電機精密', '1626.T': '情通サビ', '1627.T': '電力ガス', '1628.T': '運輸物流',
    '1629.T': '商社', '1630.T': '小売', '1631.T': '銀行', '1632.T': 'その他金融', '1633.T': '不動産'
}

US_TICKERS = ['XLB','XLC','XLE','XLF','XLI','XLK','XLP','XLRE','XLU','XLV','XLY']
JP_TICKERS = list(JP_LABELS_MAP.keys())

# ── データ取得 (KeyError: 'Adj Close' 対策) ────────────────────
def fetch_data():
    print("Fetching data...")
    # auto_adjust=Trueにすることで、Closeが修正済み株価(Adj Close)として取得されます
    p_u_raw = yf.download(US_TICKERS, period="6mo", interval="1d", threads=False, auto_adjust=True)
    p_j_raw = yf.download(JP_TICKERS, period="6mo", interval="1d", threads=False, auto_adjust=True)
    
    # 複数銘柄の場合、Close列を指定して取得
    p_u = p_u_raw['Close']
    p_j = p_j_raw['Close']
    
    return p_u.ffill().dropna(), p_j.ffill().dropna()

# ── シグナル計算 ──────────────────────────────────────────────────
def compute_signal(p_u, p_j):
    L = 60
    ret_u = p_u.pct_change().dropna()
    ret_j = p_j.pct_change().dropna()
    
    if len(ret_u) < L + 1:
        print("Not enough data.")
        return None
    
    common_idx = ret_u.index.intersection(ret_j.index)
    z_u = (ret_u.loc[common_idx] - ret_u.tail(L).mean()) / ret_u.tail(L).std().replace(0, 1e-8)
    z_j = (ret_j.loc[common_idx] - ret_j.tail(L).mean()) / ret_j.tail(L).std().replace(0, 1e-8)
    
    u_latest = z_u.iloc[-1]
    # リードラグ相関を計算
    corr = np.corrcoef(z_u.tail(L).T, z_j.tail(L).T)[:len(US_TICKERS), len(US_TICKERS):]
    signals = corr.T @ u_latest.values
    
    sig_df = pd.DataFrame({
        'ticker': JP_TICKERS,
        'label_en': [JP_LABELS_MAP[t] for t in JP_TICKERS],
        'label_jp': [JP_NAME_JP[t] for t in JP_TICKERS],
        'signal': signals
    }).sort_values('signal', ascending=False)
    
    return sig_df, u_latest

# ── 可視化 ────────────────────────────────────────────────────────
def plot_dashboard(sig_df, u_latest):
    plt.style.use('dark_background')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    fig.patch.set_facecolor('#0d1117')

    # [1] US Sector Z-scores
    ax1.bar(u_latest.index, u_latest.values, color='#58a6ff')
    ax1.set_title("Latest US Sector Status (Z-score)", fontsize=14)
    ax1.set_facecolor('#161b22')

    # [2] JP Signals (ローマ字ラベルで文字化け回避)
    colors = ['#2ea043' if x > 0 else '#da3633' for x in sig_df['signal']]
    ax2.bar(sig_df['label_en'], sig_df['signal'], color=colors)
    ax2.set_title("JP Sector Prediction Signals", fontsize=14)
    ax2.set_facecolor('#161b22')
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig('leadlag_signal_dashboard.png', facecolor='#0d1117')
    print("Dashboard saved.")

def main():
    try:
        p_u, p_j = fetch_data()
        res = compute_signal(p_u, p_j)
        if res:
            sig_df, u_latest = res
            plot_dashboard(sig_df, u_latest)
            
            # 一致したBUY/SELL情報をファイルに書き出し
            buy = sig_df.head(3)
            sell = sig_df.tail(3)
            with open("discord_msg.txt", "w", encoding="utf-8") as f:
                f.write("🚀 **日米リードラグ投資戦略レポート**\n\n")
                f.write("📈 **BUY (上位シグナル)**\n")
                for _, r in buy.iterrows():
                    f.write(f"・{r['ticker']} {r['label_jp']} (Sig: {r['signal']:.3f})\n")
                f.write("\n📉 **SELL (下位シグナル)**\n")
                for _, r in sell.iterrows():
                    f.write(f"・{r['ticker']} {r['label_jp']} (Sig: {r['signal']:.3f})\n")
    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()
