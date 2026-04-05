import matplotlib
# GUIのない環境(GitHub Actions)でのエラー防止
matplotlib.use('Agg') 

import warnings
warnings.filterwarnings('ignore')
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.patches as mpatches
import platform, os
import numpy as np
import pandas as pd
import yfinance as yf

# ── 日本語フォント設定 ──────────────────────
def _setup_japanese_font():
    """Linux(GitHub Actions)環境でフォントを確実に読み込む"""
    system = platform.system()
    font_path = None
    
    if system == 'Linux':
        # GitHub ActionsでインストールされるNoto Sansのパス候補
        paths = [
            '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc',
            '/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc',
            '/usr/share/fonts/fonts-noto-cjk/NotoSansCJK-Regular.ttc'
        ]
        for p in paths:
            if os.path.exists(p):
                font_path = p
                break
    
    if font_path:
        prop = fm.FontProperties(fname=font_path)
        matplotlib.rcParams['font.family'] = prop.get_name()
    elif system == 'Windows':
        matplotlib.rcParams['font.family'] = 'MS Gothic'
    elif system == 'Darwin':
        matplotlib.rcParams['font.family'] = 'Hiragino Sans'
        
    matplotlib.rcParams['axes.unicode_minus'] = False

_setup_japanese_font()

# ── 定義 ──────────────────────
US_TICKERS = ['XLB','XLC','XLE','XLF','XLI','XLK','XLP','XLRE','XLU','XLV','XLY']
US_LABELS = {'XLB':'素材','XLC':'通信','XLE':'エネルギー','XLF':'金融','XLI':'産業','XLK':'IT','XLP':'生活必需品','XLRE':'不動産','XLU':'公益','XLV':'ヘルスケア','XLY':'一般消費財'}
JP_TICKERS = ['1617.T','1618.T','1619.T','1620.T','1621.T','1622.T','1623.T','1624.T','1625.T','1626.T','1627.T','1628.T','1629.T','1630.T','1631.T','1632.T','1633.T']
JP_LABELS = {'1617.T':'食品','1618.T':'エネルギー資源','1619.T':'建設・資材','1620.T':'素材・化学','1621.T':'医薬品','1622.T':'自動車・輸送機','1623.T':'鉄鋼・非鉄','1624.T':'機械','1625.T':'電機・精密','1626.T':'情報通信・サービス','1627.T':'電力・ガス','1628.T':'運輸・物流','1629.T':'商社・卸売','1630.T':'小売','1631.T':'銀行','1632.T':'金融(除銀行)','1633.T':'不動産'}

US_CYCLICAL=['XLB','XLE','XLF','XLRE']; US_DEFENSIVE=['XLK','XLP','XLU','XLV']
JP_CYCLICAL=['1618.T','1625.T','1629.T','1631.T']; JP_DEFENSIVE=['1617.T','1621.T','1627.T','1630.T']

# ── ロジック ──────────────────────
def build_prior_subspace(us_tickers, jp_tickers, us_c, us_d, jp_c, jp_d):
    N_U, N_J = len(us_tickers), len(jp_tickers)
    N = N_U + N_J
    v1 = np.ones(N) / np.sqrt(N)
    v2_raw = np.concatenate([np.ones(N_U)/N_U, -np.ones(N_J)/N_J])
    v2_raw -= v2_raw.dot(v1) * v1
    v2 = v2_raw / np.linalg.norm(v2_raw)
    
    sign_vec = np.zeros(N)
    for i, t in enumerate(us_tickers):
        if t in us_c: sign_vec[i] = 1.0
        elif t in us_d: sign_vec[i] = -1.0
    for j, t in enumerate(jp_tickers):
        idx = N_U + j
        if t in jp_c: sign_vec[idx] = 1.0
        elif t in jp_d: sign_vec[idx] = -1.0
        
    v3_raw = sign_vec - sign_vec.dot(v1)*v1 - sign_vec.dot(v2)*v2
    v3 = v3_raw / np.linalg.norm(v3_raw) if np.linalg.norm(v3_raw) > 1e-10 else v3_raw
    return np.column_stack([v1, v2, v3])

def compute_signal(p_us, p_jp, L=60, K=3, lam=0.9):
    us_a = [t for t in US_TICKERS if t in p_us.columns]
    jp_a = [t for t in JP_TICKERS if t in p_jp.columns]
    
    ret_u = p_us[us_a].pct_change().dropna()
    ret_j = p_jp[jp_a].pct_change().dropna()
    common = ret_u.index.intersection(ret_j.index)
    
    df_z = pd.concat([ret_u.loc[common], ret_j.loc[common]], axis=1).tail(L)
    Z = ((df_z - df_z.mean()) / df_z.std(ddof=0).replace(0, 1e-8)).values
    Ct = np.nan_to_num(np.corrcoef(Z.T), nan=0.0)
    
    V0 = build_prior_subspace(us_a, jp_a, US_CYCLICAL, US_DEFENSIVE, JP_CYCLICAL, JP_DEFENSIVE)
    C_reg = (1 - lam) * Ct + lam * (V0 @ V0.T)
    
    vals, vecs = np.linalg.eigh(C_reg)
    Vt_K = vecs[:, np.argsort(vals)[::-1][:K]]
    
    V_U, V_J = Vt_K[:len(us_a), :], Vt_K[len(us_a):, :]
    z_U_today = ((ret_u.iloc[-1] - ret_u.tail(L).mean()) / ret_u.tail(L).std(ddof=0).replace(0,1e-8)).values
    
    f_t = V_U.T @ z_U_today
    z_hat_J = V_J @ f_t
    
    sig_df = pd.DataFrame({'ticker':jp_a, 'label':[JP_LABELS.get(t,t) for t in jp_a], 'signal':z_hat_J}).set_index('ticker')
    return sig_df, f_t, ret_u.iloc[-1].to_dict(), us_a

def main():
    print("Fetching data from Yahoo Finance...")
    p_u = yf.download(US_TICKERS, period='1y')['Close']
    p_j = yf.download(JP_TICKERS, period='1y')['Close']
    
    sig_df, f_t, us_ret, us_a = compute_signal(p_u, p_j)

    # ── ダッシュボード描画 ──
    fig = plt.figure(figsize=(20, 16), facecolor='#0d1117')
    gs = fig.add_gridspec(3, 2, hspace=0.45, wspace=0.3)
    
    BG, GRID = '#161b22', '#21262d'
    BUY_C, SELL_C = '#2ea043', '#da3633'

    # [1] US Returns
    ax1 = fig.add_subplot(gs[0, :], facecolor=BG)
    vals1 = [us_ret.get(t,0)*100 for t in us_a]
    bars1 = ax1.bar(range(len(us_a)), vals1, color=[BUY_C if v>=0 else SELL_C for v in vals1])
    ax1.set_title('[US] 米国セクターETF 当日 Close-to-Close リターン', color='white', fontsize=15, fontweight='bold', pad=20)
    ax1.set_xticks(range(len(us_a)))
    ax1.set_xticklabels([US_LABELS[t] for t in us_a], color='#8b949e', rotation=25)
    ax1.tick_params(colors='#8b949e')
    ax1.yaxis.grid(True, color=GRID, linestyle='--')
    for bar, val in zip(bars1, vals1):
        ax1.text(bar.get_x()+bar.get_width()/2, val + (0.05 if val>=0 else -0.15), f'{val:+.2f}%', ha='center', color='white', fontsize=9, fontweight='bold')

    # [2] JP Signals
    ax2 = fig.add_subplot(gs[1, :], facecolor=BG)
    df_s = sig_df.sort_values('signal', ascending=False)
    bars2 = ax2.bar(range(len(df_s)), df_s['signal'], color=[BUY_C if v>=0 else SELL_C for v in df_s['signal']])
    ax2.set_title('[JP] 日本セクターETF 翌日予測シグナル (PCA SUB リードラグ)', color='white', fontsize=15, fontweight='bold', pad=20)
    ax2.set_xticks(range(len(df_s)))
    ax2.set_xticklabels(df_s['label'], color='#8b949e', rotation=30)
    ax2.tick_params(colors='#8b949e')
    ax2.yaxis.grid(True, color=GRID, linestyle='--')
    for bar, val in zip(bars2, df_s['signal']):
        ax2.text(bar.get_x()+bar.get_width()/2, val + (0.002 if val>=0 else -0.006), f'{val:+.3f}', ha='center', color='white', fontsize=9, fontweight='bold')

    # [3] Factor Scores
    ax3 = fig.add_subplot(gs[2, 0], facecolor=BG)
    f_names = ['Global\n(グローバル)', 'Country Spread\n(国スプレッド)', 'Cyclical/Def\n(シクリカル)']
    ax3.bar(f_names, f_t, color=[BUY_C if v>=0 else SELL_C for v in f_t])
    ax3.set_title('共通ファクタースコア f_t', color='white', fontsize=14, fontweight='bold')
    ax3.tick_params(colors='#8b949e')
    ax3.axhline(0, color='#484f58', linewidth=1)

    # [4] Trading Summary (証券コードリスト完全再現)
    ax4 = fig.add_subplot(gs[2, 1], facecolor=BG)
    ax4.axis('off')
    buy_list = df_s[df_s['signal'] > 0].sort_values('signal', ascending=False).head(6)
    sell_list = df_s[df_s['signal'] < 0].sort_values('signal').head(6)
    
    y_pos = 0.95
    ax4.text(0.05, y_pos, "[BUY] 翌日寄り付き買い推奨", color='#3fb950', fontweight='bold', fontsize=14)
    y_pos -= 0.09
    for tk, row in buy_list.iterrows():
        # 日本語フォントを適用するため特定のフォントファミリーを指定しない形式で描画
        ax4.text(0.05, y_pos, f"{tk:7} {row['label']:16}", color='white', fontsize=11)
        ax4.text(0.75, y_pos, f"sig={row['signal']:+.4f}", color='#58a6ff', fontsize=11)
        y_pos -= 0.07
    
    y_pos -= 0.04
    ax4.text(0.05, y_pos, "[SELL] 翌日空売り推奨", color='#f85149', fontweight='bold', fontsize=14)
    y_pos -= 0.09
    for tk, row in sell_list.iterrows():
        ax4.text(0.05, y_pos, f"{tk:7} {row['label']:16}", color='white', fontsize=11)
        ax4.text(0.75, y_pos, f"sig={row['signal']:+.4f}", color='#f85149', fontsize=11)
        y_pos -= 0.07

    today_str = pd.Timestamp.today().strftime('%Y-%m-%d')
    plt.suptitle(f'日米業種リードラグ投資戦略  部分空間正則化PCA\nシグナル生成日: {today_str} → 翌営業日執行', color='white', fontsize=18, fontweight='bold', y=0.97)
    
    plt.savefig('leadlag_signal_dashboard.png', facecolor='#0d1117', bbox_inches='tight', dpi=150)
    print("Dashboard saved: leadlag_signal_dashboard.png")

if __name__ == '__main__':
    main()
