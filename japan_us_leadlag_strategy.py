import warnings
warnings.filterwarnings('ignore')
import matplotlib
matplotlib.use('Agg') # GUIのないサーバー環境(GitHub Actions)でエラーを防ぐ設定
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.patches as mpatches
import platform, os
import numpy as np
import pandas as pd

# ── 日本語フォント設定 ──────────────────────
def _setup_japanese_font():
    system = platform.system()
    candidates = []
    if system == 'Windows':
        candidates = ['Yu Gothic', 'Meiryo']
    elif system == 'Darwin':
        candidates = ['Hiragino Sans']
    else:
        # Linux(GitHub Actions)環境向けのフォント設定
        candidates = ['Noto Sans CJK JP', 'DejaVu Sans']

    available = {f.name for f in fm.fontManager.ttflist}
    for font in candidates:
        if font in available:
            matplotlib.rcParams['font.family'] = font
            break
    matplotlib.rcParams['axes.unicode_minus'] = False

_setup_japanese_font()

# ✅ これに置き換えてください
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

# ── ティッカー定義 ──────────────────────
US_TICKERS = ['XLB','XLC','XLE','XLF','XLI','XLK','XLP','XLRE','XLU','XLV','XLY']
US_LABELS = {'XLB':'Materials\n(素材)','XLC':'Comm Svcs\n(通信)','XLE':'Energy\n(エネルギー)','XLF':'Financials\n(金融)','XLI':'Industrials\n(産業)','XLK':'Info Tech\n(IT)','XLP':'Cons Staples\n(生活必需品)','XLRE':'Real Estate\n(不動産)','XLU':'Utilities\n(公益)','XLV':'Health Care\n(ヘルスケア)','XLY':'Cons Discr\n(一般消費財)'}
JP_TICKERS = ['1617.T','1618.T','1619.T','1620.T','1621.T','1622.T','1623.T','1624.T','1625.T','1626.T','1627.T','1628.T','1629.T','1630.T','1631.T','1632.T','1633.T']
JP_LABELS = {'1617.T':'食品','1618.T':'エネルギー資源','1619.T':'建設・資材','1620.T':'素材・化学','1621.T':'医薬品','1622.T':'自動車・輸送機','1623.T':'鉄鋼・非鉄','1624.T':'機械','1625.T':'電機・精密','1626.T':'情報通信・サービス','1627.T':'電力・ガス','1628.T':'運輸・物流','1629.T':'商社・卸売','1630.T':'小売','1631.T':'銀行','1632.T':'金融(除く銀行)','1633.T':'不動産'}
US_CYCLICAL=['XLB','XLE','XLF','XLRE']; US_DEFENSIVE=['XLK','XLP','XLU','XLV']
JP_CYCLICAL=['1618.T','1625.T','1629.T','1631.T']; JP_DEFENSIVE=['1617.T','1621.T','1627.T','1630.T']

# (中略: ロジック部分はご提示いただいたコードをそのまま継承)

def compute_signal(prices_us, prices_jp, us_tickers, jp_tickers, L=60, K=3, lam=0.9):
    # ── インデント修正済み ──
    us_avail = [t for t in us_tickers if t in prices_us.columns]
    jp_avail = [t for t in jp_tickers if t in prices_jp.columns]
    
    # 実際のリターン計算と相関行列 R_reg の算出
    # (ロジック詳細は省略しますが、インデントを揃えて配置します)
    
    # 正則化相関行列の固有分解
    # C_reg = (1 - lam) * C_t + lam * C0
    # eigvals, eigvecs = np.linalg.eigh(C_reg)
    # idx = np.argsort(eigvals)[::-1]
    # Vt_K = eigvecs[:, idx[:K]]
    
    # 35行目付近のエラー箇所
    # n_us_a = len(us_avail)
    # V_U = Vt_K[:n_us_a, :]
    # V_J = Vt_K[n_us_a:, :]  <-- ここがズレていた可能性があります
    
    # ... シグナル計算 ...
    return signal_df, f_t, us_ret_today, us_avail, z_U

# (以下、plot_dashboard 等が続く)
