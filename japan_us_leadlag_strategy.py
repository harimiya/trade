import warnings
warnings.filterwarnings('ignore')

import matplotlib
# GUIのないサーバー環境で動作させるための設定
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.patches as mpatches
import platform, os
import numpy as np
import pandas as pd

# ── フォント設定（エラー回避用） ──────────────────────
def _setup_japanese_font():
    """GitHub Actions(Linux)環境でもエラーにならないようフォント設定を調整"""
    system = platform.system()
    candidates = []
    if system == 'Windows':
        candidates = ['Yu Gothic', 'Meiryo']
    elif system == 'Darwin':
        candidates = ['Hiragino Sans']
    else:
        # Linux環境用の一般的なフォント
        candidates = ['Noto Sans CJK JP', 'DejaVu Sans', 'Liberation Sans']

    available = {f.name for f in fm.fontManager.ttflist}
    for font in candidates:
        if font in available:
            matplotlib.rcParams['font.family'] = font
            break
    matplotlib.rcParams['axes.unicode_minus'] = False

_setup_japanese_font()

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

# ── ティッカー & ラベル定義 ──────────────────────
US_TICKERS = ['XLB','XLC','XLE','XLF','XLI','XLK','XLP','XLRE','XLU','XLV','XLY']
US_LABELS = {'XLB':'Materials\n(素材)','XLC':'Comm Svcs\n(通信)','XLE':'Energy\n(エネルギー)','XLF':'Financials\n(金融)','XLI':'Industrials\n(産業)','XLK':'Info Tech\n(IT)','XLP':'Cons Staples\n(生活必需品)','XLRE':'Real Estate\n(不動産)','XLU':'Utilities\n(公益)','XLV':'Health Care\n(ヘルスケア)','XLY':'Cons Discr\n(一般消費財)'}
JP_TICKERS = ['1617.T','1618.T','1619.T','1620.T','1621.T','1622.T','1623.T','1624.T','1625.T','1626.T','1627.T','1628.T','1629.T','1630.T','1631.T','1632.T','1633.T']
JP_LABELS = {'1617.T':'食品','1618.T':'エネルギー資源','1619.T':'建設・資材','1620.T':'素材・化学','1621.T':'医薬品','1622.T':'自動車・輸送機','1623.T':'鉄鋼・非鉄','1624.T':'機械','1625.T':'電機・精密','1626.T':'情報通信・サービス','1627.T':'電力・ガス','1628.T':'運輸・物流','1629.T':'商社・卸売','1630.T':'小売','1631.T':'銀行','1632.T':'金融(除く銀行)','1633.T':'不動産'}
US_CYCLICAL = ['XLB','XLE','XLF','XLRE']; US_DEFENSIVE = ['XLK','XLP','XLU','XLV']
JP_CYCLICAL = ['1618.T','1625.T','1629.T','1631.T']; JP_DEFENSIVE = ['1617.T','1621.T','1627.T','1630.T']

# ── ロジック部分 ──────────────────────
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
        elif t in us_d: sign_
