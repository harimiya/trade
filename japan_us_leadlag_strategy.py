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
        # GitHub ActionsでインストールされるNoto Sansのパス
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
JP_LABELS = {'1617.T':'食品','1618.T':'エネルギー','1619.T':'建設・資材','1620.T':'素材・化学','1621.T':'医薬品','1622.T':'自動車・輸送','1623.T':'鉄鋼・非鉄','1624.T':'機械','1625.T':'電機・精密','1626.T':'情報通信','1627.T':'電力・ガス','1628.T':'運輸・物流','1629.T':'商社・卸売','1630.T':'小売','1631.T':'銀行','1632.T':'金融(除銀行)','1633.T':'不動産'}

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
        if t in us_
