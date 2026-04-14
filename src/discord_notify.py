"""
discord_notify.py
Discord Webhook 通知モジュール
・シグナル検知時：銘柄名・エントリー・売却予定日を含むEmbed
・日次サマリー：スキャン結果の集計
・エラー通知：例外発生時の赤Embed
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))

# Discord Embed カラー
COLOR_BUY    = 0x27AE60   # 緑：買いシグナル
COLOR_GRAY   = 0x95A5A6   # 灰：サマリー（シグナルなし）
COLOR_RED    = 0xE74C3C   # 赤：エラー
COLOR_GOLD   = 0xF1C40F   # 金：サマリー（シグナルあり）


def _webhook_url() -> Optional[str]:
    url = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        logger.warning("DISCORD_WEBHOOK_URL not set — Discord通知をスキップ")
    return url or None


def _post(url: str, payload: dict) -> bool:
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code in (200, 204):
            return True
        logger.error(f"Discord HTTP {r.status_code}: {r.text[:200]}")
        return False
    except requests.RequestException as e:
        logger.error(f"Discord post error: {e}")
        return False


def _next_business_day(base: datetime) -> datetime:
    """翌営業日を返す（土→月、日→月）"""
    d = base + timedelta(days=1)
    while d.weekday() >= 5:   # 5=土, 6=日
        d += timedelta(days=1)
    return d


def _add_business_days(base: datetime, n: int) -> datetime:
    """base から n 営業日後の日付を返す"""
    d = base
    added = 0
    while added < n:
        d += timedelta(days=1)
        if d.weekday() < 5:
            added += 1
    return d


def notify_signals(signals: list[dict]) -> None:
    """
    シグナル1件ごとに Discord Embed を送信。
    戦略A（+20%利確 / -15%ロスカット / 最大60営業日）の情報を含む。
    10件ごとにバッチ送信（Discord上限）。
    """
    url = _webhook_url()
    if not url or not signals:
        return

    now_jst = datetime.now(JST)
    now_str = now_jst.strftime("%Y-%m-%d %H:%M JST")

    # 銘柄コードでグルーピング
    by_ticker: dict[str, list[dict]] = {}
    for s in signals:
        by_ticker.setdefault(s["ticker"], []).append(s)

    embeds = []
    for ticker, items in by_ticker.items():
        s0 = items[0]
        role_label = "🟣 自社上場" if s0.get("ticker_role") == "self" else "🔵 親会社"
        src_label  = {
            "nikoukei":    "📰 nikoukei",
            "nsearch":     "💰 nsearch(10億+)",
            "p_portal":    "🏛 調達ポータル",
            "meti":        "⚡ 経産省",
            "kkj":         "🏢 官公需",
            "mlit_kanto":  "🛣 国交省関東",
            "mod":         "⚔️ 防衛省",
            "mof":         "💴 財務省",
            "mlit_kyu":    "🌊 国交省九州",
            "mlit_chu":    "🏔 国交省中国",
            "nexco_e":     "🛣 NEXCO東日本",
            "nexco_c":     "🛣 NEXCO中日本",
            "nexco_w":     "🛣 NEXCO西日本",
            "tokyo":       "🗼 東京都",
            "osaka":       "🏯 大阪府",
            "ipa":         "🔐 IPA",
            "jrtt":        "🚅 JRTT",
            "ur":          "🏠 UR都市機構",
            "water":       "💧 水資源機構",
            "mlit_tohoku": "🌾 国交省東北",
        }.get(s0.get("source", ""), f"📋 {s0.get('source','')}")

        # ── 日付計算（戦略A）──────────────────────────────────
        # エントリー：翌営業日
        entry_dt  = _next_business_day(now_jst)
        entry_str = entry_dt.strftime("%Y/%m/%d（%a）").replace(
            "Mon","月").replace("Tue","火").replace("Wed","水").replace(
            "Thu","木").replace("Fri","金")

        # 売却予定日：最大60営業日後（強制決済日）
        max_exit_dt  = _add_business_days(entry_dt, 60)
        max_exit_str = max_exit_dt.strftime("%Y/%m/%d（%a）").replace(
            "Mon","月").replace("Tue","火").replace("Wed","水").replace(
            "Thu","木").replace("Fri","金")

        # TP目標価格メモ
        tp_note  = "エントリー価格 × 1.20 に達した時点で即日利確"
        sl_note  = "エントリー価格 × 0.85 を下回った時点で即日損切"

        # 案件リスト（最大5件）
        project_lines = []
        for i, item in enumerate(items[:5]):
            amt = f"\n　　落札金額: {item['amount']}円" if item.get("amount") else ""
            project_lines.append(
                f"**{i+1}.** {item['winner']}\n"
                f"　　{item['project_name'][:50]}\n"
                f"　　発注者: {item.get('client','不明')}{amt}\n"
                f"　　入札日: {item.get('bid_date','不明')}"
            )
        if len(items) > 5:
            project_lines.append(f"… 他 {len(items)-5} 件")

        embed = {
            "title": f"📡  公共入札落札銘柄の売買戦略",
            "description": (
                f"## {ticker}  {s0['company']}\n"
                + "\n\n".join(project_lines)
            ),
            "color": COLOR_BUY,
            "fields": [
                # 戦略情報
                {"name": "📌 戦略",
                 "value": "**戦略A（ベースライン）**\nTP +20% ／ SL -15% ／ 最大 60営業日",
                 "inline": False},
                # エントリー
                {"name": "🟢 エントリー",
                 "value": f"**{entry_str}** 寄り付き 成行買い",
                 "inline": True},
                # 売却予定日（強制決済）
                {"name": "📅 売却予定日（最大保有）",
                 "value": f"**{max_exit_str}**",
                 "inline": True},
                {"name": "\u200b", "value": "\u200b", "inline": True},
                # 利確・損切
                {"name": "✅ 利確条件",
                 "value": tp_note,
                 "inline": False},
                {"name": "🛑 損切条件",
                 "value": sl_note,
                 "inline": False},
                # メタ情報
                {"name": "ロール",   "value": role_label, "inline": True},
                {"name": "ソース",   "value": src_label,  "inline": True},
                {"name": "検知件数", "value": f"{len(items)} 件", "inline": True},
            ],
            "footer": {
                "text": (
                    f"入札落札シグナル  •  検知日時: {now_str}  "
                    "•  ※投資判断は自己責任でお願いします"
                )
            },
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        embeds.append(embed)

    # 10件ずつ送信
    for i in range(0, len(embeds), 10):
        chunk = embeds[i: i + 10]
        ok = _post(url, {
            "username":   "入札シグナル Bot",
            "avatar_url": "https://cdn.jsdelivr.net/npm/twemoji@14.0.2/assets/72x72/1f4e1.png",
            "embeds":     chunk,
        })
        if ok:
            logger.info(f"Discord: sent {len(chunk)} signal embeds (batch {i//10+1})")


def notify_summary(
    signals: list[dict],
    nikoukei_pages: int,
    nsearch_items:  int,
) -> None:
    """毎日のスキャン完了サマリー（シグナルなしでも送信）"""
    url = _webhook_url()
    if not url:
        return

    now_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M JST")
    self_cnt   = sum(1 for s in signals if s.get("ticker_role") == "self")
    parent_cnt = sum(1 for s in signals if s.get("ticker_role") == "parent")

    if signals:
        tickers    = list(dict.fromkeys(s["ticker"] for s in signals))
        ticker_str = "  ".join(f"`{t}`" for t in tickers[:10])
        if len(tickers) > 10:
            ticker_str += f"  … 他{len(tickers)-10}銘柄"
        color       = COLOR_GOLD
        description = f"**{len(signals)} 件のシグナルを検知しました**\n\n{ticker_str}"
    else:
        color       = COLOR_GRAY
        description = "本日は新規シグナルはありませんでした。"

    _post(url, {
        "username":   "入札シグナル Bot",
        "avatar_url": "https://cdn.jsdelivr.net/npm/twemoji@14.0.2/assets/72x72/1f4e1.png",
        "embeds": [{
            "title":       f"📋  本日のスキャン完了  —  {now_str}",
            "description": description,
            "color":       color,
            "fields": [
                {"name": "nikoukei スキャン", "value": f"{nikoukei_pages} ページ", "inline": True},
                {"name": "nsearch 取得",      "value": f"{nsearch_items} 件",       "inline": True},
                {"name": "\u200b",            "value": "\u200b",                    "inline": True},
                {"name": "シグナル総数",       "value": f"{len(signals)} 件",        "inline": True},
                {"name": "自社上場",           "value": f"{self_cnt} 件",            "inline": True},
                {"name": "親会社",             "value": f"{parent_cnt} 件",          "inline": True},
            ],
            "footer": {"text": "戦略A: TP+20% / SL-15% / 最大60営業日"},
        }],
    })
    logger.info("Discord: summary sent")


def notify_error(message: str, detail: str = "") -> None:
    """エラー発生時の赤Embed通知"""
    url = _webhook_url()
    if not url:
        return

    desc = message
    if detail:
        desc += f"\n```\n{detail[:800]}\n```"

    _post(url, {
        "username": "入札シグナル Bot",
        "embeds": [{
            "title":       "⚠️  スクレイパー エラー",
            "description": desc,
            "color":       COLOR_RED,
            "footer":      {"text": datetime.now(JST).strftime("%Y-%m-%d %H:%M JST")},
        }],
    })
