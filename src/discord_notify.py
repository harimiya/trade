"""
discord_notify.py
─────────────────────────────────────────────────────────────────────────────
Discord Webhook 通知モジュール

使い方:
    from discord_notify import notify_signals, notify_error, notify_summary
─────────────────────────────────────────────────────────────────────────────
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

# ─────────────────────────────────────────────
# Discord の Embed カラー定数
# ─────────────────────────────────────────────
COLOR_GREEN  = 0x27AE60   # 利確・勝ち
COLOR_RED    = 0xE74C3C   # ロスカット・エラー
COLOR_BLUE   = 0x2980B9   # 親会社シグナル
COLOR_PURPLE = 0x8E44AD   # 自社上場シグナル
COLOR_ORANGE = 0xE67E22   # nsearch（10億+）
COLOR_GRAY   = 0x95A5A6   # 情報


def _post(webhook_url: str, payload: dict) -> bool:
    """Discord Webhook に POST する共通関数"""
    try:
        resp = requests.post(
            webhook_url,
            json=payload,
            timeout=15,
            headers={"Content-Type": "application/json"},
        )
        # Discord は 204 No Content を返す
        if resp.status_code in (200, 204):
            return True
        logger.error(f"Discord returned {resp.status_code}: {resp.text[:200]}")
        return False
    except requests.RequestException as e:
        logger.error(f"Discord post failed: {e}")
        return False


def _webhook_url() -> Optional[str]:
    url = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        logger.warning("DISCORD_WEBHOOK_URL not set — skipping notification")
    return url or None


# ─────────────────────────────────────────────
# シグナル通知（メイン）
# ─────────────────────────────────────────────

def notify_signals(signals: list[dict]) -> None:
    """
    シグナルを銘柄ごとにまとめて Discord Embed で通知。
    銘柄数が多い場合は複数メッセージに分割（Discord 上限 10 Embed / メッセージ）。
    """
    url = _webhook_url()
    if not url or not signals:
        return

    now_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M JST")

    # 銘柄コードでグルーピング
    by_ticker: dict[str, list[dict]] = {}
    for s in signals:
        by_ticker.setdefault(s["ticker"], []).append(s)

    embeds = []
    for ticker, items in by_ticker.items():
        s0   = items[0]
        role = s0.get("ticker_role", "parent")

        color = COLOR_PURPLE if role == "self" else COLOR_BLUE
        if s0.get("source") == "nsearch":
            color = COLOR_ORANGE

        role_label = "🟣 自社上場" if role == "self" else "🔵 親会社"
        src_label  = "📰 nikoukei" if s0.get("source") == "nikoukei" else "💰 nsearch(10億+)"

        # 案件リスト（最大5件まで表示）
        project_lines = []
        for i, item in enumerate(items[:5]):
            amt = f"　落札金額: {item['amount']}円" if item.get("amount") else ""
            project_lines.append(
                f"**{i+1}.** {item['winner']}\n"
                f"　{item['project_name'][:45]}\n"
                f"　発注者: {item.get('client','不明')}{amt}\n"
                f"　入札日: {item.get('bid_date','不明')}"
            )
        if len(items) > 5:
            project_lines.append(f"… 他 {len(items)-5} 件")

        embed = {
            "title": f"🔔  {ticker}  {s0['company']}",
            "description": "\n\n".join(project_lines),
            "color": color,
            "fields": [
                {"name": "ロール",      "value": role_label,  "inline": True},
                {"name": "ソース",      "value": src_label,   "inline": True},
                {"name": "検知案件数",  "value": f"{len(items)} 件", "inline": True},
                {"name": "エントリー",  "value": "翌営業日 寄り付き 成行買い", "inline": False},
                {"name": "出口ルール",  "value": "+20% 利確　/　-15% ロスカット　/　最大 60 日", "inline": False},
            ],
            "footer": {"text": f"入札落札シグナル  •  {now_str}"},
        }
        embeds.append(embed)

    # 10 Embed ずつ送信（Discord の上限）
    for i in range(0, len(embeds), 10):
        chunk = embeds[i : i + 10]
        payload = {
            "username": "入札シグナル Bot",
            "avatar_url": "https://cdn.jsdelivr.net/npm/twemoji@14.0.2/assets/72x72/1f4e1.png",
            "embeds": chunk,
        }
        ok = _post(url, payload)
        if ok:
            logger.info(f"Discord: sent {len(chunk)} embeds (batch {i//10 + 1})")


# ─────────────────────────────────────────────
# 日次サマリー通知
# ─────────────────────────────────────────────

def notify_summary(signals: list[dict], nikoukei_pages: int, nsearch_new: int) -> None:
    """
    スキャン完了サマリーを Discord に送る（シグナルがなかった日も含む）。
    """
    url = _webhook_url()
    if not url:
        return

    now_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M JST")
    self_cnt   = sum(1 for s in signals if s.get("ticker_role") == "self")
    parent_cnt = sum(1 for s in signals if s.get("ticker_role") == "parent")
    nik_cnt    = sum(1 for s in signals if s.get("source") == "nikoukei")
    nsr_cnt    = sum(1 for s in signals if s.get("source") == "nsearch")

    if signals:
        tickers = list(dict.fromkeys(s["ticker"] for s in signals))
        ticker_str = "  ".join(f"`{t}`" for t in tickers[:10])
        if len(tickers) > 10:
            ticker_str += f"  … 他{len(tickers)-10}銘柄"
        color       = COLOR_GREEN
        description = f"**{len(signals)} 件のシグナルを検知しました**\n\n{ticker_str}"
    else:
        color       = COLOR_GRAY
        description = "本日は新規シグナルはありませんでした。"

    payload = {
        "username": "入札シグナル Bot",
        "avatar_url": "https://cdn.jsdelivr.net/npm/twemoji@14.0.2/assets/72x72/1f4e1.png",
        "embeds": [{
            "title": f"📋  本日のスキャン完了  —  {now_str}",
            "description": description,
            "color": color,
            "fields": [
                {"name": "nikoukei スキャン",  "value": f"{nikoukei_pages} ページ", "inline": True},
                {"name": "nsearch 新規取得",   "value": f"{nsearch_new} 件",       "inline": True},
                {"name": "\u200b",             "value": "\u200b",                  "inline": True},
                {"name": "シグナル総数",        "value": f"{len(signals)} 件",      "inline": True},
                {"name": "自社上場",            "value": f"{self_cnt} 件",          "inline": True},
                {"name": "親会社",              "value": f"{parent_cnt} 件",        "inline": True},
            ],
            "footer": {"text": "入札落札シグナル  •  nikoukei.co.jp + nsearch.jp"},
        }],
    }
    _post(url, payload)
    logger.info("Discord: summary sent")


# ─────────────────────────────────────────────
# エラー通知
# ─────────────────────────────────────────────

def notify_error(message: str, detail: str = "") -> None:
    """スクレイパーの重大エラーを Discord に通知"""
    url = _webhook_url()
    if not url:
        return

    now_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M JST")
    desc = message
    if detail:
        desc += f"\n```\n{detail[:800]}\n```"

    payload = {
        "username": "入札シグナル Bot",
        "embeds": [{
            "title": "⚠️  スクレイパー エラー",
            "description": desc,
            "color": COLOR_RED,
            "footer": {"text": now_str},
        }],
    }
    _post(url, payload)
