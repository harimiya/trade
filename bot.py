import datetime
import os
import time
import pandas as pd
import requests
import yfinance as yf

# Discord Webhook URL (環境変数から取得)
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")


def get_tse_symbols():
    """JPX公式から東証全銘柄のコードと市場区分を取得"""
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    try:
        df = pd.read_excel(url)
        target_markets = [
            "プライム（内国株式）",
            "スタンダード（内国株式）",
            "グロース（内国株式）",
        ]
        filtered_df = df[df["市場・商品区分"].isin(target_markets)]

        symbols = [f"{code}.T" for code in filtered_df["コード"]]
        name_map = dict(
            zip([f"{code}.T" for code in filtered_df["コード"]], filtered_df["銘柄名"])
        )
        market_map = dict(
            zip(
                [f"{code}.T" for code in filtered_df["コード"]],
                filtered_df["市場・商品区分"],
            )
        )
        return symbols, name_map, market_map
    except Exception as e:
        print(f"銘柄リスト取得エラー: {e}")
        return [], {}, {}


def check_breakout(symbols, name_map, market_map, lookback_days=250):
    """ブレイクアウト判定 (バッチ処理)"""
    breakout_list = []
    batch_size = 300
    total = len(symbols)

    print(f"全{total}銘柄の検証を開始します...")

    for i in range(0, total, batch_size):
        batch_symbols = symbols[i : i + batch_size]
        print(f"Processing {i} to {min(i + batch_size, total)}...")

        try:
            data = yf.download(
                batch_symbols, period="1y", group_by="ticker", progress=False
            )

            for symbol in batch_symbols:
                try:
                    if symbol not in data or data[symbol].empty:
                        continue

                    df = data[symbol].dropna()
                    if len(df) < lookback_days:
                        continue

                    latest_close = df["Close"].iloc[-1]
                    latest_high = df["High"].iloc[-1]
                    past_max_high = df["High"].iloc[-lookback_days:-1].max()

                    if latest_high > past_max_high:
                        breakout_list.append(
                            {
                                "code": symbol.replace(".T", ""),
                                "name": name_map.get(symbol, ""),
                                "market": market_map.get(
                                    symbol, ""
                                ).replace("（内国株式）", ""),
                                "close": round(latest_close, 1),
                                "prev_high": round(past_max_high, 1),
                            }
                        )
                except Exception:
                    continue
        except Exception as e:
            print(f"Batch processing error: {e}")

        time.sleep(1)

    return breakout_list


def send_discord_notification(breakout_list):
    """Discordへ結果を送信"""
    if not DISCORD_WEBHOOK_URL:
        print("DISCORD_WEBHOOK_URL が設定されていません。")
        return

    today_str = datetime.date.today().strftime("%Y-%m-%d")

    if not breakout_list:
        payload = {
            "content": f"【新高値ブレイクアウト bot】\n**{today_str}**: 該当する銘柄はありませんでした。"
        }
    else:
        # Discord用の埋め込み（Embed）形式で作成
        fields = []
        for item in breakout_list[:25]:  # Discord Embed制限考慮（最大25件まで）
            fields.append(
                {
                    "name": f"[{item['code']}] {item['name']} ({item['market']})",
                    "value": f"終値: `{item['close']}円` (過去高値: `{item['prev_high']}円`)",
                    "inline": True,
                }
            )

        embed = {
            "title": f"🚀 【{today_str}】新高値ブレイクアウト通知",
            "description": f"過去52週（250営業日）高値を更新した銘柄（該当 {len(breakout_list)} 件）",
            "color": 3066993,  # 緑系カラー
            "fields": fields,
        }

        if len(breakout_list) > 25:
            embed["footer"] = {
                "text": f"※表示上限のため他 {len(breakout_list) - 25} 銘柄を省略しています。"
            }

        payload = {"embeds": [embed]}

    requests.post(DISCORD_WEBHOOK_URL, json=payload)


if __name__ == "__main__":
    symbols, name_map, market_map = get_tse_symbols()
    if symbols:
        breakouts = check_breakout(symbols, name_map, market_map, lookback_days=250)
        send_discord_notification(breakouts)
