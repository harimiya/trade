import datetime
import os
import sys
import time
import pandas as pd
import requests
import yfinance as yf

# Discord Webhook URL (環境変数から取得)
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")


def generate_tse_symbols():
    """
    東証の銘柄コード（4桁数字＋末尾.T）を動的に生成。
    JPXの外部ファイルに依存しないため、サイト改変で止まることがありません。
    """
    print("--- 1. 東証銘柄リストを生成中 ---")
    symbols = [f"{code}.T" for code in range(1300, 9999)]
    print(f"✅ 生成完了: 照会対象スキャン範囲 = {len(symbols)} 件")
    return symbols


def check_breakout(symbols, lookback_days=250):
    """
    直近1年間（250営業日）の高値更新を判定
    """
    print("--- 2. 株価データ取得および新高値判定開始 ---")
    breakout_list = []

    # 1バッチあたり400銘柄ずつ取得（処理高速化）
    batch_size = 400
    total = len(symbols)

    for i in range(0, total, batch_size):
        batch_symbols = symbols[i : i + batch_size]
        print(
            f"Processing: {i+1}〜{min(i + batch_size, total)} / Total {total}..."
        )

        try:
            # 過去1年分(1y)のデータを一括取得
            data = yf.download(
                batch_symbols,
                period="1y",
                group_by="ticker",
                progress=False,
                threads=True,
            )

            for symbol in batch_symbols:
                try:
                    # 銘柄データ抽出
                    if len(batch_symbols) == 1:
                        df = data
                    else:
                        if symbol not in data.columns.levels[0]:
                            continue
                        df = data[symbol]

                    df = df.dropna(subset=["High", "Close"])

                    # データ数が不足している場合や上場廃止・存在しないコードはスキップ
                    if len(df) < lookback_days:
                        continue

                    # 最新日（直近営業日）のデータ
                    latest_high = float(df["High"].iloc[-1])
                    latest_close = float(df["Close"].iloc[-1])

                    # 過去N日間の最高値（直近営業日を除いた過去データ）
                    past_max_high = float(
                        df["High"].iloc[-lookback_days:-1].max()
                    )

                    # 【ブレイクアウト判定】最新日の高値が直近250営業日の最高値を上回ったか
                    if latest_high > past_max_high and past_max_high > 0:
                        code_str = symbol.replace(".T", "")
                        breakout_list.append(
                            {
                                "code": code_str,
                                "close": round(latest_close, 1),
                                "latest_high": round(latest_high, 1),
                                "prev_high": round(past_max_high, 1),
                            }
                        )

                except Exception:
                    continue

        except Exception as e:
            print(f"⚠️ バッチ処理エラー ({i}~): {e}")

        time.sleep(0.5)

    print(f"✅ 判定完了: 新高値更新銘柄数 = {len(breakout_list)} 件")
    return breakout_list


def send_discord_notification(breakout_list):
    """Discord Webhookに判定結果を通知"""
    print("--- 3. Discord通知処理 ---")
    if not DISCORD_WEBHOOK_URL:
        print("❌ DISCORD_WEBHOOK_URL が環境変数に設定されていません。")
        sys.exit(1)

    today_str = datetime.date.today().strftime("%Y-%m-%d")

    if not breakout_list:
        payload = {
            "content": (
                f"【直近1年高値ブレイクアウト Bot】\n**{today_str}**:"
                " 本日、1年高値を更新した該当銘柄はありませんでした。"
            )
        }
    else:
        fields = []
        for item in breakout_list[:25]:  # Discord表示制限（上限25件）
            fields.append(
                {
                    "name": f"銘柄コード: [{item['code']}]",
                    "value": (
                        f"終値: `{item['close']}円` | 本日高値:"
                        f" `{item['latest_high']}円`\n(過去250日高値:"
                        f" `{item['prev_high']}円`)"
                    ),
                    "inline": True,
                }
            )

        embed = {
            "title": f"🚀 【{today_str}】直近1年高値ブレイクアウト通知",
            "description": (
                "東証上場銘柄の中で、過去250営業日の最高値を更新した銘柄一覧"
                f" (該当: 全 {len(breakout_list)} 銘柄)"
            ),
            "color": 3066993,  # エメラルドグリーン
            "fields": fields,
        }

        if len(breakout_list) > 25:
            embed["footer"] = {
                "text": (
                    "※Discord表示上限のため、全"
                    f" {len(breakout_list)} 銘柄中 上位25件のみ表示しています。"
                )
            }

        payload = {"embeds": [embed]}

    res = requests.post(DISCORD_WEBHOOK_URL, json=payload)
    if res.status_code in [200, 204]:
        print("✅ Discordへ正常に通知を送信しました。")
    else:
        print(
            f"❌ Discord送信失敗 (Status Code: {res.status_code}): {res.text}"
        )


if __name__ == "__main__":
    symbols = generate_tse_symbols()
    breakouts = check_breakout(symbols, lookback_days=250)
    send_discord_notification(breakouts)
