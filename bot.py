import datetime
import os
import sys
import time
import pandas as pd
import requests
import yfinance as yf

# Discord Webhook URL (環境変数から取得)
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")


def get_tse_symbols():
    """JPX公式から東証全銘柄（プライム・スタンダード・グロース）を取得"""
    print("--- 1. JPXから東証上場銘柄リストを取得中 ---")
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
    }

    try:
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        df = pd.read_excel(res.content)

        # 市場区分の判定 (表記ブレ対策)
        target_condition = df["市場・商品区分"].astype(str).str.contains(
            "プライム|スタンダード|グロース", na=False
        )
        filtered_df = df[target_condition]

        symbols = []
        name_map = {}
        market_map = {}

        for _, row in filtered_df.iterrows():
            # 銘柄コード（数字4桁または英数字組合せ）
            code_str = str(row["コード"]).strip()
            symbol = f"{code_str}.T"

            symbols.append(symbol)
            name_map[symbol] = str(row["銘柄名"])
            # 「プライム（内国株式）」等の表記から「（内国株式）」を削除
            market_clean = str(row["市場・商品区分"]).split("（")[0]
            market_map[symbol] = market_clean

        print(f"✅ 取得完了: 対象銘柄数 = {len(symbols)} 件")
        return symbols, name_map, market_map

    except Exception as e:
        print(f"❌ JPXからの銘柄リスト取得エラー: {e}")
        return [], {}, {}


def check_breakout(symbols, name_map, market_map, lookback_days=250):
    """
    直近1年間（250営業日）の高値更新を判定
    """
    print("--- 2. 株価データ取得および新高値判定開始 ---")
    breakout_list = []

    # 1バッチあたり300銘柄ずつ取得
    batch_size = 300
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
                    # 複数銘柄データ構造から該当銘柄データを抽出
                    if len(batch_symbols) == 1:
                        df = data
                    else:
                        if symbol not in data.columns.levels[0]:
                            continue
                        df = data[symbol]

                    df = df.dropna(subset=["High", "Close"])

                    # 1年間（250営業日）データに満たないIPO間もない銘柄等はスキップ
                    if len(df) < lookback_days:
                        continue

                    # 最新日（直近営業日）のデータ
                    latest_high = float(df["High"].iloc[-1])
                    latest_close = float(df["Close"].iloc[-1])

                    # 過去N日間の最高値（直近営業日を除いた過去データ）
                    past_max_high = float(
                        df["High"].iloc[-lookback_days:-1].max()
                    )

                    # 【条件】最新日の高値が直近250営業日の最高値を上回ったか
                    if latest_high > past_max_high:
                        breakout_list.append(
                            {
                                "code": symbol.replace(".T", ""),
                                "name": name_map.get(symbol, "不明"),
                                "market": market_map.get(symbol, "東証"),
                                "close": round(latest_close, 1),
                                "latest_high": round(latest_high, 1),
                                "prev_high": round(past_max_high, 1),
                            }
                        )

                except Exception:
                    continue

        except Exception as e:
            print(f"⚠️ バッチ処理エラー ({i}~): {e}")

        time.sleep(1)  # サーバー負荷軽減のためのウエイト

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
        # Discord Embeds制限 (最大25フィールド) に配慮
        fields = []
        for item in breakout_list[:25]:
            fields.append(
                {
                    "name": (
                        f"[{item['code']}] {item['name']}"
                        f" ({item['market']})"
                    ),
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
                "東証全市場の中で、過去250営業日の最高値を更新した銘柄一覧"
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
    symbols, name_map, market_map = get_tse_symbols()

    if not symbols:
        print("❌ 銘柄リストが空のため処理を中止します。")
        sys.exit(1)

    breakouts = check_breakout(
        symbols, name_map, market_map, lookback_days=250
    )
    send_discord_notification(breakouts)
