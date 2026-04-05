import os
# ...（既存のインポート）
from flask import Flask, request # Flaskを追加

app = Flask(__name__) # Flaskアプリの定義

# --- 既存の関数（calculate_signal, create_plotなど）はそのまま ---

@app.route("/", methods=["GET", "POST"]) # ルートへのアクセスを受け付ける
def run_main(req): # Cloud Runから呼ばれる関数
    try:
        signals = calculate_signal()
        img_path = create_plot(signals)
        
        # Discord送信
        with open(img_path, 'rb') as f:
            files = {'file': ('signal.png', f, 'image/png')}
            payload = {'content': '📊 **本日の日米リードラグ・予測シグナル**'}
            requests.post(DISCORD_WEBHOOK_URL, data=payload, files=files)
            
        return 'Success', 200
    except Exception as e:
        print(f"Error: {e}") # ログに出力
        return str(e), 500

if __name__ == "__main__":
    try:
        # メイン処理を呼び出す
        signals = calculate_signal()
        img_path = create_plot(signals)
        
        with open(img_path, 'rb') as f:
            files = {'file': ('signal.png', f, 'image/png')}
            payload = {'content': '📊 **本日の日米リードラグ・予測シグナル**'}
            requests.post(DISCORD_WEBHOOK_URL, data=payload, files=files)
        print("Successfully sent to Discord")
    except Exception as e:
        print(f"Error occurred: {e}")
        exit(1) # エラーが出たらGitHub側に失敗を通知
