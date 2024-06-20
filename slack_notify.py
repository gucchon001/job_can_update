import requests
import configparser
import traceback

# 設定ファイルから設定を読み込む関数
def load_settings(filename="settings.ini"):
    config = configparser.ConfigParser()
    config.read(filename, encoding="utf-8")
    return config

# ステータスコードに応じたエラーメッセージを返す関数
def get_error_message(status_code):
    if status_code == 400:
        return "不正なリクエストです。詳細はログファイルをご覧ください。"
    elif status_code == 401:
        return "認証に失敗しました。詳細はログファイルをご覧ください。"
    elif status_code == 403:
        return "アクセス権限がありません。詳細はログファイルをご覧ください。"
    elif status_code == 404:
        return "リクエストされたリソースが見つかりません。詳細はログファイルをご覧ください。"
    elif status_code == 408:
        return "リクエストタイムアウトが発生しました。詳細はログファイルをご覧ください。"
    elif status_code == 500:
        return "サーバーエラーが発生しました。詳細はログファイルをご覧ください。"
    elif status_code == 502:
        return "不正なゲートウェイです。詳細はログファイルをご覧ください。"
    elif status_code == 503:
        return "サービス利用不可。詳細はログファイルをご覧ください。"
    elif status_code == 504:
        return "ゲートウェイタイムアウトが発生しました。詳細はログファイルをご覧ください。"
    else:
        return "予期せぬエラーが発生しました。詳細はログファイルをご覧ください。"

# Slackにエラーメッセージを送信する関数
def send_slack_error_message(e, config=None):
    if not config:
        config = load_settings()
    webhook_url = config['Slack']['SLACK_WEBHOOK_URL']
    bot_name = config['Slack']['BOT_NAME']
    user_id = config['Slack']['USER_ID']
    icon_emoji = config['Slack'].get('ICON_EMOJI', ':exclamation:')  # 設定ファイルからアイコン絵文字を読み込む
    error_message = f"<@{user_id}>" + (get_error_message(e.response.status_code) if hasattr(e, 'response') else get_error_message(500))

    payload = {
        "text": error_message,
        "username": bot_name,
        "icon_emoji": icon_emoji  # 設定ファイルから読み込んだアイコン絵文字を使用
    }
    response = requests.post(webhook_url, json=payload)
    if response.status_code != 200:
        raise ValueError(f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}")