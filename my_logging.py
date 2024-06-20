import logging
from logging.handlers import RotatingFileHandler
import configparser
import os

def setup_department_logger(name):
    # 既存のロガー設定を変更せず、すべてのスクリプトからのロギングが一貫して記録されるようにします。
    # この関数が複数回呼び出されても、ロガーの設定が重複して適用されないようにチェックします。
    logger = logging.getLogger(name)
    if not logger.handlers:
        # 設定ファイルからログ設定を読み込む
        config = configparser.ConfigParser()
        config.read('settings.ini', encoding='utf-8')

        # ログレベル、ログファイルのパス、ファイル名を取得
        log_level = config['logging']['level']
        log_file = config['logging']['logfile']

        # ログファイルの完全なパスを生成
        log_file_full_path = os.path.join(log_file)

        # ログのフォーマットを設定
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # ファイルハンドラの設定（ログローテーション対応）
        file_handler = RotatingFileHandler(log_file_full_path, maxBytes=10000000, backupCount=10)
        file_handler.setLevel(logging.getLevelName(log_level))
        file_handler.setFormatter(formatter)

        # コンソールハンドラの設定
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.getLevelName(log_level))
        console_handler.setFormatter(formatter)

        # ロガーにハンドラを追加
        logger.setLevel(logging.getLevelName(log_level))
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger
