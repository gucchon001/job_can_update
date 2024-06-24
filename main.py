import configparser
import os
from datetime import datetime
import pandas as pd
from utils import (
    read_csv_file, get_new_records, write_csv_file, update_records,
    get_target_date, update_application_count, setup_department_logger
)

LOGGER = setup_department_logger('main_logger')

def main():
    config = configparser.ConfigParser()
    config.read('settings.ini', encoding='utf-8')
    file_path = config.get('DEFAULT', 'file_path')
    new_file = config.get('DEFAULT', 'new_file')
    old_file = config.get('DEFAULT', 'old_file')
    output_file = config.get('DEFAULT', 'output_file')
    target_date_str = config.get('DEFAULT', 'target_date')
    target_date = get_target_date(target_date_str)

    try:
        new_df = read_csv_file(os.path.join(file_path, new_file))
        old_df = read_csv_file(os.path.join(file_path, old_file))

        if '応募ID' not in new_df.columns or '応募ID' not in old_df.columns:
            raise ValueError("両方のCSVファイルに'応募ID'列が存在しません。")

        with open('exclude_words.txt', 'r', encoding='utf-8') as f:
            exclude_words = [line.strip() for line in f]

        new_records, new_record_ids = get_new_records(new_df, old_df, exclude_words)
        updated_df = pd.concat([old_df, new_records], ignore_index=True)
        updated_df, updated_records = update_records(new_df, updated_df, target_date, new_record_ids)
        updated_df = update_application_count(updated_df, new_record_ids)
        write_csv_file(os.path.join(file_path, output_file), updated_df)

        LOGGER.info(f"新規レコード追加数: {len(new_records)}件")
        LOGGER.info(f"更新レコード数: {updated_records}件")

    except Exception as e:
        LOGGER.error(f"予期しないエラーが発生しました: {e}", exc_info=True)

if __name__ == '__main__':
    main()