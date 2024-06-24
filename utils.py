import os
import pandas as pd
import glob
from datetime import datetime, timedelta
import logging
import configparser

def setup_department_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def read_csv_file(file_path):
    """CSVファイルを読み込んでDataFrameを返す関数"""
    try:
        df = pd.read_csv(file_path, encoding='cp932', dtype=str)
        return df
    except FileNotFoundError as e:
        logging.error(f"ファイルが見つかりませんでした: {e}")
        raise

def get_new_records(new_df, old_df, exclude_words):
    """新しいレコードを取得する関数"""
    new_df = new_df[~new_df['教室名'].str.contains('|'.join(exclude_words), na=False)]
    new_ids = set(new_df['応募ID'])
    old_ids = set(old_df['応募ID'])
    new_record_ids = list(new_ids - old_ids)
    new_records = new_df[new_df['応募ID'].isin(new_record_ids)]
    return new_records, new_record_ids

def write_csv_file(file_path, df):
    """DataFrameをCSVファイルに書き込む関数"""
    try:
        if os.path.exists(file_path):
            new_backup_path = manage_backup_files(file_path)
            os.rename(file_path, new_backup_path)
            logging.info(f"既存の {os.path.basename(file_path)} を {os.path.basename(new_backup_path)} としてバックアップしました。")
        df.to_csv(file_path, index=False, encoding='cp932')
        logging.info(f"{file_path} に新しいデータを書き込みました。")
    except Exception as e:
        logging.error(f"ファイルの書き込みエラー: {e}")
        raise

def update_records(new_df, old_df, target_date, new_record_ids):
    """指定された日付以降のレコードを更新する関数"""
    columns_to_update = ['企業群（セグメント）', 'セグメント名', '入学年', '提出ステータス', '採用ステータス',
                         '研修初日', '在籍確認', 'データタイプ', '最終提出日', '請求確定日', '提出期限',
                         '提出期限超過月数', '保留月数', '最終変更者', '更新日']
    date_columns = ['研修初日', '最終提出日', '請求確定日', '提出期限', '更新日']

    for col in date_columns:
        try:
            new_df[col] = pd.to_datetime(new_df[col], errors='coerce').dt.strftime('%Y/%m/%d')
            old_df[col] = pd.to_datetime(old_df[col], errors='coerce').dt.strftime('%Y/%m/%d')
        except Exception as e:
            logging.error(f"列 {col} の日付変換中にエラーが発生しました: {e}")

    new_df = new_df.fillna('')
    old_df = old_df.fillna('')
    new_df['研修初日'] = new_df['研修初日'].apply(lambda x: '2100/12/31' if x == '9999/12/31' else x)
    target_date_str = target_date.strftime('%Y/%m/%d')
    new_records = new_df[(new_df['更新日'] >= target_date_str) & (~new_df['応募ID'].isin(new_record_ids))]

    old_record_count = len(old_df)
    original_columns = old_df.columns.tolist()
    old_df.set_index('応募ID', inplace=True)
    new_records.set_index('応募ID', inplace=True)

    updated_records = 0
    for index, row in new_records.iterrows():
        if index in old_df.index:
            record_updated = False
            for col in columns_to_update:
                old_value = old_df.at[index, col]
                new_value = row[col]
                if isinstance(old_value, pd.Series):
                    old_value = old_value.iloc[0] if not old_value.empty else ''
                if isinstance(new_value, pd.Series):
                    new_value = new_value.iloc[0] if not new_value.empty else ''
                if pd.notna(old_value) and pd.notna(new_value) and old_value != new_value and old_value != '' and new_value != '':
                    record_updated = True
                    old_df.at[index, col] = new_value
            if record_updated:
                updated_records += 1

    old_df.reset_index(inplace=True)
    old_df = old_df[original_columns]
    remaining_new_records = new_df[new_df['応募ID'].isin(new_record_ids) & ~new_df.index.isin(new_records.index)]
    updated_df = pd.concat([old_df, remaining_new_records], ignore_index=True)

    return updated_df, updated_records

def get_target_date(date_str):
    """設定ファイルの日付文字列を解析して日付を返す関数"""
    if date_str.lower() == 'yesterday':
        return datetime.now().date() - timedelta(days=1)
    else:
        return datetime.strptime(date_str, '%Y-%m-%d').date()

def update_application_count(updated_df, new_record_ids):
    """新しいレコードの応募回数を更新する関数"""
    try:
        new_records = updated_df[updated_df['応募ID'].isin(new_record_ids)]
        updated_df['応募回数'] = pd.to_numeric(updated_df['応募回数'], errors='coerce')
        
        member_ids = new_records['会員ID'].unique()
        for member_id in member_ids:
            if pd.isna(member_id):
                continue
            member_records = new_records[new_records['会員ID'] == member_id]
            last_count = updated_df[updated_df['会員ID'] == member_id]['応募回数'].max()
            if pd.isna(last_count):
                last_count = -1
            for index, row in member_records.iterrows():
                if pd.isna(row['応募回数']):
                    last_count += 1
                    updated_df.at[index, '応募回数'] = last_count
        
        updated_df['応募回数'] = updated_df['応募回数'].fillna(0).astype(int)
        updated_df.loc[updated_df['応募ID'].isin(new_record_ids), '応募回数（セグメント）'] = updated_df.loc[updated_df['応募ID'].isin(new_record_ids), '応募回数'].apply(lambda x: 1 if x == 0 else 2)
        
        return updated_df
    except Exception as e:
        logging.error(f"update_application_count: 応募回数の更新中にエラーが発生しました: {e}", exc_info=True)
        raise

def manage_backup_files(file_path):
    """バックアップファイルを管理し、古いファイルを削除する関数"""
    try:
        config = configparser.ConfigParser()
        config.read('settings.ini', encoding='utf-8')
        bkup_path = config.get('DEFAULT', 'bkup_path')
        output_file = os.path.basename(file_path)
        file_name_without_ext = os.path.splitext(output_file)[0]
        os.makedirs(bkup_path, exist_ok=True)
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(days=14)
        backup_pattern = os.path.join(bkup_path, f"{file_name_without_ext}_*.csv")
        for backup_file in glob.glob(backup_pattern):
            file_time = datetime.strptime(os.path.basename(backup_file).split('_')[-1].split('.')[0], '%Y%m%d%H%M%S')
            if file_time < cutoff_time:
                os.remove(backup_file)
        timestamp = current_time.strftime('%Y%m%d%H%M%S')
        new_backup_filename = f"{file_name_without_ext}_{timestamp}.csv"
        new_backup_path = os.path.join(bkup_path, new_backup_filename)
        return new_backup_path
    except Exception as e:
        logging.error(f"バックアップファイル管理中にエラーが発生しました: {e}")
        raise