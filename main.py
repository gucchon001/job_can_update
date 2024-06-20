import os
import configparser
import pandas as pd
from datetime import datetime, timedelta
from my_logging import setup_department_logger

# ロガーの設定
LOGGER = setup_department_logger('main_logger')

def read_csv_file(file_path):
    """CSVファイルを読み込んでDataFrameを返す関数"""
    try:
        df = pd.read_csv(file_path, encoding='cp932', dtype=str)
        return df
    except FileNotFoundError as e:
        LOGGER.error(f"ファイルが見つかりませんでした: {e}")
        raise

def get_new_records(new_df, old_df, exclude_words):
    """新しいレコードを取得する関数"""
    # 除外ワードを含むレコードを除外
    new_df = new_df[~new_df['教室名'].str.contains('|'.join(exclude_words), na=False)]
    
    new_ids = set(new_df['応募ID'])
    old_ids = set(old_df['応募ID'])
    new_record_ids = list(new_ids - old_ids)
    new_records = new_df[new_df['応募ID'].isin(new_record_ids)]
    return new_records, new_record_ids

def write_csv_file(file_path, df):
    """DataFrameをCSVファイルに書き込む関数"""
    try:
        df.to_csv(file_path, index=False, encoding='cp932')
        LOGGER.info(f"{file_path}に書き込みました。")
    except Exception as e:
        LOGGER.error(f"ファイルの書き込みエラー: {e}")
        raise

def update_records(new_df, old_df, target_date, new_record_ids):
    """指定された日付以降のレコードを更新する関数"""
    # 指定された列を取得
    columns_to_update = ['企業群（セグメント）', 'セグメント名', '入学年', '提出ステータス', '採用ステータス',
                         '研修初日', '在籍確認', 'データタイプ', '最終提出日', '請求確定日', '提出期限',
                         '提出期限超過月数', '保留月数', '最終変更者', '更新日']

    # 日付列を指定
    date_columns = ['研修初日', '最終提出日', '請求確定日', '提出期限', '更新日']

    # 日付列を適切な形式に変換
    for col in date_columns:
        try:
            new_df[col] = pd.to_datetime(new_df[col], errors='coerce').dt.strftime('%Y/%m/%d')
            old_df[col] = pd.to_datetime(old_df[col], errors='coerce').dt.strftime('%Y/%m/%d')
            LOGGER.info(f"{col}の変換後の値:\n{new_df[col].head()}")
        except Exception as e:
            LOGGER.error(f"列 {col} の日付変換中にエラーが発生しました: {e}")
            LOGGER.error(new_df[col].head())

    # 特定の日付を置換
    new_df['研修初日'] = new_df['研修初日'].apply(lambda x: '2100/12/31' if x == '9999/12/31' else x)

    # 指定された日付を文字列形式に変換
    target_date_str = target_date.strftime('%Y/%m/%d')
    LOGGER.info(f"変換後のtarget_date_str: {target_date_str}")

    # データフレームの状態をログ出力
    LOGGER.info(f"新規データフレームの最初の5行:\n{new_df.head()}")
    LOGGER.info(f"旧データフレームの最初の5行:\n{old_df.head()}")

     # 指定された日付以降のレコードを抽出
    new_records = new_df[new_df['更新日'] >= target_date_str]

    # 新規レコードで追加したレコードを除外
    new_records = new_records[~new_records['応募ID'].isin(new_record_ids)]

    LOGGER.info(f"更新対象レコード数: {len(new_records)}件")

    # 更新前のレコード数を取得
    old_record_count = len(old_df)

    # 元の列順序を保存
    original_columns = old_df.columns.tolist()

    # 指定された列の値を更新
    old_df.set_index('応募ID', inplace=True)
    new_records.set_index('応募ID', inplace=True)

    LOGGER.info("更新前のレコード数: {}件".format(old_record_count))
    LOGGER.info("更新対象のレコード数: {}件".format(len(new_records)))

    # 更新されたレコード数を計算するための変数を初期化
    updated_records = 0

    # マッチングのログ出力と更新前後の値の比較
    for index, row in new_records.iterrows():
        if index in old_df.index:
            record_updated = False
            for col in columns_to_update:
                old_value = old_df.at[index, col]
                new_value = row[col]
                if old_value != new_value:
                    record_updated = True
            if record_updated:
                updated_records += 1
        else:
            LOGGER.info("マッチしなかったレコード: 応募ID={}".format(index))

    old_df.update(new_records[columns_to_update])
    
    old_df.reset_index(inplace=True)

    # 元の列順序に戻す
    old_df = old_df[original_columns]

    # 新規レコードを詰めて追加
    remaining_new_records = new_df[new_df['応募ID'].isin(new_record_ids) & ~new_df.index.isin(new_records.index)]
    updated_df = pd.concat([old_df, remaining_new_records], ignore_index=True)

    # 最終行数を再取得してからカウント
    final_record_count = len(updated_df)
    LOGGER.info("最終レコード数: {}件".format(final_record_count))
    updated_records = final_record_count - old_record_count

    LOGGER.info("更新されたレコード数: {}件".format(updated_records))

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
        LOGGER.info("update_application_count: 応募回数の更新を開始")
        
        # 新規レコードのみを対象にする
        new_records = updated_df[updated_df['応募ID'].isin(new_record_ids)]
        
        # 応募回数列を数値型に変換
        updated_df['応募回数'] = pd.to_numeric(updated_df['応募回数'], errors='coerce')
        
        # 会員IDごとに処理
        member_ids = new_records['会員ID'].unique()
        for member_id in member_ids:
            if pd.isna(member_id):
                LOGGER.warning(f"update_application_count: 会員IDが欠損しているレコードをスキップします。")
                continue
            
            # 会員IDのレコードを取得
            member_records = new_records[new_records['会員ID'] == member_id]
            
            # 会員IDのレコード数を取得
            total_count = len(member_records)
            
            # 応募回数が最後にカウントされているレコードの数を取得
            last_count = updated_df[updated_df['会員ID'] == member_id]['応募回数'].max()
            if pd.isna(last_count):
                last_count = -1
            
            # 応募回数列がブランクの会員IDのレコードから順に、レコードの数をカウントアップ
            for index, row in member_records.iterrows():
                if pd.isna(row['応募回数']):
                    last_count += 1
                    updated_df.at[index, '応募回数'] = last_count
        
        # 応募回数列のNaNを0に置換
        updated_df['応募回数'] = updated_df['応募回数'].fillna(0)
        
        # 応募回数列を整数型に変換
        updated_df['応募回数'] = updated_df['応募回数'].astype(int)
        
        # 応募回数（セグメント）を更新
        updated_df.loc[updated_df['応募ID'].isin(new_record_ids), '応募回数（セグメント）'] = updated_df.loc[updated_df['応募ID'].isin(new_record_ids), '応募回数'].apply(lambda x: 1 if x == 0 else 2)
        
        LOGGER.info("update_application_count: 応募回数の更新が完了")
        return updated_df
    except Exception as e:
        LOGGER.error(f"update_application_count: 応募回数の更新中にエラーが発生しました: {e}")
        raise

def main():
    # settings.iniファイルの読み込み
    config = configparser.ConfigParser()
    config.read('settings.ini', encoding='utf-8')

    # 設定値の取得
    file_path = config.get('DEFAULT', 'file_path')
    new_file = config.get('DEFAULT', 'new_file')
    old_file = config.get('DEFAULT', 'old_file')
    output_file = config.get('DEFAULT', 'output_file')
    target_date_str = config.get('DEFAULT', 'target_date')

    # 更新対象の日付を取得
    target_date = get_target_date(target_date_str)

    try:
        # new_fileとold_fileを読み込む
        new_df = read_csv_file(os.path.join(file_path, new_file))
        old_df = read_csv_file(os.path.join(file_path, old_file))

        # "応募ID"列が両方のDataFrameに存在するか確認
        if '応募ID' not in new_df.columns or '応募ID' not in old_df.columns:
            raise ValueError("両方のCSVファイルに'応募ID'列が存在しません。")

        # 除外ファイルを読み込む
        exclude_file = 'exclude_words.txt'
        with open(exclude_file, 'r', encoding='utf-8') as f:
            exclude_words = [line.strip() for line in f]

        # 新しいレコードを取得
        new_records, new_record_ids = get_new_records(new_df, old_df, exclude_words)

        # 新しいレコードをold_fileに追加
        updated_df = pd.concat([old_df, new_records], ignore_index=True)

        # 指定された日付以降のレコードを更新
        updated_df, updated_records = update_records(new_df, updated_df, target_date, new_record_ids)

        # 新しいレコードの応募回数を更新
        LOGGER.info("main: 新しいレコードの応募回数の更新を開始")
        updated_df = update_application_count(updated_df, new_record_ids)
        LOGGER.info("main: 新しいレコードの応募回数の更新が完了")

        # 更新されたデータをoutput_fileに書き込む
        write_csv_file(os.path.join(file_path, output_file), updated_df)

        # 追加したレコード数をロギングに追加
        new_record_count = len(new_records)
        LOGGER.info(f"新規レコード追加数: {new_record_count}件")

        # 更新したレコード数をロギングに追加
        LOGGER.info(f"更新レコード数: {updated_records}件")

    except Exception as e:
        LOGGER.error(f"予期しないエラーが発生しました: {e}")

if __name__ == '__main__':
    main()