# -*- coding: utf-8 -*-
"""
Created on Fri May 24 10:54:55 2024

@author: tmnk015
"""

import pandas as pd
import configparser

# 設定ファイルを読み込む
def read_config(file_path):
    config = configparser.ConfigParser()
    config.read(file_path, encoding='utf-8')
    return config

# データを読み込む（必要なカラムのみ）
def load_data(file_path, columns=None, encoding='cp932'):
    return pd.read_csv(file_path, usecols=columns, encoding=encoding)

# データを前処理する
def preprocess_data(df):
    df['応募日'] = pd.to_datetime(df['応募日時']).dt.date
    df = df.drop_duplicates(subset='応募ID', keep=False)
    df = df.sort_values(by='応募日時')
    return df

# 応募回数と累積応募回数を計算する
def calculate_counts(df):
    df['何回目応募か'] = df.groupby('会員ID').cumcount() + 1
    df['累積応募回数'] = df.groupby('会員ID')['応募ID'].transform('count')
    return df

# データを保存する
def save_data(df, file_path, encoding='cp932'):
    df.to_csv(file_path, index=False, encoding=encoding)
    print(f'データが {file_path} に保存されました。')

# データをマージする
def merge_data(original_df, processed_df):
    merged_df = original_df.merge(processed_df, on='応募ID', how='left')
    merged_df = merged_df.drop(columns=['応募日_y', '会員ID_y'])
    merged_df.rename(columns={'応募日_x': '応募日', '会員ID_x': '会員ID'}, inplace=True)
    return merged_df

# メイン処理を実行する
def main():
    config = read_config('settings.ini')

    input_file_path = config['other']['input_file_path']
    count_file_path = config['other']['count_file_path']
    output_file_path = config['other']['output_file_path']

    columns = ['応募日時', '応募ID', '会員ID']  # 必要なカラムのみ読み込む
    df = load_data(input_file_path, columns=columns)
    df = preprocess_data(df)
    df = calculate_counts(df)
    save_data(df, count_file_path)

    jobcan_df = load_data(input_file_path)  # 元データ全体を読み込む
    merged_df = merge_data(jobcan_df, df)
    save_data(merged_df, output_file_path)

if __name__ == "__main__":
    main()
