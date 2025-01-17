import pandas as pd
import datetime as dt

from dateutil.relativedelta import relativedelta
import numpy as np
import json

class CSVProcessor:
    """
    CSVファイルを処理するクラス
    """
    def __init__(self, input_file, output_file,subjectcodes_path):
        self.input_file = input_file
        self.output_file = output_file
        self.code_file = subjectcodes_path
        self.carryover_df = None

    def generate_id(self, datas):
        """
        Generate IDs for each row in the datas DataFrame based on the specified rules.

        Args:
            datas (pd.DataFrame): DataFrame containing 'Date', 'Amount', 'Remarks', and optionally 'ID'.
                Date (str): Date in the format 'YYYY-MM-DD'.
                Amount (float): Transaction amount.
                Remarks (str): Remarks for the transaction.
                ID (int, optional): Transaction ID. If not provided, it will be generated.

        Returns:
            pd.DataFrame: The input DataFrame with an added/updated 'ID' column.
        """
        def generate_single_id(row):
            existing_id = row.get('ID')
            if pd.isna(existing_id) or existing_id is None:
                date_str = row['Date']
                date_part = date_str.replace("-", "")
                amount = row['Amount']
                sign = "1" if float(amount) >= 0 else "0"
                remark = row['Remarks']
                remark_suffix = remark[-2:] if len(remark) >= 2 else remark
                # IDを生成し、Remarkの後ろ2文字を連結
                generated_id = f"{date_part}{remark_suffix}{sign}"
                return int(generated_id)
            else:
                return int(existing_id)

        # Apply the generate_single_id function to each row in the DataFrame
        datas['ID'] = datas.apply(generate_single_id, axis=1)
        return datas

    def fill_remarks(self):
        """
        備考欄を埋める
        """
        pass

    def sort_csv(self,datas):
        """
        CSVファイルをIDでソートする
        Returns:
            dataframe: Sorted dataframe by ID
        """
        datas_sorted = datas.sort_values(by='ID')
        return datas_sorted

    def add_yearmonth_column(self,df):
        """
        日付列から年月を抽出して新しい列を追加する
        Returns:
            dataframe: Dataframe with 'Year' and 'Month' columns added
        """
        df['Date']=pd.to_datetime(df['Date'])
        df['Year']=df['Date'].dt.year
        df['Month']=df['Date'].dt.month
        df['YearMonth'] = df['Date'].str[:7]
        return df

    def apply_subject_from_code(self,df):
        """
        Add Subject from code
        Returns:
            dataframe: Dataframe with 'Subject' columns added as string
        """
        df['Subject'] = df.apply(lambda data: self.find_by_id(self.code_file, data['SubjectCode'], axis=1))
        return df

    def find_by_id(self, search_id):
        """
        JSONファイルから指定されたIDの要素を検索する
        Returns:
            str: 見つかった要素のキー
        """
        search_id=str(search_id)
        try:
            # JSONファイルを読み込む
            with open(self.code_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except FileNotFoundError:
            print(f"Error: File '{self.code_file}' not found.")
            return None
        except json.JSONDecodeError:
            print(f"Error: File '{self.code_file}' is not a valid JSON.")
            return None

        # 指定されたidを持つ要素を検索する
        for key, value in data.items():
            # valueが辞書であり、'id'キーが存在するかどうかを確認
            if isinstance(value, dict) and 'id' in value:
                if value['id'] == search_id:
                    return key

        print(f"Error: ID '{search_id}' not found in JSON.")
        return None

    def remove_duplicates(self, df, column_name):
        # 重複データを削除
        return df.drop_duplicates(subset=column_name, keep='first')

    def preprocess_and_pivot(self,df):
        """
        Preprocess the data read from the CSV file, and pivot and format the DataFrame for making balance sheet.

        Args:
            Dataframe: Dataframe containing 'Date', 'SubjectCode', 'Amount'

        Returns:
            Dataframe: processed dataframe index is YearMonth, columns are SubjectCode, values are Amount
        """
        # データの読み込み
        columns_to_read=['Date','SubjectCode','Amount']
        df = df[columns_to_read]

        # データの前処理
        df['YearMonth'] = df['Date'].str[:7]
        df = df[['YearMonth','SubjectCode','Amount']]

        # ピボットとフォーマット 月ごとの科目別合計金額を計算
        # pivot_dfは、月ごとの科目別合計金額を持つDataFrame 行は月、列は科目コード
        pivot_df = df.pivot_table(index='YearMonth', columns='SubjectCode', values='Amount', aggfunc='sum').reset_index()
        pivot_df = pivot_df.fillna(0)
        pivot_df = pivot_df.astype({col: int for col in pivot_df.columns if col != 'YearMonth'})

        return pivot_df

    def calculate_balances(self, processed_df):
        """
        Calculate balances.

        Args:
            dataframe: pivoted dataframe index is YearMonth, columns are SubjectCode, values are Amount

        Returns:
            dataframe: processed dataframe
        """
        asset_columns = [col for col in processed_df.columns if str(col).startswith('100')]
        liability_columns = [col for col in processed_df.columns if str(col).startswith('200')]
        income_columns = [col for col in processed_df.columns if str(col).startswith('400')]
        expense_columns = [col for col in processed_df.columns if str(col).startswith('500')]

        processed_df["TotalLiabilities"]=processed_df[liability_columns].sum(axis=1)
        processed_df["TotalIncome"]=processed_df[income_columns].sum(axis=1)
        processed_df["TotalExpenses"]=processed_df[expense_columns].sum(axis=1)
        processed_df["TotalAssets"]=processed_df[asset_columns].sum(axis=1)
        processed_df["NetIncome"]= processed_df['TotalIncome']-processed_df['TotalExpenses']
        processed_df["TotalEquity"]= processed_df['TotalAssets']-processed_df['TotalLiabilities']

        for i in range(1,len(self.df)):
            processed_df.at[i, 'TotalAssets'] += processed_df.at[i-1, 'TotalAssets']
            processed_df.at[i, 'TotalLiabilities'] += processed_df.at[i-1, 'TotalLiabilities']
            processed_df.at[i, 'TotalIncome'] += processed_df.at[i-1, 'TotalIncome']
            processed_df.at[i, 'TotalExpenses'] += processed_df.at[i-1, 'TotalExpenses']
            processed_df.at[i, 'NetIncome'] += processed_df.at[i-1, 'NetIncome']
            processed_df.at[i, 'TotalEquity'] += processed_df.at[i-1, 'TotalEquity']

        return processed_df

    def carryover_data(self,df):
        """
        Process and generate carryover data from the CSV file.
        Which is balance sheet data.

        Returns:
            dataframe: carryover dataframe
        """
        # データのコピー (元のデータを変更しないようにする)
        df = df.copy()

        # 資産（1で始まるカラム）と負債（2で始まるカラム）をフィルタリング
        asset_columns = [col for col in df.columns if str(col).startswith('1')]
        liability_columns = [col for col in df.columns if str(col).startswith('2')]

        # 繰り越し用のカラム（資産と負債）
        carryover_columns = asset_columns + liability_columns

        # データを保存するリスト
        data = []

        # 各月ごとに処理
        for month in df['YearMonth'].values:
            # 'YearMonth'がmonthと一致する行を抽出
            item = df[df['YearMonth'] == month]
            for carryover_column in carryover_columns:
                carryover_value = item[carryover_column].values[0]
                formatted_month = month + "-01"
                date_obj = dt.strptime(formatted_month, "%Y-%m-%d")

                # 1か月後の日付を計算
                date_obj += relativedelta(months=1)

                # date_objを文字列に変換
                date_obj = date_obj.strftime("%Y-%m-%d")

                # データをリストに追加
                data.append({
                    'Date': date_obj,
                    'SubjectCode': carryover_column,
                    'Amount': carryover_value,
                    'Remarks': "Carryover 99"
                })

        # 新しい DataFrame を作成
        carryover_df = pd.DataFrame(data, columns=['Date', 'SubjectCode', 'Amount', 'Remarks'])
        carryover_df = self.generate_id(carryover_df)
        return carryover_df

    def process_csv(self):
        """
        CSVファイルの処理を行う
        すべてのメソッドはここで実行される
        """
        datas = pd.read_csv(self.input_file)

        # 入力したデータを読み込んで処理を行う
        datas = (datas.pipe(self.generate_id)
                    .pipe(self.apply_subject_from_code)
                    .pipe(self.sort_csv)
                    .pipe(self.add_yearmonth_column)
                    .pipe(self.remove_duplicates('ID')))

        carryover_df = self.preprocess_and_pivot(datas)

        combined_df = pd.concat([datas, carryover_df], ignore_index=True)

        # 処理されたデータを新しいCSVファイルに保存する
        datas.to_csv(self.output_file, index=False)
        print("CSV processing completed.")

    def month_end_close(self, carryover_df, closing_file_path):
        """
        月末処理を行う
        """
        df = pd.read_csv(self.output_file)
        self.generate_id(carryover_df)
        # dfにcarryover_dfを追加
        df = pd.concat([df, carryover_df])
        print(df)

        # IDでソート
        df = df.sort_values('ID')
        # IDを文字列に変換
        df['ID'] = df['ID'].astype(str)
        # 重複データを削除
        df = df.drop_duplicates(subset='ID', keep='first')
        # CSVファイルに保存
        df.to_csv(closing_file_path, index=False)
        print("Month-end closing completed.")
