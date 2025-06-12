import pandas as pd

from datetime import datetime
from dateutil.relativedelta import relativedelta

from processor import csv_processor

class BalanceSheet(csv_processor):
    def __init__(self, csv_file_path):
        """
        Initialize BalanceSheet object.

        Args:
            csv_file_path (str): Path to the CSV file containing the data.
        """
        self.csv_file_path = csv_file_path
        self.df = None

    def preprocess_and_pivot(self):
        """
        Preprocess the data read from the CSV file, and pivot and format the DataFrame.

        Args:

        Returns:
            Dataframe: processed dataframe
        """
        # データの読み込み
        columns_to_read=['Date','SubjectCode','Amount']
        df = pd.read_csv(self.csv_file_path, usecols=columns_to_read)

        # データの前処理
        df['YearMonth'] = df['Date'].str[:7]
        df = df[['YearMonth','SubjectCode','Amount']]

        # ピボットとフォーマット
        # 月ごとの科目別合計金額を計算
        # pivot_dfは、月ごとの科目別合計金額を持つDataFrame 行は月、列は科目コード
        pivot_df = df.pivot_table(index='YearMonth', columns='SubjectCode', values='Amount', aggfunc='sum').reset_index()
        pivot_df = pivot_df.fillna(0)
        pivot_df = pivot_df.astype({col: int for col in pivot_df.columns if col != 'YearMonth'})

        # self.dfに保存
        return pivot_df

    def calculate_balances(self, processed_df):
        """
        Calculate balances.

        Args:

        Returns:
            None
        """
        asset_columns = [col for col in processed_df.columns if str(col).startswith('100')]
        liability_columns = [col for col in processed_df.columns if str(col).startswith('200')]
        income_columns = [col for col in processed_df.columns if str(col).startswith('400')]
        expense_columns = [col for col in processed_df.columns if str(col).startswith('500')]

        processed_df["TotalLiabilities"]=processed_df[liability_columns].sum(axis=1)
        processed_df["TotalIncome"]=processed_df[income_columns].sum(axis=1)
        processed_df["TotalExpenses"]=processed_df[expense_columns].sum(axis=1)
        processed_df["TotalAssets"]=processed_df[asset_columns].sum(axis=1)
        processed_df["NetIncome"]= processed_df['TotalIncome']-self.df['TotalExpenses']
        processed_df["TotalEquity"]= processed_df['TotalAssets']-self.df['TotalLiabilities']

        for i in range(1,len(self.df)):
            processed_df.at[i, 'TotalAssets'] += processed_df.at[i-1, 'TotalAssets']
            processed_df.at[i, 'TotalLiabilities'] += processed_df.at[i-1, 'TotalLiabilities']
            processed_df.at[i, 'TotalIncome'] += processed_df.at[i-1, 'TotalIncome']
            processed_df.at[i, 'TotalExpenses'] += processed_df.at[i-1, 'TotalExpenses']
            processed_df.at[i, 'NetIncome'] += processed_df.at[i-1, 'NetIncome']
            processed_df.at[i, 'TotalEquity'] += processed_df.at[i-1, 'TotalEquity']

        return processed_df

    def carryover_data(self):
        """
        Process and generate carryover data from the CSV file.
        Which is balance sheet data.

        Returns:
            None
        """
        # データのコピー (元のデータを変更しないようにする)
        df = self.df.copy()

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
                date_obj = datetime.strptime(formatted_month, "%Y-%m-%d")

                # 1か月後の日付を計算
                date_obj += relativedelta(months=1)

                # データをリストに追加
                data.append({
                    'Date': date_obj,
                    'SubjectCode': carryover_column,
                    'Amount': carryover_value,
                    'Remarks': "Carryover 99"
                })

        # 新しい DataFrame を作成
        self.carryover_df = pd.DataFrame(data, columns=['Date', 'SubjectCode', 'Amount', 'Remarks'])
        self.carryover_df = self.carryover_df.fillna('')


    def show_summary(self):
        """
        Print the summary DataFrame.

        Args:

        Returns:
            None
        """
        print(self.df)

    def save_summary(self, output_file_path):
        """
        Save the summary DataFrame to a CSV file.

        Args:
            output_file_path (str): Path to the output CSV file.

        Returns:
            None
        """
        self.df.to_csv(output_file_path, index=False)

    def make_balance_sheet(self, output_file_path):
        """
        Run all methods in the correct order to generate the balance sheet.

        Args:
            output_file_path (str): Path to the output CSV file.

        Returns:
            None
        """
        pivoted_df = self.preprocess_and_pivot()
        self.calculate_balances(pivoted_df)
        self.show_summary()
        self.carryover_data()
        self.save_summary(output_file_path)
