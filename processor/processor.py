import pandas as pd
import datetime as dt
import calendar

from dateutil.relativedelta import relativedelta
import numpy as np
import json

class CSVProcessor:
    """
    CSVファイルを処理するクラス
    """
    def __init__(self, input_file, output_file,subjectcodes_path,balance_sheet_path=None):
        self.input_file = input_file
        self.output_file = output_file
        self.code_file = subjectcodes_path
        self.balancesheet_path = balance_sheet_path
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
            yearmongh column added as string
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
        Args:
            search_id (int): 検索するID
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

    def remove_duplicates(self, df):
        # ID, Date, Amount, Remarksが全て同じ行を削除
        return df.drop_duplicates(subset=['ID', 'Date', 'Amount', 'Remarks'], keep='first')

    def preprocess_and_pivot(self,df):
        """
        Preprocess the data read from the CSV file, and pivot and format the DataFrame for making balance sheet.

        Args:
            Dataframe: Dataframe containing 'Date', 'SubjectCode', 'Amount'
                Date (str): Date in the format 'YYYY-MM-DD'.
                SubjectCode (str): Subject code.
                Amount (int): Account amount.

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

    def get_subject_sum(self,df):
        """
        Get the sum of the each subject code.
        Args:
            dataframe: Dataframe containing 'SubjectCode', 'Amount'
                SubjectCode (str): Subject code.
                Amount (int): Account amount.
        Returns:
            dataframe: Dataframe containing 'SubjectCode', 'Amount', 'YearMonth'
        """
        yearmonths = df['YearMonth'].values
        yearmonths = np.unique(yearmonths)
        rows = []
        for yearmonth in yearmonths:
            item = df[df['YearMonth'] == yearmonth]
            subjects = item['SubjectCode'].unique()
            for subject in subjects:
                value = pd.to_numeric(item[item['SubjectCode'] == subject]['Amount'], errors='coerce').sum()
                row = {
                    'YearMonth': yearmonth,
                    'SubjectCode': subject,
                    'Amount': value
                }
                rows.append(row)
        sums = pd.DataFrame(rows)
        sums = sums.sort_values(by=['YearMonth', 'SubjectCode'])
        return sums

    def calculate_balances(self, subject_sums):
        """
        Calculate balances.

        Args:
            dataframe: pivoted dataframe index is YearMonth, columns are SubjectCode, values are Amount

        Returns:
            dataframe: processed dataframe
        """
        yearmonths = subject_sums['YearMonth'].values
        yearmonths = np.unique(yearmonths)
        rows = []

        for yearmonth in yearmonths:
            item = subject_sums[subject_sums['YearMonth'] == yearmonth]
            asset_total = pd.to_numeric(item.filter(regex='^1\d{2}').stack(), errors='coerce').fillna(0).sum()
            liability_total = pd.to_numeric(item.filter(regex='^2\d{2}').stack(), errors='coerce').fillna(0).sum()
            income_total = pd.to_numeric(item.filter(regex='^4\d{2}').stack(), errors='coerce').fillna(0).sum()
            expense_total = pd.to_numeric(item.filter(regex='^5\d{2}').stack(), errors='coerce').fillna(0).sum()
            net_income = 0 - income_total - expense_total
            total_equity = 0 - asset_total - liability_total
            row = {
                'YearMonth': yearmonth,
                'TotalAssets': asset_total,
                'TotalLiabilities': liability_total,
                'TotalIncome': income_total,
                'TotalExpenses': expense_total,
                'NetIncome': net_income,
                'TotalEquity': total_equity
            }
            rows.append(row)

        # Create DataFrame from the list of rows
        balance_sheet_df = pd.DataFrame(rows)
        balance_sheet_df = balance_sheet_df.astype({
            'YearMonth': 'str',
            'TotalAssets': 'int64',
            'TotalLiabilities': 'int64',
            'TotalIncome': 'int64',
            'TotalExpenses': 'int64',
            'NetIncome': 'int64',
            'TotalEquity': 'int64'
        })
        return balance_sheet_df

    def get_date_for_carryover(self,formatted_day):
        '''
        Get the date for the carryover data.
        Args:
            formatted_day (str): Date string in the format 'YYYY-MM-DD'
        Returns:
            datetime: Closing date for the carryover data
            datetime: Opening date for the carryover data
        '''

        # Parse the formatted_day string to a datetime object
        date_obj = dt.datetime.strptime(formatted_day, '%Y-%m-%d')

        # Get the last day of the month
        last_day = calendar.monthrange(date_obj.year, date_obj.month)[1]
        last_day_date = dt.datetime(date_obj.year, date_obj.month, last_day)
        last_day_date = last_day_date.strftime('%Y-%m-%d')

        # Get the first day of the next month
        next_first_day_date = date_obj + relativedelta(months=1)
        next_first_day_date = next_first_day_date.strftime('%Y-%m-%d')

        return last_day_date, next_first_day_date

    def month_close_and_carryover(self,processed_df, pivot_df):
        """
        Process and generate carryover data from the CSV file.
        Which is balance sheet data.

        Args:
            dataframe: processed dataframe, which is the output of preprocess_and_pivot

        Returns:
            dataframe: carryover dataframe
        """
        df = processed_df
        yearmonths = df['YearMonth'].values
        yearmonths = np.unique(yearmonths)
        rows = []

        subject_sums = self.get_subject_sum(df)
        balance_sheet_df = self.calculate_balances(pivot_df)

        # 次月繰越用のデータを生成
        for yearmonth in yearmonths:
            # 繰越データの日付を設定
            formatted_day = yearmonth + "-01"
            closing_date, opening_date = self.get_date_for_carryover(formatted_day)
            datas = df[df['YearMonth'] == yearmonth]
            equity = balance_sheet_df[balance_sheet_df['YearMonth'] == yearmonth]['TotalEquity'].values[0]
            net_income = balance_sheet_df[balance_sheet_df['YearMonth'] == yearmonth]['NetIncome'].values[0]
            rows.append({
                'Date': str(closing_date),
                'SubjectCode': 300,
                'Amount': int(equity),
                'Remarks': 'Carryover 99'
            })
            rows.append({
                'Date': str(closing_date),
                'SubjectCode': 600,
                'Amount': int(net_income),
                'Remarks': 'Carryover 99'
            })

            # 資産と負債の合計を計算し、繰越データを生成
            # SubjectCodeによって処理を分ける
            subjects = datas['SubjectCode'].unique()

            for subject in subjects:
                value = pd.to_numeric(datas[datas['SubjectCode'] == subject]['Amount'].values, errors='coerce').fillna(0).sum()
                rows.append({
                    'Date': str(closing_date),
                    'SubjectCode': subject,
                    'Amount': int(value),
                    'Remarks': 'Carryover 99'
                })
        carryovers = pd.DataFrame(rows)
        print(carryovers)
        carryovers = carryovers.astype({
            'Date': 'str',
            'SubjectCode': 'int64',
            'Amount': 'int64',
            'Remarks': 'str'
        })
        carryovers = (carryovers.pipe(self.generate_id)
                                .pipe(self.apply_subject_from_code)
                                .pipe(self.add_yearmonth_column)
                                .pipe(self.remove_duplicates))
        df = pd.concat([df, carryovers], ignore_index=True)
        df['ID'] = df['ID'].astype(str)
        df = self.remove_duplicates(df)
        df = self.sort_csv(df)
        return df

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
                    .pipe(self.remove_duplicates))

        pivot_df = self.preprocess_and_pivot(datas)
        balansheet_df = self.calculate_balances(pivot_df)
        pd.to_csv(balansheet_df, self.balancesheet_path)

        datas = self.month_close_and_carryover(datas, balansheet_df,pivot_df)

        # 処理されたデータを新しいCSVファイルに保存する
        datas.to_csv(self.output_file, index=False)
        return print("CSV processing completed.")
