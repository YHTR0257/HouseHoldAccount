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
        self.yearmonth = None

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
                remark_suffix = remark[-3:] if len(remark) >= 3 else remark
                # IDを生成し、Remarkの後ろ3文字を連結
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
            dataframe: Dataframe with 'Year' and 'Month','YearMonth' columns added
            yearmongh column added as string
        """
        df['Date']=pd.to_datetime(df['Date'])
        df['Year']=df['Date'].dt.year
        df['Month']=df['Date'].dt.month
        df['YearMonth'] = df['Date'].apply(lambda x: x.strftime('%Y-%m'))
        return df

    def apply_subject_from_code(self,df):
        """
        Add Subject from code
        Returns:
            dataframe: Dataframe with 'Subject' columns added as string
        """
        print(df.columns)
        df['Subject'] = df.apply(lambda data: self.find_by_id(data['SubjectCode']),axis=1)
        return df

    def find_by_id(self, search_id):
        """
        JSONファイルから指定されたIDの要素を検索する
        Args:
            search_id (str): 検索するID
        Returns:
            str: 見つかった要素のキー
        """
        # IDが正しいかどうかを確認
        if not search_id.isdigit():
            print(f"Error: ID '{search_id}' is not a valid ID.")
            return None
        search_id=str(search_id)
        try:
            # JSONファイルを読み込む
            with open(self.code_file, 'r', encoding='utf-8') as f:
                #dtype: id: str, subject: str
                data = pd.read_csv(f, dtype={'id':str,'subject':str})
        except FileNotFoundError:
            print(f"Error: File '{self.code_file}' not found.")
            return None

        # 指定されたidを持つ要素を検索
        try:
            subject = data[data['id'] == search_id]['subject'].values[0]
            return subject
        except IndexError:
            print(f"Error: ID '{search_id}' not found in the codes file.")
            return None

    def remove_duplicates(self, df):
        # ID, Date, Amount, Remarksが全て同じ行を削除
        return df.drop_duplicates(subset=['ID', 'Date', 'Amount', 'Remarks'], keep='first')

    def get_monthly_summery(self,df):
        """
        Calculate the sum of each subject code for each month. YearMonth is the index, SubjectCode is the column, and Amount is the value.

        Args:
            Dataframe: Dataframe containing 'Date', 'SubjectCode', 'Amount'
                YearMonth (str): Year and month in 'YYYY-MM' format.
                SubjectCode (str): Subject code.
                Amount (int): Account amount.

        Returns:
            Dataframe: processed dataframe index is YearMonth, columns are SubjectCode, values are Amount
        """
        # データの読み込み
        columns_to_read=['YearMonth','SubjectCode','Amount']
        df = df[columns_to_read]
        df = df[df['YearMonth'] == self.yearmonth]

        # ピボットとフォーマット 月ごとの科目別合計金額を計算
        # sum_of_subjectは、月ごとの科目別合計金額を持つDataFrame 行は月、列は科目コード
        sum_of_subjects = df.pivot_table(index='YearMonth', columns='SubjectCode', values='Amount', aggfunc='sum').reset_index()
        sum_of_subjects = sum_of_subjects.fillna(0)

        each_category_rows = []
        for category in ['1','2','4','5']:
            category_items = []
            for item in df['SubjectCode']:
                if item.startswith(category):
                    category_row = {
                        'YearMonth': self.yearmonth,
                        'SubjectCode': item,
                        'Amount': df[df['SubjectCode'] == item]['Amount'].values[0]
                    }
                    category_items.append(category_row)
            category_items = pd.DataFrame(category_items)
            category_sum = pd.to_numeric(category_items['Amount'], errors='coerce').sum()
            each_category_row = {
                'YearMonth': self.yearmonth,
                'SubjectCode': category[0] + '00',
                'Amount': category_sum
            }
            each_category_rows.append(each_category_row)
        sum_of_categories = pd.DataFrame(each_category_rows)
        sum_of_categories = sum_of_categories.pivot_table(index='YearMonth',columns='SubjectCode',values='Amount').reset_index()
        #Caluculate net income and total equity
        sum_of_categories['NetIncome'] = 0 - sum_of_categories['400'] - sum_of_categories['500']
        sum_of_categories['TotalEquity'] = 0 - sum_of_categories['100'] - sum_of_categories['200']
        sum_of_categories = sum_of_categories.rename(columns={
            '100':'TotalAssets',
            '200':'TotalLiabilities',
            '400':'TotalIncome',
            '500':'TotalExpenses'
            })
        sum_of_subjects = sum_of_subjects.astype({col: int for col in sum_of_subjects.columns if col != 'YearMonth'})
        sum_of_categories = sum_of_categories.astype({col: int for col in sum_of_categories.columns if col != 'YearMonth'})
        return sum_of_subjects, sum_of_categories

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

    def get_carryover_data(self,sum_of_subjects, sum_of_categories):
        sum_of_subjects = sum_of_subjects[sum_of_subjects['YearMonth'] == self.yearmonth]
        sum_of_categories = sum_of_categories[sum_of_categories['YearMonth'] == self.yearmonth]
        closing_date,next_first_day_date = self.get_date_for_carryover(self.yearmonth+'-01')
        carryover_data = []

        for initial in [['TotalEquity','300',closing_date],['TotalEquity','300',next_first_day_date],['NetIncome','600',closing_date]]:
            row1 = {
                'Date': initial[2],
                'SubjectCode': initial[1],
                'Amount': sum_of_categories[initial[0]].values[0],
                'Remarks': 'Carryover '+ initial[1]
            }
            carryover_data.append(row1)
        for initial in ['1','2']:
            for item in sum_of_subjects.columns:
                if str(item).startswith(initial):
                    row = {
                        'Date': next_first_day_date,
                        'SubjectCode': item,
                        'Amount': sum_of_subjects[item].values[0],
                        'Remarks': 'Carryover '+ str(item)
                    }
                    carryover_data.append(row)
        carryover_df = pd.DataFrame(carryover_data)
        carryover_df = carryover_df.sort_values(by=['Date', 'SubjectCode']).reset_index(drop=True)
        return carryover_df

    def process_csv(self):
        """
        CSVファイルの処理を行う
        すべてのメソッドはここで実行される
        """
        datas = pd.read_csv(self.input_file)
        datas = datas.astype({
            'Date': 'str',
            'SubjectCode': 'str',
            'Amount': 'int64',
            'Remarks': 'str'
        })

        # 入力したデータを読み込んで処理を行う
        datas = (datas.pipe(self.generate_id)
                    .pipe(self.apply_subject_from_code)
                    .pipe(self.sort_csv)
                    .pipe(self.add_yearmonth_column)
                    .pipe(self.remove_duplicates))

        for yearmonth in datas['YearMonth'].unique():
            self.yearmonth = yearmonth
            sum_of_subjects,sum_of_categories = self.get_monthly_summery(datas)
            carryover_datas = self.get_carryover_data(sum_of_subjects,sum_of_categories)
            datas = pd.concat([datas,carryover_datas],ignore_index=True)
            datas = (datas.pipe(self.generate_id)
                        .pipe(self.apply_subject_from_code)
                        .pipe(self.sort_csv)
                        .pipe(self.add_yearmonth_column)
                        .pipe(self.remove_duplicates))
        pivot_df = self.preprocess_and_pivot(datas)
        balansheet_df = self.calculate_balances(pivot_df)
        pd.to_csv(balansheet_df, self.balancesheet_path)

        carryover_datas = self.get_carryover_data(datas, balansheet_df,pivot_df)

        # 処理されたデータを新しいCSVファイルに保存する
        datas.to_csv(self.output_file, index=False)
        return print("CSV processing completed.")
