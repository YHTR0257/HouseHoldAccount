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

    def get_subject_sum(df):
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
        each_subject_rows = []
        each_category_rows = []
        for yearmonth in yearmonths:
            items_each_yearmonth = df[df['YearMonth'] == yearmonth]
            items_each_yearmonth = items_each_yearmonth.astype({'Amount': int, 'SubjectCode': str})

            for subject in items_each_yearmonth['SubjectCode'].unique():
                subject_sum = pd.to_numeric(items_each_yearmonth[items_each_yearmonth['SubjectCode'] == subject]['Amount'], errors='coerce').sum()
                each_subject_row = {
                    'YearMonth': yearmonth,
                    'SubjectCode': subject,
                    'Amount': subject_sum
                }
                each_subject_rows.append(each_subject_row)
            for category in ['1','2','4','5']:
                category_items = []
                for item in items_each_yearmonth['SubjectCode']:
                    if item.startswith(category):
                        category_row = {
                            'YearMonth': yearmonth,
                            'SubjectCode': item,
                            'Amount': items_each_yearmonth[items_each_yearmonth['SubjectCode'] == item]['Amount'].values[0]
                        }
                        category_items.append(category_row)
                category_items = pd.DataFrame(category_items)
                category_sum = pd.to_numeric(category_items['Amount'], errors='coerce').sum()
                each_category_row = {
                    'YearMonth': yearmonth,
                    'SubjectCode': category[0] + '00',
                    'Amount': category_sum
                }
                each_category_rows.append(each_category_row)
            print(each_category_rows)
        sums_each_subject = pd.DataFrame(each_subject_rows)
        sums_each_subject = sums_each_subject.sort_values(by=['YearMonth', 'SubjectCode'])
        sums_each_category = pd.DataFrame(each_category_rows)
        sums_each_category = sums_each_category.pivot_table(index='YearMonth',columns='SubjectCode',values='Amount')
        sums_each_category.index.name = 'YearMonth'
        sums_each_category = sums_each_category.reset_index()
        #Caluculate net income and total equity
        sums_each_category['TotalEquity'] = 0 - sums_each_category['100'] - sums_each_category['200']
        sums_each_category['NetIncome'] = 0 - sums_each_category['400'] - sums_each_category['500']
        sums_each_category = sums_each_category.rename(columns={
            '100':'TotalAssets',
            '200':'TotalLiabilities',
            '400':'TotalIncome',
            '500':'TotalExpenses'
            })
        return sums_each_subject, sums_each_category

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
        df = df.astype({
            'Date': 'str',
            'SubjectCode': 'str',
            'Amount': 'int64',
            'Remarks': 'str',
            'YearMonth': 'str'
        })
        yearmonths = df['YearMonth'].values
        yearmonths = np.unique(yearmonths)
        rows = []

        sums_each_subject = self.get_subject_sum(df)
        asset_ = sums_each_subject[sums_each_subject['SubjectCode'].str.startswith('1')]
        carryover_sums = df[df['SubjectCode'].str.startswith(('1', '2'))] # asset and liability
        balance_sheet_df = self.calculate_balances(pivot_df)

        # Process the carryover data for each yearmonth
        for yearmonth in yearmonths:
            last_day_date, next_first_day_date = self.get_date_for_carryover(yearmonth + '-01')
            for item in [['NetIncome','300'],['TotalEquity','600']]:
                row = {
                    'Date': last_day_date,
                    'SubjectCode': item[1],
                    'Amount': balance_sheet_df[balance_sheet_df['YearMonth'] == yearmonth][item[0]].values[0],
                    'Remarks': 'Carryover 99'
                }
                rows.append(row)

        carryovers = pd.DataFrame(rows)
        print(carryovers)
        carryovers = carryovers.astype({
            'Date': 'str',
            'SubjectCode': 'str',
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

        pivot_df = self.preprocess_and_pivot(datas)
        balansheet_df = self.calculate_balances(pivot_df)
        pd.to_csv(balansheet_df, self.balancesheet_path)

        datas = self.month_close_and_carryover(datas, balansheet_df,pivot_df)

        # 処理されたデータを新しいCSVファイルに保存する
        datas.to_csv(self.output_file, index=False)
        return print("CSV processing completed.")
