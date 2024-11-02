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
    def __init__(self, input_file, output_file,summary_file,subjectcodes_path='./codes.csv'):
        self.input_file = input_file
        self.output_file = output_file
        self.summary_file = summary_file
        self.carryover_df = None
        self.yearmonth = None
        try:
            with open(subjectcodes_path, 'r', encoding='utf-8') as f:
                self.codes = pd.read_csv(f, dtype={'id':str,'subject':str})
        except FileNotFoundError:
            print(f"Error: File '{subjectcodes_path}' not found.")
            return None

    def generate_single_id(self, row):
        """
        IDを生成する
        Args:
            row (Series): 行
        Returns:
            int: 生成されたID
        """
        existing_id = row.get('ID')
        if pd.isna(existing_id) or existing_id is None:
            date_str = str(row['Date'])
            date_str = dt.datetime.strptime(date_str, '%Y%m%d').strftime('%Y%m%d')
            amount = row['Amount']
            sign = "1" if int(amount) >= 0 else "0"
            remark = row['Remarks']
            remark_suffix = remark[-3:] if len(remark) >= 3 else remark
            # IDを生成し、Remarkの後ろ3文字を連結
            generated_id = f"{date_str}{remark_suffix}{sign}"
            return str(generated_id)
        else:
            return str(existing_id)

    def fill_param(self,df):
        """
        Add Category from code
        Returns:
            dataframe: Dataframe with 'Category', 'CategoryName', 'CategoryNum' and 'ID'
            columns added as string
        """
        for index, row in df.iterrows():
            item = self.find_by_id(row['SubjectCode'])
            if item is not None:
                id = self.generate_single_id(row)
                df.loc[index, 'Subject'] = item['subject'].values[0]
                df.loc[index, 'CategoryName'] = item['category_name'].values[0]
                df.loc[index, 'CategoryNum'] = str(item['category_num'].values[0])
                df.loc[index, 'ID'] = str(id)
        return df

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
        df['YearMonth'] = df['Date'].apply(lambda x: x.strftime('%Y%m'))
        return df

    def apply_subject_from_code(self,df):
        """
        Add Subject from code
        Returns:
            dataframe: Dataframe with 'Subject' columns added as string
        """
        df['Subject'] = df.apply(lambda data: self.find_by_id(data['SubjectCode']),axis=1)
        return df

    def find_by_id(self, search_id):
        """
        codefileから指定されたIDの要素を検索する
        Args:
            search_id (str): 検索するID
        Returns:
            str: 見つかった要素のキー
        """
        # IDが正しいかどうかを確認
        search_id=str(search_id)
        if not search_id.isdigit():
            print(f"Error: ID '{search_id}' is not a valid ID.")
            return None

        # 指定されたidを持つ要素を検索
        try:
            item = self.codes[self.codes['id'] == search_id]
            return item
        except IndexError:
            print(f"Error: ID '{search_id}' not found in the codes file.")
            return None

    def remove_duplicates(self, df):
        # ID, Date, Amount, Remarksが全て同じ行を削除
        return df.drop_duplicates(subset=['ID', 'Date', 'Amount', 'Remarks'], keep='first')

    def get_monthly_summary(self, df):
        """
        Calculate the sum of each subject code in df
        Args:
            Dataframe: Dataframe containing 'YearMonth', 'CategoryNum', 'SubjectCode', 'Amount'
                YearMonth (str): Year and month in 'YYYYMM' format.
                CategoryNum (str): Category number which has one order.
                SubjectCode (str): Subject code.
                Amount (int): Account amount.
        Returns:
            Dataframe: Dataframe containing 'YearMonth', 'CategoryNum', 'SubjectCode', 'Amount', 'Remarks' which is
            used for carryover data.
            Dataframe: Dataframe has columns '1', '2', '3', '4', '5', '6' which are the sum of each category.
            Dataframe: Dataframe has columns 'YearMonth', 'SubjectCode', 'Amount' which are the sum of each subject code.
        """

        # Validation check
        if df.empty:
            print("No data found.")
            return pd.DataFrame(columns=['YearMonth', 'CategoryNum', 'SubjectCode', 'Amount', 'Remarks']), pd.DataFrame(columns=['YearMonth', 'CategoryNum', 'CategoryName', 'Amount']), pd.DataFrame(columns=['YearMonth'])
        # Check if the columns are in the dataframe
        if not all(col in df.columns for col in ['YearMonth', 'CategoryNum', 'SubjectCode', 'Amount']):
            print("Columns are missing.")
            return pd.DataFrame(columns=['YearMonth', 'CategoryNum', 'SubjectCode', 'Amount', 'Remarks']), pd.DataFrame(columns=['YearMonth', 'CategoryNum', 'CategoryName', 'Amount']), pd.DataFrame(columns=['YearMonth'])

        sums_subject = df.groupby(['YearMonth','CategoryNum','SubjectCode']).agg({'Amount':'sum'}).reset_index()
        sums_category = df.groupby(['YearMonth','CategoryNum','CategoryName']).agg({'Amount':'sum'}).reset_index()
        for index, row in sums_subject.iterrows():
            sums_subject.loc[index, 'Remarks'] = f"Carryover {row['SubjectCode']}"
        pv_sums_subject = sums_subject.pivot_table(index='YearMonth',columns='SubjectCode',values='Amount').reset_index()
        pv_sums_category = sums_category.pivot_table(index='YearMonth',columns='CategoryNum',values='Amount').reset_index()
        print(pv_sums_category)
        equity = 0 - pv_sums_category.loc[0,'1'] - pv_sums_category.loc[0,'2']
        netincome = 0 - pv_sums_category.loc[0,'4'] - pv_sums_category.loc[0,'5']
        pv_sums_category.loc[0, '3'] = int(equity)
        pv_sums_category.loc[0, '6'] = int(netincome)

        pv_sums_category.columns.name = None
        pv_sums_category = pv_sums_category[['YearMonth', '1', '2', '3', '4', '5', '6']]
        pv_sums_category.reset_index(drop=True)

        pv_sums_subject = pv_sums_subject.fillna(0)
        pv_sums_category = pv_sums_category.fillna(0)
        sums_subject = sums_subject.fillna(0)

        pv_sums_category.columns.name = None
        return sums_subject, pv_sums_category, pv_sums_subject

    def get_date_for_carryover(self,formatted_day):
        '''
        Get the date for the carryover data.
        Args:
            formatted_day (str): Date string in the format 'YYYYMMDD'
        Returns:
            datetime: The last date in formatted_day's month. Closing date for the carryover data
            datetime: The next 1st in formatted_day's month. Opening date for the carryover data
        '''

        date_obj = dt.datetime.strptime(formatted_day, '%Y%m%d')

        # Get the last day of the month
        last_day_date = dt.datetime(date_obj.year, date_obj.month, calendar.monthrange(date_obj.year, date_obj.month)[1])

        # Get the first day of the next month
        next_first_day_date = last_day_date + relativedelta(months=1)
        next_first_day_date = dt.datetime(next_first_day_date.year, next_first_day_date.month, 1)

        last_day_date = last_day_date.strftime('%Y%m%d')
        next_first_day_date = next_first_day_date.strftime('%Y%m%d')
        return last_day_date, next_first_day_date

    def get_carryover_data(self,s_sbj, pv_cat):
        """
        Get the carryover data for the next month.
        """
        carryover_df = pd.DataFrame(columns=['YearMonth', 'CategoryNum', 'SubjectCode', 'Amount', 'Remarks'])
        # Data validation
        if s_sbj.empty or pv_cat.empty:
            print("No data found.")
            return carryover_df

        # Check if the columns are in the dataframe
        if not all(col in s_sbj.columns for col in ['YearMonth', 'CategoryNum', 'SubjectCode', 'Amount', 'Remarks']):
            print("Columns are missing.")
            return carryover_df

        # Get the last day of the month and the first day of the next month
        last_day_date, next_first_day_date = self.get_date_for_carryover(self.yearmonth + '01')


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
        subject_summary = pd.DataFrame()
        category_summary = pd.DataFrame()

        for yearmonth in datas['YearMonth'].unique():
            self.yearmonth = yearmonth
            processdata = datas[datas['YearMonth'] == yearmonth]
            sums_sbj, pv_cat, pv_sbj = self.get_monthly_summary(processdata)
            datas = pd.concat([datas,carryover_datas],ignore_index=True)
            datas = (datas.pipe(self.generate_id)
                        .pipe(self.apply_subject_from_code)
                        .pipe(self.sort_csv)
                        .pipe(self.add_yearmonth_column)
                        .pipe(self.remove_duplicates))
            print(f"Processing {yearmonth} completed.")

        # Summaryの列を並び替え
        summary_columns  = subject_summary.columns.tolist()
        summary_columns.remove('YearMonth')
        summary_columns.sort()
        summary_columns.insert(0,'YearMonth')
        subject_summary = subject_summary[summary_columns]

        category_summary = category_summary[['YearMonth','TotalAssets','TotalLiabilities','TotalEquity','TotalIncome','TotalExpenses','NetIncome']]

        summary = pd.merge(subject_summary,category_summary,on='YearMonth')
        summary = summary.fillna(0)
        summary = summary.astype({col: int for col in summary.columns if col != 'YearMonth'})

        # 処理されたデータを新しいCSVファイルに保存する
        datas.to_csv(self.output_file, index=False)
        summary.to_csv(self.summary_file,index=False)
        return print("CSV processing completed.")
