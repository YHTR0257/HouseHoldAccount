import pandas as pd
import datetime as dt
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

    def process_csv(self):
        """
        CSVファイルの処理を行う
        すべてのメソッドはここで実行される
        """
        datas = pd.read_csv(self.input_file)

        # pipeを使用してメソッドを連鎖的に適用
        datas = (datas.pipe(self.generate_id)
                    .pipe(self.apply))
        file_path = 'codes.json'  # JSONファイルのパス
        datas['Subject'] = datas.apply(lambda data:self.find_by_id(file_path, data['SubjectCode']),axis=1)

        # CSVファイルをIDでソートする
        datas = self.sort_csv(datas)

        # 日付列から月を抽出して新しい列を追加する
        datas = self.add_yearmonth_column(datas)

        # 重複データを削除する
        datas = datas.drop_duplicates(subset='ID',keep='first')

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
