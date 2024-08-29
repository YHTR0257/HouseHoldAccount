# from balancesheet.balance_sheet import BalanceSheet
from processor.csv_processor import CSVProcessor

init_data_path = 'datas/test.csv'
output_modified_path = 'datas/test_modified.csv'

input_file_path = 'datas/test_modified.csv'
output_summary_path = 'datas/testbalance_sheet.csv'
codes_path = 'codes.json'

"""
CSVを読み込んで処理を行う
"""

# CSVProcessorクラスのインスタンスを作成
csv_processor = CSVProcessor(init_data_path, output_modified_path,codes_path)

# CSVファイルの処理を行う
csv_processor.process_csv()

"""
バランスシートを生成して保存する
"""

# BalanceSheetクラスのインスタンスを作成
balance_sheet = BalanceSheet(input_file_path)

# バランスシートの生成と保存
balance_sheet.make_balance_sheet(output_summary_path)
carryover_df = balance_sheet.carryover_df
print(carryover_df)

# 繰り越しデータを加えたCSVを完成版のものとして保存
closing_file_path = 'datas/test_closing.csv'
csv_processor.month_end_close(carryover_df, closing_file_path)
print("Month-end closing completed.")
