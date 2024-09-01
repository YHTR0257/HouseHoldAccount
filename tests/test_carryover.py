from processor.processor import CSVProcessor
import pandas as pd

def test_get_subject_sum():
    # Sample data for testing
    data = {
        'YearMonth': ['2023-01', '2023-02', '2023-02', '2023-02', '2023-02'],
        'SubjectCode': ['100', '200', '100', '200', '300'],
        'Amount': [10, 20, 30, -400, 50]
    }
    df = pd.DataFrame(data)

    expected_data = {
        'YearMonth': ['2023-01', '2023-02', '2023-02', '2023-02'],
        'SubjectCode': ['100', '100', '200', '300'],
        'Amount': [10, 30, -380, 50]
    }
    expected_df = pd.DataFrame(expected_data)

    csv_processor = CSVProcessor('tests/test.csv', None, subjectcodes_path='codes.json',balance_sheet_path=None)
    result_df = csv_processor.get_subject_sum(df)

    result_df = result_df.reset_index(drop=True)
    expected_df = expected_df.reset_index(drop=True)

    # Check if the result matches the expected DataFrame
    pd.testing.assert_frame_equal(result_df, expected_df)

def test_caluculate_balances():
    csv_processor = CSVProcessor('tests/test.csv', None, subjectcodes_path='codes.json',balance_sheet_path=None)
    df = pd.read_csv('tests/test.csv')
    pivot_df = csv_processor.preprocess_and_pivot(df)
    balances_df = csv_processor.calculate_balances(pivot_df)
    expected_balancesheet = {
        'YearMonth': ['2024-03', '2024-04'],
        'TotalAssets': [-7500, -7500],
        'TotalLiabilities': [-1500, -1500],
        'TotalIncome': [-5500, -5500],
        'TotalExpenses': [14500, 14500],
        'NetIncome': [-9000, -9000],
        'TotalEquity': [9000, 9000]
    }
    expected_balance_df = pd.DataFrame(expected_balancesheet, columns=['YearMonth', 'TotalAssets', 'TotalLiabilities', 'TotalIncome', 'TotalExpenses', 'NetIncome', 'TotalEquity'])

    pd.testing.assert_frame_equal(balances_df, expected_balance_df)

# def test_month_close_and_carryover():
#     csv_processor = CSVProcessor(None,None, subjectcodes_path='codes.json',balance_sheet_path=None)
#     test_df = pd.read_csv('tests/carryover.csv')
#     expected_data = pd.read_csv('tests/carryover_expected.csv')
#     test_pivot_df = csv_processor.preprocess_and_pivot(test_df)
#     actual_data = csv_processor.month_close_and_carryover(test_df, test_pivot_df)
#     pd.testing.assert_frame_equal(actual_data, expected_data)
