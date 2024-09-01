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
    sample_data = {
        'YearMonth': ['2024-01', '2024-02'],
        '100': [-400, -400],
        '101': [500, 600],
        '200': [-300, -400],
        '400': [-500, -600],
        '500': [700, 800]
    }
    df = pd.DataFrame(sample_data)
    balances_df = csv_processor.calculate_balances(df)
    expected_balancesheet = {
        'YearMonth': ['2024-01', '2024-02'],
        'TotalAssets': [100, 200],
        'TotalLiabilities': [-300, -400],
        'TotalIncome': [-500, -600],
        'TotalExpenses': [700, 800],
        'NetIncome': [-200, -200],
        'TotalEquity': [200, 200]
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
