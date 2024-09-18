from processor.processor import CSVProcessor
import pandas as pd

def test_caluculate_balances():
    csv_processor = CSVProcessor('tests/test.csv', None, subjectcodes_path='codes.csv',balance_sheet_path=None)
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

def test_getting_subject_sum():
    csv_processor = CSVProcessor(None, None, subjectcodes_path='codes.csv',balance_sheet_path=None)
    df = pd.read_csv('tests/test.csv')
    df = csv_processor.add_yearmonth_column(df)
    sums_subject,sums_category = csv_processor.get_subject_sum(df)
    expected_data = {
        'YearMonth': ['2024-03', '2024-04'],
        'TotalAssets': [-6000, -6000],
        'TotalLiabilities': [-4000, -4000],
        'TotalIncome': [-5500, -5500],
        'TotalExpenses': [15500, 15500],
        'TotalEquity': [10000, 10000],
        'NetIncome': [-10000, -10000]
    }

def test_month_close_and_carryover():
    csv_processor = CSVProcessor(None,None, subjectcodes_path='codes.csv',balance_sheet_path=None)
    test_df = pd.read_csv('tests/carryover.csv')
    expected_data = pd.read_csv('tests/carryover_expected.csv')
    test_pivot_df = csv_processor.preprocess_and_pivot(test_df)
    actual_data = csv_processor.month_close_and_carryover(test_df, test_pivot_df)
    pd.testing.assert_frame_equal(actual_data, expected_data)
