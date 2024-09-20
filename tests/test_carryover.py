from processor.processor import CSVProcessor
import pandas as pd

class TestCSVProcessor:
    def test_get_monthly_summery(self):
        csv_processor = CSVProcessor('tests/test.csv', None, subjectcodes_path='codes.csv',balance_sheet_path=None)
        test_data = {
            'YearMonth': ['2024-01', '2024-01', '2024-01', '2024-01'],
            'SubjectCode': ['100', '200', '400', '500'],
            'Amount': [2000, -1000, -2000, 1000]
        }
        df = pd.DataFrame(test_data)
        print(df)
        csv_processor.yearmonth = '2024-01'
        sum_of_subjects, sum_of_categories = csv_processor.get_monthly_summery(df)
        expected_sum_of_subjects = {
            'YearMonth': ['2024-01'],
            '100': [2000],
            '200': [-1000],
            '400': [-2000],
            '500': [1000]
        }
        expected_sum_of_categories = {
            'YearMonth': ['2024-01'],
            'TotalAssets': [2000],
            'TotalLiabilities': [-1000],
            'TotalIncome': [-2000],
            'TotalExpenses': [1000],
            'NetIncome': [1000],
            'TotalEquity': [-1000]
        }
        expected_sum_of_subjects_df = pd.DataFrame(expected_sum_of_subjects)
        expected_sum_of_categories_df = pd.DataFrame(expected_sum_of_categories)

        sum_of_subjects.columns.name = None
        sum_of_categories.columns.name = None
        expected_sum_of_subjects_df = expected_sum_of_subjects_df.astype({col: int for col in expected_sum_of_subjects_df.columns if col != 'YearMonth'})
        expected_sum_of_categories_df = expected_sum_of_categories_df.astype({col: int for col in expected_sum_of_categories_df.columns if col != 'YearMonth'})
        print("sum_of_subjects \n", sum_of_subjects)
        print(sum_of_subjects.columns)
        print(expected_sum_of_subjects_df.head(1))
        print("sum_of_categories \n",sum_of_categories)
        print(expected_sum_of_categories_df.head(1))
        pd.testing.assert_frame_equal(sum_of_subjects, expected_sum_of_subjects_df)
        pd.testing.assert_frame_equal(sum_of_categories, expected_sum_of_categories_df)

    def test_get_carryover_data(self):
        csv_processor = CSVProcessor('tests/test.csv', None, subjectcodes_path='codes.csv',balance_sheet_path=None)
        test_data = {
            'YearMonth': ['2024-01', '2024-01', '2024-01', '2024-01'],
            'SubjectCode': ['100', '200', '400', '500'],
            'Amount': [2000, -1000, -2000, 1000]
        }
        df = pd.DataFrame(test_data)

        csv_processor.yearmonth = '2024-01'
        sum_of_subjects, sum_of_categories = csv_processor.get_monthly_summery(df)
        carryover_data = csv_processor.get_carryover_data(sum_of_subjects, sum_of_categories)
        expected_carryover_data = {
            'Date': ['2024-01-31', '2024-01-31','2024-02-01','2024-02-01', '2024-02-01'],
            'SubjectCode': ['300', '600', '100', '200','300'],
            'Amount': [-1000, 1000, 2000, -1000, -1000],
            'Remarks': ['Carryover 300', 'Carryover 600', 'Carryover 100', 'Carryover 200', 'Carryover 300']
        }
        expected_carryover_data_df = pd.DataFrame(expected_carryover_data)
        expected_carryover_data_df = expected_carryover_data_df.sort_values(by=['Date', 'SubjectCode']).reset_index(drop=True)
        pd.testing.assert_frame_equal(carryover_data, expected_carryover_data_df)
