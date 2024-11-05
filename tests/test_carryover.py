import pandas as pd
import pytest

from processor.processor import CSVProcessor

class TestCSVProcessor:
    def setup_class(self):
        self.csv_processor = CSVProcessor('tests/test.csv', 'tests/result.csv', subjectcodes_path='codes.csv', summary_file='tests/summary.csv')
        self.test_data = pd.read_csv('tests/test.csv', dtype={'Date': 'str', 'SubjectCode': 'str', 'Amount': 'int'})
        self.test_data = self.csv_processor.fill_param(self.test_data)
        self.test_data = self.csv_processor.add_yearmonth_column(self.test_data)

    def test_a_month_summary(self):
        test_data = self.test_data[self.test_data['YearMonth'] == '202403']
        test_s_sbj, test_pv_cat, test_pv_sbj = self.csv_processor.get_monthly_summary(test_data)

        test_pv_cat = test_pv_cat.astype({'1': 'int32', '2': 'int32', '3': 'int32', '4': 'int32', '5': 'int32', '6': 'int32'})
        test_pv_cat.columns.name = None
        test_pv_cat = test_pv_cat[['YearMonth', '1', '2', '3', '4', '5', '6']]
        test_pv_cat.reset_index(drop=True)

        test_pv_sbj.columns.name = None

        s_sbj = {
            'YearMonth': ['202403', '202403', '202403', '202403', '202403', '202403', '202403'],
            'CategoryNum': ['1', '2', '4', '4', '5', '5', '5'],
            'SubjectCode': ['101', '200', '400', '490', '500', '531', '590'],
            'Amount': [-7500, -1500, -5000, -500, 3000, 1500, 10000],
            'Remarks': ['Carryover 101', 'Carryover 200', 'Carryover 400', 'Carryover 490', 'Carryover 500', 'Carryover 531', 'Carryover 590']
            }

        pv_cat = {
            'YearMonth': ['202403'],
            '1': [-7500],
            '2': [-1500],
            '3': [9000],
            '4': [-5500],
            '5': [14500],
            '6': [-9000]
            }

        pv_sbj = {
            'YearMonth': ['202403'],
            '101' : [-7500],
            '200' : [-1500],
            '400' : [-5000],
            '490' : [-500],
            '500' : [3000],
            '531' : [1500],
            '590' : [10000]
            }

        expected_s_sbj = pd.DataFrame(s_sbj)
        expected_s_sbj = expected_s_sbj.astype({'Amount': 'int32'})
        expected_pv_cat = pd.DataFrame(pv_cat)
        expected_pv_cat = expected_pv_cat.astype({'1': 'int32', '2': 'int32', '3': 'int32', '4': 'int32', '5': 'int32', '6': 'int32'})
        expected_pv_sbj = pd.DataFrame(pv_sbj)
        expected_pv_sbj = expected_pv_sbj.astype({'101': 'int32', '200': 'int32', '400': 'int32', '490': 'int32', '500': 'int32', '531': 'int32', '590': 'int32'})

        # Validate sums_subject
        assert not test_s_sbj.empty
        assert 'Remarks' in test_s_sbj.columns
        assert 'Amount' in test_s_sbj.columns
        assert 'SubjectCode' in test_s_sbj.columns
        assert 'CategoryNum' in test_s_sbj.columns
        assert 'YearMonth' in test_s_sbj.columns
        pd.testing.assert_frame_equal(test_s_sbj, expected_s_sbj)

        # Validate sums_category
        assert not test_pv_cat.empty
        assert 'YearMonth' in test_pv_cat.columns
        assert '1' in test_pv_cat.columns
        assert '2' in test_pv_cat.columns
        assert '3' in test_pv_cat.columns
        assert '4' in test_pv_cat.columns
        assert '5' in test_pv_cat.columns
        assert '6' in test_pv_cat.columns
        print(test_pv_cat)
        print(expected_pv_cat)
        pd.testing.assert_frame_equal(test_pv_cat, expected_pv_cat)

        # Validate pv_subject
        assert not test_pv_sbj.empty
        assert 'YearMonth' in test_pv_sbj.columns
        assert '101' in test_pv_sbj.columns
        assert '200' in test_pv_sbj.columns
        assert '400' in test_pv_sbj.columns
        assert '490' in test_pv_sbj.columns
        assert '500' in test_pv_sbj.columns
        assert '531' in test_pv_sbj.columns
        assert '590' in test_pv_sbj.columns
        pd.testing.assert_frame_equal(test_pv_sbj, expected_pv_sbj)

    def test_get_monthly_summary_empty(self):
        test_data = self.test_data[self.test_data['YearMonth'] == '202504']
        test_s_sbj, test_s_cat, test_pv_sbj = self.csv_processor.get_monthly_summary(test_data)

        expected_s_sbj = pd.DataFrame(columns=['YearMonth', 'CategoryNum', 'SubjectCode', 'Amount', 'Remarks'])
        expected_s_cat = pd.DataFrame(columns=['YearMonth', 'CategoryNum', 'CategoryName', 'Amount'])
        expected_pv_sbj = pd.DataFrame(columns=['YearMonth'])

        pd.testing.assert_frame_equal(test_s_sbj, expected_s_sbj)
        pd.testing.assert_frame_equal(test_s_cat, expected_s_cat)
        pd.testing.assert_frame_equal(test_pv_sbj, expected_pv_sbj)

    def test_get_date_for_carryover(self):
        ts_last, ts_fst = self.csv_processor.get_date_for_carryover('20240201')
        assert ts_last == '2024-02-29'
        assert ts_fst == '2024-03-01'

    def test_get_carryover_df(self):
        test_df = self.test_data

        for ym in test_df['YearMonth'].unique():
            test_data = test_df[test_df['YearMonth'] == ym]
            self.csv_processor.yearmonth = ym
            s_sbj, pv_cat, pv_sbj = self.csv_processor.get_monthly_summary(test_data)
            test_m_df = self.csv_processor.get_carryover_data(s_sbj, pv_cat)
            test_m_df = self.csv_processor.fill_param(test_m_df)
            print(test_m_df)
            test_df = pd.concat([test_df, test_m_df], ignore_index=True)
            test_df = self.csv_processor.fill_param(test_df)
            test_df = self.csv_processor.sort_csv(test_df)
            test_df = test_df.reset_index(drop=True)
            print(test_df)

            assert not test_df.empty
            assert 'Date' in test_df.columns
            assert 'SubjectCode' in test_df.columns
            assert 'Amount' in test_df.columns
            assert 'Remarks' in test_df.columns
            assert 'Year' in test_df.columns
            assert 'Month' in test_df.columns
            assert 'YearMonth' in test_df.columns
            assert 'ID' in test_df.columns
            assert 'Subject' in test_df.columns
            assert 'CategoryName' in test_df.columns
            assert 'CategoryNum' in test_df.columns

        test_df = self.csv_processor.fill_param(test_df)
        test_df = self.csv_processor.sort_csv(test_df)
        test_df.reset_index(drop=True)
        test_df['Year'] = test_df['Year'].astype('int32')
        test_df['Month'] = test_df['Month'].astype('int32')
        test_df['YearMonth'] = test_df['YearMonth'].astype('str')

        expected_df = pd.read_csv('tests/test_outputs.csv')
        expected_df = expected_df.astype({'Date':'str', 'Amount': 'int32', 'ID': 'str', 'SubjectCode': 'str', 'CategoryNum': 'str'})
        expected_df.reset_index(drop=True)
        expected_df = self.csv_processor.sort_csv(expected_df)

        expected_df['Date'] = pd.to_datetime(expected_df['Date'], format='%Y-%m-%d')
        expected_df['Year'] = expected_df['Year'].astype('int32')
        expected_df['Month'] = expected_df['Month'].astype('int32')
        expected_df['YearMonth'] = expected_df['YearMonth'].astype('str')


        pd.testing.assert_frame_equal(test_df, expected_df, check_dtype=False)
