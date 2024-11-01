import pandas as pd
import datetime as dt
import pytest

from processor.processor import CSVProcessor

class TestCSVProcessor:
    def setup_class(self):
        self.csv_processor = CSVProcessor(None, None, subjectcodes_path='codes.csv', summary_file=None)
        self.test_data = pd.read_csv('tests/test.csv')
        self.test_data = self.csv_processor.fill_param(self.test_data)
        self.test_data = self.csv_processor.add_yearmonth_column(self.test_data)

    def test_find_by_id(self):
        test = "100"
        subject = self.csv_processor.find_by_id(test)
        assert subject.loc[0, 'category_name'] == "Assets"
        assert subject.loc[0, 'subject'] == "Cash"

    def test_fill_param(self):
        test_data = {
            'Date' : ['20240303', '20240303'],
            'SubjectCode' : ['200', '101'],
            'Amount' : ['-2000', '-2145'],
            'Remarks' : ['Starbucks 001', 'Tempura 002'],
            'Year' : ['2024', '2024'],
            'Month' : ['3', '3']
        }
        test_df = pd.DataFrame(test_data)
        actual_df = self.csv_processor.fill_param(test_df)
        expected_data = {
            'Date' : ['20240303', '20240303'],
            'SubjectCode' : ['200', '101'],
            'Amount' : ['-2000', '-2145'],
            'Remarks' : ['Starbucks 001', 'Tempura 002'],
            'Year' : ['2024', '2024'],
            'Month' : ['3', '3'],
            'Subject' : ['Credit Card Debt', 'UFJ'],
            'CategoryName' : ['Liabilities', 'Assets'],
            'CategoryNum': ['2', '1'],
            'ID' : ['202403030010', '202403030020']
        }
        expected_df = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(actual_df, expected_df, check_dtype=False)

    def test_add_yearmonth_column(self):
        test_data = {
            'Date' : ['20240303', '20240303'],
            'SubjectCode' : ['200', '101'],
            'Amount' : ['-2000', '-2145'],
            'Remarks' : ['Starbucks 001', 'Tempura 002']
        }
        test_df = pd.DataFrame(test_data)
        actual_df = self.csv_processor.add_yearmonth_column(test_df)
        expected_data = {
            'Date' : ['20240303', '20240303'],
            'SubjectCode' : ['200', '101'],
            'Amount' : ['-2000', '-2145'],
            'Remarks' : ['Starbucks 001', 'Tempura 002'],
            'Year' : ['2024', '2024'],
            'Month' : ['3', '3'],
            'YearMonth' : ['202403', '202403']
        }
        expected_df = pd.DataFrame(expected_data)
        for index, row in expected_df.iterrows():
            expected_df.loc[index, 'Date'] = dt.datetime.strptime(row['Date'], '%Y%m%d')
        expected_df["Year"] = expected_df["Year"].astype('int32')
        actual_df['Year'] = actual_df['Year'].astype('int32')
        expected_df["Month"] = expected_df["Month"].astype('int32')
        actual_df['Month'] = actual_df['Month'].astype('int32')
        expected_df["YearMonth"] = expected_df["YearMonth"].astype('str')
        actual_df['YearMonth'] = actual_df['YearMonth'].astype('str')
        pd.testing.assert_frame_equal(actual_df, expected_df, check_dtype=False)
