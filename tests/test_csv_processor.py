import pandas as pd
from processor.processor import CSVProcessor

def test_find_by_id():
    csv_processor = CSVProcessor(None, None, subjectcodes_path='codes.csv',summary_file=None)
    test = "100"
    subject = csv_processor.find_by_id(test)
    assert subject.loc[0, 'category_name'] == "Assets"
    assert subject.loc[0, 'subject'] == "Cash"

def test_fill_param():
    csv_processor = CSVProcessor(None, None, subjectcodes_path='codes.csv',summary_file=None)
    test_data = {
        'Date' : ['20240303', '20240303'],
        'SubjectCode' : ['200', '101'],
        'Amount' : ['-2000', '-2145'],
        'Remarks' : ['Starbucks 001', 'Tempura 002'],
        'Year' : ['2024', '2024'],
        'Month' : ['3', '3']
    }
    test_df = pd.DataFrame(test_data)
    actual_df = csv_processor.fill_param(test_df)
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
