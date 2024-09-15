import pandas as pd
from processor.processor import CSVProcessor

# def test_process_csv_file():
#     csv_processor = CSVProcessor('tests/test.csv', None, subjectcodes_path='codes.csv',balance_sheet_path=None)
#     file_path = '/c:/Users/grand/Documents/HouseholdAccount/tests/test.csv'
#     expected_data = [
#         {'Date': '2024-03-03', 'ID': '20240303001', 'SubjectCode': '200', 'Amount': '-2000', 'Remarks': 'Starbucks 01', 'Subject': 'Credit Card Debt', 'Year': '2024', 'Month': '3'},
#         {'Date': '2024-03-03', 'ID': '20240303002', 'SubjectCode': '101', 'Amount': '-2145', 'Remarks': 'Tempura 02', 'Subject': 'UFJ', 'Year': '2024', 'Month': '3'},
#         {'Date': '2024-03-03', 'ID': '20240303003', 'SubjectCode': '101', 'Amount': '-2896', 'Remarks': 'Plug 03', 'Subject': 'UFJ', 'Year': '2024', 'Month': '3'},
#     ]

#     actual_data = csv_processor.process_csv(file_path)
#     assert actual_data == expected_data

# def test_remove_duplicates():
#     test_data = {
#         'Date' : ['2024-03-03', '2024-03-03', '2024-03-03'],
#         'ID' : ['20240303001', '20240303002', '20240303001'],
#         'SubjectCode' : ['200', '101', '200'],
#         'Amount' : ['-2000', '-2145', '-2000'],
#         'Remarks' : ['Starbucks 01', 'Tempura 02', 'Starbucks 01'],
#         'Subject' : ['Credit Card Debt', 'UFJ', 'Credit Card Debt'],
#         'Year' : ['2024', '2024', '2024'],
#         'Month' : ['3', '3', '3']
#     }
#     test_df = pd.DataFrame(test_data)
#     csv_processor = CSVProcessor(None, None, subjectcodes_path='codes.csv',balance_sheet_path=None)
#     actual_df = csv_processor.remove_duplicates(test_df)
#     expected_data = {
#         'Date' : ['2024-03-03', '2024-03-03'],
#         'ID' : ['20240303001', '20240303002'],
#         'SubjectCode' : ['200', '101'],
#         'Amount' : ['-2000', '-2145'],
#         'Remarks' : ['Starbucks 01', 'Tempura 02'],
#         'Subject' : ['Credit Card Debt', 'UFJ'],
#         'Year' : ['2024', '2024'],
#         'Month' : ['3', '3']
#     }
#     expected_df = pd.DataFrame(expected_data)
#     pd.testing.assert_frame_equal(actual_df, expected_df)

def test_find_by_id():
    csv_processor = CSVProcessor(None, None, subjectcodes_path='codes.csv',balance_sheet_path=None)
    test = "100"
    subject = csv_processor.find_by_id(test)
    assert subject == "Cash"


def test_get_subject_name():
    csv_processor = CSVProcessor(None, None, subjectcodes_path='codes.csv',balance_sheet_path=None)
    test_data = {
        'Date' : ['2024-03-03', '2024-03-03'],
        'ID' : ['20240303001', '20240303002'],
        'SubjectCode' : ['200', '101'],
        'Amount' : ['-2000', '-2145'],
        'Remarks' : ['Starbucks 01', 'Tempura 02'],
        'Year' : ['2024', '2024'],
        'Month' : ['3', '3']
    }
    test_df = pd.DataFrame(test_data)
    actual_df = csv_processor.apply_subject_from_code(test_df)
    expected_data = {
        'Date' : ['2024-03-03', '2024-03-03'],
        'ID' : ['20240303001', '20240303002'],
        'SubjectCode' : ['200', '101'],
        'Amount' : ['-2000', '-2145'],
        'Remarks' : ['Starbucks 01', 'Tempura 02'],
        'Year' : ['2024', '2024'],
        'Month' : ['3', '3'],
        'Subject' : ['Credit Card Debt', 'UFJ']
    }
    expected_df = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(actual_df, expected_df)