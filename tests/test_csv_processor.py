import pandas as pd
from processor.processor import CSVProcessor

def test_process_csv_file():
    csv_processor = CSVProcessor('tests/test.csv', None, 'codes.json')
    file_path = '/c:/Users/grand/Documents/HouseholdAccount/tests/test.csv'
    expected_data = [
        {'Date': '2024-03-03', 'ID': '20240303001', 'SubjectCode': '200', 'Amount': '-2000', 'Remarks': 'Starbucks 01', 'Subject': 'Credit Card Debt', 'Year': '2024', 'Month': '3'},
        {'Date': '2024-03-03', 'ID': '20240303002', 'SubjectCode': '101', 'Amount': '-2145', 'Remarks': 'Tempura 02', 'Subject': 'UFJ', 'Year': '2024', 'Month': '3'},
        {'Date': '2024-03-03', 'ID': '20240303003', 'SubjectCode': '101', 'Amount': '-2896', 'Remarks': 'Plug 03', 'Subject': 'UFJ', 'Year': '2024', 'Month': '3'},
    ]

    actual_data = csv_processor.process_csv(file_path)
    assert actual_data == expected_data

def test_caluculate_balances():
    csv_processor = CSVProcessor('tests/test.csv', None, 'codes.json')
    df = pd.read_csv('tests/test.csv')
    processed_df = csv_processor.preprocess_and_pivot(df)
    balances_df = csv_processor.calculate_balances(processed_df)
    expected_data = {
        'YearMonth': ['2024-03', '2024-04'],
        'TotalAssets': [-7500, -7500],
        'TotalLiabilities': [-1500, -1500],
        'TotalIncome': [-5500, -5500],
        'TotalExpenses': [14500, 14500],
        'NetIncome': [-9000, -9000],
        'TotalEquity': [9000, 9000]
    }
    expected_df = pd.DataFrame(expected_data, columns=['YearMonth', 'TotalAssets', 'TotalLiabilities', 'TotalIncome', 'TotalExpenses', 'NetIncome', 'TotalEquity'])

    pd.testing.assert_frame_equal(balances_df, expected_df)

def test_remove_duplicates():
    test_data = {
        'Date' : ['2024-03-03', '2024-03-03', '2024-03-03'],
        'ID' : ['20240303001', '20240303002', '20240303001'],
        'SubjectCode' : ['200', '101', '200'],
        'Amount' : ['-2000', '-2145', '-2000'],
        'Remarks' : ['Starbucks 01', 'Tempura 02', 'Starbucks 01'],
        'Subject' : ['Credit Card Debt', 'UFJ', 'Credit Card Debt'],
        'Year' : ['2024', '2024', '2024'],
        'Month' : ['3', '3', '3']
    }
    test_df = pd.DataFrame(test_data)
    csv_processor = CSVProcessor(None, None, 'codes.json')
    actual_df = csv_processor.remove_duplicates(test_df, 'ID')
    expected_data = {
        'Date' : ['2024-03-03', '2024-03-03'],
        'ID' : ['20240303001', '20240303002'],
        'SubjectCode' : ['200', '101'],
        'Amount' : ['-2000', '-2145'],
        'Remarks' : ['Starbucks 01', 'Tempura 02'],
        'Subject' : ['Credit Card Debt', 'UFJ'],
        'Year' : ['2024', '2024'],
        'Month' : ['3', '3']
    }
    expected_df = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(actual_df, expected_df)

def test_month_end_close():
    csv_processor = CSVProcessor('tests/test.csv', 'tests/test_output.csv', 'codes.json')
