import unittest
import csv
from processor import process_csv_file

class TestCSVProcessing(unittest.TestCase):
    def test_process_csv_file(self):
        file_path = '/c:/Users/grand/Documents/HouseholdAccount/datas/test.csv'
        expected_data = [
            {'Date': '2024-03-03', 'ID': '20240303001', 'SubjectCode': '200', 'Amount': '-2000', 'Remarks': 'Starbucks 01', 'Subject': 'Credit Card Debt', 'Year': '2024', 'Month': '3'},
            {'Date': '2024-03-03', 'ID': '20240303002', 'SubjectCode': '101', 'Amount': '-2145', 'Remarks': 'Tempura 02', 'Subject': 'UFJ', 'Year': '2024', 'Month': '3'},
            {'Date': '2024-03-03', 'ID': '20240303003', 'SubjectCode': '101', 'Amount': '-2896', 'Remarks': 'Plug 03', 'Subject': 'UFJ', 'Year': '2024', 'Month': '3'},
            # Add more expected data here
        ]

        actual_data = process_csv_file(file_path)
        self.assertEqual(actual_data, expected_data)

if __name__ == '__main__':
    unittest.main()