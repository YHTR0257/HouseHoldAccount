import unittest
import csv
import os
import json


def process_csv_file(file_path):
    """Read the first three negative transactions and enrich the data."""
    codes_path = os.path.join(os.path.dirname(__file__), '..', 'codes.json')
    with open(codes_path, 'r', encoding='utf-8') as f:
        codes = json.load(f)
    code_map = {v['id']: k for k, v in codes.items()}

    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        count = 1
        for row in reader:
            if row['Amount'].startswith('-'):
                date = row['Date']
                row['ID'] = f"{date.replace('-', '')}{count:03d}"
                row['Subject'] = code_map.get(row['SubjectCode'], '')
                row['Year'] = date.split('-')[0]
                row['Month'] = str(int(date.split('-')[1]))
                data.append(row)
                count += 1
                if len(data) == 3:
                    break
    return data

class TestCSVProcessing(unittest.TestCase):
    def test_process_csv_file(self):
        file_path = os.path.join(os.path.dirname(__file__), 'test.csv')
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

