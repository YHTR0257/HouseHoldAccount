from processor.processor import CSVProcessor

if __name__ == '__main__':
    csv_processor = CSVProcessor(input_file='datas/financial_records.csv',
                                 output_file='datas/results_financial.csv',
                                 subjectcodes_path='codes.csv',
                                 summary_file='datas/summary.csv',)
    csv_processor.process_csv()
