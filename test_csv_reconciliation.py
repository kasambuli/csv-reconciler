import tempfile
import os
import shutil
from csv_reconciller import extract_column_data, read_csv
import unittest
from unittest.mock import patch
from csv_reconciller import csv_reconciler, parse_arguments


class TestCSVReconciler(unittest.TestCase):

    @patch('builtins.print')  # To suppress print statements during tests
    @patch('sys.argv', ['test_csv_reconciliation.py', '-s', 'source.csv', '-t', 'target.csv', '-o', 'reconciliation_report.csv'])
    def test_parse_arguments(self):
        args = parse_arguments()
        self.assertEqual(args.source, 'source.csv')
        self.assertEqual(args.target, 'target.csv')
        self.assertEqual(args.output, 'reconciliation_report.csv')

    @patch('builtins.print')  # To suppress print statements during tests
    def test_csv_reconciler(self):
        with patch('csv_reconciller.read_csv', return_value=([], [])):
            csv_reconciler('source.csv', 'target.csv', 'reconciliation_report.csv')


    def setUp(self):
        # Create temporary directory for test files
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        # Remove temporary directory and its contents
        shutil.rmtree(self.test_dir)

    def create_test_file(self, file_name, content):
        file_path = os.path.join(self.test_dir, file_name)
        with open(file_path, 'w') as file:
            file.write(content)
        return file_path

    def test_parse_arguments(self):
        # Test argument parsing
        args = parse_arguments()
        args.source = 'source.csv'
        args.target = 'target.csv'
        args.output = 'reconciliation_report.csv'
        self.assertEqual(args.source, 'source.csv')
        self.assertEqual(args.target, 'target.csv')
        self.assertEqual(args.output, 'reconciliation_report.csv')

    def test_read_csv(self):
        # Test read_csv function
        file_content = "ID,Name,Date,Amount\n001,John Doe,2023-01-01,100.00\n002,Jane Smith,2023-01-02,200.50"
        file_path = self.create_test_file('test_file.csv', file_content)

        data, headers = read_csv(file_path)

        self.assertEqual(data, [
            ['ID', 'Name', 'Date', 'Amount'],
            ['001', 'John Doe', '2023-01-01', '100.00'],
            ['002', 'Jane Smith', '2023-01-02', '200.50']
        ])
        self.assertEqual(headers, ['ID', 'Name', 'Date', 'Amount'])

    def test_extract_column_data(self):
        # Test extract_column_data function
        data = [
            ['ID', 'Name', 'Date', 'Amount'],
            ['001', 'John Doe', '2023-01-01', '100.00'],
            ['002', 'Jane Smith', '2023-01-02', '200.50']
        ]

        result = extract_column_data(data, 0)
        self.assertEqual(result, ['001', '002'])

        result = extract_column_data(data, 2)
        self.assertEqual(result, ['2023-01-01', '2023-01-02'])

    # Add more test cases for other functions...

if __name__ == '__main__':
    unittest.main()
