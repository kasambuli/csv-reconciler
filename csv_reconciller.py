import csv
import sys
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(description="CSV Reconciler Tool")
    parser.add_argument('-s', '--source', required=True, help='Path to the source CSV file')
    parser.add_argument('-t', '--target', required=True, help='Path to the target CSV file')
    parser.add_argument('-o', '--output', required=True, help='Path to save the output reconciliation report')
    return parser.parse_args()


def csv_reconciler(source_path: str, target_path: str, output_file: str) -> None:
    try:
        source_data, source_headers = read_csv(source_path)
        target_data, target_headers = read_csv(target_path)
    except FileNotFoundError as err:
        print(f"Error: {err}")
        sys.exit(1)
    except csv.Error as err:
        print(f"Error reading CSV file: {err}")
        sys.exit(1)

    source_first_column_data = extract_column_data(source_data, 0)
    target_first_column_data = extract_column_data(target_data, 0)

    found_discrepancies = find_discrepancies(source_data, target_data)

    id_comparison_result = id_comparison(source_first_column_data, target_first_column_data)
    index_discrepancies = index_discrepancy(id_comparison_result, source_data, target_data)
    data_size_check = data_size_discrepancy(source_data, target_data)

    generate_report(output_file, index_discrepancies, data_size_check, found_discrepancies)


def read_csv(file_path: str) -> tuple:
    with open(file_path, 'r') as csv_file:
        data = list(csv.reader(csv_file))
        headers = data[0] if data else None
        return data, headers


def generate_report(output_file: str, index_discrepancies: list, data_size_check: list,
                    found_discrepancies: list) -> None:
    headers = ["Type", "Record Identifier", "Field", "Source Value", "Target Value"]
    with open(output_file, 'w', newline='') as csv_report:
        writer = csv.writer(csv_report)
        writer.writerow(headers)

        for row in index_discrepancies:
            writer.writerow(row)

        for row in data_size_check:
            writer.writerow(row)

        for row in found_discrepancies:
            writer.writerow(row)

    print("Reconciliation completed:")
    print(f"- Records missing in target: {len(index_discrepancies)}")
    print(f"- Records missing in source: {len(index_discrepancies)}")
    print(f"- Records with field discrepancies: {len(found_discrepancies)}")
    print(f"Report saved to: {output_file}")


def data_size_discrepancy(source_data: list, target_data: list) -> list:
    result = []
    if len(source_data) != len(target_data):
        err = (
            f"Data Discrepancy, Data length inconsistent. Source has {len(source_data)} records compared to {len(target_data)} records in the Target")
        result.append([err])
    return result


def find_discrepancies(source_data: list, target_data: list) -> list:
    source_headers = source_data[0] if source_data else []
    target_headers = target_data[0] if target_data else []

    common_headers = get_common_headers(source_headers, target_headers)
    discrepancies = []

    if source_data is None or target_data is None:
        print(f"No Data Contained in Source{source_data} or Target {target_data}")
        return discrepancies

    if common_headers:
        for idx, (source_row, target_row) in enumerate(zip(source_data[1:], target_data[1:]), start=1):
            discrepancies.extend(field_discrepancy(source_row, target_row, common_headers))

    return discrepancies


def field_discrepancy(source_row: list, target_row: list, common_headers: list) -> list:
    discrepancies = []

    for header in common_headers:
        source_idx = common_headers.index(header)
        target_idx = common_headers.index(header)

        source_value = source_row[source_idx]
        target_value = target_row[target_idx]

        if source_value != target_value:
            discrepancy_data = ["Field Discrepancy", source_idx | target_idx, header, source_value, target_value]
            discrepancies.append(discrepancy_data)

    return discrepancies


def get_common_headers(source_headers: list, target_headers: list) -> list:
    return list(set(source_headers) & set(target_headers))


def index_discrepancy(comparison: dict, source_data: list, target_data: list) -> list:
    temp = []
    result = []

    for comparison in comparison.keys():
        if comparison == 'target':
            idx = comparison.get(comparison)[0] + 1
            temp.append(f'Missing in source, {target_data[idx]}')
        if comparison == 'source':
            idx = comparison.get(comparison)[0] + 1
            temp.append(f'Missing in target, {source_data[idx]}')

    for item in temp:
        arr_fmt = str(item).replace("[", "")
        arr_fmt = str(arr_fmt).replace("]", "")
        res = str(arr_fmt).replace("'", "")
        result.append([res])

    return result


def extract_column_data(data: list, column_index: int) -> list:
    if data:
        return [row[column_index] for row in data[1:]]
    return []


def id_comparison(source: list, target: list) -> dict:
    missing_in_target = [idx for idx, source_id in enumerate(source) if source_id not in target]
    missing_in_source = [idx for idx, target_id in enumerate(target) if target_id not in source]

    result = {}
    if missing_in_target:
        result['target'] = missing_in_target
    if missing_in_source:
        result['source'] = missing_in_source

    return result


# try:
#     csv_reconciler('source.csv', 'target.csv', 'reconciliation_report.csv')
# except Exception as e:
#     print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    args = parse_arguments()
    csv_reconciler(args.source, args.target, args.output)
