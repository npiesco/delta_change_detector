# detector.py
import json
import os
from deltalake import DeltaTable
import pyarrow.parquet as pq
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_delta_log(delta_log_path):
    with open(delta_log_path, 'r') as log_file:
        return [json.loads(line) for line in log_file]

def extract_log_info(log_entries):
    add_paths = []
    remove_paths = []
    mode = None
    for entry in log_entries:
        if 'add' in entry:
            add_paths.append(entry['add']['path'])
        if 'remove' in entry:
            remove_paths.append(entry['remove']['path'])
        if 'commitInfo' in entry and 'operationParameters' in entry['commitInfo']:
            mode = entry['commitInfo']['operationParameters'].get('mode')
    return add_paths, remove_paths, mode

def detect_changes(delta_path, id_column, column_name, id_value):
    try:
        logging.info(f"Attempting to open Delta table at: {delta_path}")
        
        delta_table = DeltaTable(delta_path)
        history = delta_table.history()
        logging.info(f"Successfully opened Delta table. History length: {len(history)}")

        new_value, change_version, records = None, None, []
        found_matching_record = False
        matching_record_printed = False

        for version in reversed(range(len(history))):
            version_info = history[version]
            version_num = version_info['version']
            operation = version_info['operation']
            timestamp = datetime.fromtimestamp(version_info['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
            
            delta_log_path = os.path.join(delta_path, '_delta_log', f'{version_num:020}.json')
            logging.debug(f"Processing version {version_num} with log path {delta_log_path}")
            
            try:
                with open(delta_log_path, 'r') as log_file:
                    log_entries = [json.loads(line) for line in log_file]
                logging.debug(f"Successfully read log file {delta_log_path}")
            except Exception as e:
                logging.error(f"Error reading log file {delta_log_path}: {str(e)}")
                continue

            add_paths, remove_paths, mode = extract_log_info(log_entries)

            version_table = DeltaTable(delta_path, version=version_num)
            current_value = None

            for file in version_table.files():
                try:
                    table = pq.read_table(os.path.join(delta_path, file))
                    logging.debug(f"Reading Parquet file: {file}")
                    id_column_data = table.column(id_column)
                    column_data = table.column(column_name)
                    
                    for i in range(len(id_column_data)):
                        id_value_str = str(id_value)
                        id_column_value_str = str(id_column_data[i].as_py())
                        logging.debug(f"Comparing id_value: {id_value_str} with id_column_value: {id_column_value_str}")
                        if id_column_value_str == id_value_str:
                            found_matching_record = True
                            current_value = column_data[i].as_py()
                            if not matching_record_printed:
                                logging.info(f"Found matching record: {current_value}")
                                matching_record_printed = True
                            break
                    if found_matching_record:
                        break
                except Exception as e:
                    logging.error(f"Error reading parquet file {file}: {str(e)}")

            if current_value is not None:
                if new_value is None:
                    new_value = current_value
                elif current_value != new_value:
                    old_value = current_value
                    change_version = version_num

                    if version + 1 < len(history):
                        previous_version_info = history[version + 1]
                        previous_version_num = previous_version_info['version']
                        previous_delta_log_path = os.path.join(delta_path, '_delta_log', f'{previous_version_num:020}.json')
                        
                        try:
                            with open(previous_delta_log_path, 'r') as log_file:
                                previous_log_entries = [json.loads(line) for line in log_file]
                            logging.debug(f"Successfully read previous log file {previous_delta_log_path}")
                        except Exception as e:
                            logging.error(f"Error reading previous log file {previous_delta_log_path}: {str(e)}")
                            continue

                        previous_add_paths, previous_remove_paths, previous_mode = extract_log_info(previous_log_entries)
                        previous_parquet_file_path = previous_add_paths[0] if previous_add_paths else None
                        previous_operation = previous_version_info['operation']
                        previous_timestamp = datetime.fromtimestamp(previous_version_info['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')

                        records.append({
                            "id_column": id_column,
                            "original_record": True,
                            "modified_record": False,
                            "old_value": old_value,
                            "new_value": None,
                            "parquet_file_path": previous_parquet_file_path,
                            "delta_log_path": previous_delta_log_path,
                            "operation": previous_operation,
                            "mode": previous_mode,
                            "timestamp": previous_timestamp,
                            "version": previous_version_num
                        })

                    records.append({
                        "id_column": id_column,
                        "original_record": False,
                        "modified_record": True,
                        "old_value": old_value,
                        "new_value": new_value,
                        "parquet_file_path": add_paths[0] if add_paths else None,
                        "delta_log_path": delta_log_path,
                        "operation": operation,
                        "mode": mode,
                        "timestamp": timestamp,
                        "version": change_version
                    })
                    break

        if not found_matching_record:
            missing_record_message = f"No records found matching {id_column} = {id_value}"
            logging.info(missing_record_message)
            print(missing_record_message)
        elif change_version is None:
            no_change_message = f"No changes detected for {column_name} where {id_column} = {id_value}"
            logging.info(no_change_message)
            print(no_change_message)
        else:
            print("Changes detected:")
            for record in records:
                print(f"Version: {record.get('version')}")
                print(f"Operation: {record.get('operation')}")
                print(f"Mode: {record.get('mode')}")
                print(f"ID Column: {record.get('id_column')}")
                print(f"Old Value: {record.get('old_value')}")
                print(f"New Value: {record.get('new_value')}")
                print(f"Timestamp: {record.get('timestamp')}")
                print(f"Parquet File Path: {record.get('parquet_file_path')}")
                print(f"Delta Log Path: {record.get('delta_log_path')}")
                print(f"Original Record: {record.get('original_record')}")
                print(f"Modified Record: {record.get('modified_record')}")
                print("---")

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        logging.error(error_message)
        print(error_message)