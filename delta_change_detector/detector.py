# detector.py
# detector.py
import json
import os
from deltalake import DeltaTable
import pyarrow.parquet as pq
from datetime import datetime

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
        print(f"Attempting to open Delta table at: {delta_path}")
        
        delta_table = DeltaTable(delta_path)
        history = delta_table.history()

        new_value, change_version, records = None, None, []
        found_matching_record = False

        for version in reversed(range(len(history))):
            version_info = history[version]
            version_num = version_info['version']
            operation = version_info['operation']
            timestamp = datetime.fromtimestamp(version_info['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
            
            delta_log_path = os.path.join(delta_path, '_delta_log', f'{version_num:020}.json')
            
            try:
                with open(delta_log_path, 'r') as log_file:
                    log_entries = [json.loads(line) for line in log_file]
            except Exception as e:
                print(f"Error reading log file {delta_log_path}: {str(e)}")
                continue

            add_paths, remove_paths, mode = extract_log_info(log_entries)

            version_table = DeltaTable(delta_path, version=version_num)
            current_value = None

            for file in version_table.files():
                try:
                    table = pq.read_table(os.path.join(delta_path, file))
                    id_column_data = table.column(id_column)
                    column_data = table.column(column_name)
                    
                    for i in range(len(id_column_data)):
                        if id_column_data[i].as_py() == id_value:
                            found_matching_record = True
                            current_value = column_data[i].as_py()
                            break
                    if found_matching_record:
                        break
                except Exception as e:
                    print(f"Error reading parquet file {file}: {str(e)}")

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
                        except Exception as e:
                            print(f"Error reading previous log file {previous_delta_log_path}: {str(e)}")
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
            return {"error": f"No records found matching {id_column} = {id_value}"}
        elif change_version is None:
            return {"info": f"No changes detected for {column_name} where {id_column} = {id_value}"}
        else:
            return records

    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}