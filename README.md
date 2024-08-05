# Delta Table Change Detection

This Python script is designed to detect changes in Delta Lake tables by examining the Delta log files. It provides detailed information about changes to specific columns in the table, including before/after values as well as the operations that caused said changes.

## Features

- Parses Delta Lake log files to extract metadata about changes.
- Detects changes to specified columns based on a given identifier.
- Returns detailed records of changes, including the version, operation, and timestamps.
- Provides detailed information about original and modified records, including the file paths and modes.

## Requirements

- Python 3.x
- `deltalake` package
- `pyarrow` package

## Installation

To use this script, ensure you have Python installed and the required packages. You can install the packages using pip:

```bash
pip install deltalake pyarrow
```

## Usage

The main function provided by this script is `detect_changes`, which analyzes changes to a specific column for a given ID in a Delta Lake table.

### Function: `detect_changes`

#### Parameters:

- `delta_path` (str): Path to Delta table.
- `id_column` (str): Column used as an identifier to match records.
- `column_name` (str): Column whose changes you want to track.
- `id_value` (str or int): Value of identifier to search for in table.

#### Returns:

- A dictionary containing error or information messages.
- A list of records detailing changes if any were detected, including:
  - **id_column**: Identifier column.
  - **original_record**: Boolean indicating if record is original.
  - **modified_record**: Boolean indicating if record is modified.
  - **old_value**: Previous value of column.
  - **new_value**: New value of column.
  - **parquet_file_path**: Path to Parquet file containing record.
  - **delta_log_path**: Path to Delta log file.
  - **operation**: Operation that caused the change.
  - **mode**: Mode of operation.
  - **timestamp**: Timestamp of change.
  - **version**: Version number of Delta table.

#### Example:

```python
delta_path = "/path/to/delta/table"
id_column = "user_id"
column_name = "status"
id_value = 12345

changes = detect_changes(delta_table_path, unique_col, tracked_col, unique_value)
print(changes)
```

#### Example Output:
```
2024-08-05 00:40:08,784 - INFO - Attempting to open Delta table at: /path/to/delta/table
2024-08-05 00:40:08,822 - INFO - Successfully opened Delta table. History length: 40
2024-08-05 00:40:08,919 - INFO - Found matching record: Nicholas
Changes detected:
Version: 14
Operation: WRITE
Old Value: Nick
New Value: None
Timestamp: 2024-08-02 16:30:44
Parquet File Path: 14-a06fd51b-2253-4436-aa03-7caf3311115d-0.parquet
Delta Log Path: /path/to/delta/table/_delta_log/00000000000000000014.json
---
Version: 15
Operation: WRITE
Old Value: Nick
New Value: Nicholas
Timestamp: 2024-08-02 16:30:44
Parquet File Path: 15-2365a517-edd4-4a3c-b070-8c44bd24944d-0.parquet
Delta Log Path: /path/to/delta/table/_delta_log/00000000000000000015.json
---
None
```

## Functions

### `parse_delta_log(delta_log_path)`

- Parses Delta Lake log file and returns log entries.

### `extract_log_info(log_entries)`

- Extracts added paths, removed paths, and mode from log entries.

### `detect_changes(delta_path, id_column, column_name, id_value)`

- Analyzes Delta table's history to detect changes for specific column and identifier.

## Error Handling

The script includes error handling to manage file reading issues and log parsing errors. It will provide error messages when issues are encountered during execution.

## License

This project is licensed under the MIT License.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for suggestions or bug reports.

## Author

[Nicholas G. Piesco](https://github.com/npiesco/delta_change_detector)