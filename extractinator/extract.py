from datetime import datetime
from pathlib import Path
import csv
import json
import sqlite3
import hashlib

NAME_CONFIG_FILE = "extractinator/extractinator_mappings.json"
NAME_SQLITE_DB = "extractinator/supermarket_1048576.db"
PROGRESS_BAR_INCREMENT_EVERY = 10000


def print_intro():
    print(r"""           _                  _   _             _             
          | |                | | (_)           | |            
  _____  _| |_ _ __ __ _  ___| |_ _ _ __   __ _| |_ ___  _ __ 
 / _ \ \/ / __| '__/ _` |/ __| __| | '_ \ / _` | __/ _ \| '__|
|  __/>  <| |_| | | (_| | (__| |_| | | | | (_| | || (_) | |   
 \___/_/\_\\__|_|  \__,_|\___|\__|_|_| |_|\__,_|\__\___/|_|   """)


def get_hash_columns(path_config_file):
    cfg_data = {}
    with open(path_config_file, "r") as file:
        cfg_data = json.load(file)

    hash_columns = {}

    for table in cfg_data:
        if not cfg_data[table]:  # this table has no direct data to extract/hash
            continue

        hash_columns[table] = cfg_data[table]

    return hash_columns


def execute_sqlite_query(query, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    res = cursor.execute(query)
    col_names = [description[0] for description in cursor.description]
    rows = res.fetchall()

    cursor.close()
    conn.close()

    return col_names, rows


def generate_hash(row_dict, hash_columns):
    concatenated = "".join([str(row_dict[col]) if row_dict[col] is not None else "" for col in hash_columns])
    return hashlib.sha256(concatenated.encode("utf-8")).hexdigest().upper()


def main():
    print_intro()

    hash_columns = get_hash_columns(NAME_CONFIG_FILE)

    datetime_string_now = datetime.now().strftime("%Y%m%d.%H%M%S")
    print(f"Generating output files at extractinator/output/{datetime_string_now}.")

    output_dir = Path(f"extractinator/output/{datetime_string_now}")
    output_dir.mkdir(parents=True, exist_ok=True)

    for table_name, hash_cols in hash_columns.items():
        print(f"Querying table {table_name}, using hash columns {hash_cols}.")
        query = f"SELECT * FROM {table_name};"

        # newline="" is necessary since csvwriter explicitly writes \r\n after each row
        with open(file=f"extractinator/output/{datetime_string_now}/{table_name}~{datetime_string_now}.csv", mode="w", encoding="utf-8", newline="") as f:
            conn = sqlite3.connect(NAME_SQLITE_DB)
            cursor = conn.cursor()

            cursor.execute(query)
            col_names = [description[0] for description in cursor.description]

            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

            f.write(f"{",".join(col_names)},admw_file_hash\n")

            i = 0
            for row in cursor:
                row_dict = {col_name: value for col_name, value in zip(col_names, row)}   # map column names to values
                hash_value = generate_hash(row_dict, hash_cols)

                row_to_write = list(row) + [hash_value]
                writer.writerow(row_to_write)

                if i > 0 and i % PROGRESS_BAR_INCREMENT_EVERY == 0:
                    print(f"{i}...", end="")

                i += 1

            print("Done!")
            print(f"Written {i} rows to extractinator/output/{datetime_string_now}/{table_name}~{datetime_string_now}.csv.")

            cursor.close()
            conn.close()

    print("All tables have been processed. Exiting.")


if __name__ == "__main__":
    main()
