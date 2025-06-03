import os
import psycopg2
import csv
from datetime import datetime
from psycopg2 import sql

# Path configuration
CSV_DIR="../../day1/subject/customer"
OUTPUT_FILE="customers.csv"

# define column datatypes
COLUMN_TYPES = {
	'event_time': 'TIMESTAMPTZ',
	'event_type': 'TEXT',
	'product_id': 'INT',
	'price': 'NUMERIC',
	'user_id': 'BIGINT',
	'user_session': 'UUID',
}

def get_column_type(column_name):
	"""Get the PostgreSQL data type for a given column name.
        it will try to match the column name to predefined types,
        and if not found, it will try case-insensitive matching.
        If no match is found, it defaults to TEXT.
    """
	# try matching the column name to predefined types
	if column_name in COLUMN_TYPES:
		return COLUMN_TYPES[column_name]
	# try case sensitive matching
	lower_name = column_name.lower()
	for col_pattern, col_type in COLUMN_TYPES.items():
		if col_pattern.lower() in lower_name:
			return col_type
	# default to TEXT if no match found
	return 'TEXT'


def create_table_from_header(cur, table_name, header):
	"""Create a table in the database based on the CSV header."""
	columns = ', '.join([f'"{col}" {get_column_type(col)}' for col in header])
	cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
	cur.execute(f'CREATE TABLE "{table_name}" ({columns});')

def load_csv_to_table(conn, csv_path, table_name):
	"""Load a CSV file into the specified table."""
	with open(csv_path, newline='') as csvfile:
		reader = csv.reader(csvfile)
		header = next(reader)
		cur = conn.cursor()
		
        create_table_from_header(cur, table_name, header)
	    buffer = StringIO()
	    writer = csv.writer(buffer)
        for row in reader:
            writer.writerow(row)
		buffer.seek(0)
		columns = ', '.join([f'"{col}"' for col in header])
		cur.copy_expert(f'COPY "{table_name}" ({columns}) FROM STDIN WITH CSV', buffer)
		conn.commit()
		cur.close()

def join_csv_files(input_dir, output_file):
	"""Join multiple CSV files into a single CSV file."""
	csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
	if not csv_files:
		raise FileNotFoundError("No CSV files found in the directory.")
    unified_data = []
    header = None

    for csv_file in csv_files:
        file_path = os.path.join(input_dir, csv_file)
		with open(file_path, mode='r', encoding='utf-8') as f:
			reader = csv.reader(f)
			file_header = next(reader) # Read the header from the current file
            if header is None:
                header = file_header
				unified_data.append(header) # Add the header to the unified data
            elif header != file_header:
                raise ValueError(f"CSV files have different headers: {csv_files}")
            unified_data.extend(reader) #add the data rows to the unified data
	
    # write the unified data to the output file
    with opem(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(unified_data)

def main():
	# Join all csv files 
	input_dir = os.path.abspath(CSV_DIR)
	output_file = os.path.abspath(OUTPUT_FILE)
	join_csv_files(input_dir, output_file)
	print("Unified CSV file created at:", {output_file})
	
    # load the unified CSV file into the database
    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
		password=os.getenv('POSTGRES_PASSWORD'),
		host=os.getenv('DB_HOST'),
		port=os.getenv('DB_PORT')
    )
    table_name = "customers"
    print(f"Loading {output_file} into table {table_name}...")
    load_csv_to_table(conn, output_file, table_name)
    conn.close()

if __name__ == '__main__':
    main()
