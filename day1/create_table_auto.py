from dotenv import load_dotenv
load_dotenv()
import psycopg2
import os
import csv
from io import StringIO

CSV_DIR = "./subject/customer"

# define column datatypes
COLUMN_TYPES = {
	'event_time': 'TIMESTAMPTZ',
	'event_type': 'TEXT',
	'product_id': 'BIGINT',
	'price': 'NUMERIC',
	'user_id': 'BIGINT',
	'user_session': 'UUID',
}

def sanitize_table_name(filename):
	"""Sanitize the filename to create a valid table name."""
	return os.path.splitext(os.path.basename(filename))[0].replace("-", "_").replace(" ", "_")

def get_column_type(column_name):
	"""Get the PostgreSQL data type for a given column name."""
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

def main():
	conn = psycopg2.connect(
		dbname=os.getenv('POSTGRES_DB'),
		user=os.getenv('POSTGRES_USER'),
		password=os.getenv('POSTGRES_PASSWORD'),
		host=os.getenv('DB_HOST'),
		port=os.getenv('DB_PORT')
	)
	for filename in os.listdir(CSV_DIR):
		if filename.endswith('.csv'):
			csv_path = os.path.join(CSV_DIR, filename)
			table_name = sanitize_table_name(filename)
			print(f"Loading {csv_path} into table {table_name}...")
			load_csv_to_table(conn, csv_path, table_name)
	conn.close()

if __name__ == '__main__':
	main()
			