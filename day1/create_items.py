from dotenv import load_dotenv
load_dotenv()
import psycopg2
import os
import csv

def get_csv_path():
    """Determine the correct path to the CSV file"""
    # Try absolute path first
    abs_path = '/home/luifer/Numbersdontlie/Data-Science-Piscine/data/item/item.csv'
    if os.path.exists(abs_path):
        return abs_path
    
    # Try relative path from script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    rel_path = os.path.join(script_dir, 'subject', 'item', 'items.csv')
    if os.path.exists(rel_path):
        return rel_path
    
    # Try environment variable
    env_path = os.getenv('CSV_PATH')
    if env_path and os.path.exists(env_path):
        return env_path
    
    raise FileNotFoundError("Could not locate items.csv file")

# create table if not exists and also define table schema 
ITEMS_SCHEMA = """
    CREATE TABLE IF NOT EXISTS items (
        product_id INTEGER,
        category_id BIGINT,
        category_code TEXT,
        brand TEXT
    );
"""

def create_items_table(conn):
	""" create the items table in the postgreSQL database and provide the schema"""
	with conn.cursor() as cur:
		cur.execute(ITEMS_SCHEMA)
	conn.commit()

def coerce_int(value):
	"""Coerce a value to an integer, returning None if it fails."""
	if value is None:
		return None
	v = str(value).strip()
	if v == '':
		return None
	try:
		return int(v)
	except ValueError:
		return None

def load_items_csv(conn, csv_path):
    """Load items data from a CSV file into the items table."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"{csv_path} not found")

    rows_to_insert = []
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader, start=2):  # start=2 accounts for header line
            try:
                product_id = coerce_int(row.get('product_id') or row.get('id') or '')
                category_id = coerce_int(row.get('category_id') or '')
                category_code = row.get('category_code') or None
                brand = row.get('brand') or None
                rows_to_insert.append((product_id, category_id, category_code, brand))
            except Exception as e:
                # log and skip malformed row
                print(f"Skipping row {i} due to parse error: {e} -- {row}")

    if not rows_to_insert:
        print("No rows to insert.")
        return

    with conn.cursor() as cur:
        try:
            cur.executemany(
                "INSERT INTO items (product_id, category_id, category_code, brand) VALUES (%s, %s, %s, %s)",
                rows_to_insert
            )
        except Exception as e:
            # fallback: try inserting rows one by one to identify bad rows
            print("Batch insert failed, retrying row-by-row to identify problematic rows...")
            for idx, r in enumerate(rows_to_insert, start=1):
                try:
                    cur.execute(
                        "INSERT INTO items (product_id, category_id, category_code, brand) VALUES (%s, %s, %s, %s)",
                        r
                    )
                except Exception as e2:
                    print(f"Failed to insert row #{idx}: {r} -> {e2}")
                    # choose to continue or raise; here we continue
            # end row-by-row attempt
    conn.commit()

def main():
	conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
	create_items_table(conn)
	load_items_csv(conn, '/home/luifer/Numbersdontlie/Data-Science-Piscine/data/item/item.csv')
	conn.close()

if __name__ == '__main__':
	main()


