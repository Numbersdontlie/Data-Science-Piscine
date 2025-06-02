from dotenv import load_dotenv
load_dotenv()
import psycopg2
import os

def get_csv_path():
    """Determine the correct path to the CSV file"""
    # Try absolute path first
    abs_path = '/home/lperez-h/sgoinfre/Data_Science/DS0/subject/item/items.csv'
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

def load_items_csv(conn, csv_path):
	"""Load items data from a CSV file into the items table."""
	with open(csv_path, newline='') as csvfile:
		reader = csv.reader(csvfile)
		header = next(reader)
		with conn.cursor() as cur:
			for row in reader:
				cur.execute(
					"INSERT INTO items (product_id, category_id, category_code, brand) VALUES (%s, %s, %s, %s)",
					row
				)
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
	load_items_csv(conn, '/home/lperez-h/sgoinfre/Data_Science/DS0/subject/item/items.csv')
	conn.close()

if __name__ == '__main__':
	main()


