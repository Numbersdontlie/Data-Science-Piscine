from dotenv import load_dotenv
load_dotenv()
import os
import psycopg2

def main():
	"""Function to create join customers and items table."""
	conn = psycopg2.connect(
			dbname=os.getenv('POSTGRES_DB'),
			user=os.getenv('POSTGRES_USER'),
			password=os.getenv('POSTGRES_PASSWORD'),
			host=os.getenv('DB_HOST'),
			port=os.getenv('DB_PORT')
	)
	cur = conn.cursor()
	# create the items table
	cur.execute("DROP TABLE IF EXISTS tmp_items;")
	cur.execute("""CREATE TABLE tmp_items (
        product_id INT,
        category_id BIGINT,
        category_code TEXT,
        brand TEXT
    );""")
	cur.execute("COPY tmp_items FROM '../data/item.csv' WITH (FORMAT csv);")
	cur.execute("""
        WITH base_rows AS (
				SELECT DISTINCT product_id FROM tmp_items WHERE product_id IS NOT NULL 
			)
        SELECT
            b.product_id,
			(SELECT category_id FROM tmp_items WHERE product_id = b.product_id AND category_id IS NOT NULL LIMIT 1) AS category_id,
			(SELECT category_code FROM tmp_items WHERE product_id = b.product_id AND category_code IS NOT NULL LIMIT 1) AS category_code, 
			(SELECT brand FROM tmp_items WHERE product_id = b.product_id AND brand IS NOT NULL LIMIT 1) AS brand 
		FROM base_records b;
    """)
	cur.execute("ALTER TABLE tmp_items RENAME TO items;")
	conn.commit()
	cur.close()
	conn.close()

if __name__ == '__main__':
	main()

