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
	# create a query to join the customers and items tables on product_id
	cur.execute("""
		CREATE TABLE tmp AS
    	SELECT 
			c.*, 
			i.category_id,
			i.category_code,
			i.brand
		FROM customers c
		LEFT JOIN (
			SELECT DISTINCT ON (product_id) *
			FROM items
			ORDER BY product_id 
		) i ON c.product_id = i.product_id
	""")
	cur.execute("DROP TABLE customers;")
	# set the tmp table as the final table: this one is deduplicated now
	cur.execute("ALTER TABLE tmp RENAME TO customers;")
	conn.commit()
	cur.close()
	conn.close()

if __name__ == '__main__':
	main()

