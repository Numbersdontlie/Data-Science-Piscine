from dotenv import load_dotenv
load_dotenv()
import os
import psycopg2

def main():
	"""Function to remove the duplicates in the database"""
	conn = psycopg2.connect(
			dbname=os.getenv('POSTGRES_DB'),
			user=os.getenv('POSTGRES_USER'),
			password=os.getenv('POSTGRES_PASSWORD'),
			host=os.getenv('DB_HOST'),
			port=os.getenv('DB_PORT')
	)
	cur = conn.cursor()
	cur.execute("""
        CREATE TABLE temp_customers AS
        SELECT
            event_time,
            event_type,
            product_id,
            price,
            user_id,
            user_session
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY user_id, DATE_TRUNC('second', event_time)
                    ORDER BY event_time ASC
                ) AS row_num
            FROM customers
        ) t
        WHERE row_num = 1;
    """)
	# replace the original table with the deduplicated one
	cur.execute("DROP TABLE customers;")
	cur.execute("ALTER TABLE temp_customers RENAME TO customers;")
	conn.commit()
	cur.close()
	conn.close()

if __name__ == '__main__':
	main()

