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
	# create tmp table to save the records without duplicates, here we use a window function ROW_NUMBER
	# and partition it by user_id and truncate for seconds since we want to remove the duplicates that may be
	# generated for the server sending same instruction with 1 second interval, since we want to keep
	# the 1st ocurrence we order by ascending order the event_time.
	cur.execute("""
	CREATE TABLE tmp AS
    WITH duplo AS (
        SELECT *,
            LAG(event_time) OVER (
                PARTITION BY event_type, product_id, price, user_id, user_session
                ORDER BY event_time
            ) AS prev_time
        FROM customers
    ),
    filtered AS (
        SELECT *
        FROM duplo
        WHERE prev_time IS NULL OR EXTRACT(EPOCH FROM (event_time - prev_time)) > 1
    )
    SELECT event_time, event_type, product_id, price, user_id, user_session FROM filtered
	""")
	# delete the original table with duplicates
	cur.execute("DROP TABLE customers;")
	# set the tmp table as the final table: this one is deduplicated now
	cur.execute("ALTER TABLE tmp RENAME TO customers;")
	conn.commit()
	cur.close()
	conn.close()

if __name__ == '__main__':
	main()

