from dotenv import load_dotenv
load_dotenv()
import os
import psycopg2

def main():
    """This function creates a unified customers table from CSV files, it creates a temporary unlogged table
	in which we load the csv files, at the end we insert all the records in a single transaction and clean up the tmp table."""
    conn = psycopg2.connect(
			dbname=os.getenv('POSTGRES_DB'),
			user=os.getenv('POSTGRES_USER'),
			password=os.getenv('POSTGRES_PASSWORD'),
			host=os.getenv('DB_HOST'),
			port=os.getenv('DB_PORT')
	)
    cur = conn.cursor()

    # create final table to save the data after merging
    cur.execute("""CREATE TABLE customers (
        event_time TIMESTAMPTZ,
        event_type TEXT,
        product_id INT,
        price NUMERIC,
        user_id BIGINT,
        user_session UUID
    );""")
    # create union query to load a single merged csv file into the table
    cur.execute(""" INSERT INTO customers (
        SELECT * FROM data_2022_oct
        UNION ALL
        SELECT * FROM data_2022_nov
        UNION ALL
		SELECT * FROM data_2022_dec
        UNION ALL
		SELECT * FROM data_2023_jan
        UNION ALL
		SELECT * FROM data_2023_feb
    );""")
    cur.close()
    conn.commit()
    conn.close()

if __name__ == '__main__':
	main()