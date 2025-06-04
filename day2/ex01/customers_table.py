from dotenv import load_dotenv
load_dotenv()
import os
import psycopg2

def main():
    """This function creates a unified customers table from CSV files, it creates a temporary unlogged table
	in which we load the csv files, at the end we insert all the records in a single transaction and clean up the tmp table."""
    commands = [
        """CREATE UNLOGGED TABLE tmp_customers (
            event_time TIMESTAMPTZ,
            event_type TEXT,
            product_id INT,
            price NUMERIC,
            user_id BIGINT,
            user_session UUID
        );""",
        "ALTER TABLE tmp_customers SET (autovacuum_enabled = false);",
        "COPY tmp_customers FROM '../data/data_2022_oct.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');",
        "COPY tmp_customers FROM '../data/data_2022_nov.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');",
        "COPY tmp_customers FROM '../data/data_2022_dec.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');",
        "COPY tmp_customers FROM '../data/data_2023_jan.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');",
        "COPY tmp_customers FROM '../data/data_2023_feb.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');",
        "INSERT INTO customers SELECT * FROM tmp_customers ON CONFLICT (user_session, event_time) DO NOTHING;",
        "DROP TABLE tmp_customers;"
    ]
    copy_files = [
         '../data/data_2022_oct.csv',
        '../data/data_2022_nov.csv',
        '../data/data_2022_dec.csv',
        '../data/data_2023_jan.csv',
        '../data/data_2023_feb.csv'
	]
    conn = psycopg2.connect(
			dbname=os.getenv('POSTGRES_DB'),
			user=os.getenv('POSTGRES_USER'),
			password=os.getenv('POSTGRES_PASSWORD'),
			host=os.getenv('DB_HOST'),
			port=os.getenv('DB_PORT')
	)
    cur = conn.cursor()
    cur.execute("""CREATE UNLOGGED TABLE tmp_customers (
        event_time TIMESTAMPTZ,
        event_type TEXT,
        product_id INT,
        price NUMERIC,
        user_id BIGINT,
        user_session UUID
    );""")
    cur.execute("ALTER TABLE tmp_customers SET (autovacuum_enabled = false);")
    for file in copy_files:
        with open(file, 'r') as f:
            cur.copy_expert(
                "COPY tmp_customers FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')", f
            )
    cur.execute("ALTER TABLE tmp_customers SET (autovacuum_enabled = false);")
    cur.execute("""CREATE TABLE customers (
        event_time TIMESTAMPTZ,
        event_type TEXT,
        product_id INT,
        price NUMERIC,
        user_id BIGINT,
        user_session UUID
    );""")
    cur.execute("INSERT INTO customers SELECT * FROM tmp_customers;")
    cur.execute("DROP TABLE tmp_customers;")
    cur.close()
    conn.commit()
    conn.close()
     
if __name__ == '__main__':
	main()