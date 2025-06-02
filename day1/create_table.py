from dotenv import load_dotenv
load_dotenv()
import psycopg2
import os

def create_tables():
	""" Create the tables in the postgreSQL database"""
	command = (
		"""
		CREATE TABLE data_2022_oct (
			event_time TIMESTAMPTZ,
			event_type TEXT,
			product_id BIGINT,
			price NUMERIC,
			user_id BIGINT,
			user_session UUID
		)
		""")
	conn = None
	try:
		# read the connection parameters
		conn = psycopg2.connect(
		dbname=os.getenv('POSTGRES_DB'),
		user=os.getenv('POSTGRES_USER'),
		password=os.getenv('POSTGRES_PASSWORD'),
		host=os.getenv('DB_HOST'),
		port=os.getenv('DB_PORT')
		)
		cur = conn.cursor()
		cur.execute(command)
		cur.close()
		conn.commit()
	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()

if __name__ == '__main__':
	create_tables()