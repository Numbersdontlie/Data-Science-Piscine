#!/bin/bash

# Set environment variables (adjust these as needed in your Makefile)
DB_NAME="${POSTGRES_DB}"
DB_USER="${POSTGRES_USER}"
DB_PASSWORD="${POSTGRES_PASSWORD}"
DB_HOST="${DB_HOST}"
DB_PORT="${DB_PORT}"

# CSV files to import
COPY_FILES=(
    '/data/data_2022_oct.csv'
    '/data/data_2022_nov.csv'
    '/data/data_2022_dec.csv'
    '/data/data_2023_jan.csv'
    '/data/data_2023_feb.csv'
)

# Execute SQL commands through psql
psql_cmd() {
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$1"
}

# Begin transaction
psql_cmd "BEGIN;"

# Drop the customers table if it exists
psql_cmd "DROP TABLE IF EXISTS customers;"


# Import each CSV file
for file in "${COPY_FILES[@]}"; do
	base=$(basename "$file" .csv)
	table_name="${base}"
	# drop table if present
	psql_cmd "DROP TABLE IF EXISTS ${table_name};"
    # create the table
	psql_cmd "CREATE UNLOGGED TABLE ${table_name} (
        event_time TIMESTAMPTZ,
        event_type TEXT,
        product_id INT,
        price NUMERIC,
        user_id BIGINT,
        user_session UUID
    );"
    # import csv into table
	PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "\copy ${table_name} FROM '$file' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')"
	echo "Imported $file into $table_name."
done

# Commit transaction
psql_cmd "COMMIT;"

echo "all single customer csv data imported successfully."

# ------- Load items table into the infrastructure ------
psql_cmd "DROP TABLE IF EXISTS items;"

psql_cmd "CREATE TABLE items (
    product_id INT,
    category_id BIGINT,
    category_code TEXT,
    brand TEXT
);"

PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "\copy items FROM '/data/item.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')"

# Commit transaction
psql_cmd "COMMIT;"

echo "items table imported successfully."

