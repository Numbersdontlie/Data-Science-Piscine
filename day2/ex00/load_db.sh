#!/bin/bash
set -euo pipefail

# load .env from the directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
if [ -f "$ENV_FILE" ]; then
	#export key-value lines from .env file
	set -a
	# shellcheck disable=SC1090
	source "$ENV_FILE"
	set +a
else
	echo "Error: .env file not found at $ENV_FILE"
	exit 1
fi

# CSV files to import
COPY_FILES=(
    "/data/customer/data_2022_dec.csv"
    "/data/customer/data_2022_nov.csv"
    "/data/customer/data_2022_oct.csv"
    "/data/customer/data_2023_feb.csv"
    "/data/customer/data_2023_jan.csv"
)

# Execute SQL commands through psql
psql_cmd() {
    PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"
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
	PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\copy ${table_name} FROM '$file' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')"
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

PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\copy items FROM '/data/item/item.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '')"

# Commit transaction
psql_cmd "COMMIT;"

echo "items table imported successfully."

