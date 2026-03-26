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

# define file and table parameters
CSV_FILE="$SCRIPT_DIR/../data/item/item.csv"
TABLE_NAME="items"
DELIMITER=","

# define column data types
declare -A COLUMN_TYPES=(
	["product_id"]="INTEGER"
    ["category_id"]="BIGINT"
    ["category_code"]="TEXT"
    ["brand"]="TEXT"
)

# function to create table and load the data into it
import_csv_to_db() {
	if [ ! -f "$CSV_FILE" ]; then
		echo "Error: CSV file not found at $CSV_FILE"
		exit 1
	fi

	#read the header to capture colums
	HEADER=$(head -n 1 "$CSV_FILE")
	IFS="$DELIMITER" read -ra COLUMNS <<< "$HEADER"

	#prepare columns definition before creating table
	COL_DEFS=()
	for COLUMN in "${COLUMNS[@]}"; do
		COLUMN="$(echo "$COLUMN" | xargs)" #remove whitespace
		DATA_TYPE="${COLUMN_TYPES[$COLUMN]:-TEXT}"
		COL_DEFS+=("\"$COLUMN\"$DATA_TYPE")
	done

	#join column definitions with commas
	COL_DEFS_STR=$(IFS=, ; echo "${COL_DEFS[*]}")

	# create table if not exists
	echo "Creating table: $TABLE_NAME"
	PGPASSWORD="${POSTGRES_PASSWORD:-}" psql -h "${DB_HOST:-}" -p "${DB_PORT:-}" -U "${POSTGRES_USER:-}" -d "${POSTGRES_DB:-}" \
		-c "CREATE TABLE IF NOT EXISTS \"$TABLE_NAME\" ($COL_DEFS_STR);"
	
	# import csv file into table
	echo "Importing csv file into table: $TABLE_NAME"
	PGPASSWORD="${POSTGRES_PASSWORD:-}" psql -h "${DB_HOST:-}" -p "${DB_PORT:-}" -U "${POSTGRES_USER:-}" -d "${POSTGRES_DB:-}" \
    -c "\copy $TABLE_NAME FROM '$CSV_FILE' WITH (FORMAT csv, HEADER true, DELIMITER '$DELIMITER')"
  
  echo "Data import completed successfully!"
}

#execute the function
import_csv_to_db
