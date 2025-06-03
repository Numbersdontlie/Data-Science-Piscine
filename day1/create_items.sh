#!/bin/bash

# define file and table parameters
CSV_FILE="./subject/item/items.csv"
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
	#read the header to capture colums
	HEADER=$(head -1 "$CSV_FILE")

	#prepare columns definition before creating table
	COL_DEFS=()
	IFS="$DELIMITER" read -ra COLUMNS <<< "$HEADER"
	for COLUMN in "${COLUMNS[@]}"; do
		DATA_TYPE="${COLUMN_TYPES}"
		COL_DEFS+=("$DATA_TYPE")
	done
	#join column definitions
	COL_DEFS_STR=$(IFS=", "; echo "${COL_DEFS[*]}")

	#create table
	echo "Creating table"
	PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "\copy $TABLE_NAME FROM '$CSV_FILE' WITH (FORMAT csv, HEADER true, DELIMITER '$DELIMITER')"
  
  echo "Data import completed successfully!"
}

#execute the function
import_csv_to_db
