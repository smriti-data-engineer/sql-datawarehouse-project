#!/bin/bash
set -e

: "${PGUSER:?Missing PGUSER}"
: "${PGHOST:?Missing PGHOST}"
: "${PGPORT:?Missing PGPORT}"

echo "Dropping and creating database 'data_warehouse'..."
psql -U "$PGUSER" -h "$PGHOST" -p "$PGPORT" -d postgres -f create_database.sql

echo "Creating schemas in 'data_warehouse'..."
psql -U "$PGUSER" -h "$PGHOST" -p "$PGPORT" -d data_warehouse -f init_schema.sql

echo "✅ Data warehouse reset and initialized."