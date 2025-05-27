-- Drop the database if it exists
DO
$do$
BEGIN
   IF EXISTS (SELECT FROM pg_database WHERE datname = 'DataWarehouse') THEN
      PERFORM pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'DataWarehouse';
      EXECUTE 'DROP DATABASE DataWarehouse';
   END IF;
END
$do$;

-- Create a fresh database
CREATE DATABASE DataWarehouse;