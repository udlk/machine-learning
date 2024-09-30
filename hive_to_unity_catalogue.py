import logging
from pyhive import hive
import pandas as pd
from pyspark.sql import SparkSession
import requests
import json

# Configuration
HIVE_HOST = 'your_hive_host'
HIVE_PORT = 10000
HIVE_USERNAME = 'your_hive_username'

DATABRICKS_WORKSPACE_URL = 'https://<your-databricks-workspace-url>'
DATABRICKS_TOKEN = 'your-databricks-token'
CATALOG_NAME = 'your_unity_catalog_name'
SPARK_APP_NAME = 'HiveToUnityCatalogMigration'

# Set up logging
logging.basicConfig(
    filename='migration.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Establish Hive connection
def connect_to_hive():
    try:
        return hive.Connection(
            host=HIVE_HOST,
            port=HIVE_PORT,
            username=HIVE_USERNAME
        )
    except Exception as e:
        logger.error(f"Error connecting to Hive: {e}")
        raise

# Get all Hive databases
def get_all_databases(cursor):
    try:
        cursor.execute("SHOW DATABASES")
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error retrieving databases from Hive: {e}")
        raise

# Get all tables in a Hive database
def get_hive_tables(cursor, database_name):
    try:
        cursor.execute(f"USE {database_name}")
        cursor.execute("SHOW TABLES")
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error retrieving tables from Hive database '{database_name}': {e}")
        raise

# Create a catalog in Unity Catalog using Databricks REST API
def create_catalog(catalog_name):
    try:
        url = f"{DATABRICKS_WORKSPACE_URL}/api/2.0/unity-catalog/catalogs"
        headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
        data = {
            "name": catalog_name,
            "comment": "Migrated from Hive"
        }
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            logger.info(f"Catalog '{catalog_name}' created successfully.")
        else:
            logger.error(f"Error creating catalog '{catalog_name}': {response.content}")
    except Exception as e:
        logger.error(f"Error creating catalog '{catalog_name}': {e}")
        raise

# Create schema in Unity Catalog using Databricks REST API
def create_schema(schema_name, catalog_name):
    try:
        url = f"{DATABRICKS_WORKSPACE_URL}/api/2.0/unity-catalog/schemas"
        headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
        data = {
            "name": schema_name,
            "catalog_name": catalog_name,
            "comment": "Migrated from Hive"
        }
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            logger.info(f"Schema '{schema_name}' created successfully in catalog '{catalog_name}'.")
        else:
            logger.error(f"Error creating schema '{schema_name}': {response.content}")
    except Exception as e:
        logger.error(f"Error creating schema '{schema_name}': {e}")
        raise

# Use Spark to transfer data from Hive to Unity Catalog
def migrate_table_to_unity_catalog(spark, hive_database, hive_table, catalog_name, schema_name):
    try:
        # Load data from Hive table
        hive_table_df = spark.sql(f"SELECT * FROM {hive_database}.{hive_table}")

        # Write data to Unity Catalog
        hive_table_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.{hive_table}")

        logger.info(f"Table '{hive_table}' successfully migrated from Hive to Unity Catalog.")
    except Exception as e:
        logger.error(f"Error migrating table '{hive_table}' from database '{hive_database}': {e}")
        raise

# Main script
def main():
    try:
        # Connect to Hive
        hive_conn = connect_to_hive()
        hive_cursor = hive_conn.cursor()

        # Get all databases from Hive
        databases = get_all_databases(hive_cursor)

        # Create Unity Catalog
        create_catalog(CATALOG_NAME)

        # Initialize Spark session
        spark = SparkSession.builder.appName(SPARK_APP_NAME).getOrCreate()

        # Loop through each Hive database and migrate tables
        for (hive_database_name,) in databases:
            try:
                # Create schema for each database in Unity Catalog
                create_schema(hive_database_name, CATALOG_NAME)

                # Get tables from the current Hive database
                tables = get_hive_tables(hive_cursor, hive_database_name)

                # Migrate each table from the current Hive database to Unity Catalog
                for table_name, _ in tables:
                    try:
                        migrate_table_to_unity_catalog(spark, hive_database_name, table_name, CATALOG_NAME, hive_database_name)
                    except Exception as table_error:
                        logger.error(f"Failed to migrate table '{table_name}' in database '{hive_database_name}': {table_error}")
            except Exception as db_error:
                logger.error(f"Failed to process Hive database '{hive_database_name}': {db_error}")

    except Exception as e:
        logger.critical(f"Critical failure during migration process: {e}")
    finally:
        try:
            # Close connections
            if hive_cursor:
                hive_cursor.close()
            if hive_conn:
                hive_conn.close()
            logger.info("Hive connection closed.")
        except Exception as e:
            logger.error(f"Error closing Hive connection: {e}")

if __name__ == "__main__":
    main()
