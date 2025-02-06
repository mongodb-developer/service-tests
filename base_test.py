# base_test.py

import unittest
import psycopg2
from psycopg2.extras import RealDictCursor
import config

class BaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Set up the database connections for all tests.
        """
        # Step 1: Connect to the Admin Database to Create Target Database if Needed
        try:
            cls.admin_conn = psycopg2.connect(
                host=config.DB_HOST,
                port=config.DB_PORT,
                user=config.ADMIN_DB_USER,
                password=config.ADMIN_DB_PASSWORD,
                dbname="postgres"  # Default admin database
            )
            cls.admin_conn.autocommit = True
            admin_cursor = cls.admin_conn.cursor()
            admin_cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (config.DOCDB_DB_NAME,))
            exists = admin_cursor.fetchone()
            if not exists:
                print(f"Database '{config.DOCDB_DB_NAME}' does not exist. Creating it.")
                admin_cursor.execute(f'CREATE DATABASE "{config.DOCDB_DB_NAME}";')
            else:
                print(f"Database '{config.DOCDB_DB_NAME}' already exists.")
            admin_cursor.close()
            cls.admin_conn.close()
            print("Admin connection closed.")
        except psycopg2.Error as e:
            raise RuntimeError(f"Could not connect to admin database: {e}")

        # Step 2: Connect to the Target Database
        try:
            cls.test_conn = psycopg2.connect(
                host=config.DB_HOST,
                port=config.DB_PORT,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                dbname=config.DOCDB_DB_NAME
            )
            cls.test_conn.autocommit = True
            print("Connected to the target database successfully.")
        except psycopg2.Error as e:
            raise RuntimeError(f"Could not connect to Postgres-based DocDB: {e}")

        # Initialize Test Results
        cls.test_results = []

    @classmethod
    def tearDownClass(cls):
        """
        Close database connections and store test results.
        """
        # Insert test results into MongoDB Atlas
        from pymongo import MongoClient
        try:
            result_client = MongoClient(config.RESULT_DB_URI)
            result_db = result_client[config.RESULT_DB_NAME]
            result_collection = result_db[config.RESULT_COLLECTION_NAME]
            if cls.test_results:
                result_collection.insert_many(cls.test_results)
                print("Test results inserted into MongoDB Atlas.")
        except Exception as e:
            print(f"Error storing test results: {e}")
        finally:
            result_client.close()

            # Close Postgres connections
            if hasattr(cls, 'test_conn') and cls.test_conn:
                cls.test_conn.close()
                print("Database connection closed.")