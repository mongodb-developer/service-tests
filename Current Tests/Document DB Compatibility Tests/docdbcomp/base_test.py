# base_test.py

import unittest
from pymongo import MongoClient
import config

class BaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Connect to Amazon DocumentDB
        cls.docdb_client = MongoClient(config.DOCDB_URI)
        cls.docdb_db = cls.docdb_client[config.DOCDB_DB_NAME]

        # Initialize test_results list
        cls.test_results = []

    @classmethod
    def tearDownClass(cls):
        # Store the test results into the MongoDB Atlas correctness collection
        try:
            result_client = MongoClient(config.RESULT_DB_URI)
            result_db = result_client[config.RESULT_DB_NAME]
            result_collection = result_db[config.RESULT_COLLECTION_NAME]
            if cls.test_results:
                result_collection.insert_many(cls.test_results)
        except Exception as e:
            print(f"Error storing test results: {e}")
        finally:
            cls.docdb_client.close()
            result_client.close()
