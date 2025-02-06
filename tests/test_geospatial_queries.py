# tests/test_geospatial_queries.py

import unittest
import logging
import time
import json
import traceback
from datetime import datetime
import sys
import os
import decimal  # Added import for the decimal module

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

# Adjust sys.path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_test import BaseTest

class TestGeospatialQueries(BaseTest):
    """
    Test suite for geospatial queries in PostgreSQL.
    Since PostgreSQL does not support MongoDB's GEOSPHERE indexes natively without extensions like PostGIS,
    certain geospatial operations may not be supported. Tests will attempt to perform these operations
    and handle any resulting errors.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.table_name = 'test_geospatial_queries'

        # Configure logging
        cls.logger = logging.getLogger('TestGeospatialQueries')
        cls.logger.setLevel(logging.DEBUG)

        # File Handler for logging to 'test_geospatial_queries.log'
        file_handler = logging.FileHandler('test_geospatial_queries.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        cls.logger.addHandler(file_handler)

        # In-Memory Log Capture List
        cls.log_capture_list = []

        # Custom Handler to capture logs in memory
        class ListHandler(logging.Handler):
            def __init__(self, log_list):
                super().__init__()
                self.log_list = log_list

            def emit(self, record):
                log_entry = self.format(record)
                self.log_list.append(log_entry)

        # Initialize and add the custom ListHandler
        list_handler = ListHandler(cls.log_capture_list)
        list_handler.setFormatter(formatter)
        cls.logger.addHandler(list_handler)

        # Create table for geospatial queries
        try:
            with cls.test_conn.cursor() as cur:
                # Drop table if it exists to start clean
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Dropped table '{cls.table_name}' if it existed.")

                # Create table with appropriate columns
                # PostgreSQL supports POINT type for geospatial data without extensions
                cur.execute(sql.SQL("""
                    CREATE TABLE {} (
                        id SERIAL PRIMARY KEY,
                        name TEXT,
                        location POINT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """).format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Created table '{cls.table_name}' with 'location' as POINT type.")
            cls.test_conn.commit()
            cls.logger.debug(f"Setup for '{cls.table_name}' completed successfully.")
            print(f"Table '{cls.table_name}' created successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error setting up table: {e}")
            print(f"Error setting up table: {e}")
            raise

    def setUp(self):
        # Assign class variables to instance variables
        self.logger = self.__class__.logger

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

    @staticmethod
    def parse_point(point_str):
        """
        Parses a point string in the format '(x,y)' and returns a tuple (x, y).
        """
        point_str = point_str.strip('()')
        x, y = point_str.split(',')
        return float(x), float(y)

    def convert_decimals(self, obj):
        """Recursively convert Decimal objects to float."""
        if isinstance(obj, list):
            return [self.convert_decimals(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: self.convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, decimal.Decimal):
            return float(obj)  # or str(obj) if precision is important
        else:
            return obj

    def test_geospatial_queries(self):
        """
        Test geospatial queries in PostgreSQL.
        Inserts geospatial data and attempts to perform geospatial queries.
        Handles unsupported operations gracefully by capturing database errors.
        """
        table = self.table_name
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Geospatial Queries Test',
            'platform': 'postgresql',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': table,
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'query_result': {},
        }

        try:
            # Insert geospatial test data
            data = [
                {'id': 1, 'name': 'Location1', 'location': '(40,5)'},
                {'id': 2, 'name': 'Location2', 'location': '(42,10)'},
                {'id': 3, 'name': 'Location3', 'location': '(41,6)'}
            ]

            with self.test_conn.cursor() as cur:
                for doc in data:
                    cur.execute(sql.SQL("""
                        INSERT INTO {} (id, name, location)
                        VALUES (%s, %s, POINT(%s, %s));
                    """).format(sql.Identifier(table)),
                    (doc['id'], doc['name'], *self.parse_point(doc['location'])))
            self.logger.debug("Inserted geospatial test data successfully.")
            result_document['log_lines'].append('Data inserted successfully.')

            self.test_conn.commit()

            # Attempt to create a geospatial index similar to MongoDB's GEOSPHERE
            try:
                with self.test_conn.cursor() as cur:
                    cur.execute(sql.SQL("""
                        CREATE INDEX idx_location_gist ON {}
                        USING GIST (location);
                    """).format(sql.Identifier(table)))
                self.test_conn.commit()
                self.logger.debug("Created GIST index on 'location' field successfully.")
                result_document['log_lines'].append('Geospatial index created successfully.')
            except psycopg2.Error as e:
                self.test_conn.rollback()
                error_msg = f"Failed to create geospatial index: {e}"
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
                self.logger.debug(traceback.format_exc())

            # Define a polygon to query within
            # PostgreSQL does not support complex geospatial queries without PostGIS
            # We'll simulate a basic range query using bounding box logic
            # Alternatively, if PostGIS is available, use its functions
            # Since we are not using extensions, we'll use simple SQL conditions

            # Define bounding box coordinates
            min_x, min_y = 39, 4
            max_x, max_y = 43, 11

            # Perform a bounding box query to simulate $geoWithin
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql.SQL("""
                    SELECT id, name, location
                    FROM {}
                    WHERE location[0] BETWEEN %s AND %s
                      AND location[1] BETWEEN %s AND %s;
                """).format(sql.Identifier(table)),
                (min_x, max_x, min_y, max_y))
                result = cur.fetchall()
                result_document['query_result'] = result
                self.logger.debug("Executed geospatial query successfully.")
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Geospatial query executed successfully.')

        except Exception as e:
            self.test_conn.rollback()
            error_msg = f"Error during geospatial queries test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
        finally:
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            # Retrieve PostgreSQL server version dynamically
            try:
                with self.test_conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version_info = cur.fetchone()
                server_version = version_info[0] if version_info else 'unknown'
                result_document['version'] = server_version
                self.logger.debug(f"Server version retrieved: {server_version}")
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Convert all Decimal objects to float or string
            result_document = self.convert_decimals(result_document)

            # Ensure all fields in result_document are JSON serializable
            try:
                json.dumps(result_document)
            except TypeError as te:
                self.logger.error(f"JSON serialization error: {te}")
                result_document['reason'] += f" JSON serialization error: {te}"
                result_document['status'] = 'fail'

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    @classmethod
    def tearDownClass(cls):
        # Clean up: drop the table
        try:
            with cls.test_conn.cursor() as cur:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.table_name)))
            cls.test_conn.commit()
            cls.logger.debug(f"Dropped table '{cls.table_name}' during teardown.")
            print(f"Table '{cls.table_name}' dropped successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error dropping table during teardown: {e}")
            print(f"Error dropping table during teardown: {e}")
        finally:
            super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
