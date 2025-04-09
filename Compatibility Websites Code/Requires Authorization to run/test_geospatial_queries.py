# tests/test_geospatial_queries.py

import unittest
from pymongo import GEOSPHERE
from datetime import datetime
import traceback
import logging
import json
import time
from base_test import BaseTest
import config

class TestGeospatialQueries(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_geospatial_queries'

        # Define collection
        cls.docdb_coll = cls.docdb_db[cls.collection_name]

        # Drop existing collection
        cls.docdb_coll.drop()

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

    def setUp(self):
        self.docdb_coll = self.__class__.docdb_coll
        self.logger = self.__class__.logger

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

    def test_geospatial_queries(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Geospatial Queries Test',
            'platform': config.PLATFORM,
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': self.collection_name,
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': 'FAILED',
            'description': [],
            'query_result': {},
        }

        try:
            data = [
                {'_id': 1, 'name': 'Location1', 'location': {'type': 'Point', 'coordinates': [40, 5]}},
                {'_id': 2, 'name': 'Location2', 'location': {'type': 'Point', 'coordinates': [42, 10]}},
                {'_id': 3, 'name': 'Location3', 'location': {'type': 'Point', 'coordinates': [41, 6]}}
            ]
            collection.insert_many(data)
            result_document['log_lines'].append('Data inserted successfully.')
            self.logger.debug("Inserted geospatial test data successfully.")

            collection.create_index([('location', GEOSPHERE)])
            result_document['log_lines'].append('Geospatial index created successfully.')
            self.logger.debug("Created geospatial index successfully.")

            polygon = {
                'type': 'Polygon',
                'coordinates': [
                    [
                        [39, 4],
                        [43, 4],
                        [43, 11],
                        [39, 11],
                        [39, 4]
                    ]
                ]
            }
            query = {'location': {'$geoWithin': {'$geometry': polygon}}}
            result = list(collection.find(query))
            result_document['query_result'] = result
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['log_lines'].append('Geospatial query executed successfully.')
            self.logger.debug("Executed geospatial query successfully.")
        except Exception as e:
            error_msg = f"Error during geospatial queries test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            try:
                server_info = collection.database.client.server_info()
                server_version = server_info.get('version', 'unknown')
                result_document['version'] = server_version
                self.logger.debug(f"Server version retrieved: {server_version}")
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    @classmethod
    def tearDownClass(cls):
        cls.docdb_coll.drop()
        cls.logger.debug("Dropped collection during teardown.")
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
