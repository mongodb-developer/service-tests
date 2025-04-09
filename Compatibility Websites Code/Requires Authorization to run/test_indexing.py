# tests/test_indexing.py

import unittest
from pymongo import (
    ASCENDING, DESCENDING, TEXT, GEOSPHERE, GEO2D, HASHED
)
from pymongo.errors import PyMongoError
from datetime import datetime, timedelta
import traceback
from base_test import BaseTest
import logging
import time
import json
import config

class TestIndexing(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_indexing'

        # Define collection
        cls.docdb_coll = cls.docdb_db[cls.collection_name]

        # Drop existing collection
        cls.docdb_coll.drop()

        # Configure logging
        cls.logger = logging.getLogger('TestIndexing')
        cls.logger.setLevel(logging.DEBUG)
        
        # File Handler for logging to 'test_indexing.log'
        file_handler = logging.FileHandler('test_indexing.log')
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
        """Reset collection before each test"""
        self.docdb_coll.drop()
        self.logger.debug("Dropped existing collection before test.")

    def get_test_data(self, test_iteration: int):
        """Generate comprehensive test data for all index types"""
        base_time = datetime.utcnow()
        return [
            {
                '_id': f'test_{test_iteration}_1',
                'field1': 'value1',
                'field2': 'value2',
                'field3': 'text content for searching',
                'number_field': 100,
                'decimal_field': 100.50,
                'array_field': ['tag1', 'tag2', 'tag3'],
                'nested_field': {
                    'sub_field1': 'nested1',
                    'sub_field2': 'nested2',
                    'sub_array': [1, 2, 3]
                },
                'location': {
                    'type': 'Point',
                    'coordinates': [40.0, 5.0]
                },
                'multi_location': {
                    'type': 'MultiPoint',
                    'coordinates': [[40.0, 5.0], [41.0, 6.0]]
                },
                'polygon': {
                    'type': 'Polygon',
                    'coordinates': [[[0, 0], [3, 6], [6, 1], [0, 0]]]
                },
                'date_field': base_time,
                'expiry_field': base_time,
                'tags': ['mongodb', 'database', 'indexing'],
                'status': 'active',
                'version': 1,
                'score': 85,
                'metadata': {
                    'created_by': 'user1',
                    'department': 'engineering'
                }
            },
            {
                '_id': f'test_{test_iteration}_2',
                'field1': 'value3',
                'field2': 'value4',
                'field3': 'another text for full-text search',
                'number_field': 200,
                'decimal_field': 200.75,
                'array_field': ['tag4', 'tag5'],
                'nested_field': {
                    'sub_field1': 'nested3',
                    'sub_field2': 'nested4',
                    'sub_array': [4, 5, 6]
                },
                'location': {
                    'type': 'Point',
                    'coordinates': [42.0, 10.0]
                },
                'multi_location': {
                    'type': 'MultiPoint',
                    'coordinates': [[42.0, 10.0], [43.0, 11.0]]
                },
                'polygon': {
                    'type': 'Polygon',
                    'coordinates': [[[1, 1], [4, 7], [7, 2], [1, 1]]]
                },
                'date_field': base_time + timedelta(days=1),
                'expiry_field': base_time + timedelta(days=1),
                'tags': ['atlas', 'cloud', 'testing'],
                'status': 'inactive',
                'version': 2,
                'score': 90,
                'metadata': {
                    'created_by': 'user2',
                    'department': 'testing'
                }
            }
        ]

    def get_all_index_definitions(self):
        """Return comprehensive list of all possible index types and combinations"""
        return [
            # Basic Single-Field Indexes
            {
                'name': 'basic_ascending',
                'keys': [('field1', ASCENDING)],
                'options': {},
                'description': 'Basic ascending index on field1'
            },
            {
                'name': 'basic_descending',
                'keys': [('field2', DESCENDING)],
                'options': {},
                'description': 'Basic descending index on field2'
            },
            # Compound Indexes
            {
                'name': 'compound_basic',
                'keys': [('field1', ASCENDING), ('field2', DESCENDING)],
                'options': {},
                'description': 'Compound index on field1 and field2'
            },
            # Text Indexes
            {
                'name': 'text_basic',
                'keys': [('field3', TEXT)],
                'options': {
                    'default_language': 'english',
                    'weights': {'field3': 10}
                },
                'description': 'Text index on field3 with weights and default language'
            },
            # Geospatial Indexes
            {
                'name': 'geo_2dsphere',
                'keys': [('location', GEOSPHERE)],
                'options': {},
                'description': '2dsphere geospatial index on location'
            },
            # Hashed Indexes
            {
                'name': 'hashed_basic',
                'keys': [('field1', HASHED)],
                'options': {},
                'description': 'Hashed index on field1'
            },
            # Unique Indexes
            {
                'name': 'unique_single',
                'keys': [('field1', ASCENDING)],
                'options': {'unique': True},
                'description': 'Unique index on field1'
            },
            {
                'name': 'unique_compound',
                'keys': [('field1', ASCENDING), ('field2', ASCENDING)],
                'options': {'unique': True},
                'description': 'Unique compound index on field1 and field2'
            },
            # Partial Indexes
            #{
                #'name': 'partial_basic',
                #'keys': [('field1', ASCENDING)],
                #'options': {
                    #'partialFilterExpression': {
                        #'status': 'active'
                    #}
                #},
                #'description': 'Partial index on field1 where status is active'
            #},
            # Sparse Indexes
            #{
                #'name': 'sparse_basic',
                #'keys': [('optional_field', ASCENDING)],
                #'options': {'sparse': True},
                #'description': 'Sparse index on optional_field'
            #},
            # TTL Indexes
            {
                'name': 'ttl_basic',
                'keys': [('expiry_field', ASCENDING)],
                'options': {'expireAfterSeconds': 3600},
                'description': 'TTL index on expiry_field with 1-hour expiration'
            },
            # Wildcard Indexes
            {
                'name': 'wildcard_all',
                'keys': [('$**', ASCENDING)],
                'options': {},
                'description': 'Wildcard index on all fields'
            },
            # Array Indexes
            {
                'name': 'array_single',
                'keys': [('array_field', ASCENDING)],
                'options': {},
                'description': 'Index on array_field'
            },
            # Complex Compound Indexes
            {
                'name': 'complex_compound',
                'keys': [
                    ('field1', ASCENDING),
                    ('field2', DESCENDING),
                    ('nested_field.sub_field1', ASCENDING)
                ],
                'options': {'unique': True},
                'description': 'Unique compound index on field1, field2, and nested_field.sub_field1'
            }
        ]

    def get_test_query(self, index_keys, test_data):
        """Generate appropriate test query based on index type"""
        if any(key[1] == TEXT for key in index_keys):
            return {'$text': {'$search': 'content'}}

        if any(key[1] == GEOSPHERE for key in index_keys):
            return {
                'location': {
                    '$near': {
                        '$geometry': {
                            'type': 'Point',
                            'coordinates': [40.0, 5.0]
                        },
                        '$maxDistance': 5000
                    }
                }
            }

        if len(index_keys) > 1:
            query = {}
            for key, _ in index_keys[:2]:
                if key in test_data[0] and key != '$**':
                    query[key] = test_data[0][key]
            return query

        # Single Field Query
        if index_keys and index_keys[0][0] != '$**':
            key = index_keys[0][0]
            if key in test_data[0]:
                return {key: test_data[0][key]}

        return {'field1': test_data[0]['field1']}

    def test_indexing(self):
        """Test all index types and combinations on Amazon DocumentDB"""
        index_definitions = self.get_all_index_definitions()

        for idx, index_def in enumerate(index_definitions):
            test_data = self.get_test_data(idx)
            index_name = index_def['name']
            keys = index_def['keys']
            options = index_def['options']
            description = index_def.get('description', 'Indexing Test')
            test_query = self.get_test_query(keys, test_data)

            # Run test only on Amazon DocumentDB
            collection = self.docdb_coll

            start_time = time.time()
            result_document = {
                'status': 'fail',  # Default to 'fail'; will update based on conditions
                'test_name': f'Indexing Test - {description}',
                'platform': config.PLATFORM,
                'exit_code': 1,
                'elapsed': None,
                'start': datetime.utcfromtimestamp(start_time).isoformat(),
                'end': None,
                'suite': self.collection_name,
                'version': 'unknown',  # Will be updated dynamically
                'run': 1,
                'processed': True,
                'log_lines': [],
                'reason': 'FAILED',
                'description': [],
                'explain_plan': {},
            }

            try:
                # Step 1: Create Index
                try:
                    collection.drop_indexes()
                    collection.create_index(keys, name=index_name, **options)
                    result_document['log_lines'].append(f"Index '{index_name}' created successfully.")
                    self.logger.debug(f"Index '{index_name}' created successfully.")
                except PyMongoError as e_create:
                    raise Exception(f"Failed to create index '{index_name}': {str(e_create)}")

                # Step 2: Verify Index Creation
                indexes = list(collection.list_indexes())
                created_index = next(
                    (idx for idx in indexes if idx.get('name') == index_name),
                    None
                )
                if created_index:
                    result_document['log_lines'].append(f"Index '{index_name}' verified.")
                    self.logger.debug(f"Index '{index_name}' verified.")
                else:
                    raise Exception(f"Index '{index_name}' not found after creation")

                # Step 3: Insert Test Data without dropping indexes (preserve index)
                collection.delete_many({})
                collection.insert_many(test_data)
                result_document['log_lines'].append("Test data inserted successfully.")
                self.logger.debug("Test data inserted successfully.")

                # Step 4: Test Query with Explain
                explain_result = collection.find(test_query).explain()
                result_document['log_lines'].append("Explain plan obtained.")
                self.logger.debug("Explain plan obtained.")

                # Convert all Timestamps in explain_result to strings
                explain_result_serializable = json.loads(json.dumps(explain_result, default=str))
                result_document['explain_plan'] = explain_result_serializable

                # Log the formatted explain plan
                formatted_explain = json.dumps(explain_result_serializable, indent=4)
                self.logger.debug(f"Explain Plan for {index_name}:\n{formatted_explain}")

                # Step 5: Check Index Usage in Query Plan
                stages = explain_result_serializable.get('queryPlanner', {}).get('winningPlan', {})
                if self._check_index_usage(stages, index_name):
                    result_document['log_lines'].append(f"Index '{index_name}' used in query plan.")
                    self.logger.debug(f"Index '{index_name}' used in query plan.")
                    result_document['status'] = 'pass'
                    result_document['exit_code'] = 0
                    result_document['reason'] = 'PASSED'
                else:
                    # Mark as passed only if index was used in the query plan
                    raise Exception(f"Index '{index_name}' not used in query plan.")
            except Exception as e:
                error_message = str(e)
                result_document['status'] = 'fail'
                result_document['exit_code'] = 1
                result_document['reason'] = 'FAILED'
                result_document['description'].append(error_message)
                self.logger.error(f"Error for {index_name}: {error_message}\n{traceback.format_exc()}")
            finally:
                # Capture elapsed time and end time
                end_time = time.time()
                result_document['elapsed'] = end_time - start_time
                result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

                # Retrieve server version dynamically
                try:
                    server_info = collection.database.client.server_info()
                    server_version = server_info.get('version', 'unknown')
                    result_document['version'] = server_version
                    self.logger.debug(f"Server version retrieved: {server_version}")
                except Exception as ve:
                    self.logger.error(f"Error retrieving server version: {ve}")
                    result_document['version'] = 'unknown'

                # Assign captured log lines to the result document
                result_document['log_lines'] = list(self.log_capture_list)

                if result_document['status'] == 'pass' and result_document['description']:
                    warnings_msg = '; '.join(result_document['description'])
                    result_document['log_lines'].append(warnings_msg)

                print(json.dumps(result_document, indent=4))
                self.test_results.append(result_document)

                # Clean up: Do not drop collection so that indexes persist across tests if needed
                collection.delete_many({})
                self.logger.debug(f"Indexing test for '{index_name}' cleaned up.")

    def _check_index_usage(self, stages, index_name):
        """Recursively check if the index is used in the query plan"""
        if not isinstance(stages, dict):
            return False

        if stages.get('indexName') == index_name:
            return True

        for value in stages.values():
            if isinstance(value, dict):
                if self._check_index_usage(value, index_name):
                    return True
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        if self._check_index_usage(item, index_name):
                            return True
        return False

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests"""
        try:
            cls.docdb_coll.drop()
            cls.logger.debug("Dropped collection during teardown.")
        except Exception as e:
            cls.logger.error(f"Error in teardown: {str(e)}")
        finally:
            super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
