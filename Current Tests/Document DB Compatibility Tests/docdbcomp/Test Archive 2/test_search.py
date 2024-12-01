# tests/test_search.py

import unittest
from pymongo import TEXT, ASCENDING
from pymongo.errors import OperationFailure
from datetime import datetime
import logging
import json
import time
from base_test import BaseTest

class TestSearchCapabilities(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_search'
        cls.docdb_coll = cls.docdb_db['exampleCollection']
        cls.docdb_coll.drop()

        # Configure logging
        cls.logger = logging.getLogger('TestSearchCapabilities')
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler('test_search.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        cls.logger.addHandler(handler)

    def setUp(self):
        self.docdb_coll = self.__class__.docdb_coll
        self.logger = self.__class__.logger

        # Drop indexes before each test to avoid conflicts
        self.docdb_coll.drop_indexes()

        # Insert sample data
        sample_data = [
            {
                'name': 'Eugenia Lopez',
                'bio': 'Eugenia is the CEO of AdventureWorks.',
                'vectorContent': [0.51, 0.12, 0.23],
                'year': 2001,
                'language': 'en'
            },
            {
                'name': 'Cameron Baker',
                'bio': 'Cameron Baker CFO of AdventureWorks.',
                'vectorContent': [0.55, 0.89, 0.44],
                'year': 2002,
                'language': 'es'
            },
            {
                'name': 'Jessie Irwin',
                'bio': "Jessie Irwin is the former CEO of AdventureWorks and now the director of the Our Planet initiative.",
                'vectorContent': [0.13, 0.92, 0.85],
                'year': 2001,
                'language': 'fr'
            },
            {
                'name': 'Rory Nguyen',
                'bio': "Rory Nguyen is the founder of AdventureWorks and the president of the Our Planet initiative.",
                'vectorContent': [0.91, 0.76, 0.83],
                'year': 2002,
                'language': 'es',
            },
        ]

        self.docdb_coll.insert_many(sample_data)

        # Create text index on 'bio' field
        try:
            self.docdb_coll.create_index([('bio', TEXT)], name='TextIndex')
            self.logger.info('Text index created successfully.')
        except OperationFailure as e:
            self.logger.warning(f"Text index creation failed: {e}")

    def test_text_search(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = self.initialize_result_document('Text Search Test')

        try:
            text_query = {'$text': {'$search': 'CEO'}}
            results = list(collection.find(text_query))
            result_document['details']['text_search_results'] = results

            expected_names = {'Eugenia Lopez', 'Jessie Irwin'}
            result_names = set(doc['name'] for doc in results)

            if expected_names == result_names:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Text search executed successfully.')
            else:
                missing = expected_names - result_names
                error_msg = f'Missing expected results: {missing}'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during text search test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_vector_search_hnsw(self):
        self.perform_vector_search_test(index_type='hnsw', test_name='Vector Search Test - HNSW')

    def test_vector_search_ivfflat(self):
        self.perform_vector_search_test(index_type='ivfflat', test_name='Vector Search Test - IVFFlat')

    def test_hybrid_search(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = self.initialize_result_document('Hybrid Search Test')

        try:
            # Ensure vector index exists (using HNSW for this example)
            index_spec = {
                'vectorContent': 'vector'
            }
            index_options = {
                'name': 'VectorSearchIndex_HNSW',
                'vectorOptions': {
                    'type': 'hnsw',
                    'dimensions': 3,
                    'similarity': 'cosine',
                    'm': 16,
                    'efConstruction': 200
                }
            }
            collection.create_index(index_spec, **index_options)
            result_document['log_lines'].append('Vector index (HNSW) created successfully.')

            # Define the query vector
            query_vector = [0.52, 0.28, 0.12]

            # Perform hybrid search combining text and vector search
            hybrid_query = {
                '$and': [
                    {'$text': {'$search': 'CEO'}},
                    {
                        'vectorContent': {
                            '$vectorNear': {
                                'vector': query_vector,
                                'k': 2,
                                'distanceField': 'distance'
                            }
                        }
                    }
                ]
            }

            projection = {
                '_id': 0,
                'name': 1,
                'bio': 1,
                'distance': 1,
                'score': {'$meta': 'textScore'}
            }

            # Sort by textScore and distance
            results = list(collection.find(hybrid_query, projection).sort([
                ('score', {'$meta': 'textScore'}),
                ('distance', 1)
            ]))

            result_document['details']['hybrid_search_results'] = results

            expected_names = {'Eugenia Lopez'}
            result_names = set(doc['name'] for doc in results)

            if expected_names.issubset(result_names):
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Hybrid search executed successfully.')
            else:
                missing = expected_names - result_names
                error_msg = f'Missing expected results: {missing}'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during hybrid search test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_vector_search_with_number_filter(self):
        """
        Test vector search with pre-filtering using a numeric field ('year').
        """
        collection = self.docdb_coll
        start_time = time.time()
        result_document = self.initialize_result_document('Vector Search with Number Filter Test')

        try:
            # Create vector index including 'year' as a filter field
            index_spec = [
                ('vectorContent', 'vector'),
                ('year', ASCENDING)  # Including 'year' in the index
            ]
            index_options = {
                'name': 'VectorSearchIndex_HNSW_Filter_Number',
                'vectorOptions': {
                    'type': 'hnsw',
                    'dimensions': 3,
                    'similarity': 'cosine',
                    'm': 16,
                    'efConstruction': 200
                }
            }
            collection.create_index(index_spec, **index_options)
            result_document['log_lines'].append('Vector index with number filter created successfully.')

            # Define the query vector
            query_vector = [0.52, 0.28, 0.12]

            # Perform vector search with pre-filter on 'year'
            vector_query = {
                'year': 2001,
                'vectorContent': {
                    '$vectorNear': {
                        'vector': query_vector,
                        'k': 2,
                        'distanceField': 'distance'
                    }
                }
            }

            projection = {
                '_id': 0,
                'name': 1,
                'bio': 1,
                'year': 1,
                'distance': 1
            }

            results = list(collection.find(vector_query, projection))

            result_document['details']['vector_search_with_number_filter_results'] = results

            expected_names = {'Eugenia Lopez', 'Jessie Irwin'}
            result_names = set(doc['name'] for doc in results)

            if expected_names == result_names:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Vector search with number filter executed successfully.')
            else:
                missing = expected_names - result_names
                error_msg = f'Missing expected results: {missing}'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during vector search with number filter test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_vector_search_with_string_filter(self):
        """
        Test vector search with pre-filtering using a string field ('language').
        """
        collection = self.docdb_coll
        start_time = time.time()
        result_document = self.initialize_result_document('Vector Search with String Filter Test')

        try:
            # Create vector index including 'language' as a filter field
            index_spec = [
                ('vectorContent', 'vector'),
                ('language', ASCENDING)  # Including 'language' in the index
            ]
            index_options = {
                'name': 'VectorSearchIndex_HNSW_Filter_String',
                'vectorOptions': {
                    'type': 'hnsw',
                    'dimensions': 3,
                    'similarity': 'cosine',
                    'm': 16,
                    'efConstruction': 200
                }
            }
            collection.create_index(index_spec, **index_options)
            result_document['log_lines'].append('Vector index with string filter created successfully.')

            # Define the query vector
            query_vector = [0.52, 0.28, 0.12]

            # Perform vector search with pre-filter on 'language'
            vector_query = {
                'language': 'es',
                'vectorContent': {
                    '$vectorNear': {
                        'vector': query_vector,
                        'k': 2,
                        'distanceField': 'distance'
                    }
                }
            }

            projection = {
                '_id': 0,
                'name': 1,
                'bio': 1,
                'language': 1,
                'distance': 1
            }

            results = list(collection.find(vector_query, projection))

            result_document['details']['vector_search_with_string_filter_results'] = results

            expected_names = {'Cameron Baker', 'Rory Nguyen'}
            result_names = set(doc['name'] for doc in results)

            if expected_names == result_names:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Vector search with string filter executed successfully.')
            else:
                missing = expected_names - result_names
                error_msg = f'Missing expected results: {missing}'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during vector search with string filter test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def perform_vector_search_test(self, index_type, test_name):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = self.initialize_result_document(test_name)

        try:
            # Create vector index using the specified type
            index_spec = {
                'vectorContent': 'vector'
            }
            if index_type == 'hnsw':
                index_options = {
                    'name': f'VectorSearchIndex_{index_type.upper()}',
                    'vectorOptions': {
                        'type': 'hnsw',
                        'dimensions': 3,
                        'similarity': 'cosine',
                        'm': 16,
                        'efConstruction': 200
                    }
                }
            elif index_type == 'ivfflat':
                index_options = {
                    'name': f'VectorSearchIndex_{index_type.upper()}',
                    'vectorOptions': {
                        'type': 'ivfflat',
                        'dimensions': 3,
                        'similarity': 'cosine',
                        'lists': 100  # applicable for IVFFlat
                    }
                }
            else:
                raise ValueError(f"Unsupported index type: {index_type}")

            collection.create_index(index_spec, **index_options)
            result_document['log_lines'].append(f'Vector index ({index_type.upper()}) created successfully.')

            # Define the query vector
            query_vector = [0.52, 0.28, 0.12]

            # Perform vector search
            vector_query = {
                'vectorContent': {
                    '$vectorNear': {
                        'vector': query_vector,
                        'k': 2,
                        'distanceField': 'distance'
                    }
                }
            }

            projection = {
                '_id': 0,
                'name': 1,
                'bio': 1,
                'distance': 1
            }

            results = list(collection.find(vector_query, projection))

            result_document['details'][f'vector_search_results_{index_type}'] = results

            expected_names = {'Eugenia Lopez', 'Rory Nguyen'}
            result_names = set(doc['name'] for doc in results)

            if expected_names.issubset(result_names):
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append(f'Vector search ({index_type.upper()}) executed successfully.')
            else:
                missing = expected_names - result_names
                error_msg = f'Missing expected results: {missing}'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during vector search ({index_type.upper()}) test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def initialize_result_document(self, test_name):
        start_time = time.time()
        return {
            'status': 'fail',
            'test_name': test_name,
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_search',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'details': {},
        }

    def finalize_result_document(self, result_document, start_time):
        end_time = time.time()
        result_document['elapsed'] = end_time - start_time
        result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

        try:
            server_info = self.docdb_client.server_info()
            result_document['version'] = server_info.get('version', 'unknown')
        except Exception as ve:
            self.logger.error(f"Error retrieving server version: {ve}")
            result_document['version'] = 'unknown'

        result_document = json.loads(json.dumps(result_document, default=str))
        self.test_results.append(result_document)

    @classmethod
    def tearDownClass(cls):
        cls.docdb_coll.drop()
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
