import unittest
from pymongo import TEXT
from pymongo.errors import OperationFailure
from datetime import datetime
import traceback
import contextlib
import io
import sys
from base_test import BaseTest
import logging
import time
import json
# Import SearchIndexModel for vector search index creation.
from pymongo.operations import SearchIndexModel
import config

class TestSearchCapabilities(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_search'
        cls.results_collection_name = 'test_search_results'
        
        # Define the data collection using the generic document DB reference
        cls.data_collection = cls.docdb_db['exampleCollection']

        # Configure logging
        cls.logger = logging.getLogger('TestSearchCapabilities')
        cls.logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler('test_search.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        cls.logger.addHandler(file_handler)

        # In-memory log capture list and custom log handler
        cls.log_capture_list = []
        class ListHandler(logging.Handler):
            def __init__(self, log_list):
                super().__init__()
                self.log_list = log_list
            def emit(self, record):
                log_entry = self.format(record)
                self.log_list.append(log_entry)
        list_handler = ListHandler(cls.log_capture_list)
        list_handler.setFormatter(formatter)
        cls.logger.addHandler(list_handler)

        # Initialize streams and warnings list
        cls.stdout_stream = io.StringIO()
        cls.stderr_stream = io.StringIO()
        cls.log_stream = io.StringIO()
        cls.warnings_list = []

        # Initialize test results accumulator
        cls.test_results = []

    def setUp(self):
        # Clean the collection before each test
        try:
            self.data_collection.drop()
            # Recreate the collection if needed.
            self.docdb_db.create_collection('exampleCollection')
        except Exception:
            pass
        try:
            self.data_collection.drop_indexes()
        except Exception:
            pass

        # Insert sample data for testing
        sample_data = [
            {
                'name': 'Eugenia Lopez',
                'bio': 'Eugenia is the CEO of AdventureWorks.',
                'vectorContent': [0.51, 0.12, 0.23]
            },
            {
                'name': 'Cameron Baker',
                'bio': 'Cameron Baker CFO of AdventureWorks.',
                'vectorContent': [0.55, 0.89, 0.44]
            },
            {
                'name': 'Jessie Irwin',
                'bio': "Jessie Irwin is the former CEO of AdventureWorks and now the director of the Our Planet initiative.",
                'vectorContent': [0.13, 0.92, 0.85]
            },
            {
                'name': 'Rory Nguyen',
                'bio': "Rory Nguyen is the founder of AdventureWorks and the president of the Our Planet initiative.",
                'vectorContent': [0.91, 0.76, 0.83]
            },
        ]
        try:
            self.data_collection.insert_many(sample_data)
            # Create a text index on the 'bio' field if not already existing.
            self.data_collection.create_index([('bio', TEXT)], name='TextIndex')
        except OperationFailure:
            pass
        except Exception as e:
            self.logger.error(f"Unexpected exception during setUp: {e}")

        # Clear streams and warnings before each test
        self.__class__.log_capture_list.clear()
        for stream in (self.__class__.stdout_stream, self.__class__.stderr_stream, self.__class__.log_stream):
            stream.truncate(0)
            stream.seek(0)
        self.__class__.warnings_list.clear()

    # ---------------- Helper Methods ----------------
    def initialize_result_document(self, test_name):
        """Initializes the result document with a standard structure."""
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': test_name,
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
            'errors': [],
            'description': [],
            'command_result': {},
            '_start_time': start_time
        }
        return result_document

    def finalize_result_document(self, result_document):
        """Finalizes and stores the result document."""
        end_time = time.time()
        result_document['elapsed'] = end_time - result_document['_start_time']
        result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()
        try:
            server_info = self.docdb_db.client.server_info()
            server_version = server_info.get('version', 'unknown')
            result_document['version'] = server_version
        except Exception as ve:
            self.logger.error(f"Error retrieving server version: {ve}")
            result_document['version'] = 'unknown'
        result_document['log_lines'] = list(self.__class__.log_capture_list)
        del result_document['_start_time']
        # Ensure JSON serializability
        result_document = json.loads(json.dumps(result_document, default=str))
        self.__class__.test_results.append(result_document)
    # ------------------------------------------------

    def test_HNSW_vector_search(self):
        result_document = self.initialize_result_document("HNSW_vector_search")
        with contextlib.redirect_stdout(self.__class__.stdout_stream), contextlib.redirect_stderr(self.__class__.stderr_stream):
            try:
                # Ensure any existing vector search index with the same name is dropped.
                try:
                    self.data_collection.drop_search_index("VectorSearchHNSWIndex2")
                except Exception:
                    pass
                # Create HNSW vector index using the SearchIndexModel API.
                model = SearchIndexModel(
                    definition={
                        "fields": [
                            {
                                "type": "vector",
                                "path": "vectorContent",
                                "numDimensions": 3,
                                "similarity": "dotProduct",
                                "quantization": "scalar"
                            }
                        ]
                    },
                    name="VectorSearchHNSWIndex2",
                    type="vectorSearch"
                )
                created_index = self.data_collection.create_search_index(model=model)
                self.logger.debug(f"Vector search index '{created_index}' creation initiated.")
                # Poll until the index becomes queryable (timeout after 60 seconds)
                predicate = lambda idx: idx.get("queryable") is True
                timeout = time.time() + 60  # 60-second timeout
                while True:
                    indices = list(self.data_collection.list_search_indexes(created_index))
                    if indices and predicate(indices[0]):
                        break
                    if time.time() > timeout:
                        raise Exception("Timeout waiting for vector search index to become queryable.")
                    time.sleep(5)
                self.logger.debug(f"Vector search index '{created_index}' is queryable.")
                print("HNSW Vector index created successfully.")
                # Run a vector search aggregation pipeline (using $vectorSearch stage)
                pipeline = [
                    {
                        '$vectorSearch': {
                            'index': "VectorSearchHNSWIndex2",
                            'path': 'vectorContent',
                            'queryVector': [0.52, 0.28, 0.12],
                            'limit': 10,
                            'numCandidates': 150
                        }
                    },
                    {
                        '$project': {
                            '_id': 0,
                            'name': 1,
                            'bio': 1,
                            'score': {'$meta': 'vectorSearchScore'}
                        }
                    }
                ]
                results = list(self.data_collection.aggregate(pipeline))
                result_document['command_result'] = {'vector_search_results': results}
                expected_names = {'Eugenia Lopez', 'Rory Nguyen'}
                result_names = set(doc.get('name', "") for doc in results)
                if not expected_names.issubset(result_names):
                    missing = expected_names - result_names
                    result_document['errors'].append(f'Missing expected results: {missing}')
                    result_document['reason'] = 'FAILED'
                else:
                    result_document['status'] = 'pass'
                    result_document['exit_code'] = 0
                    result_document['reason'] = 'PASSED'
            except OperationFailure as e:
                result_document['errors'].append(str(e))
                result_document['reason'] = 'FAILED'
            except Exception as e:
                error_trace = traceback.format_exc()
                result_document['errors'].append(f'Exception during HNSW vector search: {e}\n{error_trace}')
                result_document['reason'] = 'FAILED'
            finally:
                result_document['warnings'] = [str(w.message) for w in self.__class__.warnings_list]
                result_document['stdout'] = self.__class__.stdout_stream.getvalue()
                result_document['stderr'] = self.__class__.stderr_stream.getvalue()
                self.finalize_result_document(result_document)





    def test_sort_not_using_text_index_ordering(self):
        result_document = self.initialize_result_document("Sort_Not_Using_Text_Index_Order")
        with contextlib.redirect_stdout(self.__class__.stdout_stream), contextlib.redirect_stderr(self.__class__.stderr_stream):
            try:
                text_query = {'$text': {'$search': 'CEO'}}
                sorted_results = list(self.data_collection.find(text_query).sort('bio', 1))
                sorted_bios = sorted(doc['bio'] for doc in sorted_results)
                actual_bios = [doc['bio'] for doc in sorted_results]
                if sorted_bios == actual_bios:
                    result_document['errors'].append("Sort operation used text index ordering, which should not be supported.")
                else:
                    result_document['status'] = 'pass'
                    result_document['exit_code'] = 0
                    result_document['reason'] = 'PASSED'
            except OperationFailure as e:
                result_document['errors'].append(str(e))
            except Exception as e:
                result_document['errors'].append(f'Exception during sort search: {e}')
            finally:
                result_document['warnings'] = [str(w.message) for w in self.__class__.warnings_list]
                result_document['stdout'] = self.__class__.stdout_stream.getvalue()
                result_document['stderr'] = self.__class__.stderr_stream.getvalue()
                self.finalize_result_document(result_document)
        self.assertEqual(result_document['status'], 'fail', msg=result_document['errors'])

    def test_basic_text_search(self):
        result_document = self.initialize_result_document("Basic_Text_Search")
        with contextlib.redirect_stdout(io.StringIO()) as stdout, contextlib.redirect_stderr(io.StringIO()) as stderr:
            try:
                text_query = {'$text': {'$search': 'CEO'}}
                results = list(self.data_collection.find(text_query))
                result_document['command_result'] = {'text_search_results': results}
                expected_names = {'Eugenia Lopez', 'Jessie Irwin'}
                result_names = set(doc['name'] for doc in results)
                if expected_names == result_names:
                    result_document['status'] = 'pass'
                    result_document['exit_code'] = 0
                    result_document['reason'] = 'PASSED'
                else:
                    missing = expected_names - result_names
                    result_document['errors'].append(f'Missing expected results: {missing}')
            except OperationFailure as e:
                result_document['errors'].append(str(e))
            except Exception as e:
                result_document['errors'].append(f'Exception during basic text search: {e}')
            finally:
                result_document['stdout'] = stdout.getvalue()
                result_document['stderr'] = stderr.getvalue()
                self.finalize_result_document(result_document)
        self.assertEqual(result_document['status'], 'pass', msg=result_document['errors'])

    def test_hybrid_text_and_vector_search(self):
        result_document = self.initialize_result_document("Hybrid_Text_and_Vector_Search")
        with contextlib.redirect_stdout(io.StringIO()) as stdout, contextlib.redirect_stderr(io.StringIO()) as stderr:
            try:
                query_vector = [0.52, 0.28, 0.12]
                search_query = {
                    "$search": {
                        "compound": {
                            "must": [
                                {
                                    "text": {
                                        "query": "CEO",
                                        "path": "bio"
                                    }
                                },
                                {
                                    "documentSearch": {
                                        "vector": query_vector,
                                        "path": "vectorContent",
                                        "k": 2
                                    }
                                }
                            ]
                        }
                    }
                }
                project_stage = {
                    '$project': {
                        'name': 1,
                        'bio': 1,
                        'score': {'$meta': 'searchScore'}
                    }
                }
                pipeline = [search_query, project_stage]
                results = list(self.data_collection.aggregate(pipeline))
                result_document['command_result'] = {'hybrid_search_results': results}
                expected_names = {'Eugenia Lopez'}
                result_names = set(doc['name'] for doc in results)
                if expected_names == result_names:
                    result_document['status'] = 'pass'
                    result_document['exit_code'] = 0
                    result_document['reason'] = 'PASSED'
                else:
                    missing = expected_names - result_names
                    result_document['errors'].append(f'Missing expected results: {missing}')
            except OperationFailure as e:
                result_document['errors'].append(str(e))
            except Exception as e:
                result_document['errors'].append(f'Exception during hybrid search: {e}')
            finally:
                result_document['stdout'] = stdout.getvalue()
                result_document['stderr'] = stderr.getvalue()
                self.finalize_result_document(result_document)



    @classmethod
    def tearDownClass(cls):
        try:
            cls.data_collection.drop()
            cls.logger.debug("Dropped exampleCollection collection during teardown.")
        except Exception as e:
            cls.logger.error(f"Error dropping exampleCollection collection: {e}")
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
