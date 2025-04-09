# tests/test_aggregation.py

import unittest
from pymongo import ASCENDING, DESCENDING, TEXT, GEOSPHERE
from pymongo.errors import PyMongoError, OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from datetime import datetime
import traceback
import contextlib
from base_test import BaseTest
import logging
import asyncio
from parameterized import parameterized
import time
import json
import config

class TestAggregation(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_aggregation'

        # Define main collection
        cls.docdb_coll = cls.docdb_db[cls.collection_name]

        # Define lookup collection
        cls.lookup_collection_name = 'test_aggregation_lookup'
        cls.docdb_lookup_coll = cls.docdb_db[cls.lookup_collection_name]

        # Define output collection
        cls.output_collection_name = 'aggregation_output'
        cls.docdb_output_coll = cls.docdb_db[cls.output_collection_name]

        # Drop existing collections to ensure a clean slate
        cls.docdb_coll.drop()
        cls.docdb_lookup_coll.drop()
        cls.docdb_output_coll.drop()

        # Initialize main collection with sample data
        initial_data = [
            {'_id': 1, 'category': 'A', 'value': 10, 'tags': ['red', 'blue'], 'location': {'type': 'Point', 'coordinates': [40, 5]}, 'parent': None, 'date': datetime(2021, 1, 1)},
            {'_id': 2, 'category': 'B', 'value': 20, 'tags': ['blue'], 'location': {'type': 'Point', 'coordinates': [42, 10]}, 'parent': 1, 'date': datetime(2021, 1, 2)},
            {'_id': 3, 'category': 'A', 'value': 15, 'tags': ['red', 'green'], 'location': {'type': 'Point', 'coordinates': [44, 15]}, 'parent': 2, 'date': datetime(2021, 1, 3)},
            {'_id': 4, 'category': 'B', 'value': 25, 'tags': ['green', 'yellow'], 'location': {'type': 'Point', 'coordinates': [46, 20]}, 'parent': 3, 'date': datetime(2021, 1, 4)},
            {'_id': 5, 'category': 'C', 'value': 30, 'tags': ['yellow'], 'location': {'type': 'Point', 'coordinates': [48, 25]}, 'parent': 4, 'date': datetime(2021, 1, 5)}
        ]

        lookup_data = [
            {'_id': 'A', 'description': 'Category A'},
            {'_id': 'B', 'description': 'Category B'},
            {'_id': 'C', 'description': 'Category C'}
        ]

        # Insert data into DocumentDB
        cls.docdb_coll.insert_many(initial_data)
        cls.docdb_lookup_coll.insert_many(lookup_data)

        # Configure logging
        cls.logger = logging.getLogger('TestAggregation')
        cls.logger.setLevel(logging.DEBUG)
        
        # File Handler for logging to 'test_aggregation.log'
        file_handler = logging.FileHandler('test_aggregation.log')
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
        # Assign class variables to instance variables for easy access
        self.collection = self.__class__.docdb_coll
        self.lookup_collection = self.__class__.docdb_lookup_coll
        self.output_collection = self.__class__.docdb_output_coll
        self.logger = self.__class__.logger

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

    def tearDown(self):
        pass  # No specific teardown needed per test

    def execute_aggregation_test(self, pipeline, description):
        """
        Executes the given aggregation pipeline on DocumentDB,
        logs the results, and records any errors encountered.
        If the pipeline contains a $documents stage, it is executed
        as a collectionless aggregation on the database.
        """
        start_time = time.time()
        result_document = {
            'status': 'fail',  # Default to 'fail'; will update based on conditions
            'test_name': f'Aggregation Test - {description}',
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
            'aggregation_result': []
        }

        try:
            with contextlib.redirect_stdout(None), contextlib.redirect_stderr(None):
                # If pipeline includes a $documents stage, run aggregation on the database
                if any("$documents" in stage for stage in pipeline):
                    agg_cursor = self.collection.database.aggregate(pipeline, cursor={})
                else:
                    agg_cursor = self.collection.aggregate(pipeline, allowDiskUse=True)
                aggregation_result = list(agg_cursor)
                result_document['aggregation_result'] = aggregation_result
                self.logger.debug('Aggregation executed successfully.')
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
        except Exception as e:
            error_trace = traceback.format_exc()
            self.logger.error(f'Error during aggregation test "{description}": {e}\n{error_trace}')
            result_document['status'] = 'fail'
            result_document['exit_code'] = 1
            result_document['reason'] = 'FAILED'
            result_document['description'].append(str(e))
        finally:
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()
            try:
                server_info = self.collection.database.client.server_info()
                server_version = server_info.get('version', 'unknown')
                result_document['version'] = server_version
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'
            result_document['log_lines'] = list(self.log_capture_list)
            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

    def test_addFields_stage(self):
        pipeline = [
            {
                '$addFields': {
                    'value_plus_ten': {'$add': ['$value', 10]},
                    'current_date': '$$NOW'
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$addFields Stage Test')

    def test_bucket_stage(self):
        pipeline = [
            {
                '$bucket': {
                    'groupBy': '$value',
                    'boundaries': [0, 15, 25, 35],
                    'default': 'Other',
                    'output': {
                        'count': {'$sum': 1},
                        'values': {'$push': '$value'}
                    }
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$bucket Stage Test')

    def test_bucketAuto_stage(self):
        pipeline = [
            {
                '$bucketAuto': {
                    'groupBy': '$value',
                    'buckets': 3,
                    'output': {
                        'count': {'$sum': 1},
                        'values': {'$push': '$value'}
                    }
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$bucketAuto Stage Test')

    def test_changeStream_stage(self):
        # $changeStream is not applicable in this context.
        self.logger.info('$changeStream Stage Test is not applicable in this context.')

    def test_collStats_stage(self):
        pipeline = [
            {
                '$collStats': {
                    'storageStats': {}
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$collStats Stage Test')

    def test_count_stage(self):
        pipeline = [
            {
                '$match': {'category': 'A'}
            },
            {
                '$count': 'total_A_category'
            }
        ]
        self.execute_aggregation_test(pipeline, '$count Stage Test')

    def test_densify_stage(self):
        pipeline = [
            {
                '$densify': {
                    'field': 'date',
                    'range': {
                        'step': 1,
                        'unit': 'day',
                        'bounds': 'full'
                    }
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$densify Stage Test')

    def test_documents_stage(self):
        pipeline = [
            {
                '$documents': [
                    {'_id': 6, 'category': 'D', 'value': 35},
                    {'_id': 7, 'category': 'E', 'value': 40}
                ]
            }
        ]
        self.execute_aggregation_test(pipeline, '$documents Stage Test')

    def test_facet_stage(self):
        pipeline = [
            {
                '$facet': {
                    'categories': [
                        {'$group': {'_id': '$category', 'count': {'$sum': 1}}}
                    ],
                    'values': [
                        {'$group': {'_id': None, 'totalValue': {'$sum': '$value'}}}
                    ]
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$facet Stage Test')

    def test_fill_stage(self):
        # Modify the $fill stage to include a top-level sortBy with exactly one element.
        pipeline = [
            {
                '$fill': {
                    'sortBy': {'date': 1},
                    'output': {
                        'value': {'method': 'linear'}
                    }
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$fill Stage Test')

    def test_geoNear_stage(self):
        try:
            self.docdb_coll.create_index([('location', GEOSPHERE)])
        except PyMongoError as e:
            self.logger.error(f'Error creating geospatial index: {e}')
        pipeline = [
            {
                '$geoNear': {
                    'near': {'type': 'Point', 'coordinates': [43, 12]},
                    'distanceField': 'dist.calculated',
                    'maxDistance': 500000,
                    'spherical': True
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$geoNear Stage Test')

    def test_graphLookup_stage(self):
        pipeline = [
            {
                '$graphLookup': {
                    'from': self.collection_name,
                    'startWith': '$parent',
                    'connectFromField': 'parent',
                    'connectToField': '_id',
                    'as': 'ancestors'
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$graphLookup Stage Test')

    def test_group_stage(self):
        pipeline = [
            {
                '$group': {
                    '_id': '$category',
                    'total_value': {'$sum': '$value'},
                    'average_value': {'$avg': '$value'}
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$group Stage Test')

    def test_indexStats_stage(self):
        pipeline = [
            {
                '$indexStats': {}
            }
        ]
        self.execute_aggregation_test(pipeline, '$indexStats Stage Test')

    def test_limit_stage(self):
        pipeline = [
            {
                '$limit': 3
            }
        ]
        self.execute_aggregation_test(pipeline, '$limit Stage Test')

    def test_lookup_stage(self):
        pipeline = [
            {
                '$lookup': {
                    'from': self.lookup_collection_name,
                    'localField': 'category',
                    'foreignField': '_id',
                    'as': 'category_info'
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$lookup Stage Test')

    def test_match_stage(self):
        pipeline = [
            {
                '$match': {'value': {'$gt': 15}}
            }
        ]
        self.execute_aggregation_test(pipeline, '$match Stage Test')

    def test_merge_stage(self):
        self.docdb_output_coll.drop()
        pipeline = [
            {
                '$group': {
                    '_id': '$category',
                    'total_value': {'$sum': '$value'}
                }
            },
            {
                '$merge': {
                    'into': self.output_collection_name,
                    'on': '_id',
                    'whenMatched': 'replace',
                    'whenNotMatched': 'insert'
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$merge Stage Test')

    def test_out_stage(self):
        self.docdb_output_coll.drop()
        pipeline = [
            {
                '$match': {'category': 'A'}
            },
            {
                '$out': self.output_collection_name
            }
        ]
        self.execute_aggregation_test(pipeline, '$out Stage Test')

    def test_project_stage(self):
        pipeline = [
            {
                '$project': {
                    'category': 1,
                    'value': 1,
                    'value_squared': {'$multiply': ['$value', '$value']}
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$project Stage Test')

    def test_redact_stage(self):
        pipeline = [
            {
                '$redact': {
                    '$cond': {
                        'if': {'$eq': ['$category', 'A']},
                        'then': '$$DESCEND',
                        'else': '$$PRUNE'
                    }
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$redact Stage Test')

    def test_replaceRoot_stage(self):
        pipeline = [
            {
                '$replaceRoot': {'newRoot': '$location'}
            }
        ]
        self.execute_aggregation_test(pipeline, '$replaceRoot Stage Test')

    def test_replaceWith_stage(self):
        pipeline = [
            {
                '$replaceWith': '$location'
            }
        ]
        self.execute_aggregation_test(pipeline, '$replaceWith Stage Test')

    def test_sample_stage(self):
        pipeline = [
            {
                '$sample': {'size': 2}
            }
        ]
        self.execute_aggregation_test(pipeline, '$sample Stage Test')

    def test_set_stage(self):
        pipeline = [
            {
                '$set': {
                    'value_incremented': {'$add': ['$value', 1]}
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$set Stage Test')

    def test_setWindowFields_stage(self):
        pipeline = [
            {
                '$setWindowFields': {
                    'partitionBy': '$category',
                    'sortBy': {'value': 1},
                    'output': {
                        'cumulativeValue': {
                            '$sum': '$value',
                            'window': {'documents': ['unbounded', 'current']}
                        }
                    }
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$setWindowFields Stage Test')

    def test_skip_stage(self):
        pipeline = [
            {
                '$skip': 2
            }
        ]
        self.execute_aggregation_test(pipeline, '$skip Stage Test')

    def test_sort_stage(self):
        pipeline = [
            {
                '$sort': {'value': -1}
            }
        ]
        self.execute_aggregation_test(pipeline, '$sort Stage Test')

    def test_sortByCount_stage(self):
        pipeline = [
            {
                '$unwind': '$tags'
            },
            {
                '$sortByCount': '$tags'
            }
        ]
        self.execute_aggregation_test(pipeline, '$sortByCount Stage Test')

    def test_unionWith_stage(self):
        pipeline = [
            {
                '$unionWith': {
                    'coll': self.lookup_collection_name
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$unionWith Stage Test')

    def test_unset_stage(self):
        pipeline = [
            {
                '$unset': ['tags', 'location']
            }
        ]
        self.execute_aggregation_test(pipeline, '$unset Stage Test')

    def test_unwind_stage(self):
        pipeline = [
            {
                '$unwind': '$tags'
            }
        ]
        self.execute_aggregation_test(pipeline, '$unwind Stage Test')

    def test_variables_in_expressions(self):
        pipeline = [
            {
                '$project': {
                    'category': 1,
                    'value': 1,
                    'is_A': {'$eq': ['$category', 'A']},
                    'now': '$$NOW',
                    'root': '$$ROOT',
                    'current': '$$CURRENT'
                }
            }
        ]
        self.execute_aggregation_test(pipeline, 'Variables in Aggregation Expressions Test')

    def test_planCacheStats_stage(self):
        pipeline = [
            {
                '$planCacheStats': {}
            }
        ]
        self.execute_aggregation_test(pipeline, '$planCacheStats Stage Test')

    #def test_shardedDataDistribution_stage(self):
        #pipeline = [
            #{
                #'$shardedDataDistribution': {}
            #}
        #]
        #self.execute_aggregation_test(pipeline, '$shardedDataDistribution Stage Test')

    #def test_search_stage(self):
        #pipeline = [
            #{
               # '$search': {
                  #  'text': {
                    #    'query': 'Category A',
                    #    'path': 'description'
                   # }
               # }
           # }
       # ]
       # self.execute_aggregation_test(pipeline, '$search Stage Test')

   # def test_searchMeta_stage(self):
        #pipeline = [
            #{
                #'$searchMeta': {
                   # 'text': {
                    #    'query': 'Category A',
                     #   'path': 'description'
                   # }
               # }
           # }
       # ]
       # self.execute_aggregation_test(pipeline, '$searchMeta Stage Test')

    def test_listSearchIndexes_stage(self):
        pipeline = [
            {
                '$listSearchIndexes': {}
            }
        ]
        self.execute_aggregation_test(pipeline, '$listSearchIndexes Stage Test')

    def test_variables(self):
        pipeline = [
            {
                '$project': {
                    'currentDate': '$$NOW',
                    'rootDocument': '$$ROOT',
                    'currentField': '$$CURRENT',
                    'userRoles': '$$USER_ROLES',
                    'clusterTime': '$$CLUSTER_TIME'
                }
            }
        ]
        self.execute_aggregation_test(pipeline, 'Variables Test')

    @classmethod
    def tearDownClass(cls):
        cls.docdb_coll.drop()
        cls.docdb_lookup_coll.drop()
        cls.docdb_output_coll.drop()
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
