# tests/test_aggregation.py

import unittest
from pymongo import ASCENDING, DESCENDING, TEXT, GEOSPHERE
from pymongo.errors import PyMongoError, OperationFailure
from datetime import datetime
import traceback
import contextlib
from base_test import BaseTest
import logging
import time
import json

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

        # Insert data into Amazon DocumentDB
        cls.docdb_coll.insert_many(initial_data)
        cls.docdb_lookup_coll.insert_many(lookup_data)

        # Configure logging
        cls.logger = logging.getLogger('TestAggregation')
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler('test_aggregation.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        cls.logger.addHandler(handler)

    def setUp(self):
        # Assign class variables to instance variables for easy access
        self.collection = self.__class__.docdb_coll
        self.lookup_collection = self.__class__.docdb_lookup_coll
        self.output_collection = self.__class__.docdb_output_coll
        self.logger = self.__class__.logger

    def execute_aggregation_test(self, pipeline, description):
        """
        Executes the given aggregation pipeline on Amazon DocumentDB,
        logs the results, and records any errors encountered.
        """
        start_time = time.time()
        result_document = {
            'status': 'fail',  # Default to 'fail'; will update based on conditions
            'test_name': f'Aggregation Test - {description}',
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': self.collection_name,
            'version': 'unknown',  # Will be updated dynamically
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'aggregation_result': []
        }

        try:
            with contextlib.redirect_stdout(None), contextlib.redirect_stderr(None):
                # Execute aggregation on Amazon DocumentDB
                try:
                    aggregation_result = list(self.collection.aggregate(pipeline, allowDiskUse=True))
                    result_document['aggregation_result'] = aggregation_result
                    result_document['log_lines'].append('Aggregation executed successfully.')
                except PyMongoError as e:
                    error_msg = f'Aggregation Error: {e}'
                    self.logger.error(error_msg)
                    result_document['description'].append(error_msg)
                    result_document['log_lines'].append(error_msg)
                    raise  # Re-raise exception to be caught by outer except

            # Determine success status
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
            # Capture elapsed time and end time
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            # Retrieve server version dynamically
            try:
                server_info = self.collection.database.client.server_info()
                server_version = server_info.get('version', 'unknown')
                result_document['version'] = server_version
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Ensure all fields are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    def test_addFields_stage(self):
        """
        Test $addFields stage.
        """
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
        """
        Test $bucket stage.
        """
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
        """
        Test $bucketAuto stage.
        """
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

    def test_collStats_stage(self):
        """
        Test $collStats stage.
        """
        pipeline = [
            {
                '$collStats': {
                    'storageStats': {}
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$collStats Stage Test')

    def test_count_stage(self):
        """
        Test $count stage.
        """
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
        """
        Test $densify stage.
        """
        # $densify is available in MongoDB 5.1+, may not be supported in DocumentDB
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

    def test_facet_stage(self):
        """
        Test $facet stage.
        """
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
        """
        Test $fill stage.
        """
        # $fill is available in MongoDB 5.3+, may not be supported in DocumentDB
        pipeline = [
            {
                '$sort': {'date': 1}
            },
            {
                '$fill': {
                    'output': {
                        'value': {'method': 'linear'}
                    }
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$fill Stage Test')

    def test_geoNear_stage(self):
        """
        Test $geoNear stage.
        """
        # Ensure geospatial index exists
        try:
            self.collection.create_index([('location', GEOSPHERE)])
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
        """
        Test $graphLookup stage.
        """
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
        """
        Test $group stage.
        """
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

    def test_limit_stage(self):
        """
        Test $limit stage.
        """
        pipeline = [
            {
                '$limit': 3
            }
        ]
        self.execute_aggregation_test(pipeline, '$limit Stage Test')

    def test_lookup_stage(self):
        """
        Test $lookup stage.
        """
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
        """
        Test $match stage.
        """
        pipeline = [
            {
                '$match': {'value': {'$gt': 15}}
            }
        ]
        self.execute_aggregation_test(pipeline, '$match Stage Test')

    def test_project_stage(self):
        """
        Test $project stage.
        """
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
        """
        Test $redact stage.
        """
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
        """
        Test $replaceRoot stage.
        """
        pipeline = [
            {
                '$replaceRoot': {'newRoot': '$location'}
            }
        ]
        self.execute_aggregation_test(pipeline, '$replaceRoot Stage Test')

    def test_replaceWith_stage(self):
        """
        Test $replaceWith stage.
        """
        pipeline = [
            {
                '$replaceWith': '$location'
            }
        ]
        self.execute_aggregation_test(pipeline, '$replaceWith Stage Test')

    def test_sample_stage(self):
        """
        Test $sample stage.
        """
        pipeline = [
            {
                '$sample': {'size': 2}
            }
        ]
        self.execute_aggregation_test(pipeline, '$sample Stage Test')

    def test_set_stage(self):
        """
        Test $set stage.
        """
        pipeline = [
            {
                '$set': {
                    'value_incremented': {'$add': ['$value', 1]}
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$set Stage Test')

    def test_skip_stage(self):
        """
        Test $skip stage.
        """
        pipeline = [
            {
                '$skip': 2
            }
        ]
        self.execute_aggregation_test(pipeline, '$skip Stage Test')

    def test_sort_stage(self):
        """
        Test $sort stage.
        """
        pipeline = [
            {
                '$sort': {'value': -1}
            }
        ]
        self.execute_aggregation_test(pipeline, '$sort Stage Test')

    def test_sortByCount_stage(self):
        """
        Test $sortByCount stage.
        """
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
        """
        Test $unionWith stage.
        """
        pipeline = [
            {
                '$unionWith': {
                    'coll': self.lookup_collection_name
                }
            }
        ]
        self.execute_aggregation_test(pipeline, '$unionWith Stage Test')

    def test_unset_stage(self):
        """
        Test $unset stage.
        """
        pipeline = [
            {
                '$unset': ['tags', 'location']
            }
        ]
        self.execute_aggregation_test(pipeline, '$unset Stage Test')

    def test_unwind_stage(self):
        """
        Test $unwind stage.
        """
        pipeline = [
            {
                '$unwind': '$tags'
            }
        ]
        self.execute_aggregation_test(pipeline, '$unwind Stage Test')

    def test_variables_in_expressions(self):
        """
        Test variables in aggregation expressions.
        """
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

    def test_variables(self):
        """
        Test variables like $$NOW, $$ROOT, etc.
        """
        pipeline = [
            {
                '$project': {
                    'currentDate': '$$NOW',
                    'rootDocument': '$$ROOT',
                    'currentField': '$$CURRENT'
                }
            }
        ]
        self.execute_aggregation_test(pipeline, 'Variables Test')

    def test_merge_stage(self):
        """
        Test $merge stage.
        """
        # Ensure output collection is clean
        self.output_collection.drop()

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
        """
        Test $out stage.
        """
        # Ensure output collection is clean
        self.output_collection.drop()

        pipeline = [
            {
                '$match': {'category': 'A'}
            },
            {
                '$out': self.output_collection_name
            }
        ]
        self.execute_aggregation_test(pipeline, '$out Stage Test')

    @classmethod
    def tearDownClass(cls):
        # Clean up collections after all tests
        cls.docdb_coll.drop()
        cls.docdb_lookup_coll.drop()
        cls.docdb_output_coll.drop()
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
