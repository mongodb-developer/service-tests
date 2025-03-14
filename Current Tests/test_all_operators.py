# tests/test_all_operators.py

import unittest
from pymongo import ASCENDING, DESCENDING, GEOSPHERE  # Removed TEXT
from pymongo.errors import PyMongoError
from datetime import datetime, timedelta
import traceback
import logging
from base_test import BaseTest
import time
import json
import config

class TestAllOperators(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_all_operators'

        # Define collection
        cls.docdb_coll = cls.docdb_db[cls.collection_name]

        # Drop existing collection to ensure a clean slate
        cls.docdb_coll.drop()

        # Insert sample data needed for various operator tests
        initial_data = [
            {"_id": 1, "value": 10, "category": "A", "tags": "tag1 tag2", "location": {"type": "Point", "coordinates": [40, 5]}, "numericArray": [1, 2, 3], "bitwiseField": 42, "dateField": datetime(2023, 1, 1), "stringField": "hello world", "nullField": None},
            {"_id": 2, "value": 20, "category": "B", "tags": "tag2 tag3", "location": {"type": "Point", "coordinates": [42, 3]}, "numericArray": [4, 5, 6], "bitwiseField": 23, "dateField": datetime(2023, 5, 15), "stringField": "test string", "nullField": None},
            {"_id": 3, "value": 30, "category": "C", "tags": "tag3 tag4", "location": {"type": "Point", "coordinates": [41, 4]}, "numericArray": [7, 8, 9], "bitwiseField": 15, "dateField": datetime(2024, 2, 28), "stringField": "another test", "nullField": None},
            {"_id": 4, "value": 40, "category": "A", "tags": "tag1 tag4", "location": {"type": "Point", "coordinates": [39, 6]}, "numericArray": [10, 11, 12], "bitwiseField": 7, "dateField": datetime(2024, 8, 18), "stringField": "sample text", "nullField": None}
        ]

        # Insert data into DocumentDB
        cls.docdb_coll.insert_many(initial_data)

        # Create indexes for geospatial queries
        cls.docdb_coll.create_index([("location", GEOSPHERE)])

        # Configure logging
        cls.logger = logging.getLogger('TestAllOperators')
        cls.logger.setLevel(logging.DEBUG)
        
        # File Handler for logging to 'test_all_operators.log'
        file_handler = logging.FileHandler('test_all_operators.log')
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
        # Assign class variables to instance variables
        self.docdb_coll = self.__class__.docdb_coll
        self.logger = self.__class__.logger

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

    def execute_and_store_query(self, query, operator_name, is_aggregation=False, is_update=False, update_operation=None, **kwargs):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': f"Operator Test - {operator_name}",
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
            'reason': '',
            'description': [],
            'query_result': {},
        }

        try:
            if is_update:
                result = collection.update_many(query, update_operation, **kwargs)
                result_document['query_result'] = {
                    'matched_count': result.matched_count,
                    'modified_count': result.modified_count
                }
                self.logger.debug(f"Update operation '{operator_name}' executed successfully.")
            elif is_aggregation:
                result = list(collection.aggregate(query))
                result_document['query_result'] = result
                self.logger.debug(f"Aggregation operator '{operator_name}' executed successfully.")
            else:
                result = list(collection.find(query))
                result_document['query_result'] = result
                self.logger.debug(f"Find operation with operator '{operator_name}' executed successfully.")

            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['log_lines'].append(f"Operator '{operator_name}' executed successfully.")
        except Exception as e:
            error_trace = traceback.format_exc()
            error_msg = f"Error executing operator '{operator_name}': {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(f"Error executing operator '{operator_name}': {e}\n{error_trace}")
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
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    # Define test methods for each operator
    def test_eq_operator(self):
        operator_name = '$eq'
        query = {"value": {"$eq": 20}}
        self.execute_and_store_query(query, operator_name)

    def test_gt_operator(self):
        operator_name = '$gt'
        query = {"value": {"$gt": 20}}
        self.execute_and_store_query(query, operator_name)

    def test_gte_operator(self):
        operator_name = '$gte'
        query = {"value": {"$gte": 30}}
        self.execute_and_store_query(query, operator_name)

    def test_in_operator(self):
        operator_name = '$in'
        query = {"value": {"$in": [10, 30]}}
        self.execute_and_store_query(query, operator_name)

    def test_lt_operator(self):
        operator_name = '$lt'
        query = {"value": {"$lt": 30}}
        self.execute_and_store_query(query, operator_name)

    def test_lte_operator(self):
        operator_name = '$lte'
        query = {"value": {"$lte": 10}}
        self.execute_and_store_query(query, operator_name)

    def test_ne_operator(self):
        operator_name = '$ne'
        query = {"value": {"$ne": 10}}
        self.execute_and_store_query(query, operator_name)

    def test_nin_operator(self):
        operator_name = '$nin'
        query = {"value": {"$nin": [10, 20]}}
        self.execute_and_store_query(query, operator_name)

    def test_and_operator(self):
        operator_name = '$and'
        query = {"$and": [{"value": {"$gt": 10}}, {"value": {"$lt": 30}}]}
        self.execute_and_store_query(query, operator_name)

    def test_or_operator(self):
        operator_name = '$or'
        query = {"$or": [{"value": 10}, {"value": 20}]}
        self.execute_and_store_query(query, operator_name)

    def test_not_operator(self):
        operator_name = '$not'
        query = {"value": {"$not": {"$gte": 30}}}
        self.execute_and_store_query(query, operator_name)

    def test_exists_operator(self):
        operator_name = '$exists'
        query = {"missingField": {"$exists": False}}
        self.execute_and_store_query(query, operator_name)

    def test_type_operator(self):
        operator_name = '$type'
        query = {"value": {"$type": "int"}}
        self.execute_and_store_query(query, operator_name)

    def test_regex_operator(self):
        operator_name = '$regex'
        query = {"stringField": {"$regex": "test"}}
        self.execute_and_store_query(query, operator_name)

    
    def test_text_operator(self):
        operator_name = '$text'
        query = {"$text": {"$search": "tag1"}}
        self.execute_and_store_query(query, operator_name)
       

    def test_geoIntersects_operator(self):
        operator_name = '$geoIntersects'
        query = {"location": {"$geoIntersects": {"$geometry": {"type": "Point", "coordinates": [40, 5]}}}}
        self.execute_and_store_query(query, operator_name)

    def test_geoWithin_operator(self):
        operator_name = '$geoWithin'
        query = {"location": {"$geoWithin": {"$centerSphere": [[40, 5], 0.1]}}}
        self.execute_and_store_query(query, operator_name)

    def test_near_operator(self):
        operator_name = '$near'
        query = {"location": {"$near": {"$geometry": {"type": "Point", "coordinates": [40, 5]}, "$maxDistance": 1000}}}
        self.execute_and_store_query(query, operator_name)

    def test_all_operator(self):
        operator_name = '$all'
        query = {"numericArray": {"$all": [1, 2]}}
        self.execute_and_store_query(query, operator_name)

    def test_elemMatch_operator(self):
        operator_name = '$elemMatch'
        query = {"numericArray": {"$elemMatch": {"$gt": 8}}}
        self.execute_and_store_query(query, operator_name)

    def test_size_operator(self):
        operator_name = '$size'
        query = {"numericArray": {"$size": 3}}
        self.execute_and_store_query(query, operator_name)

    def test_bitsAllClear_operator(self):
        operator_name = '$bitsAllClear'
        query = {"bitwiseField": {"$bitsAllClear": 8}}
        self.execute_and_store_query(query, operator_name)

    def test_bitsAllSet_operator(self):
        operator_name = '$bitsAllSet'
        query = {"bitwiseField": {"$bitsAllSet": 8}}
        self.execute_and_store_query(query, operator_name)

    def test_bitsAnyClear_operator(self):
        operator_name = '$bitsAnyClear'
        query = {"bitwiseField": {"$bitsAnyClear": 8}}
        self.execute_and_store_query(query, operator_name)

    def test_bitsAnySet_operator(self):
        operator_name = '$bitsAnySet'
        query = {"bitwiseField": {"$bitsAnySet": 8}}
        self.execute_and_store_query(query, operator_name)

    def test_currentDate_operator(self):
        operator_name = '$currentDate'
        query = {"category": "A"}
        update = {"$currentDate": {"lastModified": True}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_inc_operator(self):
        operator_name = '$inc'
        query = {"category": "B"}
        update = {"$inc": {"value": 5}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_min_operator(self):
        operator_name = '$min'
        query = {"category": "C"}
        update = {"$min": {"value": 25}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_max_operator(self):
        operator_name = '$max'
        query = {"category": "C"}
        update = {"$max": {"value": 35}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_mul_operator(self):
        operator_name = '$mul'
        query = {"category": "A"}
        update = {"$mul": {"value": 2}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_rename_operator(self):
        operator_name = '$rename'
        query = {"category": "B"}
        update = {"$rename": {"stringField": "renamedField"}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_set_operator(self):
        operator_name = '$set'
        query = {"category": "C"}
        update = {"$set": {"newField": "newValue"}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_unset_operator(self):
        operator_name = '$unset'
        query = {"category": "A"}
        update = {"$unset": {"nullField": ""}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_addToSet_operator(self):
        operator_name = '$addToSet'
        query = {"category": "B"}
        update = {"$addToSet": {"tags": "tag5"}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_pop_operator(self):
        operator_name = '$pop'
        query = {"category": "C"}
        update = {"$pop": {"numericArray": 1}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_pull_operator(self):
        operator_name = '$pull'
        query = {"category": "A"}
        update = {"$pull": {"numericArray": 2}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_push_operator(self):
        operator_name = '$push'
        query = {"category": "B"}
        update = {"$push": {"numericArray": 99}}
        self.execute_and_store_query(query, operator_name, is_update=True, update_operation=update)

    def test_abs_expression(self):
        operator_name = '$abs'
        pipeline = [{"$project": {"absValue": {"$abs": -1}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_add_expression(self):
        operator_name = '$add'
        pipeline = [{"$project": {"sum": {"$add": ["$value", 10]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_ceil_expression(self):
        operator_name = '$ceil'
        pipeline = [{"$project": {"ceilValue": {"$ceil": 4.7}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_divide_expression(self):
        operator_name = '$divide'
        pipeline = [{"$project": {"dividedValue": {"$divide": ["$value", 2]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_exp_expression(self):
        operator_name = '$exp'
        pipeline = [{"$project": {"expValue": {"$exp": "$value"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_floor_expression(self):
        operator_name = '$floor'
        pipeline = [{"$project": {"floorValue": {"$floor": 4.7}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_ln_expression(self):
        operator_name = '$ln'
        pipeline = [{"$project": {"lnValue": {"$ln": "$value"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_log_expression(self):
        operator_name = '$log'
        pipeline = [{"$project": {"logValue": {"$log": ["$value", 10]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_log10_expression(self):
        operator_name = '$log10'
        pipeline = [{"$project": {"log10Value": {"$log10": "$value"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_mod_expression(self):
        operator_name = '$mod (expression)'
        pipeline = [{"$project": {"modValue": {"$mod": ["$value", 3]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_multiply_expression(self):
        operator_name = '$multiply'
        pipeline = [{"$project": {"multipliedValue": {"$multiply": ["$value", 2]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_pow_expression(self):
        operator_name = '$pow'
        pipeline = [{"$project": {"powValue": {"$pow": ["$value", 2]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_round_expression(self):
        operator_name = '$round'
        pipeline = [{"$project": {"roundedValue": {"$round": [4.567, 2]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_sqrt_expression(self):
        operator_name = '$sqrt'
        pipeline = [{"$project": {"sqrtValue": {"$sqrt": "$value"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_subtract_expression(self):
        operator_name = '$subtract'
        pipeline = [{"$project": {"subtractedValue": {"$subtract": ["$value", 5]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_trunc_expression(self):
        operator_name = '$trunc'
        pipeline = [{"$project": {"truncatedValue": {"$trunc": 4.567}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_arrayElemAt_expression(self):
        operator_name = '$arrayElemAt'
        pipeline = [{"$project": {"elementAt": {"$arrayElemAt": ["$numericArray", 1]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_concatArrays_expression(self):
        operator_name = '$concatArrays'
        pipeline = [{"$project": {"concatenatedArray": {"$concatArrays": ["$numericArray", [13, 14]]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_filter_expression(self):
        operator_name = '$filter'
        pipeline = [{"$project": {"filteredArray": {"$filter": {"input": "$numericArray", "as": "num", "cond": {"$gt": ["$$num", 5]}}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_map_expression(self):
        operator_name = '$map'
        pipeline = [{"$project": {"mappedArray": {"$map": {"input": "$numericArray", "as": "num", "in": {"$multiply": ["$$num", 2]}}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_reduce_expression(self):
        operator_name = '$reduce'
        pipeline = [{"$project": {"sumOfArray": {"$reduce": {"input": "$numericArray", "initialValue": 0, "in": {"$add": ["$$value", "$$this"]}}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_size_expression(self):
        operator_name = '$size (expression)'
        pipeline = [{"$project": {"arraySize": {"$size": "$numericArray"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_dateAdd_expression(self):
        operator_name = '$dateAdd'
        pipeline = [{"$project": {"newDate": {"$dateAdd": {"startDate": "$dateField", "unit": "day", "amount": 5}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_dateToString_expression(self):
        operator_name = '$dateToString'
        pipeline = [{"$project": {"dateString": {"$dateToString": {"format": "%Y-%m-%d", "date": "$dateField"}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_dayOfWeek_expression(self):
        operator_name = '$dayOfWeek'
        pipeline = [{"$project": {"dayOfWeek": {"$dayOfWeek": "$dateField"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_concat_expression(self):
        operator_name = '$concat'
        pipeline = [{"$project": {"fullString": {"$concat": ["$stringField", " - appended text"]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_toLower_expression(self):
        operator_name = '$toLower'
        pipeline = [{"$project": {"lowerString": {"$toLower": "$stringField"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_strLenBytes_expression(self):
        operator_name = '$strLenBytes'
        pipeline = [{"$project": {"stringLength": {"$strLenBytes": "$stringField"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_regexMatch_expression(self):
        operator_name = '$regexMatch'
        pipeline = [{"$project": {"regexMatch": {"$regexMatch": {"input": "$stringField", "regex": "test"}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_cond_expression(self):
        operator_name = '$cond'
        pipeline = [{"$project": {"result": {"$cond": {"if": {"$gt": ["$value", 25]}, "then": "Greater than 25", "else": "25 or less"}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_ifNull_expression(self):
        operator_name = '$ifNull'
        pipeline = [{"$project": {"valueOrDefault": {"$ifNull": ["$nullField", "default value"]}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_toInt_expression(self):
        operator_name = '$toInt'
        pipeline = [{"$project": {"intValue": {"$toInt": "$stringField"}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    def test_convert_expression(self):
        operator_name = '$convert'
        pipeline = [{"$project": {"convertedValue": {"$convert": {"input": "$stringField", "to": "int", "onError": 0}}}}]
        self.execute_and_store_query(pipeline, operator_name, is_aggregation=True)

    @classmethod
    def tearDownClass(cls):
        cls.docdb_coll.drop()
        cls.logger.debug("Dropped collection during teardown.")
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()