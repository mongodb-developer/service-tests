# tests/test_timeseries.py

import unittest
from pymongo.errors import OperationFailure, CollectionInvalid
from datetime import datetime, timedelta
import traceback
import logging
import json
import time
from base_test import BaseTest

class TestTimeSeriesCapabilities(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_timeseries'
        cls.docdb_coll = cls.docdb_db['timeseriesCollection']

        cls.logger = logging.getLogger('TestTimeSeriesCapabilities')
        cls.logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler('test_timeseries.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        cls.logger.addHandler(handler)

    def setUp(self):
        self.docdb_coll = self.__class__.docdb_coll
        self.logger = self.__class__.logger

        # Drop the collection if it exists
        try:
            self.docdb_db.drop_collection('timeseriesCollection')
            self.logger.info('Dropped existing timeseriesCollection.')
        except Exception as e:
            self.logger.info('No existing timeseriesCollection to drop.')

        self.timeseries_supported = False
        try:
            timeseries_options = {
                'timeField': 'timestamp',
                'metaField': 'metadata',
                'granularity': 'seconds'
            }
            self.docdb_db.create_collection('timeseriesCollection', timeseries=timeseries_options)
            self.timeseries_supported = True
            self.logger.info('Time series collection created successfully.')
        except Exception as e:
            self.timeseries_error_message = f"Time series not supported: {str(e)}"
            self.logger.error(self.timeseries_error_message)
            try:
                self.docdb_db.create_collection('timeseriesCollection')
                self.logger.info('Regular collection created instead.')
            except Exception as e2:
                self.logger.error(f"Error creating regular collection: {e2}")

        current_time = datetime.utcnow()
        sample_data = [
            {
                'timestamp': current_time - timedelta(minutes=5),
                'metadata': {'sensorId': 1, 'type': 'temperature'},
                'value': 22.5
            },
            {
                'timestamp': current_time - timedelta(minutes=4),
                'metadata': {'sensorId': 1, 'type': 'temperature'},
                'value': 22.7
            },
            {
                'timestamp': current_time - timedelta(minutes=3),
                'metadata': {'sensorId': 2, 'type': 'humidity'},
                'value': 55.2
            },
            {
                'timestamp': current_time - timedelta(minutes=2),
                'metadata': {'sensorId': 2, 'type': 'humidity'},
                'value': 54.8
            },
            {
                'timestamp': current_time - timedelta(minutes=1),
                'metadata': {'sensorId': 1, 'type': 'temperature'},
                'value': 22.6
            },
        ]

        self.docdb_coll.insert_many(sample_data)
        self.logger.info('Sample data inserted successfully.')

    def test_time_series_insert(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series Insert Test',
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_timeseries',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'details': {},
        }

        try:
            if not self.timeseries_supported:
                raise Exception(self.timeseries_error_message)

            new_data = {
                'timestamp': datetime.utcnow(),
                'metadata': {'sensorId': 3, 'type': 'pressure'},
                'value': 101.3
            }
            collection.insert_one(new_data)
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['log_lines'].append('Time series data inserted successfully.')
        except Exception as e:
            error_msg = f"Error during time series insert test: {str(e)}"
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
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

    def test_time_series_query(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series Query Test',
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_timeseries',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'details': {},
        }

        try:
            if not self.timeseries_supported:
                raise Exception(self.timeseries_error_message)

            query = {'metadata.sensorId': 1}
            results = list(collection.find(query).sort('timestamp', 1))
            result_document['details']['query_results'] = results

            if results:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Time series query executed successfully.')
            else:
                error_msg = 'No data found for sensorId 1'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during time series query test: {str(e)}"
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
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

    def test_time_series_aggregation(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series Aggregation Test',
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_timeseries',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'details': {},
        }

        try:
            if not self.timeseries_supported:
                raise Exception(self.timeseries_error_message)

            pipeline = [
                {'$match': {'metadata.type': 'temperature'}},
                {'$group': {
                    '_id': None,
                    'avgTemperature': {'$avg': '$value'}
                }}
            ]
            results = list(collection.aggregate(pipeline))
            result_document['details']['aggregation_results'] = results

            if results:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Time series aggregation executed successfully.')
            else:
                error_msg = 'Aggregation returned no results'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during time series aggregation test: {str(e)}"
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
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

    def test_time_series_bucket_aggregation(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series Bucket Aggregation Test',
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_timeseries',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'details': {},
        }

        try:
            if not self.timeseries_supported:
                raise Exception(self.timeseries_error_message)

            current_time = datetime.utcnow()
            start_time_agg = current_time - timedelta(minutes=5)

            pipeline = [
                {'$match': {'timestamp': {'$gte': start_time_agg, '$lte': current_time}}},
                {'$bucket': {
                    'groupBy': '$timestamp',
                    'boundaries': [
                        start_time_agg,
                        start_time_agg + timedelta(minutes=1),
                        start_time_agg + timedelta(minutes=2),
                        start_time_agg + timedelta(minutes=3),
                        start_time_agg + timedelta(minutes=4),
                        start_time_agg + timedelta(minutes=5),
                        current_time
                    ],
                    'default': 'Other',
                    'output': {
                        'count': {'$sum': 1},
                        'values': {'$push': '$value'}
                    }
                }}
            ]
            results = list(collection.aggregate(pipeline))
            result_document['details']['bucket_aggregation_results'] = results

            if results:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Time series bucket aggregation executed successfully.')
            else:
                error_msg = 'Bucket aggregation returned no results'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during time series bucket aggregation test: {str(e)}"
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
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

    def test_time_series_window_functions(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series Window Functions Test',
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_timeseries',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'details': {},
        }

        try:
            if not self.timeseries_supported:
                raise Exception(self.timeseries_error_message)

            pipeline = [
                {'$match': {'metadata.type': 'temperature'}},
                {'$setWindowFields': {
                    'partitionBy': '$metadata.sensorId',
                    'sortBy': {'timestamp': 1},
                    'output': {
                        'movingAvg': {
                            '$avg': '$value',
                            'window': {
                                'documents': [-1, 0]
                            }
                        }
                    }
                }}
            ]
            results = list(collection.aggregate(pipeline))
            result_document['details']['window_function_results'] = results

            if results:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Time series window functions executed successfully.')
            else:
                error_msg = 'Window function returned no results'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during time series window functions test: {str(e)}"
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
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

    def test_time_series_delete(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series Delete Test',
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_timeseries',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'details': {},
        }

        try:
            if not self.timeseries_supported:
                raise Exception(self.timeseries_error_message)

            cutoff_time = datetime.utcnow() - timedelta(minutes=5)
            result = collection.delete_many({'timestamp': {'$lt': cutoff_time}})
            result_document['details']['deleted_count'] = result.deleted_count
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['log_lines'].append('Time series delete executed successfully.')
        except Exception as e:
            error_msg = f"Error during time series delete test: {str(e)}"
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
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

    def test_time_series_indexing(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series Indexing Test',
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_timeseries',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'details': {},
        }

        try:
            if not self.timeseries_supported:
                raise Exception(self.timeseries_error_message)

            collection.create_index('metadata.sensorId')
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['log_lines'].append('Time series indexing executed successfully.')
        except Exception as e:
            error_msg = f"Error during time series indexing test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            try:
                server_info = collection.database.client.server_info()
                result_document['version'] = server_info.get('version', 'unknown')
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

    def test_time_series_schema_validation(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series Schema Validation Test',
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_timeseries',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'details': {},
        }

        try:
            if not self.timeseries_supported:
                raise Exception(self.timeseries_error_message)

            # Drop and recreate collection with schema validation
            collection.drop()

            validator = {
                '$jsonSchema': {
                    'bsonType': 'object',
                    'required': ['timestamp', 'metadata', 'value'],
                    'properties': {
                        'timestamp': {'bsonType': 'date'},
                        'metadata': {
                            'bsonType': 'object',
                            'required': ['sensorId', 'type'],
                            'properties': {
                                'sensorId': {'bsonType': 'int'},
                                'type': {'bsonType': 'string'}
                            }
                        },
                        'value': {'bsonType': 'double'}
                    }
                }
            }

            self.docdb_db.create_collection(
                'timeseriesCollection',
                timeseries={
                    'timeField': 'timestamp',
                    'metaField': 'metadata',
                    'granularity': 'seconds'
                },
                validator=validator
            )

            # Attempt to insert invalid data
            invalid_data = {
                'timestamp': datetime.utcnow(),
                'metadata': {'sensorId': 'invalid', 'type': 'temperature'},
                'value': 'not a number'
            }
            collection.insert_one(invalid_data)
            error_msg = 'Invalid data insertion did not raise an error'
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        except Exception as e:
            if isinstance(e, OperationFailure):
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Schema validation enforced correctly.')
            else:
                error_msg = f"Error during time series schema validation test: {str(e)}"
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        finally:
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            try:
                server_info = collection.database.client.server_info()
                result_document['version'] = server_info.get('version', 'unknown')
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

    def test_time_series_compaction(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series Compaction Test',
            'platform': 'documentdb',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_timeseries',
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'details': {},
        }

        try:
            if not self.timeseries_supported:
                raise Exception(self.timeseries_error_message)

            result = self.docdb_db.command({'compact': 'timeseriesCollection'})
            result_document['details']['compaction_result'] = result
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['log_lines'].append('Time series compaction executed successfully.')
        except Exception as e:
            error_msg = f"Error during time series compaction test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            try:
                server_info = collection.database.client.server_info()
                result_document['version'] = server_info.get('version', 'unknown')
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            result_document = json.loads(json.dumps(result_document, default=str))
            self.test_results.append(result_document)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.docdb_db.drop_collection('timeseriesCollection')
            cls.logger.info('timeseriesCollection collection dropped successfully.')
        except Exception as e:
            cls.logger.error(f"Error dropping timeseriesCollection collection: {e}")
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
