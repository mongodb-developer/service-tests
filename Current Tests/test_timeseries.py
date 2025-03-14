import unittest
from pymongo.errors import OperationFailure
from datetime import datetime, timedelta
import traceback
import contextlib
import io
import sys
from base_test import BaseTest
import logging
import time
import json
import config

class TestTimeSeriesCapabilities(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.collection_name = 'test_timeseries'
        cls.results_collection_name = 'test_timeseries_results'

        # Use the documentDB client for all operations
        cls.logger = logging.getLogger('TestTimeSeriesCapabilities')
        cls.logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler('test_timeseries.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        cls.logger.addHandler(file_handler)

        # In-memory log capture list and custom handler
        cls.log_capture_list = []
        class ListHandler(logging.Handler):
            def __init__(self, log_list):
                super().__init__()
                self.log_list = log_list
            def emit(self, record):
                self.log_list.append(self.format(record))
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
        # Get the time series collection (named 'timeseriesCollection')
        self.data_collection = self.docdb_db['timeseriesCollection']
        try:
            self.data_collection.drop()
        except Exception:
            pass

        # Attempt to create a time series collection
        self.timeseries_supported = False
        self.timeseries_error_message = ''
        try:
            timeseries_options = {
                'timeField': 'timestamp',
                'metaField': 'metadata',
                'granularity': 'seconds'
            }
            self.docdb_db.create_collection('timeseriesCollection', timeseries=timeseries_options)
            self.timeseries_supported = True
            print("Time series collection created successfully.")
        except OperationFailure as e:
            error_message = f"Error creating time series collection: {e}"
            print(error_message)
            self.timeseries_error_message = error_message
            # Create a regular collection instead.
            self.docdb_db.create_collection('timeseriesCollection')
            print("Created a regular collection instead.")
        except Exception as e:
            error_message = f"Unexpected error creating time series collection: {e}"
            print(error_message)
            self.timeseries_error_message = error_message

        # Insert sample time series data
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
        try:
            self.data_collection.insert_many(sample_data)
            print("Sample data inserted successfully.")
        except OperationFailure as e:
            print(f"Error inserting sample data: {e}")

        # Clear streams and warnings before each test
        self.__class__.log_capture_list.clear()
        for stream in (self.__class__.stdout_stream, self.__class__.stderr_stream, self.__class__.log_stream):
            stream.truncate(0)
            stream.seek(0)
        self.__class__.warnings_list.clear()

    # ---------------- Helper Methods ----------------
    def initialize_result_document(self, test_name):
        start_time_val = time.time()
        result_document = {
            'status': 'fail',
            'test_name': f"Time Series Test - {test_name}",
            'platform': config.PLATFORM,
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time_val).isoformat(),
            'end': None,
            'suite': self.collection_name,
            'version': 'unknown',
            'run': 1,
            'processed': True,
            'log_lines': [],
            'errors': [],
            'reason': 'FAILED',
            'description': [],
            'command_result': {},
            '_start_time': start_time_val
        }
        return result_document

    def finalize_result_document(self, result_document):
        end_time_val = time.time()
        result_document['elapsed'] = end_time_val - result_document['_start_time']
        result_document['end'] = datetime.utcfromtimestamp(end_time_val).isoformat()
        try:
            server_info = self.docdb_db.client.server_info()
            result_document['version'] = server_info.get('version', 'unknown')
        except Exception as ve:
            self.logger.error(f"Error retrieving server version: {ve}")
            result_document['version'] = 'unknown'
        result_document['log_lines'] = list(self.__class__.log_capture_list)
        del result_document['_start_time']
        result_document = json.loads(json.dumps(result_document, default=str))
        self.__class__.test_results.append(result_document)

    def store_result(self, result_document):
        self.__class__.test_results.append(result_document)
        self.logger.debug(f"Stored result: {result_document}")
    # ------------------------------------------------

    def test_time_series_insert(self):
        result_document = self.initialize_result_document("Time_Series_Insert")
        if not self.timeseries_supported:
            result_document['errors'].append(self.timeseries_error_message)
            self.finalize_result_document(result_document)
            return
        with contextlib.redirect_stdout(self.__class__.stdout_stream), \
             contextlib.redirect_stderr(self.__class__.stderr_stream):
            try:
                new_data = {
                    'timestamp': datetime.utcnow(),
                    'metadata': {'sensorId': 3, 'type': 'pressure'},
                    'value': 101.3
                }
                self.data_collection.insert_one(new_data)
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['command_result'] = {'inserted_data': new_data}
            except Exception as e:
                result_document['errors'].append(str(e))
            finally:
                result_document['stdout'] = self.__class__.stdout_stream.getvalue()
                result_document['stderr'] = self.__class__.stderr_stream.getvalue()
                self.finalize_result_document(result_document)

    def test_time_series_query(self):
        result_document = self.initialize_result_document("Time_Series_Query")
        if not self.timeseries_supported:
            result_document['errors'].append(self.timeseries_error_message)
            self.finalize_result_document(result_document)
            return
        with contextlib.redirect_stdout(self.__class__.stdout_stream), \
             contextlib.redirect_stderr(self.__class__.stderr_stream):
            try:
                query = {'metadata.sensorId': 1}
                results = list(self.data_collection.find(query).sort('timestamp', 1))
                result_document['command_result'] = {'query_results': results}
                if results:
                    result_document['status'] = 'pass'
                    result_document['exit_code'] = 0
                    result_document['reason'] = 'PASSED'
                else:
                    result_document['errors'].append('No data found for sensorId 1')
            except Exception as e:
                result_document['errors'].append(str(e))
            finally:
                result_document['stdout'] = self.__class__.stdout_stream.getvalue()
                result_document['stderr'] = self.__class__.stderr_stream.getvalue()
                self.finalize_result_document(result_document)

    def test_time_series_aggregation(self):
        result_document = self.initialize_result_document("Time_Series_Aggregation")
        if not self.timeseries_supported:
            result_document['errors'].append(self.timeseries_error_message)
            self.finalize_result_document(result_document)
            return
        with contextlib.redirect_stdout(self.__class__.stdout_stream), \
             contextlib.redirect_stderr(self.__class__.stderr_stream):
            try:
                pipeline = [
                    {'$match': {'metadata.type': 'temperature'}},
                    {'$group': {
                        '_id': None,
                        'avgTemperature': {'$avg': '$value'}
                    }}
                ]
                results = list(self.data_collection.aggregate(pipeline))
                result_document['command_result'] = {'aggregation_results': results}
                if results:
                    result_document['status'] = 'pass'
                    result_document['exit_code'] = 0
                    result_document['reason'] = 'PASSED'
                else:
                    result_document['errors'].append('Aggregation returned no results')
            except Exception as e:
                result_document['errors'].append(str(e))
            finally:
                result_document['stdout'] = self.__class__.stdout_stream.getvalue()
                result_document['stderr'] = self.__class__.stderr_stream.getvalue()
                self.finalize_result_document(result_document)

    def test_time_series_window_functions(self):
        result_document = self.initialize_result_document("Time_Series_Window_Functions")
        if not self.timeseries_supported:
            result_document['errors'].append(self.timeseries_error_message)
            self.finalize_result_document(result_document)
            return
        with contextlib.redirect_stdout(self.__class__.stdout_stream), \
             contextlib.redirect_stderr(self.__class__.stderr_stream):
            try:
                pipeline = [
                    {'$match': {'metadata.type': 'temperature'}},
                    {'$setWindowFields': {
                        'partitionBy': '$metadata.sensorId',
                        'sortBy': {'timestamp': 1},
                        'output': {
                            'movingAvg': {
                                '$avg': '$value',
                                'window': {'documents': [-1, 0]}
                            }
                        }
                    }}
                ]
                results = list(self.data_collection.aggregate(pipeline))
                result_document['command_result'] = {'window_function_results': results}
                if results:
                    result_document['status'] = 'pass'
                    result_document['exit_code'] = 0
                    result_document['reason'] = 'PASSED'
                else:
                    result_document['errors'].append('Window function returned no results')
            except Exception as e:
                result_document['errors'].append(f'Exception during window functions: {e}')
            finally:
                result_document['stdout'] = self.__class__.stdout_stream.getvalue()
                result_document['stderr'] = self.__class__.stderr_stream.getvalue()
                self.finalize_result_document(result_document)

    def test_time_series_delete(self):
        result_document = self.initialize_result_document("Time_Series_Delete")
        if not self.timeseries_supported:
            result_document['errors'].append(self.timeseries_error_message)
            self.finalize_result_document(result_document)
            return
        with contextlib.redirect_stdout(self.__class__.stdout_stream), \
             contextlib.redirect_stderr(self.__class__.stderr_stream):
            try:
                cutoff_time = datetime.utcnow() - timedelta(minutes=5)
                result = self.data_collection.delete_many({'timestamp': {'$lt': cutoff_time}})
                result_document['command_result'] = {'deleted_count': result.deleted_count}
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
            except Exception as e:
                result_document['errors'].append(f'Exception during delete: {e}')
            finally:
                result_document['stdout'] = self.__class__.stdout_stream.getvalue()
                result_document['stderr'] = self.__class__.stderr_stream.getvalue()
                self.finalize_result_document(result_document)

    def test_time_series_indexing(self):
        result_document = self.initialize_result_document("Time_Series_Indexing")
        if not self.timeseries_supported:
            result_document['errors'].append(self.timeseries_error_message)
            self.finalize_result_document(result_document)
            return
        with contextlib.redirect_stdout(self.__class__.stdout_stream), \
             contextlib.redirect_stderr(self.__class__.stderr_stream):
            try:
                self.data_collection.create_index('metadata.sensorId')
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
            except Exception as e:
                result_document['errors'].append(f'Exception during indexing: {e}')
            finally:
                result_document['stdout'] = self.__class__.stdout_stream.getvalue()
                result_document['stderr'] = self.__class__.stderr_stream.getvalue()
                self.finalize_result_document(result_document)

    def test_time_series_schema_validation(self):
        result_document = self.initialize_result_document("Time_Series_Schema_Validation")
        if not self.timeseries_supported:
            result_document['errors'].append(self.timeseries_error_message)
            self.finalize_result_document(result_document)
            return
        try:
            self.data_collection.drop()
        except Exception:
            pass
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
        with contextlib.redirect_stdout(io.StringIO()) as stdout, \
             contextlib.redirect_stderr(io.StringIO()) as stderr:
            try:
                self.docdb_db.create_collection(
                    'timeseriesCollection',
                    timeseries={
                        'timeField': 'timestamp',
                        'metaField': 'metadata',
                        'granularity': 'seconds'
                    },
                    validator=validator
                )
                invalid_data = {
                    'timestamp': datetime.utcnow(),
                    'metadata': {'sensorId': 'invalid', 'type': 'temperature'},
                    'value': 'not a number'
                }
                self.data_collection.insert_one(invalid_data)
                result_document['errors'].append('Invalid data insertion did not raise an error')
            except OperationFailure as e:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
            except Exception as e:
                result_document['errors'].append(f'Unexpected exception: {e}')
            finally:
                result_document['stdout'] = stdout.getvalue()
                result_document['stderr'] = stderr.getvalue()
                self.finalize_result_document(result_document)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.docdb_db['timeseriesCollection'].drop()
            print("timeseriesCollection collection dropped successfully.")
        except Exception as e:
            print(f"Error dropping timeseriesCollection collection: {e}")
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
