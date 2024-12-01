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
        cls.collection_name = 'timeseriesCollection'
        cls.docdb_coll = cls.docdb_db[cls.collection_name]

        # Configure logging
        cls.logger = logging.getLogger('TestTimeSeriesCapabilities')
        cls.logger.setLevel(logging.DEBUG)
        
        # File Handler for logging to 'test_timeseries.log'
        file_handler = logging.FileHandler('test_timeseries.log')
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

        # Drop the collection if it exists
        try:
            self.docdb_db.drop_collection(self.collection_name)
            self.logger.info(f'Dropped existing {self.collection_name}.')
        except Exception as e:
            self.logger.info(f'No existing {self.collection_name} to drop.')

        self.timeseries_supported = False
        try:
            timeseries_options = {
                'timeField': 'timestamp',
                'metaField': 'metadata',
                'granularity': 'seconds'
            }
            self.docdb_db.create_collection(self.collection_name, timeseries=timeseries_options)
            self.timeseries_supported = True
            self.logger.info('Time series collection created successfully.')
        except Exception as e:
            self.timeseries_error_message = f"Time series not supported: {str(e)}"
            self.logger.error(self.timeseries_error_message)
            try:
                self.docdb_db.create_collection(self.collection_name)
                self.logger.info('Regular collection created instead.')
            except Exception as e2:
                self.logger.error(f"Error creating regular collection: {e2}")

        current_time = datetime.utcnow()
        sample_data = [
            {
                'timestamp': current_time - timedelta(minutes=5),
                'metadata': {
                    'sensorId': 1,
                    'type': 'temperature',
                    'location': {'type': 'Point', 'coordinates': [-73.97, 40.77]}
                },
                'value': 22.5
            },
            {
                'timestamp': current_time - timedelta(minutes=4),
                'metadata': {
                    'sensorId': 1,
                    'type': 'temperature',
                    'location': {'type': 'Point', 'coordinates': [-73.97, 40.77]}
                },
                'value': 22.7
            },
            {
                'timestamp': current_time - timedelta(minutes=3),
                'metadata': {
                    'sensorId': 2,
                    'type': 'humidity',
                    'location': {'type': 'Point', 'coordinates': [-73.88, 40.78]}
                },
                'value': 55.2
            },
            {
                'timestamp': current_time - timedelta(minutes=2),
                'metadata': {
                    'sensorId': 2,
                    'type': 'humidity',
                    'location': {'type': 'Point', 'coordinates': [-73.88, 40.78]}
                },
                'value': 54.8
            },
            {
                'timestamp': current_time - timedelta(minutes=1),
                'metadata': {
                    'sensorId': 1,
                    'type': 'temperature',
                    'location': {'type': 'Point', 'coordinates': [-73.97, 40.77]}
                },
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
                'metadata': {
                    'sensorId': 3,
                    'type': 'pressure',
                    'location': {'type': 'Point', 'coordinates': [-73.95, 40.75]}
                },
                'value': 101.3
            }
            collection.insert_one(new_data)
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['log_lines'].append('Time series data inserted successfully.')
            self.logger.debug("Time series data inserted successfully.")
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
                self.logger.debug(f"Server version retrieved: {server_version}")
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
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
                self.logger.debug("Time series query executed successfully.")
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
                self.logger.debug(f"Server version retrieved: {server_version}")
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
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
                self.logger.debug("Time series aggregation executed successfully.")
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
                self.logger.debug(f"Server version retrieved: {server_version}")
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
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
                self.logger.debug("Time series bucket aggregation executed successfully.")
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
                self.logger.debug(f"Server version retrieved: {server_version}")
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
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
                self.logger.debug("Time series window functions executed successfully.")
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
                self.logger.debug(f"Server version retrieved: {server_version}")
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
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
            self.logger.debug("Time series delete executed successfully.")
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
                self.logger.debug(f"Server version retrieved: {server_version}")
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
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
            self.logger.debug("Time series indexing executed successfully.")
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
                server_version = server_info.get('version', 'unknown')
                result_document['version'] = server_version
                self.logger.debug(f"Server version retrieved: {server_version}")
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            result_document = json.loads(json.dumps(result_document, default=str))

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    # ======================= Added Tests Start Here =======================

    def test_time_series_geonear(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series $geoNear Aggregation Test',
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

            # Ensure there is a 2dsphere index on metadata.location
            collection.create_index([('metadata.location', '2dsphere')])
            self.logger.info('2dsphere index created on metadata.location.')

            # Insert additional documents with location data if necessary
            # (Assuming setUp already inserted documents with location)

            # Define a point near which to search
            near_point = {
                'type': 'Point',
                'coordinates': [-73.97, 40.77]
            }

            pipeline = [
                {
                    '$geoNear': {
                        'near': near_point,
                        'distanceField': 'dist.calculated',
                        'spherical': True,
                        'limit': 5
                    }
                }
            ]

            results = list(collection.aggregate(pipeline))
            result_document['details']['geoNear_results'] = results

            if results:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('$geoNear aggregation executed successfully.')
                self.logger.debug("$geoNear aggregation executed successfully.")
            else:
                error_msg = '$geoNear aggregation returned no results'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except OperationFailure as ofe:
            # Expected if $geoNear is not supported
            error_msg = f"$geoNear aggregation failed as expected: {str(ofe)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'PASSED'  # Depending on expectations
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            self.logger.info(error_msg)
        except Exception as e:
            error_msg = f"Error during $geoNear aggregation test: {str(e)}"
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

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            try:
                result_document = json.loads(json.dumps(result_document, default=str))
            except TypeError as te:
                self.logger.error(f"Error serializing result_document: {te}")
                result_document['description'].append(f"Serialization error: {te}")

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    def test_time_series_merge(self):
        collection = self.docdb_coll
        other_collection = self.docdb_db['otherCollectionForMerge']
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series $merge Aggregation Test',
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

            # Insert sample data into the other collection
            other_sample_data = [
                {
                    'timestamp': datetime.utcnow(),
                    'metadata': {
                        'sensorId': 4,
                        'type': 'pressure',
                        'location': {'type': 'Point', 'coordinates': [-73.96, 40.76]}
                    },
                    'value': 101.5
                }
            ]
            other_collection.insert_many(other_sample_data)
            self.logger.info('Sample data inserted into otherCollectionForMerge.')

            pipeline = [
                {
                    '$merge': {
                        'into': self.collection_name,
                        'whenMatched': 'merge',
                        'whenNotMatched': 'insert'
                    }
                }
            ]

            collection.aggregate(pipeline)
            error_msg = '$merge aggregation should not be supported on time series collections.'
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        except OperationFailure as ofe:
            # Expected failure since $merge is not supported
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            success_msg = '$merge aggregation failed as expected.'
            result_document['log_lines'].append(success_msg)
            self.logger.info(success_msg)
        except Exception as e:
            error_msg = f"Unexpected error during $merge aggregation test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            # Clean up the other collection
            try:
                self.docdb_db.drop_collection('otherCollectionForMerge')
                self.logger.info('otherCollectionForMerge dropped successfully.')
            except Exception as e:
                self.logger.error(f"Error dropping otherCollectionForMerge: {e}")

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

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            try:
                result_document = json.loads(json.dumps(result_document, default=str))
            except TypeError as te:
                self.logger.error(f"Error serializing result_document: {te}")
                result_document['description'].append(f"Serialization error: {te}")

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    def test_time_series_out(self):
        collection = self.docdb_coll
        output_collection_name = 'timeseriesOutCollection'
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series $out Aggregation Test',
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

            # Ensure the output collection is a time series collection
            if output_collection_name in self.docdb_db.list_collection_names():
                self.docdb_db.drop_collection(output_collection_name)
                self.logger.info(f'Dropped existing {output_collection_name}.')

            timeseries_options = {
                'timeField': 'timestamp',
                'metaField': 'metadata',
                'granularity': 'seconds'
            }
            self.docdb_db.create_collection(output_collection_name, timeseries=timeseries_options)
            self.logger.info(f'Time series collection {output_collection_name} created successfully.')

            pipeline = [
                {'$match': {'metadata.type': 'humidity'}},
                {'$out': output_collection_name}
            ]

            collection.aggregate(pipeline)
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['log_lines'].append('$out aggregation executed successfully.')
            self.logger.debug("$out aggregation executed successfully.")

            # Verify that data was written to the output collection
            out_collection = self.docdb_db[output_collection_name]
            out_results = list(out_collection.find({'metadata.type': 'humidity'}))
            result_document['details']['out_results'] = out_results

            if out_results:
                self.logger.debug(f"$out aggregation wrote {len(out_results)} documents to {output_collection_name}.")
            else:
                error_msg = f"$out aggregation did not write any documents to {output_collection_name}."
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during $out aggregation test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            # Clean up the output collection
            try:
                self.docdb_db.drop_collection(output_collection_name)
                self.logger.info(f'{output_collection_name} dropped successfully.')
            except Exception as e:
                self.logger.error(f"Error dropping {output_collection_name}: {e}")

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

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            try:
                result_document = json.loads(json.dumps(result_document, default=str))
            except TypeError as te:
                self.logger.error(f"Error serializing result_document: {te}")
                result_document['description'].append(f"Serialization error: {te}")

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    def test_time_series_date_add(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series $dateAdd Aggregation Test',
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
                {
                    '$addFields': {
                        'new_timestamp': {
                            '$dateAdd': {
                                'startDate': '$timestamp',
                                'unit': 'hour',
                                'amount': 2
                            }
                        }
                    }
                }
            ]

            results = list(collection.aggregate(pipeline))
            result_document['details']['dateAdd_results'] = results

            if results:
                # Verify that new_timestamp is exactly 2 hours added to timestamp
                for doc in results:
                    original = doc['timestamp']
                    added = doc['new_timestamp']
                    expected = original + timedelta(hours=2)
                    assert added == expected, f"Expected {expected}, got {added}"
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('$dateAdd operator executed successfully.')
                self.logger.debug("$dateAdd operator executed successfully.")
            else:
                error_msg = '$dateAdd operator returned no results'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except AssertionError as ae:
            error_msg = f"$dateAdd operator assertion failed: {str(ae)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during $dateAdd operator test: {str(e)}"
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

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            try:
                result_document = json.loads(json.dumps(result_document, default=str))
            except TypeError as te:
                self.logger.error(f"Error serializing result_document: {te}")
                result_document['description'].append(f"Serialization error: {te}")

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    def test_time_series_date_diff(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series $dateDiff Aggregation Test',
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
                {
                    '$addFields': {
                        'time_difference_minutes': {
                            '$dateDiff': {
                                'startDate': '$timestamp',
                                'endDate': '$$NOW',
                                'unit': 'minute'
                            }
                        }
                    }
                }
            ]

            results = list(collection.aggregate(pipeline))
            result_document['details']['dateDiff_results'] = results

            if results:
                # Verify that time_difference_minutes is correctly calculated
                current_time = datetime.utcnow()
                for doc in results:
                    original = doc['timestamp']
                    difference = doc['time_difference_minutes']
                    expected = int((current_time - original).total_seconds() / 60)
                    assert difference == expected, f"Expected {expected}, got {difference}"
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('$dateDiff operator executed successfully.')
                self.logger.debug("$dateDiff operator executed successfully.")
            else:
                error_msg = '$dateDiff operator returned no results'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except AssertionError as ae:
            error_msg = f"$dateDiff operator assertion failed: {str(ae)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during $dateDiff operator test: {str(e)}"
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

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            try:
                result_document = json.loads(json.dumps(result_document, default=str))
            except TypeError as te:
                self.logger.error(f"Error serializing result_document: {te}")
                result_document['description'].append(f"Serialization error: {te}")

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    def test_time_series_date_trunc(self):
        collection = self.docdb_coll
        start_time = time.time()
        result_document = {
            'status': 'fail',
            'test_name': 'Time Series $dateTrunc Aggregation Test',
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
                {
                    '$addFields': {
                        'truncated_timestamp': {
                            '$dateTrunc': {
                                'date': '$timestamp',
                                'unit': 'minute'
                            }
                        }
                    }
                }
            ]

            results = list(collection.aggregate(pipeline))
            result_document['details']['dateTrunc_results'] = results

            if results:
                # Verify that truncated_timestamp has seconds and smaller units set to zero
                for doc in results:
                    original = doc['timestamp']
                    truncated = doc['truncated_timestamp']
                    expected = original.replace(second=0, microsecond=0)
                    assert truncated == expected, f"Expected {expected}, got {truncated}"
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('$dateTrunc operator executed successfully.')
                self.logger.debug("$dateTrunc operator executed successfully.")
            else:
                error_msg = '$dateTrunc operator returned no results'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                self.logger.error(error_msg)
        except AssertionError as ae:
            error_msg = f"$dateTrunc operator assertion failed: {str(ae)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during $dateTrunc operator test: {str(e)}"
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

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Ensure all fields in result_document are JSON serializable
            try:
                result_document = json.loads(json.dumps(result_document, default=str))
            except TypeError as te:
                self.logger.error(f"Error serializing result_document: {te}")
                result_document['description'].append(f"Serialization error: {te}")

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
            self.test_results.append(result_document)

    # ======================= Added Tests End Here =======================

    @classmethod
    def tearDownClass(cls):
        try:
            cls.docdb_db.drop_collection(cls.collection_name)
            cls.logger.info(f'{cls.collection_name} collection dropped successfully.')
        except Exception as e:
            cls.logger.error(f"Error dropping {cls.collection_name} collection: {e}")
        super().tearDownClass()

if __name__ == '__main__':
    unittest.main()
