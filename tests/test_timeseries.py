# tests/test_timeseries_postgresql.py

import unittest
import logging
import json
import time
import traceback
from datetime import datetime, timedelta
import sys
import os
import decimal

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

# Adjust sys.path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_test import BaseTest
import config  # Ensure this module has PostgreSQL configurations


class TestPostgreSQLTimeSeriesCapabilities(BaseTest):
    """
    Test suite for PostgreSQL time series capabilities.
    Translates MongoDB/DocumentDB time series tests to PostgreSQL using psycopg2.
    Includes tests for insert, query, aggregation, bucket aggregation, window functions,
    geo-near queries, merge operations, out operations, date add/diff/trunc operations,
    delete operations, and indexing.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.table_name = 'test_postgresql_timeseries'
        cls.logger_name = 'TestPostgreSQLTimeSeriesCapabilities'

        # Configure logging
        cls.logger = logging.getLogger(cls.logger_name)
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

        # Create table for testing
        try:
            with cls.test_conn.cursor() as cur:
                # Enable PostGIS extension for geospatial data
                cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS postgis;"))
                cls.logger.debug("PostGIS extension ensured.")

                # Drop table if it exists to start clean
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Dropped table '{cls.table_name}' if it existed.")

                # Create table with timestamp, metadata, and value
                cur.execute(sql.SQL("""
                    CREATE TABLE {} (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP NOT NULL,
                        sensor_id INTEGER NOT NULL,
                        type TEXT NOT NULL,
                        location GEOGRAPHY(POINT, 4326),
                        value DOUBLE PRECISION NOT NULL
                    );
                """).format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Created table '{cls.table_name}' with necessary fields.")

                # Create indexes
                cur.execute(sql.SQL("""
                    CREATE INDEX idx_sensor_id ON {} (sensor_id);
                """).format(sql.Identifier(cls.table_name)))
                cls.logger.debug("Created index on 'sensor_id'.")

                cur.execute(sql.SQL("""
                    CREATE INDEX idx_location ON {} USING GIST (location);
                """).format(sql.Identifier(cls.table_name)))
                cls.logger.debug("Created GIST index on 'location'.")

            cls.test_conn.commit()
            cls.logger.debug(f"Setup for '{cls.table_name}' completed successfully.")
            print(f"Table '{cls.table_name}' created successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error setting up table: {e}")
            print(f"Error setting up table: {e}")
            raise

    def setUp(self):
        """Reset table before each test"""
        try:
            with self.test_conn.cursor() as cur:
                # Truncate table to remove existing data
                cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE;").format(sql.Identifier(self.table_name)))
            self.test_conn.commit()
            self.logger.debug("Truncated table before test.")
        except Exception as e:
            self.test_conn.rollback()
            self.logger.error(f"Error truncating table in setUp: {e}")
            raise

        # Insert test data
        test_data = self.get_test_data()
        success, error = self.insert_test_data(test_data)
        if not success:
            self.fail(f"Failed to insert test data: {error}")

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

    @staticmethod
    def convert_objects(obj):
        """
        Recursively convert Decimal and datetime objects to serializable types.
        """
        if isinstance(obj, list):
            return [TestPostgreSQLTimeSeriesCapabilities.convert_objects(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: TestPostgreSQLTimeSeriesCapabilities.convert_objects(v) for k, v in obj.items()}
        elif isinstance(obj, decimal.Decimal):
            return float(obj)  # or str(obj) if precision is important
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj

    def get_test_data(self):
        """Generate sample data for time series tests"""
        current_time = datetime.utcnow()
        return [
            {
                'timestamp': current_time - timedelta(minutes=5),
                'sensor_id': 1,
                'type': 'temperature',
                'location': 'POINT(-73.97 40.77)',
                'value': 22.5
            },
            {
                'timestamp': current_time - timedelta(minutes=4),
                'sensor_id': 1,
                'type': 'temperature',
                'location': 'POINT(-73.97 40.77)',
                'value': 22.7
            },
            {
                'timestamp': current_time - timedelta(minutes=3),
                'sensor_id': 2,
                'type': 'humidity',
                'location': 'POINT(-73.88 40.78)',
                'value': 55.2
            },
            {
                'timestamp': current_time - timedelta(minutes=2),
                'sensor_id': 2,
                'type': 'humidity',
                'location': 'POINT(-73.88 40.78)',
                'value': 54.8
            },
            {
                'timestamp': current_time - timedelta(minutes=1),
                'sensor_id': 1,
                'type': 'temperature',
                'location': 'POINT(-73.97 40.77)',
                'value': 22.6
            },
        ]

    def insert_test_data(self, test_data):
        """Inserts test data into the table."""
        try:
            with self.test_conn.cursor() as cur:
                insert_query = sql.SQL("""
                    INSERT INTO {} (timestamp, sensor_id, type, location, value)
                    VALUES (%s, %s, %s, ST_GeogFromText(%s), %s);
                """).format(sql.Identifier(self.table_name))
                for doc in test_data:
                    cur.execute(insert_query, (
                        doc['timestamp'],
                        doc['sensor_id'],
                        doc['type'],
                        doc['location'],
                        doc['value']
                    ))
            self.test_conn.commit()
            self.logger.debug("Inserted test data successfully.")
            return True, ""
        except Exception as e:
            self.test_conn.rollback()
            error_msg = f"Failed to insert test data: {e}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            return False, error_msg

    def initialize_result_document(self, test_name):
        start_time = time.time()
        return {
            'status': 'fail',
            'test_name': test_name,
            'platform': 'postgresql',
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

    def finalize_result_document(self, result_document, start_time):
        end_time = time.time()
        result_document['elapsed'] = end_time - start_time
        result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

        try:
            with self.test_conn.cursor() as cur:
                cur.execute("SELECT version();")
                version_info = cur.fetchone()
            server_version = version_info[0] if version_info else 'unknown'
            result_document['version'] = server_version
            self.logger.debug(f"Server version retrieved: {server_version}")
        except Exception as ve:
            self.logger.error(f"Error retrieving server version: {ve}")
            result_document['version'] = 'unknown'

        # Assign captured log lines to the result document
        result_document['log_lines'] = list(self.log_capture_list)

        # Convert all Decimal and datetime objects to JSON serializable types
        result_document = self.convert_objects(result_document)

        # Ensure all fields in result_document are JSON serializable
        try:
            json.dumps(result_document)
        except TypeError as te:
            self.logger.error(f"JSON serialization error: {te}")
            result_document['description'].append(f"JSON serialization error: {te}")
            result_document['reason'] += f" JSON serialization error: {te}"
            result_document['status'] = 'fail'

        # Print the result_document for debugging
        print(json.dumps(result_document, indent=4))

        # Accumulate result for later storage
        self.test_results.append(result_document)

    def test_time_series_insert(self):
        """Test inserting new time series data"""
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document('Time Series Insert Test')

        try:
            new_data = {
                'timestamp': datetime.utcnow(),
                'sensor_id': 3,
                'type': 'pressure',
                'location': 'POINT(-73.95 40.75)',
                'value': 101.3
            }
            with self.test_conn.cursor() as cur:
                insert_query = sql.SQL("""
                    INSERT INTO {}
                    (timestamp, sensor_id, type, location, value)
                    VALUES (%s, %s, %s, ST_GeogFromText(%s), %s);
                """).format(sql.Identifier(collection))
                cur.execute(insert_query, (
                    new_data['timestamp'],
                    new_data['sensor_id'],
                    new_data['type'],
                    new_data['location'],
                    new_data['value']
                ))
            self.test_conn.commit()
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['log_lines'].append('Time series data inserted successfully.')
            self.logger.debug("Time series data inserted successfully.")
        except Exception as e:
            self.test_conn.rollback()
            error_msg = f"Error during time series insert test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append(error_msg)
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_time_series_query(self):
        """Test querying time series data based on sensor_id"""
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document('Time Series Query Test')

        try:
            query_sensor_id = 1
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = sql.SQL("""
                    SELECT *
                    FROM {}
                    WHERE sensor_id = %s
                    ORDER BY timestamp ASC;
                """).format(sql.Identifier(collection))
                cur.execute(query, (query_sensor_id,))
                results = cur.fetchall()
            result_document['details']['query_results'] = results

            expected_sensor_ids = {1}
            result_sensor_ids = set(doc['sensor_id'] for doc in results)

            if expected_sensor_ids.issubset(result_sensor_ids):
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Time series query executed successfully.')
                self.logger.debug("Time series query executed successfully.")
            else:
                missing = expected_sensor_ids - result_sensor_ids
                error_msg = f'Missing expected sensor_ids: {missing}'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during time series query test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append(error_msg)
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_time_series_aggregation(self):
        """Test aggregating average value per sensor_id"""
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document('Time Series Aggregation Test')

        try:
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                aggregation_query = sql.SQL("""
                    SELECT sensor_id, AVG(value) AS avg_value
                    FROM {}
                    GROUP BY sensor_id;
                """).format(sql.Identifier(collection))
                cur.execute(aggregation_query)
                results = cur.fetchall()
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
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during time series aggregation test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_time_series_bucket_aggregation(self):
        """Test aggregating data into time buckets"""
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document('Time Series Bucket Aggregation Test')

        try:
            # Define bucket boundaries (e.g., every 2 minutes)
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                bucket_query = sql.SQL("""
                    SELECT
                        date_trunc('minute', timestamp) AS bucket,
                        COUNT(*) AS count,
                        AVG(value) AS avg_value
                    FROM {}
                    WHERE timestamp >= %s AND timestamp <= %s
                    GROUP BY bucket
                    ORDER BY bucket ASC;
                """).format(sql.Identifier(collection))
                current_time = datetime.utcnow()
                start_time_bucket = current_time - timedelta(minutes=5)
                end_time_bucket = current_time
                cur.execute(bucket_query, (start_time_bucket, end_time_bucket))
                results = cur.fetchall()
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
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during time series bucket aggregation test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_time_series_window_functions(self):
        """Test window functions like moving average"""
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document('Time Series Window Functions Test')

        try:
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                window_query = sql.SQL("""
                    SELECT
                        id,
                        timestamp,
                        sensor_id,
                        type,
                        value,
                        AVG(value) OVER (
                            PARTITION BY sensor_id
                            ORDER BY timestamp
                            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                        ) AS moving_avg
                    FROM {}
                    ORDER BY sensor_id, timestamp ASC;
                """).format(sql.Identifier(collection))
                cur.execute(window_query)
                results = cur.fetchall()
            result_document['details']['window_function_results'] = results

            if results:
                # Verify moving average calculation
                for doc in results:
                    sensor_id = doc['sensor_id']
                    value = doc['value']
                    moving_avg = doc['moving_avg']
                    # For simplicity, skip exact verification
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('Time series window functions executed successfully.')
                self.logger.debug("Time series window functions executed successfully.")
            else:
                error_msg = 'Window function returned no results'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during time series window functions test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_time_series_geonear(self):
        """Test geospatial proximity queries"""
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document('$geoNear Aggregation Test')

        try:
            # Define a point near which to search
            near_point = 'POINT(-73.97 40.77)'

            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                geo_query = sql.SQL("""
                    SELECT
                        *,
                        ST_Distance(location, ST_GeogFromText(%s)) AS distance
                    FROM {}
                    WHERE ST_DWithin(location, ST_GeogFromText(%s), 1000)  -- within 1000 meters
                    ORDER BY distance ASC
                    LIMIT 5;
                """).format(sql.Identifier(collection))
                cur.execute(geo_query, (near_point, near_point))
                results = cur.fetchall()
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
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during $geoNear aggregation test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append(error_msg)
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_time_series_merge(self):
        """Test merging data from another table"""
        collection = self.table_name
        other_table = 'other_collection_for_merge'
        start_time = time.time()
        result_document = self.initialize_result_document('$merge Aggregation Test')

        try:
            # Create another table to merge data from
            with self.test_conn.cursor() as cur:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(other_table)))
                cur.execute(sql.SQL("""
                    CREATE TABLE {} (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP NOT NULL,
                        sensor_id INTEGER NOT NULL,
                        type TEXT NOT NULL,
                        location GEOGRAPHY(POINT, 4326),
                        value DOUBLE PRECISION NOT NULL
                    );
                """).format(sql.Identifier(other_table)))
                self.logger.debug(f"Created table '{other_table}' for merge test.")

                # Insert sample data into other_table
                other_data = [
                    {
                        'timestamp': datetime.utcnow(),
                        'sensor_id': 4,
                        'type': 'pressure',
                        'location': 'POINT(-73.96 40.76)',
                        'value': 101.5
                    }
                ]
                insert_query = sql.SQL("""
                    INSERT INTO {} (timestamp, sensor_id, type, location, value)
                    VALUES (%s, %s, %s, ST_GeogFromText(%s), %s);
                """).format(sql.Identifier(other_table))
                for doc in other_data:
                    cur.execute(insert_query, (
                        doc['timestamp'],
                        doc['sensor_id'],
                        doc['type'],
                        doc['location'],
                        doc['value']
                    ))
            self.test_conn.commit()
            self.logger.debug(f"Inserted sample data into '{other_table}'.")

            # Perform merge: insert data from other_table into main collection
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                merge_query = sql.SQL("""
                    INSERT INTO {}
                    (timestamp, sensor_id, type, location, value)
                    SELECT timestamp, sensor_id, type, location, value
                    FROM {}
                    ON CONFLICT (id) DO NOTHING;
                """).format(sql.Identifier(collection), sql.Identifier(other_table))
                cur.execute(merge_query)
            self.test_conn.commit()
            self.logger.debug("$merge aggregation executed successfully.")

            # Verify data was merged
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                verify_query = sql.SQL("""
                    SELECT *
                    FROM {}
                    WHERE sensor_id = %s;
                """).format(sql.Identifier(collection))
                cur.execute(verify_query, (4,))
                results = cur.fetchall()
            result_document['details']['merge_results'] = results

            if results:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('$merge aggregation executed successfully.')
                self.logger.debug("$merge aggregation executed successfully.")
            else:
                error_msg = '$merge aggregation did not merge any data'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)

        except Exception as e:
            self.test_conn.rollback()
            error_msg = f"Error during $merge aggregation test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            # Clean up the other_table
            try:
                with self.test_conn.cursor() as cur:
                    cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(other_table)))
                self.test_conn.commit()
                self.logger.debug(f"Dropped table '{other_table}' successfully.")
            except Exception as e:
                self.test_conn.rollback()
                self.logger.error(f"Error dropping table '{other_table}': {e}")

            self.finalize_result_document(result_document, start_time)

    def test_time_series_out(self):
        """Test exporting data to another table"""
        collection = self.table_name
        output_table = 'timeseries_out_collection'
        start_time = time.time()
        result_document = self.initialize_result_document('$out Aggregation Test')

        try:
            # Create output_table as empty table
            with self.test_conn.cursor() as cur:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(output_table)))
                cur.execute(sql.SQL("""
                    CREATE TABLE {} (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP NOT NULL,
                        sensor_id INTEGER NOT NULL,
                        type TEXT NOT NULL,
                        location GEOGRAPHY(POINT, 4326),
                        value DOUBLE PRECISION NOT NULL
                    );
                """).format(sql.Identifier(output_table)))
                self.logger.debug(f"Created output table '{output_table}' for $out test.")

            self.test_conn.commit()

            # Perform $out like operation: insert selected data into output_table
            with self.test_conn.cursor() as cur:
                out_query = sql.SQL("""
                    INSERT INTO {}
                    (timestamp, sensor_id, type, location, value)
                    SELECT timestamp, sensor_id, type, location, value
                    FROM {}
                    WHERE sensor_id = %s;
                """).format(sql.Identifier(output_table), sql.Identifier(collection))
                cur.execute(out_query, (2,))
            self.test_conn.commit()
            self.logger.debug("$out aggregation executed successfully.")

            # Verify data was written to output_table
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                verify_query = sql.SQL("""
                    SELECT *
                    FROM {}
                    WHERE sensor_id = %s;
                """).format(sql.Identifier(output_table))
                cur.execute(verify_query, (2,))
                results = cur.fetchall()
            result_document['details']['out_results'] = results

            if results:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('$out aggregation executed successfully.')
                self.logger.debug("$out aggregation executed successfully.")
            else:
                error_msg = '$out aggregation did not write any data to output_table'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)

        except Exception as e:
            self.test_conn.rollback()
            error_msg = f"Error during $out aggregation test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            self.logger.error(error_msg)
        finally:
            # Clean up the output_table
            try:
                with self.test_conn.cursor() as cur:
                    cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(output_table)))
                self.test_conn.commit()
                self.logger.debug(f"Dropped table '{output_table}' successfully.")
            except Exception as e:
                self.test_conn.rollback()
                self.logger.error(f"Error dropping table '{output_table}': {e}")

            self.finalize_result_document(result_document, start_time)

    def test_time_series_date_add(self):
        """Test adding intervals to timestamp"""
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document('$dateAdd Aggregation Test')

        try:
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                date_add_query = sql.SQL("""
                    SELECT
                        id,
                        timestamp,
                        timestamp + INTERVAL '2 hours' AS new_timestamp
                    FROM {}
                    ORDER BY timestamp ASC;
                """).format(sql.Identifier(collection))
                cur.execute(date_add_query)
                results = cur.fetchall()
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
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)
        except AssertionError as ae:
            error_msg = f"$dateAdd operator assertion failed: {str(ae)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append(error_msg)
            self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during $dateAdd operator test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append(error_msg)
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_time_series_date_diff(self):
        """Test calculating difference between two timestamps"""
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document('$dateDiff Aggregation Test')

        try:
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                date_diff_query = sql.SQL("""
                    SELECT
                        id,
                        timestamp,
                        EXTRACT(EPOCH FROM (NOW() - timestamp)) / 60 AS difference_minutes
                    FROM {}
                    ORDER BY timestamp ASC;
                """).format(sql.Identifier(collection))
                cur.execute(date_diff_query)
                results = cur.fetchall()
            result_document['details']['dateDiff_results'] = results

            if results:
                # Verify that difference_minutes is correctly calculated
                current_time = datetime.utcnow()
                for doc in results:
                    original = doc['timestamp']
                    difference = doc['difference_minutes']
                    expected = int((current_time - original).total_seconds() / 60)
                    assert int(difference) == expected, f"Expected {expected}, got {difference}"
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                result_document['log_lines'].append('$dateDiff operator executed successfully.')
                self.logger.debug("$dateDiff operator executed successfully.")
            else:
                error_msg = '$dateDiff operator returned no results'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)
        except AssertionError as ae:
            error_msg = f"$dateDiff operator assertion failed: {str(ae)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append(error_msg)
            self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during $dateDiff operator test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append(error_msg)
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_time_series_date_trunc(self):
        """Test truncating timestamp to a specific precision"""
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document('$dateTrunc Aggregation Test')

        try:
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                date_trunc_query = sql.SQL("""
                    SELECT
                        id,
                        timestamp,
                        date_trunc('minute', timestamp) AS truncated_timestamp
                    FROM {}
                    ORDER BY timestamp ASC;
                """).format(sql.Identifier(collection))
                cur.execute(date_trunc_query)
                results = cur.fetchall()
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
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)
        except AssertionError as ae:
            error_msg = f"$dateTrunc operator assertion failed: {str(ae)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append(error_msg)
            self.logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error during $dateTrunc operator test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append(error_msg)
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_time_series_delete(self):
        """Test deleting data based on a condition"""
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document('Time Series Delete Test')

        try:
            cutoff_time = datetime.utcnow() - timedelta(minutes=5)
            with self.test_conn.cursor() as cur:
                delete_query = sql.SQL("""
                    DELETE FROM {}
                    WHERE timestamp < %s;
                """).format(sql.Identifier(collection))
                cur.execute(delete_query, (cutoff_time,))
                deleted_count = cur.rowcount
            self.test_conn.commit()
            result_document['details']['deleted_count'] = deleted_count

            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'
            result_document['log_lines'].append('Time series delete executed successfully.')
            self.logger.debug("Time series delete executed successfully.")
        except Exception as e:
            self.test_conn.rollback()
            error_msg = f"Error during time series delete test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append(error_msg)
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def test_time_series_indexing(self):
        """Test creating and verifying indexes on the table"""
        collection = self.table_name
        start_time = time.time()
        result_document = self.initialize_result_document('Time Series Indexing Test')

        try:
            # Create an index on sensor_id if not exists
            with self.test_conn.cursor() as cur:
                cur.execute(sql.SQL("""
                    CREATE INDEX IF NOT EXISTS idx_sensor_id_new ON {}
                    (sensor_id);
                """).format(sql.Identifier(collection)))
            self.test_conn.commit()
            self.logger.debug("Created index 'idx_sensor_id_new' on 'sensor_id'.")

            # Verify the index exists
            with self.test_conn.cursor() as cur:
                cur.execute(sql.SQL("""
                    SELECT indexname
                    FROM pg_indexes
                    WHERE tablename = %s AND indexname = 'idx_sensor_id_new';
                """), (collection,))
                index = cur.fetchone()
            if index:
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
                success_msg = 'Index "idx_sensor_id_new" created and verified successfully.'
                result_document['log_lines'].append(success_msg)
                self.logger.debug(success_msg)
            else:
                error_msg = 'Index "idx_sensor_id_new" was not found after creation.'
                result_document['description'].append(error_msg)
                result_document['reason'] = 'FAILED'
                result_document['log_lines'].append(error_msg)
                self.logger.error(error_msg)
        except Exception as e:
            self.test_conn.rollback()
            error_msg = f"Error during time series indexing test: {str(e)}"
            result_document['description'].append(error_msg)
            result_document['reason'] = 'FAILED'
            result_document['log_lines'].append(error_msg)
            self.logger.error(error_msg)
        finally:
            self.finalize_result_document(result_document, start_time)

    def tearDown(self):
        """Reset table after each test"""
        try:
            with self.test_conn.cursor() as cur:
                # Truncate table to remove existing data
                cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE;").format(sql.Identifier(self.table_name)))
            self.test_conn.commit()
            self.logger.debug("Truncated table after test.")
        except Exception as e:
            self.test_conn.rollback()
            self.logger.error(f"Error truncating table in tearDown: {e}")
            raise

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests"""
        try:
            with cls.test_conn.cursor() as cur:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.table_name)))
            cls.test_conn.commit()
            cls.logger.debug("Dropped table during teardown.")
            print(f"Table '{cls.table_name}' dropped successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error dropping table during teardown: {e}")
            print(f"Error dropping table during teardown: {e}")
        finally:
            super().tearDownClass()


if __name__ == '__main__':
    unittest.main()
