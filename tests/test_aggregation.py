# tests/test_aggregation.py

import unittest
import logging
import time
import json
import traceback
from datetime import datetime, timedelta
import sys
import os
import decimal

from psycopg2.extras import RealDictCursor

# Adjust sys.path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_test import BaseTest

class TestAggregation(BaseTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.logger = logging.getLogger('TestAggregation')
        cls.logger.setLevel(logging.DEBUG)

        # Set up file handler and list handler
        file_handler = logging.FileHandler('test_aggregation.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        cls.logger.addHandler(file_handler)

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

        # Create tables for main collection, lookup collection, and output collection
        try:
            with cls.test_conn.cursor() as cur:
                # Enable PostGIS extension
                cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
                cls.logger.debug("PostGIS extension enabled.")

                # Drop tables if they exist to start clean
                cur.execute("DROP TABLE IF EXISTS test_aggregation CASCADE;")
                cur.execute("DROP TABLE IF EXISTS test_aggregation_lookup CASCADE;")
                cur.execute("DROP TABLE IF EXISTS aggregation_output CASCADE;")

                # Create main aggregation table
                cur.execute("""
                    CREATE TABLE test_aggregation (
                        id SERIAL PRIMARY KEY,
                        category VARCHAR(10),
                        value INTEGER,
                        tags TEXT[],
                        location GEOGRAPHY(Point, 4326),
                        parent INTEGER,
                        date TIMESTAMP
                    );
                """)

                # Create lookup table
                cur.execute("""
                    CREATE TABLE test_aggregation_lookup (
                        _id VARCHAR(10) PRIMARY KEY,
                        description TEXT
                    );
                """)

                # Create output aggregation table
                cur.execute("""
                    CREATE TABLE aggregation_output (
                        id SERIAL PRIMARY KEY,
                        category VARCHAR(10) UNIQUE,
                        total_value INTEGER,
                        average_value NUMERIC
                    );
                """)

                # Create Geospatial Index
                cur.execute("""
                    CREATE INDEX idx_location ON test_aggregation USING GIST (location);
                """)
                cls.logger.debug("Geospatial index 'idx_location' created on 'test_aggregation' table.")

            cls.test_conn.commit()
            cls.logger.debug("Tables 'test_aggregation', 'test_aggregation_lookup', and 'aggregation_output' created successfully.")
            print("Tables 'test_aggregation', 'test_aggregation_lookup', and 'aggregation_output' created successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error creating tables: {e}")
            print(f"Error creating tables: {e}")
            raise

        # Insert initial data into main and lookup tables
        initial_data = [
            {'category': 'A', 'value': 10, 'tags': ['red', 'blue'], 'location': 'POINT(40 5)', 'parent': None, 'date': datetime(2021, 1, 1)},
            {'category': 'B', 'value': 20, 'tags': ['blue'], 'location': 'POINT(42 10)', 'parent': 1, 'date': datetime(2021, 1, 2)},
            {'category': 'A', 'value': 15, 'tags': ['red', 'green'], 'location': 'POINT(44 15)', 'parent': 2, 'date': datetime(2021, 1, 3)},
            {'category': 'B', 'value': 25, 'tags': ['green', 'yellow'], 'location': 'POINT(46 20)', 'parent': 3, 'date': datetime(2021, 1, 4)},
            {'category': 'C', 'value': 30, 'tags': ['yellow'], 'location': 'POINT(48 25)', 'parent': 4, 'date': datetime(2021, 1, 5)}
        ]

        lookup_data = [
            {'_id': 'A', 'description': 'Category A'},
            {'_id': 'B', 'description': 'Category B'},
            {'_id': 'C', 'description': 'Category C'}
        ]

        try:
            with cls.test_conn.cursor() as cur:
                for data in initial_data:
                    cur.execute("""
                        INSERT INTO test_aggregation (category, value, tags, location, parent, date)
                        VALUES (%s, %s, %s, ST_GeogFromText(%s), %s, %s)
                    """, (data['category'], data['value'], data['tags'], data['location'], data['parent'], data['date']))

                for data in lookup_data:
                    cur.execute("""
                        INSERT INTO test_aggregation_lookup (_id, description)
                        VALUES (%s, %s)
                    """, (data['_id'], data['description']))
            cls.test_conn.commit()
            cls.logger.debug("Sample data inserted into 'test_aggregation' and 'test_aggregation_lookup' tables.")
            print("Sample data inserted into 'test_aggregation' and 'test_aggregation_lookup' tables.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error inserting initial data: {e}")
            print(f"Error inserting initial data: {e}")
            raise

    def setUp(self):
        # Assign class variables to instance variables for easy access
        self.logger = self.__class__.logger

        # Clear the in-memory log capture list before each test
        self.__class__.log_capture_list.clear()

    @classmethod
    def tearDownClass(cls):
        # Clean up tables after all tests
        try:
            with cls.test_conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS test_aggregation CASCADE;")
                cur.execute("DROP TABLE IF EXISTS test_aggregation_lookup CASCADE;")
                cur.execute("DROP TABLE IF EXISTS aggregation_output CASCADE;")
            cls.test_conn.commit()
            cls.logger.debug("Dropped tables during teardown.")
            print("Tables 'test_aggregation', 'test_aggregation_lookup', and 'aggregation_output' dropped successfully.")
        except Exception as e:
            cls.test_conn.rollback()
            cls.logger.error(f"Error in teardown: {e}")
            print(f"Error in teardown: {e}")
        finally:
            super().tearDownClass()

    def convert_decimals(self, obj):
        """Recursively convert Decimal objects to float."""
        if isinstance(obj, list):
            return [self.convert_decimals(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: self.convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, decimal.Decimal):
            return float(obj)  # or str(obj) if precision is important
        else:
            return obj

    def execute_aggregation_test(self, query, description):
        """
        Executes the given aggregation query on PostgreSQL,
        logs the results, and records any errors encountered.
        """
        start_time = time.time()
        result_document = {
            'status': 'fail',  # Default to 'fail'; will update based on conditions
            'test_name': f'Aggregation Test - {description}',
            'platform': 'postgresql',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': 'test_aggregation',
            'version': 'unknown',  # Will be updated dynamically
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'aggregation_result': []
        }

        try:
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query)
                aggregation_result = cur.fetchall()
                # Convert Decimal objects to float
                aggregation_result = self.convert_decimals(aggregation_result)
                result_document['aggregation_result'] = aggregation_result
                self.logger.debug('Aggregation executed successfully.')

            # Determine success status
            result_document['status'] = 'pass'
            result_document['exit_code'] = 0
            result_document['reason'] = 'PASSED'

        except Exception as e:
            self.test_conn.rollback()
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
                with self.test_conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version_info = cur.fetchone()
                server_version = version_info['version'] if version_info else 'unknown'
                result_document['version'] = server_version
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Convert all Decimal objects to float or string
            result_document = self.convert_decimals(result_document)

            # Ensure all fields are JSON serializable
            try:
                json.dumps(result_document)
            except TypeError as te:
                self.logger.error(f"JSON serialization error: {te}")
                result_document['reason'] += f" JSON serialization error: {te}"
                result_document['status'] = 'fail'

            # Accumulate result for later storage
            self.test_results.append(result_document)

    # Implement tests for each aggregation stage
    def test_addFields_stage(self):
        """
        Test $addFields stage.
        """
        # PostgreSQL equivalent: Add computed columns using SELECT with expressions
        query = """
            SELECT 
                *,
                (value + 10) AS value_plus_ten,
                CURRENT_TIMESTAMP AS current_date
            FROM test_aggregation;
        """
        self.execute_aggregation_test(query, '$addFields Stage Test')

    def test_bucket_stage(self):
        """
        Test $bucket stage.
        """
        # PostgreSQL equivalent: Use CASE WHEN for bucketing
        query = """
            SELECT 
                CASE 
                    WHEN value >= 0 AND value < 15 THEN '0-15'
                    WHEN value >= 15 AND value < 25 THEN '15-25'
                    WHEN value >= 25 AND value < 35 THEN '25-35'
                    ELSE 'Other'
                END AS bucket,
                COUNT(*) AS count,
                ARRAY_AGG(value) AS values
            FROM test_aggregation
            GROUP BY bucket;
        """
        self.execute_aggregation_test(query, '$bucket Stage Test')

    def test_bucketAuto_stage(self):
        """
        Test $bucketAuto stage.
        """
        # PostgreSQL equivalent: Approximate automatic bucketing using NTILE
        query = """
            SELECT 
                NTILE(3) OVER (ORDER BY value) AS bucket,
                COUNT(*) AS count,
                ARRAY_AGG(value) AS values
            FROM test_aggregation
            GROUP BY bucket
            ORDER BY bucket;
        """
        self.execute_aggregation_test(query, '$bucketAuto Stage Test')

    def test_collStats_stage(self):
        """
        Test $collStats stage.
        """
        # PostgreSQL equivalent: Retrieve table statistics from pg_stat_user_tables
        query = """
            SELECT 
                relname AS table_name,
                n_live_tup AS live_tuples,
                n_dead_tup AS dead_tuples,
                pg_total_relation_size(relid) AS total_size
            FROM pg_stat_user_tables
            WHERE relname = 'test_aggregation';
        """
        self.execute_aggregation_test(query, '$collStats Stage Test')

    def test_count_stage(self):
        """
        Test $count stage.
        """
        # PostgreSQL equivalent: COUNT with WHERE clause
        query = """
            SELECT COUNT(*) AS total_A_category
            FROM test_aggregation
            WHERE category = 'A';
        """
        self.execute_aggregation_test(query, '$count Stage Test')

    def test_densify_stage(self):
        """
        Test $densify stage.
        """
        # PostgreSQL does not have a direct equivalent to $densify.
        # Emulate by generating a series of dates and left joining.
        query = """
            WITH date_series AS (
                SELECT generate_series(
                    (SELECT MIN(date) FROM test_aggregation),
                    (SELECT MAX(date) FROM test_aggregation),
                    interval '1 day'
                ) AS date
            )
            SELECT 
                ds.date,
                ta.id,
                ta.category,
                ta.value
            FROM date_series ds
            LEFT JOIN test_aggregation ta ON ta.date = ds.date
            ORDER BY ds.date;
        """
        self.execute_aggregation_test(query, '$densify Stage Test')

    def test_facet_stage(self):
        """
        Test $facet stage.
        """
        # PostgreSQL equivalent: Perform multiple aggregations in one query using subqueries
        query = """
            SELECT 
                (SELECT json_agg(t) FROM (
                    SELECT category, COUNT(*) AS count
                    FROM test_aggregation
                    GROUP BY category
                ) t) AS categories,
                (SELECT json_agg(t) FROM (
                    SELECT SUM(value) AS totalValue
                    FROM test_aggregation
                ) t) AS values;
        """
        self.execute_aggregation_test(query, '$facet Stage Test')

    def test_fill_stage(self):
        """
        Test $fill stage.
        """
        # PostgreSQL does not have a direct equivalent to $fill.
        # Emulate by using window functions to carry forward values.
        query = """
            SELECT 
                id,
                category,
                value,
                tags,
                location,
                parent,
                date,
                COALESCE(value, LAG(value) OVER (ORDER BY date)) AS filled_value
            FROM test_aggregation
            ORDER BY date;
        """
        self.execute_aggregation_test(query, '$fill Stage Test')

    def test_geoNear_stage(self):
        """
        Test $geoNear stage.
        """
        # PostgreSQL equivalent: Calculate distance using ST_Distance and order by it
        query = """
            SELECT 
                *,
                ST_Distance(location, ST_GeographyFromText('SRID=4326;POINT(43 12)')) AS distance
            FROM test_aggregation
            WHERE ST_DWithin(location, ST_GeographyFromText('SRID=4326;POINT(43 12)'), 500000)
            ORDER BY distance;
        """
        self.execute_aggregation_test(query, '$geoNear Stage Test')

    def test_graphLookup_stage(self):
        """
        Test $graphLookup stage.
        """
        # PostgreSQL equivalent: Recursive CTE to find ancestors
        query = """
            WITH RECURSIVE ancestor_chain AS (
                SELECT 
                    id,
                    parent,
                    category,
                    value,
                    tags,
                    location,
                    date,
                    ARRAY[id] AS path
                FROM test_aggregation
                WHERE parent IS NOT NULL
                UNION ALL
                SELECT 
                    ta.id,
                    ta.parent,
                    ta.category,
                    ta.value,
                    ta.tags,
                    ta.location,
                    ta.date,
                    ac.path || ta.id
                FROM test_aggregation ta
                INNER JOIN ancestor_chain ac ON ta.id = ac.parent
            )
            SELECT * FROM ancestor_chain;
        """
        self.execute_aggregation_test(query, '$graphLookup Stage Test')

    def test_group_stage(self):
        """
        Test $group stage.
        """
        # PostgreSQL equivalent: GROUP BY with aggregate functions
        query = """
            SELECT 
                category,
                SUM(value) AS total_value,
                AVG(value) AS average_value
            FROM test_aggregation
            GROUP BY category;
        """
        self.execute_aggregation_test(query, '$group Stage Test')

    def test_limit_stage(self):
        """
        Test $limit stage.
        """
        # PostgreSQL equivalent: LIMIT clause
        query = """
            SELECT *
            FROM test_aggregation
            LIMIT 3;
        """
        self.execute_aggregation_test(query, '$limit Stage Test')

    def test_lookup_stage(self):
        """
        Test $lookup stage.
        """
        # PostgreSQL equivalent: LEFT JOIN with lookup table
        query = """
            SELECT 
                ta.*, 
                la.description
            FROM test_aggregation ta
            LEFT JOIN test_aggregation_lookup la ON ta.category = la._id;
        """
        self.execute_aggregation_test(query, '$lookup Stage Test')

    def test_match_stage(self):
        """
        Test $match stage.
        """
        # PostgreSQL equivalent: WHERE clause
        query = """
            SELECT *
            FROM test_aggregation
            WHERE value > 15;
        """
        self.execute_aggregation_test(query, '$match Stage Test')

    def test_project_stage(self):
        """
        Test $project stage.
        """
        # PostgreSQL equivalent: SELECT with expressions
        query = """
            SELECT 
                category,
                value,
                (value * value) AS value_squared
            FROM test_aggregation;
        """
        self.execute_aggregation_test(query, '$project Stage Test')

    def test_redact_stage(self):
        """
        Test $redact stage.
        """
        # PostgreSQL equivalent: Use CASE WHEN to include or exclude rows
        query = """
            SELECT 
                *,
                CASE 
                    WHEN category = 'A' THEN 'DESCEND'
                    ELSE 'PRUNE'
                END AS redact_decision
            FROM test_aggregation;
        """
        self.execute_aggregation_test(query, '$redact Stage Test')

    def test_replaceRoot_stage(self):
        """
        Test $replaceRoot stage.
        """
        # PostgreSQL equivalent: Select nested JSON fields as top-level fields
        # Assuming location is stored as GEOGRAPHY, we can extract coordinates
        query = """
            SELECT 
                ST_X(location::geometry) AS longitude,
                ST_Y(location::geometry) AS latitude
            FROM test_aggregation;
        """
        self.execute_aggregation_test(query, '$replaceRoot Stage Test')

    def test_replaceWith_stage(self):
        """
        Test $replaceWith stage.
        """
        # PostgreSQL equivalent: Similar to $replaceRoot
        query = """
            SELECT 
                ST_X(location::geometry) AS longitude,
                ST_Y(location::geometry) AS latitude
            FROM test_aggregation;
        """
        self.execute_aggregation_test(query, '$replaceWith Stage Test')

    def test_sample_stage(self):
        """
        Test $sample stage.
        """
        # PostgreSQL equivalent: ORDER BY RANDOM() LIMIT n
        query = """
            SELECT *
            FROM test_aggregation
            ORDER BY RANDOM()
            LIMIT 2;
        """
        self.execute_aggregation_test(query, '$sample Stage Test')

    def test_set_stage(self):
        """
        Test $set stage.
        """
        # PostgreSQL equivalent: Add or update columns using SELECT with expressions
        query = """
            SELECT 
                *,
                (value + 1) AS value_incremented
            FROM test_aggregation;
        """
        self.execute_aggregation_test(query, '$set Stage Test')

    def test_skip_stage(self):
        """
        Test $skip stage.
        """
        # PostgreSQL equivalent: OFFSET clause
        query = """
            SELECT *
            FROM test_aggregation
            ORDER BY id
            OFFSET 2;
        """
        self.execute_aggregation_test(query, '$skip Stage Test')

    def test_sort_stage(self):
        """
        Test $sort stage.
        """
        # PostgreSQL equivalent: ORDER BY clause
        query = """
            SELECT *
            FROM test_aggregation
            ORDER BY value DESC;
        """
        self.execute_aggregation_test(query, '$sort Stage Test')

    def test_sortByCount_stage(self):
        """
        Test $sortByCount stage.
        """
        # PostgreSQL equivalent: GROUP BY with COUNT and ORDER BY
        query = """
            SELECT 
                UNNEST(tags) AS tag,
                COUNT(*) AS count
            FROM test_aggregation
            GROUP BY tag
            ORDER BY count DESC;
        """
        self.execute_aggregation_test(query, '$sortByCount Stage Test')

    def test_unionWith_stage(self):
        """
        Test $unionWith stage.
        """
        # PostgreSQL equivalent: UNION ALL with another SELECT query
        query = """
            SELECT category, value, tags, location, parent, date
            FROM test_aggregation
            UNION ALL
            SELECT _id AS category, NULL AS value, NULL AS tags, NULL AS location, NULL AS parent, NULL AS date
            FROM test_aggregation_lookup;
        """
        self.execute_aggregation_test(query, '$unionWith Stage Test')

    def test_unset_stage(self):
        """
        Test $unset stage.
        """
        # PostgreSQL equivalent: Exclude columns in SELECT
        query = """
            SELECT 
                id,
                category,
                value,
                date
            FROM test_aggregation;
        """
        self.execute_aggregation_test(query, '$unset Stage Test')

    def test_unwind_stage(self):
        """
        Test $unwind stage.
        """
        # PostgreSQL equivalent: Use UNNEST to expand array elements into rows
        query = """
            SELECT 
                id,
                category,
                value,
                tag,
                location,
                parent,
                date
            FROM test_aggregation
            CROSS JOIN UNNEST(tags) AS tag;
        """
        self.execute_aggregation_test(query, '$unwind Stage Test')

    def test_variables_in_expressions(self):
        """
        Test variables in aggregation expressions.
        """
        # PostgreSQL equivalent: Use functions like CURRENT_TIMESTAMP and ROW
        query = """
            SELECT 
                category,
                value,
                (category = 'A') AS is_A,
                CURRENT_TIMESTAMP AS now,
                ROW(id, category, value, tags, location, parent, date) AS root,
                ROW(id, category, value, tags, location, parent, date) AS current
            FROM test_aggregation;
        """
        self.execute_aggregation_test(query, 'Variables in Aggregation Expressions Test')

    def test_variables(self):
        """
        Test variables like CURRENT_TIMESTAMP, etc.
        """
        # PostgreSQL equivalent: Use system functions and aliases
        query = """
            SELECT 
                CURRENT_TIMESTAMP AS currentDate,
                ROW(id, category, value, tags, location, parent, date) AS rootDocument,
                ROW(id, category, value, tags, location, parent, date) AS currentField
            FROM test_aggregation;
        """
        self.execute_aggregation_test(query, 'Variables Test')

    def test_merge_stage(self):
        """
        Test $merge stage.
        """
        # PostgreSQL equivalent: INSERT INTO ... ON CONFLICT ... DO UPDATE
        # First, perform aggregation
        aggregation_query = """
            SELECT 
                category,
                SUM(value) AS total_value,
                AVG(value) AS average_value
            FROM test_aggregation
            GROUP BY category;
        """
        start_time = time.time()  # Moved start_time here to include timing

        try:
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(aggregation_query)
                aggregation_result = cur.fetchall()
                for row in aggregation_result:
                    cur.execute("""
                        INSERT INTO aggregation_output (category, total_value, average_value)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (category) DO UPDATE
                        SET total_value = EXCLUDED.total_value,
                            average_value = EXCLUDED.average_value;
                    """, (row['category'], row['total_value'], row['average_value']))
            self.test_conn.commit()
            self.logger.debug('$merge Stage Test executed successfully.')

            # Fetch results for logging
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM aggregation_output;")
                merge_result = cur.fetchall()

            # Convert Decimals in merge_result
            merge_result = self.convert_decimals(merge_result)

            # Record the test result
            end_time = time.time()
            result_document = {
                'status': 'pass',
                'test_name': 'Aggregation Test - $merge Stage Test',
                'platform': 'postgresql',
                'exit_code': 0,
                'elapsed': end_time - start_time,
                'start': datetime.utcfromtimestamp(start_time).isoformat(),
                'end': datetime.utcfromtimestamp(end_time).isoformat(),
                'suite': 'test_aggregation',
                'version': 'unknown',
                'run': 1,
                'processed': True,
                'log_lines': list(self.log_capture_list),
                'reason': 'PASSED',
                'description': [],
                'aggregation_result': merge_result
            }
            try:
                with self.test_conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version_info = cur.fetchone()
                server_version = version_info[0] if version_info else 'unknown'
                result_document['version'] = server_version
            except Exception as ve:
                self.logger.error(f"Error retrieving server version: {ve}")
                result_document['version'] = 'unknown'

            # Convert any Decimals in result_document
            result_document = self.convert_decimals(result_document)

            # Ensure all fields are JSON serializable
            try:
                json.dumps(result_document)
            except TypeError as te:
                self.logger.error(f"JSON serialization error: {te}")
                result_document['reason'] += f" JSON serialization error: {te}"
                result_document['status'] = 'fail'

            self.test_results.append(result_document)

        except Exception as e:
            self.test_conn.rollback()
            error_trace = traceback.format_exc()
            self.logger.error(f'Error during $merge Stage Test: {e}\n{error_trace}')
            result_document = {
                'status': 'fail',
                'test_name': 'Aggregation Test - $merge Stage Test',
                'platform': 'postgresql',
                'exit_code': 1,
                'elapsed': None,
                'start': '',
                'end': '',
                'suite': 'test_aggregation',
                'version': 'unknown',
                'run': 1,
                'processed': True,
                'log_lines': list(self.log_capture_list),
                'reason': 'FAILED',
                'description': [str(e)],
                'aggregation_result': []
            }
            # Convert any Decimals in result_document
            result_document = self.convert_decimals(result_document)

            # Ensure all fields are JSON serializable
            try:
                json.dumps(result_document)
            except TypeError as te:
                self.logger.error(f"JSON serialization error: {te}")
                result_document['reason'] += f" JSON serialization error: {te}"
                result_document['status'] = 'fail'

            self.test_results.append(result_document)

    def test_out_stage(self):
        """
        Test $out stage.
        """
        # PostgreSQL equivalent: Insert query results into another table
        # First, ensure output table is clean
        try:
            with self.test_conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE aggregation_output;")
                self.test_conn.commit()
                self.logger.debug("Truncated 'aggregation_output' table before $out Stage Test.")
        except Exception as e:
            self.test_conn.rollback()
            self.logger.error(f'Error truncating aggregation_output table: {e}')

        # Perform the "out" operation by inserting matched documents into aggregation_output
        query = """
            INSERT INTO aggregation_output (category, total_value, average_value)
            SELECT 
                category,
                value AS total_value,
                value AS average_value
            FROM test_aggregation
            WHERE category = 'A';
        """
        self.execute_aggregation_test(query, '$out Stage Test')

    def test_recursive_aggregation(self):
        """
        Additional Test: Recursive Aggregation using CTEs.
        """
        # Example of a recursive aggregation similar to $graphLookup
        query = """
            WITH RECURSIVE ancestor_chain AS (
                SELECT 
                    id,
                    parent,
                    category,
                    value,
                    tags,
                    location,
                    date,
                    ARRAY[id] AS path
                FROM test_aggregation
                WHERE parent IS NOT NULL
                UNION ALL
                SELECT 
                    ta.id,
                    ta.parent,
                    ta.category,
                    ta.value,
                    ta.tags,
                    ta.location,
                    ta.date,
                    ac.path || ta.id
                FROM test_aggregation ta
                INNER JOIN ancestor_chain ac ON ta.id = ac.parent
            )
            SELECT * FROM ancestor_chain;
        """
        self.execute_aggregation_test(query, 'Recursive Aggregation Test')

    # ... (Ensure all aggregation tests are included here) ...

if __name__ == '__main__':
    unittest.main()
