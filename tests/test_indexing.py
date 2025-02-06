# tests/test_indexing.py

import unittest
import logging
import time
import json
import traceback
from datetime import datetime, timedelta
import sys
import os
import decimal  # Importing the decimal module

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

# Adjust sys.path to include the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from base_test import BaseTest

class TestIndexing(BaseTest):
    """
    Test suite for indexing in PostgreSQL.
    Translates MongoDB/DocumentDB indexing tests to PostgreSQL using psycopg2.
    Each index type is tested in a separate unittest method to provide granular results.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.table_name = 'test_indexing'

        # Configure logging
        cls.logger = logging.getLogger('TestIndexing')
        cls.logger.setLevel(logging.DEBUG)

        # File Handler for logging to 'test_indexing.log'
        file_handler = logging.FileHandler('test_indexing.log')
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

        # Create table for indexing tests
        try:
            with cls.test_conn.cursor() as cur:
                # Drop table if it exists to start clean
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Dropped table '{cls.table_name}' if it existed.")

                # Create table with appropriate columns
                cur.execute(sql.SQL("""
                    CREATE TABLE {} (
                        _id TEXT PRIMARY KEY,
                        field1 TEXT,
                        field2 TEXT,
                        field3 TEXT,
                        number_field INTEGER,
                        decimal_field NUMERIC,
                        array_field TEXT[],
                        nested_field JSONB,
                        location POINT,
                        multi_location JSONB,
                        polygon POLYGON,
                        date_field TIMESTAMP,
                        expiry_field TIMESTAMP,
                        tags TEXT[],
                        status TEXT,
                        version INTEGER,
                        score INTEGER,
                        metadata JSONB,
                        optional_field TEXT
                    );
                """).format(sql.Identifier(cls.table_name)))
                cls.logger.debug(f"Created table '{cls.table_name}' with all necessary fields.")
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
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(self.table_name)))
                # Recreate the table for the test
                cur.execute(sql.SQL("""
                    CREATE TABLE {} (
                        _id TEXT PRIMARY KEY,
                        field1 TEXT,
                        field2 TEXT,
                        field3 TEXT,
                        number_field INTEGER,
                        decimal_field NUMERIC,
                        array_field TEXT[],
                        nested_field JSONB,
                        location POINT,
                        multi_location JSONB,
                        polygon POLYGON,
                        date_field TIMESTAMP,
                        expiry_field TIMESTAMP,
                        tags TEXT[],
                        status TEXT,
                        version INTEGER,
                        score INTEGER,
                        metadata JSONB,
                        optional_field TEXT
                    );
                """).format(sql.Identifier(self.table_name)))
            self.test_conn.commit()
            self.logger.debug("Reset table before test.")
        except Exception as e:
            self.test_conn.rollback()
            self.logger.error(f"Error resetting table in setUp: {e}")
            raise

    @staticmethod
    def parse_point(point_str):
        """
        Parses a point string in the format '(x,y)' and returns a tuple (x, y).
        """
        point_str = point_str.strip('()')
        x, y = point_str.split(',')
        return float(x), float(y)

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

    def get_test_data(self, test_iteration: int):
        """Generate comprehensive test data for all index types"""
        base_time = datetime.utcnow()
        return [
            {
                '_id': f'test_{test_iteration}_1',
                'field1': 'value1',
                'field2': 'value2',
                'field3': 'text content for searching',
                'number_field': 100,
                'decimal_field': 100.50,
                'array_field': ['tag1', 'tag2', 'tag3'],
                'nested_field': {
                    'sub_field1': 'nested1',
                    'sub_field2': 'nested2',
                    'sub_array': [1, 2, 3]
                },
                'location': '(40,5)',  # Stored as POINT
                'multi_location': [
                    {'type': 'Point', 'coordinates': [40.0, 5.0]},
                    {'type': 'Point', 'coordinates': [41.0, 6.0]}
                ],
                'polygon': '((0,0), (3,6), (6,1), (0,0))',  # Stored as POLYGON
                'date_field': base_time,
                'expiry_field': base_time + timedelta(hours=1),
                'tags': ['mongodb', 'database', 'indexing'],
                'status': 'active',
                'version': 1,
                'score': 85,
                'metadata': {
                    'created_by': 'user1',
                    'department': 'engineering'
                },
                'optional_field': 'some_optional_value'
            },
            {
                '_id': f'test_{test_iteration}_2',
                'field1': 'value3',
                'field2': 'value4',
                'field3': 'another text for full-text search',
                'number_field': 200,
                'decimal_field': 200.75,
                'array_field': ['tag4', 'tag5'],
                'nested_field': {
                    'sub_field1': 'nested3',
                    'sub_field2': 'nested4',
                    'sub_array': [4, 5, 6]
                },
                'location': '(42,10)',  # Stored as POINT
                'multi_location': [
                    {'type': 'Point', 'coordinates': [42.0, 10.0]},
                    {'type': 'Point', 'coordinates': [43.0, 11.0]}
                ],
                'polygon': '((1,1), (4,7), (7,2), (1,1))',  # Stored as POLYGON
                'date_field': base_time + timedelta(days=1),
                'expiry_field': base_time + timedelta(days=1, hours=1),
                'tags': ['atlas', 'cloud', 'testing'],
                'status': 'inactive',
                'version': 2,
                'score': 90,
                'metadata': {
                    'created_by': 'user2',
                    'department': 'testing'
                },
                'optional_field': None
            }
        ]

    def insert_test_data(self, test_data):
        """Inserts test data into the table."""
        try:
            with self.test_conn.cursor() as cur:
                insert_query = sql.SQL("""
                    INSERT INTO {} (_id, field1, field2, field3, number_field, decimal_field, array_field, nested_field,
                                     location, multi_location, polygon, date_field, expiry_field, tags, status, version,
                                     score, metadata, optional_field)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, POINT(%s, %s), %s, POLYGON(%s), %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (_id) DO NOTHING;
                """).format(sql.Identifier(self.table_name))
                for doc in test_data:
                    multi_location_json = json.dumps(doc['multi_location'])
                    polygon_str = doc['polygon']
                    tags_tuple = tuple(doc['tags'])
                    metadata_json = json.dumps(doc['metadata'])
                    optional_field = doc.get('optional_field', None)
                    cur.execute(insert_query, (
                        doc['_id'],
                        doc['field1'],
                        doc['field2'],
                        doc['field3'],
                        doc['number_field'],
                        doc['decimal_field'],
                        doc['array_field'],
                        json.dumps(doc['nested_field']),
                        *self.parse_point(doc['location']),
                        multi_location_json,
                        polygon_str,
                        doc['date_field'],
                        doc['expiry_field'],
                        tags_tuple,
                        doc['status'],
                        doc['version'],
                        doc['score'],
                        metadata_json,
                        optional_field
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

    def create_and_verify_index(self, index_def):
        """
        Creates an index based on the index definition and verifies its creation.
        Returns True if successful, False otherwise.
        """
        index_name = index_def['name']
        keys = index_def['keys']
        options = index_def['options']

        try:
            with self.test_conn.cursor() as cur:
                # Prepare index keys
                formatted_keys = []
                for key, order in keys:
                    if key.startswith("(") and key.endswith(")"):
                        # For complex expressions like nested_field->>'sub_field1'
                        formatted_keys.append(sql.SQL(key + " " + order))
                    elif order == 'TEXT':
                        # For full-text search, use to_tsvector on the field
                        formatted_keys.append(sql.SQL("to_tsvector('english', {})".format(key)))
                    elif order == 'GIN':
                        # For GIN indexes on JSONB or array fields
                        formatted_keys.append(sql.SQL("{} gin_trgm_ops".format(key)))
                    elif order == 'HASH':
                        # PostgreSQL's hash indexes
                        formatted_keys.append(sql.SQL("{} HASH".format(key)))
                    else:
                        # ASC or DESC
                        formatted_keys.append(sql.SQL("{} {}".format(key, order)))
                # Create index
                # Handling GIN and GIST indexes appropriately
                if any(order in ['GIN', 'GIST'] for _, order in keys):
                    using_clause = 'GIN' if any(order == 'GIN' for _, order in keys) else 'GIST'
                else:
                    using_clause = 'BTREE'  # Default

                create_index_query = sql.SQL("""
                    CREATE INDEX {idx_name} ON {table_name}
                    USING {using_clause} ({keys});
                """).format(
                    idx_name=sql.Identifier(index_name),
                    table_name=sql.Identifier(self.table_name),
                    using_clause=sql.SQL(using_clause),
                    keys=sql.SQL(", ").join(formatted_keys)
                )
                cur.execute(create_index_query, )
            self.test_conn.commit()
            self.logger.debug(f"Created index '{index_name}' successfully.")
            return True, ""
        except psycopg2.Error as e_create:
            self.test_conn.rollback()
            error_msg = f"Failed to create index '{index_name}': {e_create}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            return False, error_msg

    def verify_index_exists(self, index_name):
        """
        Verifies if an index with the given name exists on the table.
        Returns True if exists, False otherwise.
        """
        try:
            with self.test_conn.cursor() as cur:
                cur.execute("""
                    SELECT indexname FROM pg_indexes
                    WHERE tablename = %s AND indexname = %s;
                """, (self.table_name, index_name))
                index_exists = cur.fetchone()
            if index_exists:
                self.logger.debug(f"Index '{index_name}' exists.")
                return True
            else:
                self.logger.debug(f"Index '{index_name}' does not exist.")
                return False
        except Exception as e_verify:
            self.logger.error(f"Error verifying index '{index_name}': {e_verify}")
            self.logger.debug(traceback.format_exc())
            return False

    def get_explain_plan(self, query):
        """
        Obtains the explain plan for a given query in JSON format.
        """
        try:
            with self.test_conn.cursor(cursor_factory=RealDictCursor) as cur:
                explain_query = "EXPLAIN (FORMAT JSON) " + query
                cur.execute(explain_query)
                explain_result = cur.fetchone()['QUERY PLAN'][0]
            self.logger.debug("Obtained explain plan successfully.")
            return explain_result
        except Exception as e_explain:
            self.logger.error(f"Failed to obtain explain plan: {e_explain}")
            self.logger.debug(traceback.format_exc())
            return None

    def execute_query_and_get_explain(self, query):
        """
        Executes the query and returns the explain plan.
        """
        explain_plan = self.get_explain_plan(query)
        return explain_plan

    def check_index_usage(self, explain_plan, index_name):
        """
        Recursively check if the specified index is used in the explain plan.
        """
        if isinstance(explain_plan, dict):
            for key, value in explain_plan.items():
                if key.lower() == 'relation name' and value == index_name:
                    return True
                elif key.lower() == 'index name' and value == index_name:
                    return True
                elif isinstance(value, (dict, list)):
                    if self.check_index_usage(value, index_name):
                        return True
        elif isinstance(explain_plan, list):
            for item in explain_plan:
                if self.check_index_usage(item, index_name):
                    return True
        return False

    def get_server_version(self):
        """Retrieves the PostgreSQL server version."""
        try:
            with self.test_conn.cursor() as cur:
                cur.execute("SELECT version();")
                version_info = cur.fetchone()
            server_version = version_info[0] if version_info else 'unknown'
            self.logger.debug(f"Server version retrieved: {server_version}")
            return server_version
        except Exception as ve:
            self.logger.error(f"Error retrieving server version: {ve}")
            return 'unknown'

    def run_index_test(self, index_def):
        """
        General method to run index tests.
        Creates the index, verifies it, inserts test data, executes a query,
        obtains the explain plan, and checks if the index is used.
        """
        test_iteration = 0  # Since each index is tested separately
        test_data = self.get_test_data(test_iteration)
        index_name = index_def['name']
        description = index_def.get('description', 'Indexing Test')

        start_time = time.time()
        result_document = {
            'status': 'fail',  # Default to 'fail'; will update based on conditions
            'test_name': f'Indexing Test - {description}',
            'platform': 'postgresql',
            'exit_code': 1,
            'elapsed': None,
            'start': datetime.utcfromtimestamp(start_time).isoformat(),
            'end': None,
            'suite': self.table_name,
            'version': 'unknown',  # Will be updated dynamically
            'run': 1,
            'processed': True,
            'log_lines': [],
            'reason': '',
            'description': [],
            'explain_plan': {},
        }

        try:
            # Step 1: Create Index
            success, error_msg = self.create_and_verify_index(index_def)
            if not success:
                raise Exception(error_msg)
            result_document['log_lines'].append(f"Index '{index_name}' created successfully.")
            self.logger.debug(f"Index '{index_name}' created successfully.")

            # Step 2: Insert Test Data
            success, error_msg = self.insert_test_data(test_data)
            if not success:
                raise Exception(error_msg)
            result_document['log_lines'].append("Test data inserted successfully.")
            self.logger.debug("Test data inserted successfully.")

            # Step 3: Define and Execute Query
            query = self.get_test_query(index_def['keys'], test_data)
            self.logger.debug(f"Executing query: {query}")
            result_document['log_lines'].append(f"Executing query: {query}")

            explain_plan = self.execute_query_and_get_explain(query)
            if explain_plan is None:
                raise Exception("Failed to obtain explain plan.")

            result_document['log_lines'].append("Explain plan obtained.")
            self.logger.debug("Explain plan obtained.")

            # Serialize explain plan
            explain_plan_serializable = json.loads(json.dumps(explain_plan, default=str))
            result_document['explain_plan'] = explain_plan_serializable

            # Log the formatted explain plan
            formatted_explain = json.dumps(explain_plan_serializable, indent=4)
            self.logger.debug(f"Explain Plan for {index_name}:\n{formatted_explain}")

            # Step 4: Check Index Usage in Query Plan
            if self.check_index_usage(explain_plan_serializable, index_name):
                result_document['log_lines'].append(f"Index '{index_name}' used in query plan.")
                self.logger.debug(f"Index '{index_name}' used in query plan.")
                result_document['status'] = 'pass'
                result_document['exit_code'] = 0
                result_document['reason'] = 'PASSED'
            else:
                # Mark as fail if index not used
                result_document['status'] = 'fail'
                result_document['exit_code'] = 1
                result_document['reason'] = 'FAILED'
                result_document['description'].append(f"Index '{index_name}' not used in query plan.")
                self.logger.warning(f"Index '{index_name}' not used in query plan.")

        except Exception as e:
            # Capture any exceptions, mark test as failed
            error_message = str(e)
            result_document['status'] = 'fail'
            result_document['exit_code'] = 1
            result_document['reason'] = 'FAILED'
            result_document['description'].append(error_message)
            self.logger.error(f"Error for {index_name}: {error_message}\n{traceback.format_exc()}")

        finally:
            end_time = time.time()
            result_document['elapsed'] = end_time - start_time
            result_document['end'] = datetime.utcfromtimestamp(end_time).isoformat()

            # Retrieve PostgreSQL server version dynamically
            server_version = self.get_server_version()
            result_document['version'] = server_version

            # Assign captured log lines to the result document
            result_document['log_lines'] = list(self.log_capture_list)

            # Convert all Decimal objects to float or string
            result_document = self.convert_decimals(result_document)

            # Ensure all fields in result_document are JSON serializable
            try:
                json.dumps(result_document)
            except TypeError as te:
                self.logger.error(f"JSON serialization error: {te}")
                result_document['reason'] += f" JSON serialization error: {te}"
                result_document['status'] = 'fail'

            # Print the result_document for debugging
            print(json.dumps(result_document, indent=4))

            # Accumulate result for later storage
            self.test_results.append(result_document)

            # Clean up: Drop table to reset for next test
            try:
                with self.test_conn.cursor() as cur:
                    cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(self.table_name)))
                self.test_conn.commit()
                self.logger.debug(f"Dropped table '{self.table_name}' after test.")
            except Exception as e_cleanup:
                self.test_conn.rollback()
                self.logger.error(f"Error dropping table after test: {e_cleanup}")

    # Define separate test methods for each index type within the class
    def test_basic_ascending_index(self):
        """Test basic ascending index on field1"""
        index_def = {
            'name': 'basic_ascending',
            'keys': [('field1', 'ASC')],
            'options': {},
            'description': 'Basic ascending index on field1'
        }
        self.run_index_test(index_def)

    def test_basic_descending_index(self):
        """Test basic descending index on field2"""
        index_def = {
            'name': 'basic_descending',
            'keys': [('field2', 'DESC')],
            'options': {},
            'description': 'Basic descending index on field2'
        }
        self.run_index_test(index_def)

    def test_compound_basic_index(self):
        """Test compound index on field1 and field2"""
        index_def = {
            'name': 'compound_basic',
            'keys': [('field1', 'ASC'), ('field2', 'DESC')],
            'options': {},
            'description': 'Compound index on field1 and field2'
        }
        self.run_index_test(index_def)

    def test_text_basic_index(self):
        """Test text index on field3 with English configuration"""
        index_def = {
            'name': 'text_basic',
            'keys': [('field3', 'TEXT')],
            'options': {
                'text_search_config': 'english'
            },
            'description': 'Text index on field3 with English configuration'
        }
        self.run_index_test(index_def)

    def test_geo_gist_index(self):
        """Test GIST geospatial index on location"""
        index_def = {
            'name': 'geo_gist',
            'keys': [('location', 'GIST')],
            'options': {},
            'description': 'GIST geospatial index on location'
        }
        self.run_index_test(index_def)

    def test_hashed_basic_index(self):
        """Test hashed index on field1"""
        index_def = {
            'name': 'hashed_basic',
            'keys': [('field1', 'HASH')],
            'options': {},
            'description': 'Hashed index on field1'
        }
        self.run_index_test(index_def)

    def test_unique_single_index(self):
        """Test unique index on field1"""
        index_def = {
            'name': 'unique_single',
            'keys': [('field1', 'ASC')],
            'options': {'unique': True},
            'description': 'Unique index on field1'
        }
        self.run_index_test(index_def)

    def test_unique_compound_index(self):
        """Test unique compound index on field1 and field2"""
        index_def = {
            'name': 'unique_compound',
            'keys': [('field1', 'ASC'), ('field2', 'ASC')],
            'options': {'unique': True},
            'description': 'Unique compound index on field1 and field2'
        }
        self.run_index_test(index_def)

    def test_partial_basic_index(self):
        """Test partial index on field1 where status is active"""
        index_def = {
            'name': 'partial_basic',
            'keys': [('field1', 'ASC')],
            'options': {
                'where': "status = 'active'"
            },
            'description': 'Partial index on field1 where status is active'
        }
        self.run_index_test(index_def)

    def test_sparse_basic_index(self):
        """Test sparse index on optional_field (simulated)"""
        index_def = {
            'name': 'sparse_basic',
            'keys': [('optional_field', 'ASC')],
            'options': {
                'where': "optional_field IS NOT NULL"
            },
            'description': 'Sparse index on optional_field (simulated)'
        }
        self.run_index_test(index_def)

    def test_ttl_basic_index(self):
        """Test TTL index on expiry_field with 1-hour expiration (simulated)"""
        index_def = {
            'name': 'ttl_basic',
            'keys': [('expiry_field', 'ASC')],
            'options': {
                'where': "expiry_field < NOW() + INTERVAL '1 hour'"
            },
            'description': 'TTL index on expiry_field with 1-hour expiration (simulated)'
        }
        self.run_index_test(index_def)

    def test_wildcard_all_index(self):
        """Test wildcard index on all fields in metadata"""
        index_def = {
            'name': 'wildcard_all',
            'keys': [('metadata', 'GIN')],
            'options': {},
            'description': 'Wildcard index on all fields in metadata'
        }
        self.run_index_test(index_def)

    def test_array_single_index(self):
        """Test GIN index on array_field"""
        index_def = {
            'name': 'array_single',
            'keys': [('array_field', 'GIN')],
            'options': {},
            'description': 'GIN index on array_field'
        }
        self.run_index_test(index_def)

    def test_complex_compound_index(self):
        """Test unique compound index on field1, field2, and nested_field.sub_field1"""
        index_def = {
            'name': 'complex_compound',
            'keys': [
                ('field1', 'ASC'),
                ('field2', 'DESC'),
                ("(nested_field->>'sub_field1')", 'ASC')
            ],
            'options': {'unique': True},
            'description': 'Unique compound index on field1, field2, and nested_field.sub_field1'
        }
        self.run_index_test(index_def)

    def tearDown(self):
        """No specific teardown needed as table is dropped after each test"""
        pass

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

    def get_test_query(self, index_keys, test_data):
        """Generate appropriate test query based on index type"""
        if any(order == 'TEXT' for _, order in index_keys):
            # Full-text search query
            return "SELECT * FROM {} WHERE to_tsvector('english', field3) @@ to_tsquery('english', 'content');".format(
                self.table_name
            )

        if any(order == 'GIST' for _, order in index_keys):
            # Geospatial query using distance
            return "SELECT * FROM {} WHERE ST_DWithin(location::geography, ST_MakePoint(40,5)::geography, 5000);".format(
                self.table_name
            )

        if any(order == 'HASH' for _, order in index_keys):
            # Hash index usage is limited; simulate by querying exact match
            return "SELECT * FROM {} WHERE field1 = '{}';".format(
                self.table_name, test_data[0]['field1']
            )

        # Compound and other indexes
        if len(index_keys) > 1:
            conditions = []
            for key, order in index_keys:
                if key.startswith("(") and key.endswith(")"):
                    # Extract sub_field1 from JSONB
                    conditions.append("{} = '{}'".format(key, test_data[0]['nested_field']['sub_field1']))
                else:
                    value = test_data[0].get(key, '')
                    if isinstance(value, list):
                        conditions.append("{} && ARRAY{}".format(key, tuple(value)))
                    else:
                        conditions.append("{} = '{}'".format(key, value))
            query = "SELECT * FROM {} WHERE {} ;".format(
                self.table_name,
                " AND ".join(conditions)
            )
            return query

        # Single Field Query
        if index_keys and index_keys[0][0] != 'metadata':
            key = index_keys[0][0]
            value = test_data[0].get(key, '')
            if isinstance(value, list):
                return "SELECT * FROM {} WHERE {} && ARRAY{};".format(
                    self.table_name, key, tuple(value)
                )
            else:
                return "SELECT * FROM {} WHERE {} = '{}';".format(
                    self.table_name, key, value
                )

        # Default query
        return "SELECT * FROM {} WHERE field1 = '{}';".format(
            self.table_name, test_data[0]['field1']
        )

if __name__ == '__main__':
    unittest.main()
