# config.py

import os

# ------------------------------------------------------------------------------
# Admin Credentials for Database Setup
# ------------------------------------------------------------------------------
ADMIN_DB_USER = "documentdb"                    # Superuser
ADMIN_DB_PASSWORD = "" # Replace with your actual postgres user's password

# ------------------------------------------------------------------------------
# Postgres-based DocumentDB Connection
# ------------------------------------------------------------------------------
DB_HOST = "localhost"
DB_PORT = 9712
DB_USER = "documentdb"                        # Regular user for testing
DB_PASSWORD = ""      # Replace with your documentdb user's password
DB_NAME = "testdb_documentdb_main3"           # Target database for tests

# Use this DB_NAME for any additional testing, or reuse the 'postgres' DB directly
DOCDB_DB_NAME = 'testdb_documentdb_main3'      # Ensuring consistency with DB_NAME

# ------------------------------------------------------------------------------
# MongoDB Atlas cluster for storing results (unchanged)
# ------------------------------------------------------------------------------
RESULT_DB_URI = "mongodb+srv://rahulverma:09Ph2007@vcompatibilitytest.n0kts.mongodb.net/?retryWrites=true&w=majority&appName=vcompatibilitytest"
RESULT_DB_NAME = "docdb_web_prod_new"
RESULT_COLLECTION_NAME = "correctness_new"

# ------------------------------------------------------------------------------
# KMS Providers (unchanged - not actively used in Postgres scenario)
# ------------------------------------------------------------------------------
local_master_key = os.urandom(96)
KMS_PROVIDERS = {
    'local': {
        'key': local_master_key
    }
}
KEY_VAULT_NAMESPACE = 'encryption.__keyVault'

# ------------------------------------------------------------------------------
# Paths to mongodump and mongorestore tools (not used, but kept for compatibility)
# ------------------------------------------------------------------------------
MONGODUMP_PATH = "/usr/bin/mongodump"
MONGORESTORE_PATH = "/usr/bin/mongorestore"
