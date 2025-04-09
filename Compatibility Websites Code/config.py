# config.py

# Test database connection string
DOCDB_URI = 'ENTER YOUR CONNECTION STRING HERE'
DOCDB_DB_NAME = 'testdb_vcore_new'
DOCDB_SSL_CA_FILE = 'ENTER THE PATH TO THE CA FILE IF APPLICABLE'


# MongoDB Atlas cluster for storing results
RESULT_DB_URI = 'ENTER YOUR ATLAS CLUSTER CONNECTING STRING HERE TO RECORD RESULTS'
RESULT_DB_NAME = 'cosmos_new_test'
RESULT_COLLECTION_NAME = 'correctness'
PLATFORM = "CosmosDB"  # e.g., "CosmosDB" or "MongoDB" or another descriptor


# KMS Providers for field-level encryption (example using a local master key)
import os
local_master_key = os.urandom(96)

KMS_PROVIDERS = {
    'local': {
        'key': local_master_key
    }
}


# Key Vault Namespace for field-level encryption
KEY_VAULT_NAMESPACE = 'encryption.__keyVault'

# Paths to mongodump and mongorestore tools
MONGODUMP_PATH = '/Users/rahul.verma/Desktop/vCore Compatibility Test Suite/mongodb-database-tools-macos-arm64-100.10.0/bin/mongodump'        # Update with the path to your mongodump tool
MONGORESTORE_PATH = '/Users/rahul.verma/Desktop/vCore Compatibility Test Suite/mongodb-database-tools-macos-arm64-100.10.0/bin/mongorestore'  # Update with the path to your mongorestore tool
