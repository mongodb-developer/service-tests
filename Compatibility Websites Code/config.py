# config.py

# Amazon DocumentDB connection
DOCDB_URI = ''
DOCDB_DB_NAME = 'testdb_vcore_new'
DOCDB_SSL_CA_FILE = ''
# MongoDB Atlas cluster for storing results
RESULT_DB_URI = ''
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
MONGODUMP_PATH = ''        # Update with the path to your mongodump tool
MONGORESTORE_PATH = ''  # Update with the path to your mongorestore tool
