# config.py
# config.py


# Amazon DocumentDB connection
DOCDB_URI = 'enter your Document DB connection string here'
DOCDB_DB_NAME = 'testdb_documentdb'

# MongoDB Atlas cluster for storing results
RESULT_DB_URI = 'enter your Atlas connection string where the results will be stored'
RESULT_DB_NAME = 'docdb_web_prod'
RESULT_COLLECTION_NAME = 'correctness'

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
