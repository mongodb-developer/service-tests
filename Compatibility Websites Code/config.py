# config.py

# Amazon DocumentDB connection
DOCDB_URI = 'mongodb+srv://rahulverma:09Ph2007@vcorecomp40.global.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000'
DOCDB_DB_NAME = 'testdb_vcore_new'
DOCDB_SSL_CA_FILE = '/Users/rahul.verma/Desktop/Updated\ Compatibility\ Test\ Suite/docdbcompnew.pem'
# MongoDB Atlas cluster for storing results
RESULT_DB_URI = 'mongodb+srv://rahulverma:09Ph2007@comp-web-prod.n0kts.mongodb.net/?retryWrites=true&w=majority&appName=comp-web-prod'
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
