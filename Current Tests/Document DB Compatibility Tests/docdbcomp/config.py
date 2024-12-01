# config.py
# config.py

# MongoDB Atlas cluster for test data (original MongoDB)
#MONGODB_URI = 'mongodb+srv://rahulverma:09Ph2007@vcompatibilitytest.n0kts.mongodb.net/?retryWrites=true&w=majority&appName=vcompatibilitytest'
#MONGODB_DB_NAME = 'testdb_mongodb'

# Amazon DocumentDB connection
DOCDB_URI = 'mongodb://rahulverma:09Ph2007@docdbcomp.cluster-cxjimx8ndms0.eu-west-3.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=/home/ubuntu/docdbcomp/global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false'
DOCDB_DB_NAME = 'testdb_documentdb'

# MongoDB Atlas cluster for storing results
RESULT_DB_URI = 'mongodb+srv://rahulverma:09Ph2007@vcompatibilitytest.n0kts.mongodb.net/?retryWrites=true&w=majority&appName=vcompatibilitytest'
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
MONGODUMP_PATH = '/Users/rahul.verma/Desktop/vCore Compatibility Test Suite/mongodb-database-tools-macos-arm64-100.10.0/bin/mongodump'        # Update with the path to your mongodump tool
MONGORESTORE_PATH = '/Users/rahul.verma/Desktop/vCore Compatibility Test Suite/mongodb-database-tools-macos-arm64-100.10.0/bin/mongorestore'  # Update with the path to your mongorestore tool
