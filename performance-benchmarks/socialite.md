# Overview

This readme is intended to provide steps for any individual to reproduce the Socialite results published as part of MongoDB's comparison and analysis of AWS DocumentDB. 


## Environment

In order to accurately reproduce the same results, the following AWS and Atlas clusters will need to be provisioned:


### MongoDB Cluster:

*   Atlas Cluster (M80 on AWS)

### DocumentDB Cluster:

*   Document DB Cluster (3 R4.4xLarge instances across 3 Availability Zones)

### Load Driver Instance:

*   For the purposes of driving test load, you must provision an AWS instance for each cluster:
    *   **MongoDB Atlas** - provision an instance (r4.16xlarge) within the same region and [vpc peered](https://docs.atlas.mongodb.com/security-vpc-peering/) to the Atlas cluster.
    *   **AWS DocumentDB** - provision an instance  (r4.16xlarge)  within the same region **AND** VPC as the DocumentDB cluster.


## Setting Up Socialite

*   Clone the following Github repository [https://github.com/mongodb-labs/socialite](https://github.com/mongodb-labs/socialite)
*   General instructions for configuring Socialite can be found [here](https://github.com/mongodb-labs/socialite/blob/master/docs/building.md)

### Configurations For Running Socialite

Sample configuration is available [here](https://github.com/mongodb-labs/socialite/blob/master/sample-config.yml)

Use the following configuration values for your tests:

```
totalUsers=100000
activeUsers=10000
duration=3600
sessionDuration=30
concurrency=512
maxFollows=5000
messages=20
```

### Running Socialite

The benchmark can be executed using the following [bench-run.sh](https://github.com/mongodb-labs/socialite/blob/master/bin/bench-run.sh) script.
