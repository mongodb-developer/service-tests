
# Overview
This readme is intended to provide steps for any individual to reproduce the YCSB results published as part of MongoDB's comparison and analysis of AWS Document DB. 

# Environment
In order to accurately reproduce the same results, the following AWS and Atlas clusters will need to be provisioned:

## MongoDB Cluster:

*   Atlas Cluster (M60 NVMe on AWS)

## DocumentDB Cluster:

*   Document DB Cluster (3 R4.4xLarge instances across 3 Availability Zones)

## Load Driver Instance:

*  For the purposes of driving test load, you must provision an AWS instance for each cluster:
    *  **MongoDB Atlas** - provision an instance (r4.16xlarge) within the same region and [vpc peered](https://docs.atlas.mongodb.com/security-vpc-peering/) to the Atlas cluster.
    *  **AWS DocumentDB **- provision an instance  (r4.16xlarge)  within the same region **AND** VPC as the DocumentDB cluster.


## Setting Up YCSB

*  Clone the YCSB repo available a [MongoDB Labs](https://github.com/mongodb-labs/YCSB)
*  Follow the [instructions](https://github.com/mongodb-labs/YCSB/blob/master/ycsb-mongodb/mongodb/README.md) to install and run YCSB

## Configurations For Running YCSB
In order to reproduce published results, the following steps should be taken:

### Large Data Load Workload File

*   requestdistribution=zipfian
*   recordcount=81920000
*   operationcount=20000000
*   workload=com.yahoo.ycsb.workloads.CoreWorkload
*   readallfields=true
*   readproportion=1.0
*   updateproportion=0
*   scanproportion=0
*   insertproportion=0.0
*   requestdistribution=zipfian
*   fieldcount=25

### Small Data Load Workload File

*   requestdistribution=zipfian
*   recordcount=4096000
*   operationcount=20000000
*   workload=com.yahoo.ycsb.workloads.CoreWorkload
*   readallfields=true
*   readproportion=1.0
*   updateproportion=0
*   scanproportion=0
*   insertproportion=0.0
*   requestdistribution=zipfian
*   fieldcount=25


## Running YCSB

Command Line to load data:
```
./bin/ycsb load mongodb -s -P workloads/workload_small -threads 64 -p mongodb.url=mongodb://[username]:[password]@[aws.or.atlas.cluster.com]:27017/?replicaSet=rs0&w=majority
```

Command Line to run benchmark:

```
./bin/ycsb run mongodb -s -P workloads/workload_small -threads [64, 128, 256] -p mongodb.url=mongodb://[username]:[password]@[aws.or.atlas.cluster.com]:27017/?replicaSet=rs0&w=majority
```
