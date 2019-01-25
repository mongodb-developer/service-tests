# The MongoDB API Tester
This repository is used to perform correctness testing against a MongoDB API endpoint and analyze the results. It also contains intructions for how to run performance tests.

## Performance Testing

* Use [these intructions to run YCSB](https://github.com/mongodb-developer/service-tests/blob/master/ycsb.md)
* Use [these instructions to run Socialite](https://github.com/mongodb-developer/service-tests/blob/master/socialite.md)

## Correctness Testing
There are two main components to the correctness tests:

* The **test runner**, distributed as a Docker image that checks out the MongoDB repository and runs API correctness  tests
* The **results analyzer**, a Python script that loads the raw output from the test into MongoDB, analyzes it to classify failures as either unsupported features or potential bugs, and outputs a CSV file for easy review. 

# Test Runner Docker
The Docker image in this repository can run API acceptance tests against a service implementing the MongoDB API. Everything you need to run these tests is built into it.

There are only 3 things you need to run this project:

 * [Docker](https://docs.docker.com/install/),
 * Git (or you can just download this repo as a zip file from Github),
 * A bash shell.

The tests this harness runs are the subset of official MongoDB correctness tests that treat the system under test as a black box, without relying on fixtures or assumptions about the server's internal state.

We built 5 test suites that make sense in this DBaaS context and they validate most of the features of the MongoDB 3.6 API.

## Recommended infrastructure

### MongoDB Atlas

I ran the tests in 18 minutes with:
 * MongoDB Atlas M30 Cluster with 1200 IOPS.
 * The tests were running from a PC with I7 + 16Go RAM + optical fiber connection.

### AWS DocumentDB

I ran the tests in 41 minutes in the Amazon Cloud with:
 * DocumentDB db.r4.large with 3 instances.
 * EC2 t3.xlarge with image "Amazon Linux AMI 2018.03.0 (HVM), SSD Volume Type - ami-08935252a36e25f85".

Note: If you provision a server other than Amazon Linux or Ubuntu, you will have to adapt the setup scripts accordingly to install Docker and Git.

## Instructions to run the tests

### On MongoDB Atlas

 * Build the image - it's a bit long (2-3 minutes) so go get that coffee, it's on me!

```sh
./0_docker-build.sh
```

 * Create a MongoDB Atlas Cluster v3.6, create an admin user and whitelist your public IP address. Find some help [here](https://www.youtube.com/watch?v=SIiVjgEDI7M&list=PL4RCxklHWZ9smTpR3hUdq53Su601yCPLj).
 * Collect the MongoDB Atlas connection string for the next command.
 * Run the 5 test suites.

```sh
./1_docker-run.sh mongodb+srv://<USER>:<PASSWORD>@mongodb-tests-abcde.mongodb.net
```

 * You can monitor by looking at the `results` folder.
 * Or you can `docker ps -a` and `docker logs -f <CONTAINER_NAME>` to check what is currently running.
 * When it's done, you just have to collect the logs in the standard output of each containers

```sh
./2_docker-collect-logs-rm.sh
```

 * All the results (JSON + STDOUT) are in the results folder.

### On AWS DocumentDB

 * Setup a DocumentDB Cluster and an EC2 server using the instructions provided [here](https://docs.aws.amazon.com/documentdb/latest/developerguide/getting-started.html).
 * Step 1 will help you install the DocumentDB Cluster and setup the security aspect.
 * Step 2 will help you setup an EC2 server in the same VPC because it's currently not possible to connect to DocumentDB from the outside of the VPC and AWS.
 * The step 3 is optional as we are going to use the MongoDB Shell from our Docker image but you can still go through step 3 to validate your setup and that you are allowed to connect from this EC2 server.
   - The cluster and EC2 client should be in the same Security Group.
   - The client should be able to communicate over TCP port 27017 with the cluster.
 * Once you are sure that you can connect from this EC2 server to DocumentDB execute this script on the EC2 client:

For the Amazon Linux image:
```sh
sudo yum update -y
sudo yum install docker git -y
sudo service docker start
sudo usermod -a -G docker ec2-user
```

For the Ubuntu image:
```sh
sudo apt-get update
sudo apt-get upgrade -y
sudo apt-get install docker git -y
sudo service docker start
sudo usermod -a -G docker ubuntu
```

 * For that last command to take effect, you need to log out and log in again to your EC2 server.
 * Then run this script.

```sh
git clone https://github.com/mongodb-developer/service-tests.git
cd ./service-tests
./0_docker-build.sh
./1_docker-run.sh 'mongodb://<USER>:<PASSWORD>@documentdb-tests.cluster-c23gwlgcxzrp.eu-west-1.docdb.amazonaws.com:27017/?replicaSet=rs0&ssl=true'
./2_docker-collect-logs-rm.sh
```

Notes:
 1. Please copy and paste the DocumentDB URI from the AWS interface but fix it so it looks exactly like the one above.
 2. The AWS certificate which is required to access any Document DB cluster is built into the Docker Image (see the Dockerfile for more details).

 * When it's over, you can collect the logs and the JSON files in the `results` folder.

## Test Results

These results were produced on Jan 16, 2019 around 9pm GMT.

### AWS DocumentDB v3.6 - 1.0.200710

The version 3.6 is the version you can see on the AWS website. The version 1.0.200710 was extracted from `db.serverStatus().version`.

| Tests Suite | Time execution (sec) | Number of tests | Succeeded | Skipped | Failed | Errored |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| Decimal | 4.43 | 13 | 7 | 0 | 6 | 0 |
| Json Schema | 8.30 | 24 | 0 | 0 | 24 | 0 |
| Change Streams | 2.77 | 9 | 0 | 0 | 9 | 0 |
| Aggregation | 861.16 | 149 | 45 | 0 | 104 | 0 |
| Core | 1459.58 | 833 | 325 | 0 | 508 | 0 |
| TOTAL | 2336.24 | 1028 | 377 | 0 | 651 | 0 |
| PERCENTAGES | | 100% | 36.67% | 0% | 63.33% | 0% |

[DOWNLOAD THE TEST RESULTS](https://s3-eu-west-1.amazonaws.com/developer-advocacy-public/MongoDB-DocumentDB-Tests/AWS-DocumentDB-Results-v369-2019-01-16.zip).

### MongoDB Atlas v3.6.9

| Tests Suite | Time execution (sec) | Number of tests | Succeeded | Skipped | Failed | Errored |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| Decimal | 4.68 | 13 | 13 | 0 | 0 | 0 |
| Json Schema | 35.73 | 24 | 24 | 0 | 0 | 0 |
| Change Streams | 14.64 | 9 | 9 | 0 | 0 | 0 |
| Aggregation | 329.13 | 149 | 149 | 0 | 0 | 0 |
| Core | 1260.79 | 833 | 833 | 0 | 0 | 0 |
| TOTAL | 1644.97 | 1028 | 1028 | 0 | 0 | 0 |
| PERCENTAGES | | 100% | 100% | 0% | 0% | 0% |

[DOWNLOAD THE TEST RESULTS](https://s3-eu-west-1.amazonaws.com/developer-advocacy-public/MongoDB-DocumentDB-Tests/MongoDB_Atlas_Results_v369_2019_01_16.zip).

## Pro tips

 * You just need to build the image once then you can run the tests as much as you want.
 * If you re-run the tests with the script `1_docker-run.sh`, the `results` folder is reset so please save it before if you want to keep the previous results.
 * If you want to run the test suites individually, you can. Read the `1_docker-run.sh` and run the docker command manually with the correct URI.

# Test Results Analyzer
Once you have finished running the test suite, the next step is to analyze and understand the failures from the run. 

The goal of this analysis is to allow us to quickly see what core MongoDB features are and are not supported by a given implementation. The analysis attempts to categorize failures into **UNSUPPORTED** and **FURTHER_INVESTIGATION** to help reduce the amount of debugging and allow us to quickly focus on more *interesting* failures.

There are about 1000 correctness tests that are run, and looking at the each failure one by one would take too much time. The analyzer lets you quickly sift through the various tests, categorize a large set of them, and then do further analysis as needed. At this time, the analyzer only supports classifying DocumentDB errors, as it relies on the specific error messages that service emits.

## Requirements

1. **Python 3.6**
1. **A MongoDB instance**: The results are analyzed in MongoDB. If you're testing Atlas and another service side-by-side, you can use the Atlas instance you tested, or you can just use a free tier instance. **Note: This code will not work against DocumentDB (as of 2019-01-22), as it uses aggregation features that DocumentDB does not implement.** 
1. **Pymongo**: This driver is needed for connecting to MongoDB instance.
1. **[dnspython](http://www.dnspython.org/)**: You will need this if using Atlas and `mongodb+srv` connection strings.

### Steps to Run
After running the docker tests for a given target platform the results will be stored
in the `./results` directory. The Python program can then be run by executing the following:

```
$ python analyze.py --mdburl "mongodb://MDB_URL" --platform EMULATION_PLATFORM_NAME
```

The MongoDB URL (`--mdburl`) should point to the Atlas instance you have configured to perform the data analysis with. The Platform (`--platform`) should be a string representing the MongoDB-compatible platform that has been tested. e.g. one of `atlas`, `cosmosdb`, `foundationdb`, or `documentdb`. There are no restrictions on what that string is but you should make it meaningful to avoid any confusion.

The `--mdburl` and `--platform` flag are required; optional parameters are:

1. **--drop**: Drop the previous database set by `--db`, default: **False**
2. **--version**: Database version run against, default: **v3.6.9**
3. **--db**: Database to store the results in, default: **results**
4. **--coll**: Collection to store results in, default: **correctness**
5. **--run**: Eun number, if we want to keep multiple results of the same platform, default: **1**
6. **--csv**: CSV file name to be output, default: **./results.csv**
7. **--csvfilter**: Filter to be applied to CSV processing, default: **{}**
8. **--rdir**: Results directory to use, default: **./results**

The `results.csv` will be generated for all data in the database (assuming an open filter). This CSV can be imported to the spreadsheet of your choice and further analysis can be done. The CSV file will contain the file, the suite, platform, version, run, status, reason, description.

## Further analysis
After digesting the results and finding the more interesting failures, we can look deeper by running those specific tests against the service with the following steps:

### 1. Clone MongoDB repository and check out version
The version checked out is the default version we tested with.
```
$ git clone --branch=v3.6.9-dbaas-testing https://github.com/mongodb/mongo.git
$ cd ./mongo
```

### 2. Identify and run test
Choose an 'interesting' test to run, and execute it using the following command:
```
$ /path/to/mongo PLATFORM_URL path/to/test.js
```
The `/path/to/mongo` is the shell used to execute the javascript, the `PLATFORM_URL` is the emulation platform that the failure was recorded on, the `path/to/test.js` is the test file that failed.

The mongo shell will kick off the script and the results will be written to stdout, with a stack trace (if there is a failure). From there you can investigate the test file, copy it, change it, etc.

Should you find anything glaringly problematic with the tests, please reach out and let us know.

## TODO
1. Document Layer (FoundationDB): Run correctness results through `analyze.py`
2. CosmosDB: Run correctness results through `analyze.py`
3. Automate more of the analysis, it is very simplistic right now

# Authors & Contact

## Test runner

 * Maxime BEUGNET
 * Developer Advocate @ MongoDB
 * maxime@mongodb.com

## Results analyzer

* Shawn McCarthy
* Developer Advocate @ MongoDB
* shawn.mccarthy@mongodb.com
 
 # LICENSE

 This repository is published under the Server Side Public License (SSPL) v1. See individual files (LICENSE) for details.
