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

We built 6 test suites that make sense in this DBaaS context and they validate most of the features of the MongoDB 4.0, 4.2 or 4.4 API.

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
 * Version should either be 4.0, 4.2 or 4.4, depending on the suite you plan on running.

```sh
./0_docker-build.sh <version>
```

 * Create a MongoDB Atlas Cluster v4.0, 4.2 or v4.4, create an admin user and whitelist your public IP address. Find some help [here](https://www.youtube.com/watch?v=SIiVjgEDI7M&list=PL4RCxklHWZ9smTpR3hUdq53Su601yCPLj).
 * Collect the MongoDB Atlas connection string for the next command.
 * Run the 5 test suites.

```sh
./1_docker-run.sh 'mongodb+srv://<USER>:<PASSWORD>@mongodb-tests-abcde.mongodb.net' <version>
```

 * You can monitor by looking at the `results` folder.
 * Or you can `docker ps -a` and `docker logs -f <CONTAINER_NAME>` to check what is currently running.
 * All the results (JSON + STDOUT) are in the `results-<version>` folder.

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
./0_docker-build.sh <version>
./1_docker-run.sh 'mongodb://<USER>:<PASSWORD>@documentdb-tests.cluster-c23gwlgcxzrp.eu-west-1.docdb.amazonaws.com:27017/?replicaSet=rs0&ssl=true' <version>
```

Notes:
 1. Please copy and paste the DocumentDB URI from the AWS interface but confirm that it contains the same flags and none extra.
 2. The AWS certificate which is required to access any Document DB cluster is built into the Docker Image (see the Dockerfile for more details). There is no need to pass the "ssl_ca_certs" argument found in the connection string AWS provides.

 * When it's over, you can collect the logs and the JSON files in the `results-<version>` folder.

## Test Results

- Download Cosmos latest logs: [2021_03_04](https://developer-advocacy-public.s3-eu-west-1.amazonaws.com/MongoDB-DocumentDB-Tests/cosmosdb-44-2021_03_04.zip)
- Download Atlas & DocDB latest logs: [2020_11_13](https://developer-advocacy-public.s3-eu-west-1.amazonaws.com/MongoDB-DocumentDB-Tests/atlas-docdb-2020_11_13.zip)

### Online results

Available at:
* https://www.isdocumentdbreallymongodb.com/
* https://www.iscosmosdbreallymongodb.com/

### AWS DocumentDB v4.0 with MongoDB v4.0 Tests ─ Nov 13, 2020

| Tests Suite | Time execution (sec) | Number of tests | Succeeded | Skipped | Failed | Errored |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| Decimal | 3.37 | 13 | 7 | 0 | 6 | 0 |
| Json Schema | 6.67 | 24 | 0 | 0 | 24 | 0 |
| Change Streams | 606.01 | 20 | 0 | 0 | 20 | 0 |
| Aggregation | 899.83 | 166 | 76 | 0 | 90 | 0 |
| Core | 1789.21 | 849 | 348 | 0 | 501 | 0 |
| Transactions | 94.48 | 32 | 12 | 0| 20 | 0 |
| TOTAL | 3399.57 | 1104 | 443 | 0 | 661 | 0 |
| PERCENTAGES | | 100% | 40.13% | 0% | 59.87% | 0% |

### AWS DocumentDB v4.0 with MongoDB v4.4 Tests ─ May 20, 2021

| Tests Suite | Time execution (sec) | Number of tests | Succeeded | Skipped | Failed | Errored |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| Decimal | 1.95 | 15 | 9 | 0 | 6 | 0 |
| Json Schema | 4.39 | 26 | 2 | 0 | 24 | 0 |
| Change Streams | 184.02 | 22 | 2 | 0 | 20 | 0 |
| Aggregation | 811.16 | 236 | 82 | 0 | 154 | 0 |
| Core | 1857.68 | 891 | 348 | 0 | 543 | 0 |
| Transactions | 108.36 | 49 | 25 | 0 | 24 | 0 |
| TOTAL | 2967.56 | 1239 | 468 | 0 | 771 | 0 |
| PERCENTAGES | | 100% | 37.77% | 0% | 62.23% | 0% |

### Azure CosmosDB v4.0 with MongoDB v4.4 Tests ─ Mar 4, 2021

| Tests Suite | Time execution (sec) | Number of tests | Succeeded | Skipped | Failed | Errored |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| Decimal | 19.83 | 15 | 10 | 0 | 5 | 0 |
| Json Schema | 43.05 | 26 | 2 | 0 | 24 | 0 |
| Change Streams | 38.00 | 22 | 2 | 0 | 20 | 0 |
| Aggregation | 1625.44 | 236 | 88 | 0 | 148 | 0 |
| Core | 8531.10 | 891 | 303 | 0 | 588 | 0 |
| Transactions | 28.32 | 49 | 4 | 0 | 45 | 0 |
| TOTAL | 10285.74 | 1239 | 409 | 0 | 830 | 0 |
| PERCENTAGES | | 100% | 33.01% | 0% | 66.99% | 0% |

### MongoDB Atlas v4.0 with MongoDB v4.0 Tests ─ Nov 13, 2020

| Tests Suite | Time execution (sec) | Number of tests | Succeeded | Skipped | Failed | Errored |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| Decimal | 3.77 | 13 | 13 | 0 | 0 | 0 |
| Json Schema | 17.29  | 24 | 24 | 0 | 0 | 0 |
| Change Streams | 38.27 | 20 | 20 | 0 | 0 | 0 |
| Aggregation | 123.14 | 166 | 166 | 0 | 0 | 0 |
| Core | 457.96 | 849 | 849 | 0 | 0 | 0 |
| Transactions | 16.64 | 32 | 32 | 0 | 0 | 0 |
| TOTAL | 657.07 | 1104 | 1104 | 0 | 0 | 0 |
| PERCENTAGES | | 100% | 100% | 0% | 0% | 0% |

### MongoDB Atlas v4.2 with MongoDB v4.2 Tests ─ Nov 13, 2020

| Tests Suite | Time execution (sec) | Number of tests | Succeeded | Skipped | Failed | Errored |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| Decimal | 1.60 | 15 | 15 | 0 | 0 | 0 |
| Json Schema | 15.15  | 26 | 26 | 0 | 0 | 0 |
| Change Streams | 47.83 | 24 | 24 | 0 | 0 | 0 |
| Aggregation | 156.42 | 206 | 206 | 0 | 0 | 0 |
| Core | 435.17 | 879 | 879 | 0 | 0 | 0 |
| Transactions | 24.54 | 39 | 39 | 0 | 0 | 0 |
| TOTAL | 680.71 | 1189 | 1189 | 0 | 0 | 0 |
| PERCENTAGES | | 100% | 100% | 0% | 0% | 0% |

### MongoDB Atlas v4.4 with MongoDB v4.4 Tests ─ Nov 13, 2020

| Tests Suite | Time execution (sec) | Number of tests | Succeeded | Skipped | Failed | Errored |
| --- | :---: | :---: | :---: | :---: | :---: | :---: |
| Decimal | 1.83 | 15 | 15 | 0 | 0 | 0 |
| Json Schema | 15.32  | 26 | 26 | 0 | 0 | 0 |
| Change Streams | 40.81 | 22 | 22 | 0 | 0 | 0 |
| Aggregation | 223.77 | 234 | 234 | 0 | 0 | 0 |
| Core | 480.89 | 890 | 890 | 0 | 0 | 0 |
| Transactions | 34.67 | 49 | 49 | 0 | 0 | 0 |
| TOTAL | 797.29 | 1236 | 1236 | 0 | 0 | 0 |
| PERCENTAGES | | 100% | 100% | 0% | 0% | 0% |

## Pro tips

 * You just need to build the image once then you can run the tests as much as you want.
 * If you re-run the tests with the script `1_docker-run.sh`, the `results` folder is reset so please save it before if you want to keep the previous results.
 * If you want to run the test suites individually, you can. Read the `1_docker-run.sh` and run the docker command manually with the correct URI.

# Test Results Analyzer

Once you have finished running the test suite, the next step is to analyze and understand the failures from the run. 

The goal of this analysis is to allow us to quickly see what core MongoDB features are and are not supported by a given implementation. The analysis attempts to categorize failures into **UNSUPPORTED** and **FURTHER_INVESTIGATION** to help reduce the amount of debugging and allow us to quickly focus on more *interesting* failures.

There are about 1000 correctness tests that are run, and looking at the each failure one by one would take too much time. The analyzer lets you quickly sift through the various tests, categorize a large set of them, and then do further analysis as needed. At this time, the analyzer only supports classifying DocumentDB errors, as it relies on the specific error messages that service emits.

## Requirements

1. **Python 3.6+**
1. **A MongoDB instance**: The results are analyzed in MongoDB. If you're testing Atlas and another service side-by-side, you can use the Atlas instance you tested, or you can just use a free tier instance. **Note: This code will not work against DocumentDB (as of 2019-01-22), as it uses aggregation features that DocumentDB does not implement.** 
1. **[pymongo](https://pypi.org/project/pymongo/) package**: This driver is needed for connecting to MongoDB instance.
1. **[dnspython](http://www.dnspython.org/) package**: You will need this if using Atlas and `mongodb+srv` connection strings.

### Steps to Run
After running the docker tests for a given target platform the results will be stored
in the `./results` directory. The Python program can then be run by executing the following:

```
$ pip install -r requirements.txt
$ python analyze.py --mdburl "mongodb://MDB_URL" --platform EMULATION_PLATFORM_NAME
```

The MongoDB URL (`--mdburl`) should point to the Atlas instance you have configured to perform the data analysis with. The Platform (`--platform`) should be a string representing the MongoDB-compatible platform that has been tested. e.g. one of `atlas`, `cosmosdb`, `foundationdb`, or `documentdb`. There are no restrictions on what that string is but you should make it meaningful to avoid any confusion.

The `--mdburl` and `--platform` flag are required; optional parameters are:

1. **--drop**: Drop the previous database set by `--db`, default: **False**
2. **--version**: Database version run against, default: **v4.0**
3. **--db**: Database to store the results in, default: **results**
4. **--coll**: Collection to store results in, default: **correctness**
5. **--run**: Eun number, if we want to keep multiple results of the same platform, default: **1**
6. **--csv**: CSV file name to be output, default: **./results.csv**
7. **--csvfilter**: Filter to be applied to CSV processing, default: **{}**
8. **--rdir**: Results directory to use, default: **./results-4.0**

The `results.csv` will be generated for all data in the database (assuming an open filter). This CSV can be imported to the spreadsheet of your choice and further analysis can be done. The CSV file will contain the file, the suite, platform, version, run, status, reason, description.

## Further analysis
After digesting the results and finding the more interesting failures, we can look deeper by running those specific tests against the service with the following steps:

### 1. Clone MongoDB repository and check out version
The version checked out is the default version we tested with.
```
$ git clone --branch=v4.0 https://github.com/mongodb/mongo.git
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
2. Automate more of the analysis, it is very simplistic right now

# Authors & Contact

## Test runner

 * Maxime BEUGNET <maxime@mongodb.com> - Senior Developer Advocate @ MongoDB
 * Craig Homa <craig.homa@mongodb.com> - Market Intelligence Analyst
 * Greg McKeon - Former MongoDB employee - Competitive Analyst

## Results analyzer

* Shawn McCarthy - Former MongoDB employee - Developer Advocate
 
# LICENSE

This repository is published under the Server Side Public License (SSPL) v1. See individual files (LICENSE) for details.

