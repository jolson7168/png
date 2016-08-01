# Map Reduce
The purpose of this Python project is to automate the loading of an Amazon Redshift schema with data output from a elastic map reduce job

## Background
This project is to test if a large number of compressed, uuencoded data files stored in S3 can be selectively aggregated using elastic map reduce (emr), and the results of the aggregation written to Red Shift.
Ideally, the map reduce functionality will be implemented in Python, and executed on a AWS emr cluster that is set up specifically for the aggregation. Upon completion, the emr cluster is destroyed.

The target workflow is aggregation of files in S3 once a day.

## Installation
External packages needed:
```
boto3		(sudo pip install boto3)
mrjob       (sudo pip install mrjob)
```

Don't forget you need to have your AWS credentials in ~/.aws/credentials:
```
[Credentials]
aws_access_key_id = <your_access_key_here>
aws_secret_access_key = <your_secret_key_here>
```

### Config Files
You also need to define ~/.mrjob.conf. A template file can be found in this project under ./config. Insert your values into this file, and install at ~/.mrjob.conf

## Operation
You can launch a map reduce job using the shell scripts in ./scripts. mr-local.sh will run a map reduce job on the local machine. This is for debugging. In this case, the input files
need to be stored locally, with the path identified in mr-local.sh.

mr-emr.sh can be used to deploy a job on AWS. In this case, the input files should be stored in S3, and the bucket identified in mr-emr.sh.

Logging goes into S3 by default, as dictated by the ~/.mrjob.conf

## Issues
This is prototype code, and needs to be hardened prior to production. 

## TODOs
1. Write output of map reduce job to Red Shift schema
2. Harden existing code for production
3. Polish logging
