# Fetcher
The purpose of this Python project is to automate file movement acquired via a 'wget' source to a target bucket on S3.

## Background
If a data feed acquireable solely via wget needs to be aggregated long term with other data sets stored on S3, this code can be used to faciltate movement. The program can be invoked with a date on the command line, and it will fetch the data files for that date. If two dates are provided, it will fetch all the source files between those dates at an hours' interval, inclusive. If no date is provided, it will return the previous hour's data.

An S3 bucket can be set in the configuration files, along with the AWS account's credentials.

## Installation
External packages needed:
```
boto3		(sudo pip install boto3)
```

Don't forget you need to have your AWS credentials in ~/.aws/credentials:
```
[Credentials]
aws_access_key_id = <your_access_key_here>
aws_secret_access_key = <your_secret_key_here>
```

### Config Files
There is a template config file in the ./config directory. Rename the file by dropping the '.template' part of the filename, and change the settings in the file to your preference.

## Operation
You can launch the app using the shell scripts in ./scripts/fetch. fetcherDefault.sh will gather up the source files sourced from the previous hour, fetcherRange can be used to gather up all the source files between two dates.

Logging goes into ./logs by default. 

## Issues
This is prototype code, and needs to be hardened prior to production. It is meant to serve as base functionality to test whether this operation can be ported to AWS's Lambda framework.

## TODOs
1. Port and test functionality under the Lambda framework.
2. Harden existing code for production
3. Add terraform and salt scripts to project to automatically provision machines, in the event #1 above fails or takes too much effort.
