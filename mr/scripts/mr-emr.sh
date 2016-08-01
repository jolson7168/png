# pass in the bucket name, and the glob for the files you wish to aggregate
python ../src/mr01.py -r emr s3://$1/*20160725*
