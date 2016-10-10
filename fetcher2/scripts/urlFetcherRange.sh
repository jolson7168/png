#!/bin/bash

echo `date +"%Y-%m-%d %H:%M:%S"` Starting urlFetcher job.... >../logs/`date +"%Y%m%d%H%M%S"`_urlFetcher.log 
python ../src/urlFetcher.py -c ../config/urlFetcher.conf --startDate 2016/10/09/00 --endDate 2016/10/09/23
echo `date +"%Y-%m-%d %H:%M:%S"` urlFetcher job complete! >>../logs/`date +"%Y%m%d%H%M%S"`_urlFetcher.log
