import os
import sys
import time
import logging
import urllib2
import cStringIO
import StringIO
import base64
import json
import gzip

from urlparse import urlparse
from ConfigParser import RawConfigParser
from datetime import datetime
from datetime import timedelta

import boto3

cfg = RawConfigParser()

def currentDayStr():
    return time.strftime("%Y%m%d")

def currentTimeStr():
    return time.strftime("%H:%M:%S")

def initLog(rightNow):
    logger = logging.getLogger(cfg.get('logging', 'logName'))
    logPath=cfg.get('logging', 'logPath')
    logFilename=cfg.get('logging', 'logFileName')  
    hdlr = logging.FileHandler(logPath+rightNow+logFilename)
    formatter = logging.Formatter(cfg.get('logging', 'logFormat'),cfg.get('logging', 'logTimeFormat'))
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    logger.setLevel(logging.INFO)
    return logger

def getCmdLineParser():
    import argparse
    desc = 'Execute urlFetcher'
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-c', '--config_file', default='../config/urlFetcher.conf',
                        help='configuration file name (*.ini format)')

    parser.add_argument('-s', '--startDate', default = datetime.now(),
                        help='start pulling data from date YYYY/MM/DD/HH. Default: last hour')

    parser.add_argument('-e', '--endDate', default=None,
                        help='Optional. Pull data until date YYYY/MM/DD/HH ')
    return parser

def getFileList(logger, url, apiKey = None, thisDate = None):
    timeProc = time.time()
    timers={}

    results = {}
    if thisDate and apiKey:
        requestURL = '{0}/{1}/{2}'.format(url, apiKey, thisDate)
    else:
        requestURL = url

    logger.info("   Requesting {0}".format(requestURL))
    username = cfg.get('fetch', 'login')
    password = cfg.get('fetch', 'password')

    req = urllib2.Request(requestURL)

    base64string = base64.encodestring('%s:%s' % (username, password))[:-1]
    authheader =  "Basic %s" % base64string
    req.add_header("Authorization", authheader)
    results['tag'] = 'day request'
    results['requestURL'] = requestURL
    results['api'] = apiKey
    results['date'] = thisDate
    results['fileList']=[]
    try:
        x = 0
        timeStart = time.time()
        handle = urllib2.urlopen(req)
        requestTime = round((time.time() - timeStart),3)
        if handle.getcode() != 200:
            retries = cfg.get('fetch', 'retries')
            current = 1
            sleep = 10
            notFixed = True
            statuses = []
            while ((current <= retries) and (notFixed)):
                time.sleep(sleep)
                handle = urllib2.urlopen(req)
                if handle.getcode() == 200:
                    notFixed = False
                sleep = sleep + 10
                current = current + 1
                statuses.append(handle.getcode())
            if notFixed:                    
                raise IOError('timed out after {0} attempts with statuses {1}'.format(current, statuses))
            else:
                results['retries'] = current
                results['retry statuses'] = statuses
        for line in handle:
            x = x + 1
            if "<a href=" in line:
                quote1 = line.find('"')
                quote2 = line.find('"', quote1+1)
                results['fileList'].append({'url':line[quote1+1:quote2]})
    except IOError, e:
        requestTime = round((time.time() - timeStart),3)
        thisResult = {'line':x, 'error':'True', 'message':e}        
        results['fileList'].append(thisResult)
        pass
    results['executionTime'] = round((time.time() - timeProc),3)
    results['httpStatus'] = handle.getcode()
    msg = "   Finished {0} ".format(requestURL)
    logger.info(msg)
    return results






def sendToQueue(queueName, message, logger):
    timeStart = time.time()
    response = queueName.send_message(MessageBody=message)
    requestTime = round((time.time() - timeStart),3)
    msg = 'Wrote to queue {0}. Response MessageID: {1}. Time: {2}'.format(queueName, response.get('MessageId'), requestTime)
    logger.info(msg)


def main(argv):

    # Overhead to manage command line opts and config file
    p = getCmdLineParser()
    args = p.parse_args()

    cfg.read(args.config_file)

    if args.endDate:  
        endDate = datetime.strptime(args.endDate, "%Y/%m/%d/%H")
        startDate =  datetime.strptime(args.startDate, "%Y/%m/%d/%H")
    else:
        if cfg.has_option('fetch','startoffset'):
            startOffset = int(cfg.get('fetch','startoffset'))
        else:
            startOffset = -1
        if cfg.has_option('fetch','endoffset'):
            endOffset = int(cfg.get('fetch','endoffset'))
        else:
            endOffset = -1

        startDate =  (args.startDate + timedelta(hours = startOffset)).replace(minute=0, second=0, microsecond=0)
        endDate = (args.startDate + timedelta(hours = endOffset)).replace(minute=0, second=0, microsecond=0)

    apiKeys = {}
    apiKeyMap = cfg.get('fetch','apiKeys').split('|')
    for pair in apiKeyMap:
        apiKeys[pair.split(',')[0]] = pair.split(',')[1]

    # Get the logger going
    logger = initLog(time.strftime("%Y%m%d%H%M%S"))
    logger.info('Starting Run: '+time.strftime("%Y%m%d%H%M%S")+'  ==============================')
    if endDate:
        msg = "Running from {0} to {1}....".format(startDate.strftime('%Y/%m/%d %H:00'), endDate.strftime('%Y/%m/%d %H:00'))
    else:
        msg = "Running {0}....".format(startDate.strftime('%Y/%m/%d %H:00'))
    logger.info(msg)
    

    doQueue = False
    if cfg.get('sqs','useSQS') == 'Y':
        # Set up the connection to SQS...
        doQueue = True
        sqs = boto3.resource('sqs')
        # Get the queue
        urlQueue = sqs.get_queue_by_name(QueueName=cfg.get('sqs','queueForURLs'))
        metricsQueue = sqs.get_queue_by_name(QueueName=cfg.get('sqs','queueForMetrics'))



    uploadList = []
    currentDate = startDate
    while currentDate <= endDate:
        thisDate = currentDate.strftime('%Y/%m/%d/%H')
        logger.info("Fetching day: {0}".format(thisDate))
        results = []
        manifest = []
        fileList = []
        for eachKey in apiKeys:
            results = getFileList(logger, url = cfg.get('fetch', 'url'), apiKey = eachKey, thisDate = thisDate)
            if doQueue:
                sendToQueue(metricsQueue, json.dumps(results), logger)
                if 'fileList' in results:
                    for result in results['fileList']:
                        sendToQueue(urlQueue, result['url'], logger)         
        currentDate = currentDate + timedelta(hours = 1)

        
    # Clean up
    logger.info('Done! '+time.strftime("%Y%m%d%H%M%S")+'  ==============================')

if __name__ == "__main__":
    main(sys.argv[1:])

