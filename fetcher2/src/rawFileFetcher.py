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
import botocore

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
    desc = 'Execute rawFileFetcher'
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-c', '--config_file', default='../config/rawFileFetcher.conf',
                        help='configuration file name (*.ini format)')

    return parser

# How about some logging here?? And beef this up....
def dumpFileS3(aFile, conn, bucket, logger):
    try:
        folder = aFile.split('_')[2][:8]
        keyName = aFile.replace(cfg.get('store', 'temp'),folder)
        conn.Object(bucket, keyName).load()
        msg = "   Key {0} already exists in bucket {1}.".format(keyName, bucket)
        logger.error(msg)
    except botocore.exceptions.ClientError as e:
        pass
        if e.response['Error']['Code'] == "404":
            conn.meta.client.upload_file(aFile, bucket, keyName, ExtraArgs= {"Metadata": {"mode": "33204","uid": "1000","gid": "1000"}})
            try:
                os.remove(aFile)
            except OSError as e:
                pass
                msg = 'Error deleting OS file {0}'.format(aFile)
                logger.info(msg)

def getURL(logger, url, s3, bucketName, rawFileQueue):

    timeProc = time.time()
    timers={}
    results={}
    username = cfg.get('fetch', 'login')
    password = cfg.get('fetch', 'password')
    base64string = base64.encodestring('%s:%s' % (username, password))[:-1]
    try:
        logger.info("   Requesting {0}".format(url))
        req = urllib2.Request(url)
        req.add_header("Authorization", "Basic %s" % base64string)
        results['requested at'] =  datetime.strftime(datetime.utcnow(), '%Y-%m-%dT%H:%M:%S.%fZ')   
        timeStart = time.time()
        handle = urllib2.urlopen(req)
        timers['request time'] = round((time.time() - timeStart),3)
        if handle.getcode() != 200:
            retries = cfg.get('store', 'http_retries')
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
                raise IOError('timed out after {0} attempts with statuses {1}'.format(cfg.get('store', 'http_retries'), statuses))
            else:
                results['retries'] = current
                results['retry statuses'] = statuses
               
        results['request http response'] = handle.getcode()
        results['request time'] = timers['request time']

        back = len(url) - url.rfind("/") - 1
        fname = url[-1*back:]

        timeWrite = time.time()
        written = False
        compressedFile = StringIO.StringIO(handle.read())

        back = len(url) - url.rfind("/") - 1
        fname = url[-1*back:]

        fileName = cfg.get('store', 'temp')+'/'+fname
 
        with open(fileName, 'w') as outfile:
            outfile.write(compressedFile.read())
            outfile.close()
        results['written at'] = datetime.strftime(datetime.utcnow(), '%Y-%m-%dT%H:%M:%S.%fZ')
        written = True

        dumpFileS3(fileName, s3, bucketName, logger)
        val = {'bucket': bucketName, 'key': fileName.replace(cfg.get('store', 'temp'),fileName.split('_')[2][:8])}
        sendToQueue(rawFileQueue, json.dumps(val), logger)
        fileSize = 0
        compressedFile.seek(0, os.SEEK_END)
        fileSize = compressedFile.tell()
        results['size'] = fileSize

        if written:
            msg = "   Wrote {0} from {1}. Size: {2} Timers: {3}".format(fileName, url, fileSize, timers)
            logger.info(msg)  
    except IOError, e:
        msg =  "   {0} requesting file: {1}".format(e,url)
        logger.error(msg)
        results['status'] = msg
        pass

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


    # Get the logger going
    logger = initLog(time.strftime("%Y%m%d%H%M%S"))
    logger.info('Starting Run: '+time.strftime("%Y%m%d%H%M%S")+'  ==============================')
    
    doQueue = False
    if cfg.get('sqs','useSQS') == 'Y':
        # Set up the connection to SQS...
        doQueue = True
        sqs = boto3.resource('sqs')
        # Get the queue
        urlQueue = sqs.get_queue_by_name(QueueName=cfg.get('sqs','queueForURLs'))
        rawFileQueue = sqs.get_queue_by_name(QueueName=cfg.get('sqs','queueForRawFiles'))
        metricsQueue = sqs.get_queue_by_name(QueueName=cfg.get('sqs','queueForMetrics'))

    s3_client = boto3.client('s3')
    s3 = boto3.resource('s3')

    done = False
    while not done:
        messages = urlQueue.receive_messages(WaitTimeSeconds = 10)
        if len(messages) == 0:
            done = True
        else:
            results = getURL(logger, messages[0].body, s3,cfg.get('store','targetbucket'), rawFileQueue)
            sendToQueue(metricsQueue, json.dumps(results), logger)
            messages[0].delete()



    # Clean up
    logger.info('Done! '+time.strftime("%Y%m%d%H%M%S")+'  ==============================')

if __name__ == "__main__":
    main(sys.argv[1:])

