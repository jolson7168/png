import os
import sys
import time
import logging
import urllib
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
import collections

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
    desc = 'Execute fileCleaner'
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-c', '--config_file', default='../config/fileCleaner.conf',
                        help='configuration file name (*.ini format)')

    return parser

# How about some logging here?? And beef this up....
def dumpFileS3(aFile, conn, bucket, logger):
    try:
        keyName = aFile.replace(cfg.get('store', 'temp')+'/','')
        conn.Object(bucket, keyName).load()
        msg = "   Key {0} already exists in bucket {1}.".format(keyName, bucket)
        logger.error(msg)
        os.remove(aFile)
    except botocore.exceptions.ClientError as e:
        pass
        if e.response['Error']['Code'] == "404":
            conn.meta.client.upload_file(aFile, bucket, keyName, ExtraArgs= {"Metadata": {"mode": "33204","uid": "1000","gid": "1000"}})
            os.remove(aFile)



def sendToQueue(queueName, message, logger):
    timeStart = time.time()
    response = queueName.send_message(MessageBody=message)
    requestTime = round((time.time() - timeStart),3)
    msg = 'Wrote to queue {0}. Response MessageID: {1}. Time: {2}'.format(queueName, response.get('MessageId'), requestTime)
    logger.info(msg)

def get_between(aline, delim1, delim2):
    try:
        if delim2 == 'EOL':
            start = aline.index( delim1 ) + len( delim1 )
            return aline[start:]
        else:        
            start = aline.index( delim1 ) + len( delim1 )
            end = aline.index( delim2, start )
            return aline[start:end]
    except Exception as e:
        msg = 'Expecting delimiters: {0} and {1} in line: {2}'.format(delim1, delim2, aline)
        raise KeyError(msg)


def decode_data(line):
    try:
        retval = base64.b64decode(urllib.unquote(line).decode('utf8'))
        return retval
    except Exception as e:
        msg = 'Problem decoding {0} from base64'.format(line)
        raise TypeError(msg)

def cleanLine(line, timeStart, fileName, lineNo):
    retval = {}
    try:
        if 'data=' in line:
            try:
                retval = json.loads(decode_data(get_between(line, 'data=', '&')))
            except Exception as e:
                pass 
                retval['status'] = 'Error decoding data'
                retval['data'] = get_between(line, 'data=', '&')

        id1 = None
        if '&s=' in line:
            id1 = get_between(line, '&s=', '&')
        elif ' s=' in line:
            id1 = get_between(line, ' s=', '&')
        else:
            pass

        if id1:
            if id1[:3] == '109':
                id1 = int(id1[3:])
            else:
                id1 = int(id1)
            retval['id'] = id1

        n = None
        if '&n=' in line:
            n = get_between(line, '&n=', '&')
        elif ' n=' in line:
            n = get_between(line, ' n=', '&')
        else:
            pass
        if n:
            retval['event'] = n

        st1 = None
        if '&st1=' in line:
            st1 = get_between(line, '&st1=', '&')
        elif ' st1=' in line:
            st1 = get_between(line, ' st1=', '&')
        else:
            pass
        if st1:
            retval['st1'] = st1

        st2 = None
        if '&st2=' in line:
            st2 = get_between(line, '&st2=', '&')
        elif ' st2=' in line:
            st2 = get_between(line, ' st2=', '&')
        else:
            pass
        if st2:
            retval['st2'] = st2

        st3 = None
        if '&st3=' in line:
            st3 = get_between(line, '&st3=', '&')
        elif ' st3=' in line:
            st3 = get_between(line, ' st3=', '&')
        else:
            pass
        if st3:
            retval['st3'] = st3

        sdk = None
        if '&sdk=' in line:
            sdk = get_between(line, '&sdk=', '&')
        elif ' sdk3=' in line:
            sdk = get_between(line, ' sdk=', '&')
        else:
            pass
        if sdk:
            retval['sdk'] = sdk

        ts = None
        if '&ts=' in line:
            ts = get_between(line, '&ts=', '&')
        elif ' ts=' in line:
            ts = get_between(line, ' ts=', '&')
        else:
            pass
        if ts:
            retval['ts1'] = int(ts)

        v = None
        if '&v=' in line:
            v = get_between(line, '&v=', '&')
        elif ' v=' in line:
            v = get_between(line, ' v=', '&')
        else:
            pass
        if v:
            retval['v'] = v

        kt_v = None
        if '&kt_v=' in line:
            kt_v = get_between(line, '&kt_v=', '&')
        elif ' kt_v=' in line:
            kt_v = get_between(line, ' kt_v=', '&')
        else:
            pass
        if kt_v:
            retval['kt_v'] = kt_v


        scheme = None
        if 'scheme=' in line:
            scheme = get_between(line, 'scheme=', 'EOL').replace('\n','')
        else:
            pass
        if scheme:
            retval['scheme'] = scheme


        offset = int(line.split(' ')[0])
        retval['serverTime'] = timeStart+offset

        event = line.split(' ')[1]
        retval['messageType'] = event

    except Exception as e:
        msg = 'Error decoding line {0} on line {1} in fileName {2}, error: {3}'.format(line, lineNo, fileName, e)
        raise TypeError(msg)
 
    return retval

def getDate(fname):
    return fname.split('_')[2][:8]

def cleanFile(logger, s3_client, s3, pathObj, targetBucket, targetQueue, tempLoc, apiKeys):

    masterSet = []
    tempFileName = tempLoc+'/'+pathObj['key']
    s3_client.download_file(pathObj['bucket'], pathObj['key'], tempFileName)
    with gzip.open(tempFileName.replace('.gz','_cleaned.gz'), 'wb') as outFile:
        with gzip.open(tempFileName, 'rb') as inFile:
            current = 0
            for line in inFile:
                dupe = False
                if 's=' in line:
                    firstSpace = line.find(' ')
                    line2 = line[firstSpace:]
                    if line2 not in masterSet:
                        masterSet.append(line2)
                    else:
                        dupe = True

                current = current + 1
                if current == 2:
                    api = line.split(' ')[1]
                    offset = int(line.split(' ')[3])
                elif line[0] == '#':
                    pass
                else:
                    cleanedObj = cleanLine(line, offset, pathObj['key'], current)
                    cleanedObj['upsightSource'] = pathObj['key']
                    cleanedObj['sourceLineNumber'] = current
                    cleanedObj['api'] = apiKeys[api]
                    cleanedObj['requestDate'] = getDate(pathObj['key'])
                    if not dupe:
                        outFile.write(json.dumps(cleanedObj)+'\n')
            inFile.close()
            try:
                os.remove(tempFileName)
            except OSError as e:
                pass
                msg = 'Error deleting OS file {0}'.format(tempFileName)
                logger.info(msg)
    outFile.close()

    valid = False
    try:
        with gzip.open(tempFileName.replace('.gz','_cleaned.gz'), 'rb') as infileTest:
            test = infileTest.read()
        infileTest.close()
        valid = True    
    except Exception as e:
        pass

    if valid:
        dumpFileS3(tempFileName.replace('.gz','_cleaned.gz'), s3, targetBucket, logger)
        targetObj = {"bucket": targetBucket,"key": tempFileName.replace('.gz','_cleaned.gz').replace(tempLoc+'/','')}
        sendToQueue(targetQueue, json.dumps(targetObj), logger)
    return valid

def main(argv):

    # Overhead to manage command line opts and config file
    p = getCmdLineParser()
    args = p.parse_args()

    cfg.read(args.config_file)


    # Get the logger going
    logger = initLog(time.strftime("%Y%m%d%H%M%S"))
    logger.info('Starting Run: '+time.strftime("%Y%m%d%H%M%S")+'  ==============================')

    apiKeys = {}
    apiKeyMap = cfg.get('fetch','apiKeys').split('|')
    for pair in apiKeyMap:
        apiKeys[pair.split(',')[0]] = pair.split(',')[1]
    
    doQueue = False
    if cfg.get('sqs','useSQS') == 'Y':
        # Set up the connection to SQS...
        doQueue = True
        sqs = boto3.resource('sqs')
        # Get the queue
        rawFileQueue = sqs.get_queue_by_name(QueueName=cfg.get('sqs','queueForRawFiles'))
        cleanFileQueue = sqs.get_queue_by_name(QueueName=cfg.get('sqs','queueForCleanFiles'))
        metricsQueue = sqs.get_queue_by_name(QueueName=cfg.get('sqs','queueForMetrics'))

    s3_client = boto3.client('s3')
    s3 = boto3.resource('s3')

    done = False
    while not done:
        messages = rawFileQueue.receive_messages(WaitTimeSeconds = 10)
        if len(messages) == 0:
            done = True
        else:
            results = cleanFile(logger, s3_client, s3, json.loads(messages[0].body), cfg.get('store','targetbucket'), cleanFileQueue, cfg.get('store','temp'), apiKeys)
            if results:
                sendToQueue(metricsQueue, json.dumps(results), logger)
                messages[0].delete()
            else:
                logger.info('Error process file {0} - did not pass gzip test'.format(json.loads(messages[0].body)))
                


    # Clean up
    logger.info('Done! '+time.strftime("%Y%m%d%H%M%S")+'  ==============================')

if __name__ == "__main__":
    main(sys.argv[1:])

