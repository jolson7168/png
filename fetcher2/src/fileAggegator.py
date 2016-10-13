import os
import sys
import time
import logging
import base64
import json
import gzip
import shutil

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

def getVals(dataObj):

    serverTimeOffset = 0
    if 'serverTimeOffset' in dataObj:
        serverTimeOffset = dataObj['serverTimeOffset']


    serverTime = 0
    if 'serverTime' in dataObj:
        serverTime = dataObj['serverTime']

    # Inside data one
    ts = 0
    if 'ts' in dataObj:
        ts = dataObj['ts']

    # Outside one
    ts1 = 0
    if 'ts1' in dataObj:                                      
        ts1 = dataObj['ts1']

    upsightSource = ''
    if 'upsightSource' in dataObj:
        upsightSource = dataObj['upsightSource']      

    sourceLineNumber = 0
    if 'sourceLineNumber' in dataObj:
        sourceLineNumber = dataObj['sourceLineNumber']
        

    return [serverTimeOffset, serverTime, ts, ts1, upsightSource, sourceLineNumber]

def isDupe(existing, candidate):
#offsetthreshold = 10
#ts1threshold = 1000     
#ts2threshold = 10        


    #if abs((existing[0] - candidate[0]) < int(cfg.get('dupes', 'offsetthreshold'))) or abs((existing[1] - candidate[1]) < int(cfg.get('dupes', 'ts1threshold'))) or abs((existing[2] - candidate[2]) < int(cfg.get('dupes', 'ts2threshold'))):
    if ((existing[0] - candidate[0]) == 0) or ((existing[2] - candidate[2]) == 0):
        return existing[4], existing[5]
    else:    
        return None, 0

def rezipFile(aFile, logger):
    mtu_masterSet = {}
    try:
        with gzip.open(aFile, 'rb') as infile:
            with open(aFile[:-3], 'wb') as outfile:
                for line in infile:
                    if ('"api": "android"' in line) or ('"api": "ios"' in line):
                        if '"messageType": "mtu"' in line:
                            if ('"id":' in line):
                                dataObj = json.loads(line)
                                dupe = False
                                thisKey = getVals(dataObj)
                                if dataObj["id"] in mtu_masterSet:
                                    for eachTransaction in mtu_masterSet[dataObj["id"]]:
                                        origFilename, origLine = isDupe(eachTransaction, thisKey)
                                        if origLine > 0:
                                            dupe = True
                                            break
                                    if dupe:
                                        dataObj['dupeFileName'] = origFileName
                                        dataObj['dupeLineNo'] = origLine
                                        line = json.dumps(dataObj)                           
                                    else:
                                        mtu_masterSet[dataObj["id"]].append(thisKey)
                                else:
                                    mtu_masterSet[dataObj["id"]]=[]
                                    mtu_masterSet[dataObj["id"]].append(thisKey)
                    outfile.write(line)
            outfile.close()
        infile.close()
        os.remove(aFile)        
        with open(aFile[:-3], 'rb') as infile2:
            with gzip.open(aFile, 'wb') as outfile2:
                for line2 in infile2:
                    outfile2.write(line2)
            outfile2.close()
        infile2.close()    
        os.remove(aFile[:-3])
    except Exception as e:
        msg = "Exception re-zipping files. Error: {0} ".format(e)
        logger.error(msg)

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
        cleanFileQueue = sqs.get_queue_by_name(QueueName=cfg.get('sqs','queueForCleanFiles'))
        metricsQueue = sqs.get_queue_by_name(QueueName=cfg.get('sqs','queueForMetrics'))

    s3_client = boto3.client('s3')
    s3 = boto3.resource('s3')

    done = False
    masterList = []
    while not done:
        messages = cleanFileQueue.receive_messages(WaitTimeSeconds = 10)
        if len(messages) == 0:
            done = True
        else:
            if messages[0].body not in masterList:
                masterList.append(messages[0].body)
                pathObj = json.loads(messages[0].body)
                tempLoc = cfg.get('store','temp')
                fileDate = pathObj['key'].split('_')[2][:8]
                targetName = tempLoc +'/'+fileDate+'.gz'
                tempKey = pathObj['key'].replace(pathObj['key'][:pathObj['key'].find('/')+1], '')
                thisFileName = tempLoc+'/'+tempKey
                s3_client.download_file(pathObj['bucket'], pathObj['key'], thisFileName)
                with open(targetName, 'ab') as outfile:
                    valid = True
                    try:
                        with gzip.open(thisFileName, 'rb') as infileTest:
                            test = infileTest.read()
                    except Exception as e:
                        pass
                        print('Error: {0} - {1}'.format(thisFileName, e))
                        valid = False
                    if valid:    
                        with open(thisFileName, 'rb') as infile:
                            try:
                                shutil.copyfileobj(infile, outfile)
                            except Exception as e:
                                pass
                                print('Error: {0} - {1}'.format(thisFileName, e))
                            infile.close()
                    outfile.close()
                os.remove(thisFileName)
            messages[0].delete()
    rezipFile(targetName, logger)
    dumpFileS3(targetName, s3, cfg.get('store','targetbucket'),logger)
    os.remove(targetName)



    # Clean up
    logger.info('Done! '+time.strftime("%Y%m%d%H%M%S")+'  ==============================')

if __name__ == "__main__":
    main(sys.argv[1:])

