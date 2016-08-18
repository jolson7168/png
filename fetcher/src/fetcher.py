import os
import sys
import time
import logging
import urllib2
import cStringIO
import StringIO
import base64
import json


from urlparse import urlparse
from ConfigParser import RawConfigParser
from datetime import datetime
from datetime import timedelta

import boto3
import botocore
import psycopg2

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
    desc = 'Execute fetcher'
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-c', '--config_file', default='../config/fetcher.conf',
                        help='configuration file name (*.ini format)')

    parser.add_argument('-s', '--startDate', default = datetime.now(),
                        help='start pulling data from date YYYY/MM/DD/HH. Default: last hour')

    parser.add_argument('-e', '--endDate', default=None,
                        help='Optional. Pull data until date YYYY/MM/DD/HH ')
    return parser

def executeManifest(logger, manifest, apiKeys):

    timeProc = time.time()
    timers={}
    fileList = []
    username = cfg.get('fetch', 'login')
    password = cfg.get('fetch', 'password')
    base64string = base64.encodestring('%s:%s' % (username, password))[:-1]
    logger.info("Size of manifest: {0}".format(len(manifest)))
    for item in manifest:
        for eachURL in item['fileList']:
            try:
                logger.info("   Requesting {0}".format(eachURL['url']))
                req = urllib2.Request(eachURL['url'])
                req.add_header("Authorization", "Basic %s" % base64string)
                eachURL['requested at'] =  datetime.strftime(datetime.utcnow(), '%Y-%m-%dT%H:%M:%S.%fZ')   
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
                        eachURL['retries'] = current
                        eachURL['retry statuses'] = statuses
               
                eachURL['request http response'] = handle.getcode()
                eachURL['request time'] = timers['request time']
    
                back = len(eachURL['url']) - eachURL['url'].rfind("/") - 1
                fname = eachURL['url'][-1*back:]

                timeWrite = time.time()
                written = False
                compressedFile = StringIO.StringIO(handle.read())

                fileDate = fname.split("_")[2][:8]
                apiKey = apiKeys[fname.split("_")[1]]
                
                fileName = cfg.get('store', 'temp')+'/'+fileDate+'_'+apiKey+'.gz'
                fileList.append(fileName)          
                with open(fileName, 'a') as outfile:
                    outfile.write(compressedFile.read())
                    outfile.close()
                    eachURL['written at'] = datetime.strftime(datetime.utcnow(), '%Y-%m-%dT%H:%M:%S.%fZ')
                    written = True

                compressedFile.seek(0, os.SEEK_END)
                fileSize = compressedFile.tell()
                eachURL['size'] = fileSize

                if written:
                    msg = "   Wrote {0} from {1}. Size: {2} Timers: {3}".format(fileName, eachURL['url'], fileSize, timers)
                    logger.info(msg)  

            except IOError, e:
                msg =  "   {0} requesting file: {1}".format(e,eachURL['url'])
                logger.error(msg)
                eachURL['status'] = msg
                pass

    return manifest, fileList

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
    results['requestURL'] = requestURL
    results['api'] = apiKey
    results['date'] = thisDate
    results['fileList']=[]
    try:
        x = 0
        timeStart = time.time()
        handle = urllib2.urlopen(req)
        requestTime = round((time.time() - timeStart),3)
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

def manifestToDb(results):
    writeList = []
    for item1 in results:
        if "fileList" in item1:
            for item in item1["fileList"]:            
                if 'url' in item:
                    url = item['url']
                    #2016080704
                    datadate = item['url'].split('_')[3]
                    fixedDataDate = datetime.strptime(datadate, "%Y%m%d%H")
                    datadateWrite = datetime.strftime(fixedDataDate, "%Y-%m-%d %H:%M:%S")
                    apikey = item['url'].split('_')[2]
                    key1 =item['url'].split('_')[4].split('.')[0]
                    key2 = item['url'].split('_')[4].split('.')[1]
                    key3 = item['url'].split('_')[4].split('.')[2]
                    key4 = item['url'].split('_')[4].split('.')[3]
                    if 'status' in item:
                        status = item['status']
                    else:
                        status = None 
                    # "2016-08-07T17:29:21.274678Z" 
                    if 'requested at' in item:
                        requestedAt = item['requested at']
                        requestedAtS = datetime.strptime(requestedAt, "%Y-%m-%dT%H:%M:%S.%fZ")
                        requestedAtWrite = datetime.strftime(requestedAtS, "%Y-%m-%d %H:%M:%S")
                    else:
                        requestedAt = None

                    if 'request time' in item:
                        requestTime = item['request time']
                    else:
                        requestTime = None
                    if 'request http response' in item:
                        requestResponse = item['request http response']
                    else:   
                        requestResponse = None

                    if 'size' in item:
                        size = item['size']
                    else:
                        size = None

                    t1 = (datadateWrite,url,apikey,key1,key2,key3,key4,requestedAtWrite,requestTime,size,requestResponse, status,)
                    writeList.append(t1)

    conn_string = "dbname='{0}' port='{1}' user='{2}' password='{3}' host='{4}'".format(cfg.get('database', 'name'), cfg.get('database', 'port'), cfg.get('database', 'user'), cfg.get('database', 'password'), cfg.get('database', 'host'))
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in writeList)
    sql = "INSERT INTO {0} VALUES ".format(cfg.get('database', 'table')) + args_str
    cur.execute("INSERT INTO {0} VALUES ".format(cfg.get('database', 'table')) + args_str) 
    conn.commit()
    cur.close()                
    conn.close()
    return True

# How about some logging here??
def dumpFilesS3(fileList, conn, bucket, logger):
    for eachFile in fileList:
        try:
            keyName = eachFile.replace(cfg.get('store', 'temp')+'/','')
            #timeStart = time.time()
            conn.Object(bucket, keyName).load()
            #timers['duplicate time'] = round((time.time() - timeStart),3)
            msg = "   Key {0} all ready exists in bucket {1}.".format(keyName, bucket)
            logger.error(msg)
        except botocore.exceptions.ClientError as e:
            pass
            #timers['duplicate time'] = round((time.time() - timeStart),3)
            if e.response['Error']['Code'] == "404":
                data = open(eachFile, 'r')
                conn.Bucket(bucket).put_object(Key=keyName, Body = data)

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
        startDate =  (args.startDate + timedelta(hours = startOffset)).replace(minute=0, second=0, microsecond=0)
        endDate = (args.startDate + timedelta(hours = -1)).replace(minute=0, second=0, microsecond=0)

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
    
    currentDate = startDate
    while currentDate <= endDate:
        thisDate = currentDate.strftime('%Y/%m/%d/%H')
        logger.info("Fetching day: {0}".format(thisDate))
        results = []
        manifest = []
        fileList = []
        for eachKey in apiKeys:
            results = getFileList(logger, url = cfg.get('fetch', 'url'), apiKey = eachKey, thisDate = thisDate)
            manifest.append(results)

        fetchFilesResults, fileList = executeManifest(logger, manifest, apiKeys)
        

        if cfg.get('store', 'storage') == 'S3':
            s3_client = boto3.client(
                's3'        )
            s3 = boto3.resource('s3')
            dumpFilesS3(fileList, s3, cfg.get('store', 'location'), logger)         


        if cfg.get('database', 'archiveResults') == 'Y':
            final = manifestToDb(fetchFilesResults)
        currentDate = currentDate + timedelta(hours = 1)

    # Clean up
    logger.info('Done! '+time.strftime("%Y%m%d%H%M%S")+'  ==============================')

if __name__ == "__main__":
    main(sys.argv[1:])

