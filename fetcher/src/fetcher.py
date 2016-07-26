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

from boto.s3.connection import S3Connection
from boto.s3.key import Key

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

    parser.add_argument('-s', '--startDate', default=(datetime.now() + timedelta(hours = -1)) .strftime("%Y/%m/%d/%H"),
                        help='start pulling data from date YYYY/MM/DD/HH. Default: last hour')

    parser.add_argument('-e', '--endDate', default=None,
                        help='Optional. Pull data until date YYYY/MM/DD/HH ')
    return parser


def authGetURL(logger, url, apiKey = None, thisDate = None, requestType = None, conn = None):
    timeProc = time.time()
    timers={}

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
    try:
        timeStart = time.time()
        handle = urllib2.urlopen(req)
        timers['request time'] = round((time.time() - timeStart),3)
        if requestType:
            for line in handle:
                if "<a href=" in line:
                    quote1 = line.find('"')
                    quote2 = line.find('"', quote1+1)
                    authGetURL(logger = logger, url = line[quote1+1:quote2], conn = conn)
        else:
            back = len(requestURL) - requestURL.rfind("/") - 1
            fname = requestURL[-1*back:]
            timeWrite = time.time()

            if cfg.get('store', 'storage') == 'local':
                compressedFile = StringIO.StringIO(handle.read())
                fileName = cfg.get('store', 'location')+'/'+fname           
                with open(fileName, 'w') as outfile:
                    outfile.write(compressedFile.read())
                    outfile.close()
            else:
                compressedFile = cStringIO.StringIO(handle.read())
                bucket = conn.get_bucket(cfg.get('store', 'location'))
                k = Key(bucket)
                k.key = fname
                k.set_contents_from_string(compressedFile.getvalue())
                fileName = "s3://{0}/{1}".format(cfg.get('store', 'location'), fname)

            compressedFile.seek(0, os.SEEK_END)
            fileSize = compressedFile.tell()

            timers['write time'] = round((time.time() - timeWrite),3)
            timers['proc time'] = round((time.time() - timeProc),3)
            msg = "   Wrote {0} from {1}. Size: {2} Timers: {3}".format(fileName, requestURL, fileSize, timers)
            logger.info(msg)
            
    except IOError, e:
        msg =  "   {0} requesting file: {1}".format(e,requestURL)
        logger.error(msg)
        






def main(argv):

    # Overhead to manage command line opts and config file
    p = getCmdLineParser()
    args = p.parse_args()

    cfg.read(args.config_file)

    startDate =  datetime.strptime(args.startDate, "%Y/%m/%d/%H")
    if args.endDate:  
        endDate = datetime.strptime(args.endDate, "%Y/%m/%d/%H")
    else:
        endDate = None

    apiKeys = cfg.get('fetch', 'apiKeys').split(',')
    # Get the logger going
    logger = initLog(time.strftime("%Y%m%d%H%M%S"))
    logger.info('Starting Run: '+time.strftime("%Y%m%d%H%M%S")+'  ==============================')
    if endDate:
        msg = "Running from {0} to {1}....".format(startDate.strftime('%Y/%m/%d %H:00'), endDate.strftime('%Y/%m/%d %H:00'))
    else:
        msg = "Running {0}....".format(startDate.strftime('%Y/%m/%d %H:00'))
    logger.info(msg)
    
    if endDate is None:
        endDate = startDate
    if cfg.get('store', 'storage') == 'S3':
        conn = S3Connection(cfg.get('store', 'awsKey'), cfg.get('store', 'awsSecret'))

    for eachKey in apiKeys:
        currentDate = startDate
        while currentDate <= endDate:
            thisDate = currentDate.strftime('%Y/%m/%d/%H')
            authGetURL(url = cfg.get('fetch', 'url'), apiKey = eachKey, thisDate = thisDate, logger = logger, requestType = 'main', conn = conn)
            currentDate = currentDate + timedelta(hours = 1)


    # Clean up
    logger.info('Done! '+time.strftime("%Y%m%d%H%M%S")+'  ==============================')

if __name__ == "__main__":
    main(sys.argv[1:])

