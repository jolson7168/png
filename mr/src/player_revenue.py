#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
from mr3px.csvprotocol import CsvProtocol
import re
import base64
import json
import urllib
import os
import sys
from datetime import datetime
from datetime import timedelta
from ConfigParser import RawConfigParser

EVTTYPE = 1
MTUTYPE = 2
class MRCountEvents(MRJob):

    OUTPUT_PROTOCOL = CsvProtocol


    masterList = {}
    masterList2 = {}
    eventList = []
    apiKeys = {}

    def get_json(self, data):
        try:
            retval = json.loads(data)          
            return retval
        except TypeError as e:
            raise TypeError(e)
        except ValueError as e:
            msg = 'Problem serializing JSON: {0}'.format(data)
            raise ValueError(msg)

    def init_get_events(self):

        self.masterList = {}
        self.currentLine = 1


    def getKey(self, msg, msgType):
        retval = []
        complete = False
        if "messageType" in msg:
            if "api" in msg:
                if "id" in msg:
                    if "serverTime" in msg:
                            retval.append(msg["messageType"])
                            retval.append(msg["api"])
                            retval.append(msg["id"])
                            if 'ts' in msg:
                                try:
                                    startedString = datetime.fromtimestamp(int(msg['ts'])/1000).strftime("%Y-%m-%d %H:%M:%S")                    
                                    retval.append(startedString)
                                    if msgType == MTUTYPE:
                                        if "v" in msg:
                                            retval.append(msg["v"])
                                            complete = True
                                    elif msgType == EVTTYPE:
                                        if "event" in msg:
                                            if "price" in msg:
                                                retval.append(msg["price"])
                                                complete = True
                                except Exception as e:
                                    pass

        if complete:
            return tuple(retval)
        else:
            sys.stderr.write('{0}{1}'.format(msg,'\n'))
            return []

    def getOppositeKey(self, key, inType, outType):
        retval = []
        if inType == MTUTYPE:
            retval.append('evt')
        else:
            retval.append('mtu')
        retval.append(key[1])
        retval.append(key[2])
        retval.append(key[3])
        if inType == MTUTYPE:
            retval.append(float(key[4]) / 100)
        elif inType == EVTTYPE:
            retval.append(str(int(key[4] * 100)))
        return tuple(retval)


    def get_events(self, _, line):

        try:
            dataObj = self.get_json(line)
        except Exception as e:
            pass
            sys.stderr.write('ERROR: {0} {1} Line: {2} File: {3}{4}'.format(e, line, self.currentLine,jobconf_from_env('mapreduce.map.input.file'),'\n'))

        if dataObj:
            stat = 'None'
            if 'event' in dataObj:
                stat = dataObj['event']
            elif 'messageType' in dataObj:
                if dataObj['messageType'] == 'mtu':
                    if 'api' in dataObj:
                        if (dataObj['api'] == 'android') or (dataObj['api'] == 'ios'):
                            if 'v' in dataObj: 
                                stat = 'mtu'
                        elif dataObj['api'] == 'desktop':
                            if 'gameUnlocking' in dataObj:
                                stat = 'mtu'
            row = [] 
            if stat in ['mtu', 'purchase','tmpunlock_with_money_success']:
                try:
                    id1 = ''
                    if 'id' in dataObj:
                        id1 = dataObj['id']
                    row.append(id1)

                    api = ''
                    if 'api' in dataObj:
                        api = dataObj['api']
                    row.append(api)

                    row.append(stat)
                    micro = False
                    nowSting = ''
                    if 'serverTime' in dataObj:
                        timeval = dataObj['serverTime']
                        now = int(timeval)
                        if len(str(timeval)) > 10:
                            nowTimeStamp = datetime.fromtimestamp(now/1000)
                            micro = True
                        else:
                            nowTimeStamp = datetime.fromtimestamp(now)
                        nowString = nowTimeStamp.strftime("%Y-%m-%d %H:%M:%S")
                    row.append(nowString)
                    
                    #Fix
                    startedString = ''
                    now = 0 
                    if 'ts' in dataObj:
                        try:
                            started = int(dataObj['ts'])
                            now = started
                            startedTimeStamp = datetime.fromtimestamp(started/1000)
                            startedString = startedTimeStamp.strftime("%Y-%m-%d %H:%M:%S")                    
                        except ValueError as e:
                            pass
                            startedString = ''
                        except TypeError as e:
                            pass
                            startedString = ''
                    row.append(startedString)


                    bankroll = ''
                    if 'bankroll' in dataObj:
                        try:
                            bankroll = int(dataObj['bankroll'])
                        except ValueError as e:
                            pass
                            bankroll = ''
                        except TypeError as e:
                            pass
                            bankroll = ''
                    row.append(bankroll)


                    gameLevel = ''
                    if 'gameLevel' in dataObj:
                        try:
                            gameLevel = int(dataObj['gameLevel'])
                        except ValueError as e:
                            pass
                            gameLevel = ''
                        except TypeError as e:
                            pass
                            gameLevel = ''
                    row.append(gameLevel)

                    level = ''
                    if 'level' in dataObj:
                        level = dataObj['level']
                    row.append(level)

                    hoid = ''
                    if 'hotOfferId' in dataObj:
                        hoid = dataObj['hotOfferId']
                        try:
                            hoid = int(hoid)
                        except ValueError as e:
                            pass
                            if hoid == 'null':
                                hoid = ''
                        except TypeError as e:
                            pass
                            hoid = ''
                    row.append(hoid)

                    hoex = ''
                    if 'hotOfferExpires' in dataObj:
                        hoex = dataObj['hotOfferExpires']
                        try:
                            hoex = int(hoex)
                            hoexTimeStamp = datetime.fromtimestamp(hoex/1000)
                            hoex = hoexTimeStamp.strftime("%Y-%m-%d %H:%M:%S")
                        except ValueError as e:
                            pass                        
                            hoex = ''
                        except TypeError as e:
                            pass                        
                            hoex = ''
                    row.append(hoex)

                    saleName = ''
                    if 'saleName' in dataObj:
                        saleName = dataObj['saleName']
                        if saleName == 'null':
                            saleName = ''
                        if saleName == 'None':
                            saleName = ''
                    row.append(saleName)

                    saleMult = ''
                    if 'saleMultiplier' in dataObj:
                        saleMult = dataObj['saleMultiplier']
                        try:
                            saleMult = float(saleMult)
                        except ValueError as e:
                            pass
                            saleMult = ''
                        except TypeError as e:
                            pass                        
                            saleMult = ''
                    row.append(saleMult)

                    price = ''
                    if 'price' in dataObj:
                        price = dataObj['price']
                        try:
                            price = float(price)
                        except ValueError as e:
                            pass                        
                            price = ''
                        except TypeError as e:
                            pass                        
                            price = ''
                    elif 'v' in dataObj:
                        price1 = dataObj['v']
                        try:
                            price = (float(price1)/100)
                        except ValueError as e:
                            pass
                            price = ''
                        except TypeError as e:
                            pass
                            price = ''
                    row.append(price)

                    ca = ''
                    if 'creditAmount' in dataObj:
                        try:
                            ca = int(dataObj['creditAmount'])
                        except ValueError as e:
                            pass                        
                            ca = ''
                        except TypeError as e:
                            pass                        
                            ca = ''
                    row.append(ca)

                    eventVal = ''
                    if 'eventValue' in dataObj:
                        try:
                            if isinstance(dataObj['eventValue'], (int, long)):
                                eventVal = int(dataObj['eventValue'])
                            elif isinstance(dataObj['eventValue'], float):
                                eventVal = float(dataObj['eventValue'])
                            elif dataObj['eventValue'] is None:                                
                                eventVal = ''
                            else:
                                eventVal = dataObj['eventValue']
                        except ValueError as e:
                            pass
                            eventVal = ''
                        except ValueError as e:
                            pass                        
                            eventVal = ''
                        
                    row.append(eventVal) 

                    ettb = ''
                    if 'timeTillBonus' in dataObj:
                        ettb = int(dataObj['timeTillBonus'])
                        try:
                            ettbTimeStamp = datetime.fromtimestamp((ettb+now)/1000)
                            ettb = ettbTimeStamp.strftime("%Y-%m-%d %H:%M:%S")
                        except ValueError as e:
                            pass                        
                            ettb = ''
                        except TypeError as e:
                            pass
                            ettb = ''
                    row.append(ettb)

                    xp = ''
                    if 'xp' in dataObj:
                        try:
                            xp = int(dataObj['xp'])
                        except ValueError as e:
                            pass                        
                            xp = ''
                        except TypeError as e:
                            pass
                            xp = ''
                    row.append(xp)

                    room=''
                    if 'room' in dataObj:
                        room = dataObj['room']
                        if room == 'null':
                            room = ''
                    row.append(room)

                    tz=''
                    if 'tz' in dataObj:
                        tz = dataObj['tz']
                        if tz == 'null':
                            tz = ''
                    row.append(tz)


                    tester = ''
                    if 'isTester' in dataObj:
                        tester = dataObj['isTester']
                    row.append(tester)


                    appVersion=''
                    if 'appVersion' in dataObj:
                        appVersion = dataObj['appVersion']
                    row.append(appVersion)

                    deviceOS=''
                    if 'deviceOS' in dataObj:
                        deviceOS = dataObj['deviceOS']
                    row.append(deviceOS)


                    deviceOSVersion=''
                    if 'deviceOSVersion' in dataObj:
                        deviceOSVersion = dataObj['deviceOSVersion']
                    row.append(deviceOSVersion)

                    deviceName=''
                    if 'deviceName' in dataObj:
                        deviceName = (dataObj['deviceName'].replace(',','-'))
                    row.append(deviceName)
                
                    row.append(jobconf_from_env('mapreduce.map.input.file'))
                    row.append(self.currentLine)

                    fileName = ''
                    if 'upsightSource' in dataObj:
                        fileName = dataObj['upsightSource']
                    row.append(fileName)

                    lineNo = ''
                    if 'sourceLineNumber' in dataObj:
                        lineNo = dataObj['sourceLineNumber']
                    row.append(lineNo)

                    dupeFileName = ''
                    if 'dupeFileName' in dataObj:
                        dupeFileName = dataObj['dupeFileName']
                    row.append(dupeFileName)

                    dupeLineNo = ''
                    if 'dupeLineNo' in dataObj:
                        dupeLineNo = dataObj['dupeLineNo']
                    row.append(dupeLineNo)

                except KeyError as e:
                    sys.stderr.write('ERROR: Missing expected key: {0} Line: {1} File: {2}{3}'.format(e, self.currentLine,jobconf_from_env('mapreduce.map.input.file'),'\n'))
                    pass
                except Exception as e:
                    sys.stderr.write('ERROR: {0} Line: {1} File: {2}{3}'.format(e, self.currentLine,jobconf_from_env('mapreduce.map.input.file'),'\n'))
                    pass

                if "messageType" in dataObj:
                    if dataObj["messageType"] == "mtu":
                        key = self.getKey(dataObj, MTUTYPE)
                        sys.stderr.write('key1: {0}{1}'.format(key, '\n'))
                        if len(key) > 0:
                            oppositeKey = self.getOppositeKey(key, MTUTYPE, EVTTYPE)
                            if oppositeKey not in self.masterList:
                                self.masterList[key] = row
                    elif dataObj["messageType"] == "evt":
                        key = self.getKey(dataObj, EVTTYPE)
                        sys.stderr.write('key2: {0}{1}'.format(key, '\n'))
                        if len(key) > 0:
                            oppositeKey = self.getOppositeKey(key, EVTTYPE, MTUTYPE)
                            if oppositeKey in self.masterList:
                                del self.masterList[oppositeKey]
                            self.masterList[key] = row

        self.currentLine = self.currentLine + 1
                 
    def final_get_events(self):
        for key, val in self.masterList.iteritems():
            yield key, val




    def steps(self):
        return [MRStep(
                       mapper_init=self.init_get_events,
                       mapper=self.get_events,
                       mapper_final=self.final_get_events,
                       )]


if __name__ == '__main__':
    MRCountEvents.run()
