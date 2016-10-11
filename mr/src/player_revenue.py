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
from ConfigParser import RawConfigParser

class MRCountEvents(MRJob):

    OUTPUT_PROTOCOL = CsvProtocol

    def get_json(self, data):
        try:
            retval = json.loads(data)          
            return retval
        except TypeError as e:
            raise TypeError(e)
        except ValueError as e:
            msg = 'Problem serializing JSON: {0}'.format(decoded)
            raise ValueError(msg)

    def init_get_events(self):

        self.currentLine = 0

    def get_events(self, _, line):

        self.currentLine = self.currentLine + 1
        dataObj = self.get_json(line)

        stat = 'None'
        if 'event' in dataObj:
            stat = dataObj['event']
        elif 'messageType' in dataObj:
            if dataObj['messageType'] == 'mtu':
                if 'api' in dataObj:
                    if dataObj['api'] in ['android','ios']: 
                        stat = 'mtu'

        if stat in ['mtu', 'purchase']:
            try:
                row = [] 

                id1 = ''
                if 'id' in dataObj:
                    id1 = dataObj['id']
                row.append(id1)

                api = ''
                if 'api' in dataObj:
                    api = dataObj['api']
                row.append(api)


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
                if 'ts' in dataObj:
                    try:
                        started = int(dataObj['ts'])
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


                yield None, row

            except KeyError as e:
                sys.stderr.write('ERROR: Missing expected key: {0} Line: {1} File: {2}{3}'.format(e, self.currentLine,jobconf_from_env('mapreduce.map.input.file'),'\n'))
                pass
            except Exception as e:
                sys.stderr.write('ERROR: {0} Line: {1} File: {2}{3}'.format(e, self.currentLine,jobconf_from_env('mapreduce.map.input.file'),'\n'))
                pass
     
    def steps(self):
        return [MRStep(
                       mapper_init=self.init_get_events,
                       mapper=self.get_events)]

if __name__ == '__main__':
    MRCountEvents.run()
