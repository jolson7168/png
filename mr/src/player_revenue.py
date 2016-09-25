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

    def decode_data(self, line):
        try:
            retval = base64.b64decode(urllib.unquote(line).decode('utf8'))
            return retval
        except Exception as e:
            msg = 'Problem decoding {0} from base64'.format(line)
            raise TypeError(msg)

    def deconstruct_filename(self, path):
        retval={}
        retval['date'] = (path.split("_")[0])[-8:]
        retval['api'] = (path.split("_")[1]).split(".")[0]
        return retval

    def get_between(self, aline, delim1, delim2):
        if (delim1 in aline) and (delim2 in aline):        
            start = aline.index( delim1 ) + len( delim1 )
            end = aline.index( delim2, start )
            return aline[start:end]
        else:
            msg = 'Expecting delimiters: {0} and {1} in line: {2}'.format(delim1, delim2, aline)
            raise KeyError(msg)

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
        if line[0] != '#':
            stat = 'None'
            if 'n=' in line:
                stat = self.get_between(line, 'n=', '&')
            elif 'mtu ' in line:
                if self.deconstruct_filename(jobconf_from_env('mapreduce.map.input.file'))['api'] <> 'desktop':
                    stat = 'mtu'
            if stat in ['mtu', 'purchase']:
                try:
                    row = [] 
                    coded = self.get_between(line, 'data=', '&')
                    decoded = self.decode_data(coded)
                    line = line.replace(coded, decoded)

                    dataObj = self.get_json(decoded)

                    if '&s=' in line:
                        id1 = self.get_between(line, '&s=', '&')
                    elif ' s=' in line:
                        id1 = self.get_between(line, ' s=', '&')
                    else:
                        msg = 'No ID tags [(&s=) or ( s=)] present in line: {0}'.format(line)
                        raise KeyError(msg)

                    if id1[:3] == '109':
                        id1 = int(id1[3:])
                    row.append(id1)

                    row.append(self.deconstruct_filename(jobconf_from_env('mapreduce.map.input.file'))['api'].replace('"',''))

                    micro = False
                    nowSting = ''
                    if '&ts=' in line:
                        timeval = self.get_between(line, '&ts=', '&')
                        now = int(timeval)
                        if len(timeval) > 10:
                            nowTimeStamp = datetime.fromtimestamp(now/1000)
                            micro = True
                        else:
                            nowTimeStamp = datetime.fromtimestamp(now)
                        nowString = nowTimeStamp.strftime("%Y-%m-%d %H:%M:%S")
                    row.append(nowString)
                    

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

                    v1=''
                    if '&v=' in line:
                        v = self.get_between(line, '&v=', '&')
                        try:
                            v1 = int(v)
                        except ValueError as e:
                            pass
                            v1 = ''
                        except TypeError as e:
                            pass
                            v1 = ''
                    row.append(v1)
                
                    row.append(jobconf_from_env('mapreduce.map.input.file'))
                    row.append(self.currentLine)

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
