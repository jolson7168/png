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
            decoded = self.decode_data(data)
            retval = json.loads(decoded)          
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
            if 'n=' in line:
                stat = self.get_between(line, 'n=', '&')
            else:
                stat = 'None'
            if stat in ['purchase']:
                try:
                    row = [] 
                    coded = self.get_between(line, 'data=', '&')
                    decoded = self.decode_data(coded)
                    line = line.replace(coded, decoded)
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

                    nowSting = ""
                    if '&ts=' in line:
                        now = int(self.get_between(line, '&ts=', '&'))
                        nowTimeStamp = datetime.fromtimestamp(now/1000)
                        nowString = nowTimeStamp.strftime("%Y-%m-%d %H:%M:%S")

                    row.append(nowString)
                    
                    startedString = ""
                    if '{"ts":' in line:
                        started = int(self.get_between(line, '{"ts":', ','))
                        startedTimeStamp = datetime.fromtimestamp(started/1000)
                        startedString = startedTimeStamp.strftime("%Y-%m-%d %H:%M:%S")
                    row.append(startedString)

                    bankroll = ""
                    if '"bankroll":' in line:
                        bankroll = int(self.get_between(line, '"bankroll":', ','))
                    row.append(bankroll)

                    gameLevel = ""
                    if '"gameLevel":' in line:
                        gameLevel = int(self.get_between(line, '"gameLevel":', ','))
                    row.append(gameLevel)

                    level = ""
                    if '"level":' in line:
                        level = int(self.get_between(line, '"level":', ','))
                    row.append(level)

                    hoid = ""
                    if '"hotOfferId":' in line:
                        hoid = self.get_between(line, '"hotOfferId":', ',')
                        try:
                            hoid = int(hoid)
                        except ValueError as e:
                            pass
                            if hoid == 'null':
                                hoid = ""
                    row.append(hoid)

                    hoex = ""
                    if '"hotOfferExpires":' in line:
                        hoex = self.get_between(line, '"hotOfferExpires":', ',')
                        try:
                            hoex = int(hoex)
                            hoexTimeStamp = datetime.fromtimestamp(hoex/1000)
                            hoex = hoexTimeStamp.strftime("%Y-%m-%d %H:%M:%S")
                        except ValueError as e:
                            pass                        
                            hoex = ""
                    row.append(hoex)

                    saleName = ""
                    if '"saleName":' in line:
                        saleName = self.get_between(line, '"saleName":', ',').replace('null','')
                    row.append(saleName)

                    saleMult = ""
                    if '"saleMultiplier":' in line:
                        saleMult = self.get_between(line, '"saleMultiplier":', ',')
                        try:
                            saleMult = float(saleMult)
                        except ValueError as e:
                            pass                        
                            saleMult = ""
                    row.append(saleMult)

                    price = ""
                    if '"price":' in line:
                        price = self.get_between(line, '"price":', ',')
                        try:
                            price = float(price)
                        except ValueError as e:
                            pass                        
                            price = ""
                    row.append(price)

                    ca = ""
                    if '"creditAmount":' in line:
                        ca = int(self.get_between(line, '"creditAmount":', ','))
                    row.append(ca)

                    eventVal = ""
                    if '"eventValue":' in line:
                        eventVal = self.get_between(line, '"eventValue":', ',').replace('"','')
                        try:
                            eventVal = float(eventVal)
                        except ValueError as e:
                            pass                        
                            eventVal = ""
                    row.append(eventVal)

                    ttb = ""
                    if '"timeTillBonus":' in line:
                        ttb = int(self.get_between(line, '"timeTillBonus":', ','))
                        nextBonus = started + ttb
                        nextBonusTimeStamp = datetime.fromtimestamp(nextBonus/1000)
                        nextBonusString = nextBonusTimeStamp.strftime("%Y-%m-%d %H:%M:%S")
                    row.append(nextBonusString)

                    xp = ""
                    if '"xp":' in line:
                        xp = int(self.get_between(line, '"xp":', ','))
                    row.append(xp)

                    room=""
                    if '"room":' in line:
                        room = self.get_between(line, '"room":', ',').replace('null','')
                    row.append(room)

                    tz=""
                    if '"tz":' in line:
                        tz = self.get_between(line, '"tz":', '}').replace('null','')
                    row.append(tz)


                    tester = ""
                    if '"isTester":' in line:
                        tester = self.get_between(line, '"isTester":', ',').replace('null','').replace('"','')
                    row.append(tester)

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
