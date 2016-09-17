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
        try:        
            start = aline.index( delim1 ) + len( delim1 )
            end = aline.index( delim2, start )
            return aline[start:end]
        except Exception as e:
            msg = 'Expecting delimiters: {0} and {1} in line: {2}'.format(delim1, delim2, aline)
            sys.stderr.write('{0}{1}'.format(msg, '\n'))
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
            if 'n=' in line:
                stat = self.get_between(line, 'n=', '&')
            else:
                stat = 'None'

            tester = ""
            if '"isTester":' in line:
                tester = self.get_between(line, '"isTester":', ',').replace('null','').replace('"','')
           
            if tester != 'True':
                if stat in ['spin_completed', 'game_started']:  #'spin_started'
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
                        fname = self.deconstruct_filename(jobconf_from_env('mapreduce.map.input.file'))
                        row.append(datetime.strftime(datetime.strptime(fname['date'], "%Y%m%d"), "%Y-%m-%d"))
                        row.append(fname['api']) 
                        row.append(stat) 
                        
                        st1 = ''
                        if 'st1=' in line:
                            st1 = self.get_between(line, 'st1=', '&')
                        elif 'st2=' in line:
                            st1 = self.get_between(line, 'st2=', '&')
                        row.append(st1) 

                        st3 = ''
                        if 'st3=' in line:
                            st3 = self.get_between(line, 'st3=', '&')
                        row.append(st3) 

                        eventStartedTime = ""
                        row.append(eventStartedTime) 
                        
                        eventStartedLocalTime = ""
                        row.append(eventStartedLocalTime) 

                        startBankroll = ""
                        row.append(startBankroll) 

                        startEventVal = ""
                        row.append(startEventVal) 


                        startGameLevel = ""
                        row.append(startGameLevel) 

                        sttb = ""
                        row.append(sttb) 

                        micro = False
                        nowSting = ""
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
                        
                        startedString = ""
                        if 'ts' in dataObj:
                            started = int(dataObj['ts'])
                            startedTimeStamp = datetime.fromtimestamp(started/1000)
                            startedString = startedTimeStamp.strftime("%Y-%m-%d %H:%M:%S")
                        row.append(startedString)

                        tslsString = ""
                        if 'timeSinceLastSpin' in dataObj:
                            if isinstance(dataObj['timeSinceLastSpin'], (int, long)):
                                tsls = int(dataObj['timeSinceLastSpin'])
                                if micro:
                                    tslsTimeStamp = datetime.fromtimestamp((started-tsls)/1000)
                                else:
                                    tslsTimeStamp = datetime.fromtimestamp((started-tsls))
                                tslsString = tslsTimeStamp.strftime("%Y-%m-%d %H:%M:%S")
                        row.append(tslsString)

                        bet = ""
                        if 'bet' in dataObj:
                            bet = int(dataObj['bet'])
                        row.append(bet)

                        win = ""
                        if 'win' in dataObj:
                            win = int(dataObj['win'])
                        row.append(win)

                        levelUp = ""
                        if 'levelUp' in dataObj:
                            levelUp = dataObj['levelUp']
                        row.append(levelUp)


                        xp = ""
                        if 'xp' in dataObj:
                            xp = int(dataObj['xp'])
                        row.append(xp)


                        bankroll = ""
                        if 'bankroll' in dataObj:
                            bankroll = int(dataObj['bankroll'])
                        row.append(bankroll)

                        eventVal = ""
                        if 'eventValue' in dataObj:
                            try:
                                if isinstance(dataObj['eventValue'], (int, long)):
                                    eventVal = int(dataObj['eventValue'])
                                elif isinstance(dataObj['eventValue'], float):
                                    eventVal = float(dataObj['eventValue'])
                                elif dataObj['eventValue'] is None:                                
                                    eventVal = ""
                                else:
                                    eventVal = dataObj['eventValue']
                            except ValueError as e:
                                pass                        
                        row.append(eventVal) 


                        tournamentID = ""
                        if 'tournamentId' in dataObj:
                            if isinstance(dataObj['tournamentId'], basestring):
                                tournamentID = dataObj['tournamentId']
                                if tournamentID == 'null':
                                    tournamentID = ""
                            elif isinstance(dataObj['tournamentId'], float):
                                tournamentID = float(dataObj['tournamentId'])
                            elif isinstance(dataObj['tournamentId'], (int, long)):
                                tournamentID = int(dataObj['tournamentId'])
                            else:
                                tournamentID = ""
                        row.append(tournamentID)

                        gameLevel = ""
                        if 'gameLevel' in dataObj:
                            gameLevel = int(dataObj['gameLevel'])
                        row.append(gameLevel)

                        ettb = ""
                        if 'timeTillBonus' in dataObj:
                            ettb = int(dataObj['timeTillBonus'])
                            try:
                                ettbTimeStamp = datetime.fromtimestamp((ettb+now)/1000)
                                ettb = ettbTimeStamp.strftime("%Y-%m-%d %H:%M:%S")
                            except ValueError as e:
                                pass                        
                                ettb = ""
                        row.append(ettb)


                        tz=""
                        if 'tz' in dataObj:
                            tz = dataObj['tz']
                            if tz == 'null':
                                tz = ""
                        row.append(tz)


                        appVersion=""
                        if 'appVersion' in dataObj:
                            appVersion = dataObj['appVersion']
                        row.append(appVersion)

                        deviceOS=""
                        if 'deviceOS' in dataObj:
                            deviceOS = dataObj['deviceOS']
                        row.append(deviceOS)


                        deviceOSVersion=""
                        if 'deviceOSVersion' in dataObj:
                            deviceOSVersion = dataObj['deviceOSVersion']
                        row.append(deviceOSVersion)

                        deviceName=""
                        if 'deviceName' in dataObj:
                            deviceName = (dataObj['deviceName'].replace(',','-'))
                        row.append(deviceName)

                        row.append(jobconf_from_env('mapreduce.map.input.file'))
                        row.append(self.currentLine)

                        yield None, row

                    except KeyError as e:
                        sys.stderr.write('ERROR: Missing expected key: {0} Line: {1} File: {2}{3}'.format(e, self.currentLine,jobconf_from_env('mapreduce.map.input.file'),'\n'))
                        pass
                    except Exception as e:
                        sys.stderr.write('ERROR: {0} Line: {1} File: {2}{3}'.format(e, line,jobconf_from_env('mapreduce.map.input.file'),'\n'))
                        pass
     
    def steps(self):
        return [MRStep(
                       mapper_init=self.init_get_events,
                       mapper=self.get_events)]

if __name__ == '__main__':
    MRCountEvents.run()
