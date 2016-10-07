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
            msg = 'Problem serializing JSON: {0}'.format(decoded)
            raise ValueError(msg)

    def init_get_events(self):

        self.masterList = {}
        self.currentLine = 1

    def get_events(self, _, line):
        id1 = '0'
        api = 'None'
        event = 'None'
        dataDate = 'None'
        key = ('None', 'None', 'None', 'None')
        try:         
            dataObj = self.get_json(line)

            id1 = 'None'
            if 'id' in dataObj:
                id1 = dataObj['id']
            
            event = 'None'
            if 'event' in dataObj:
                event = dataObj['event']

            dataDateStamp = datetime.strptime(self.deconstruct_filename(jobconf_from_env('mapreduce.map.input.file'))['date'], "%Y%m%d")
            dataDate = datetime.strftime(dataDateStamp, "%Y-%m-%d")

            api = 'None'
            if 'api' in dataObj:
                api = dataObj['api']

            timeval = ''
            if 'serverTime' in dataObj:
                timeval = dataObj['serverTime']
            if len(timeval) > 0:
                now = int(timeval)
                if len(timeval) > 10:
                    nowTimeStamp = datetime.fromtimestamp(now/1000)
                    micro = True
                else:
                    nowTimeStamp = datetime.fromtimestamp(now)

                if (nowTimeStamp > (dataDateStamp - timedelta(hours=1))) and (nowTimeStamp< (dataDateStamp + timedelta(days=1))):
                    key = (dataDate, api, id1, event)

        except KeyError as e:
            sys.stderr.write('ERROR: Missing expected key: {0} Line: {1} File: {2}{3}'.format(e, self.currentLine,jobconf_from_env('mapreduce.map.input.file'),'\n'))
            pass
        except Exception as e:
            sys.stderr.write('ERROR: {0} Line: {1} File: {2}{3}'.format(e, self.currentLine,jobconf_from_env('mapreduce.map.input.file'),'\n'))
            pass

        self.masterList.setdefault(key, 0)
        self.masterList[key] = self.masterList[key] + 1
        self.currentLine = self.currentLine + 1
                 
    def final_get_events(self):
        for key, val in self.masterList.iteritems():
            yield key, val

    def sum_events1(self, key, counts):
        yield None, (key, sum(counts))

    def getRow(self, row):
        retval = []
        # This is awkward
        cfg = RawConfigParser()
        # This is awkward, too
        cfg.read('/etc/mr/event_counts.conf')
        self.eventList = cfg.get('data','events').split(',')
        for event in self.eventList:
            if event in row:
                val = row[event]
            else:
                val = 0
            retval.append(val)
        return retval

    def sum_events2(self, _, thepairs):
        for pairs in thepairs:
            if (pairs[0][0], pairs[0][1], pairs[0][2]) not in self.masterList2:
                self.masterList2[(pairs[0][0], pairs[0][1], pairs[0][2])] = {}
            self.masterList2[(pairs[0][0], pairs[0][1], pairs[0][2])][pairs[0][3]] = pairs[1]      

    def clean_up(self):

        for key in self.masterList2:
            row=[]
            row.append(key[0])
            row.append(key[1])
            row.append(key[2])
            row2 = self.getRow(self.masterList2[key])
            for item in row2:
                row.append(item)
            yield None, row

    def steps(self):
        return [MRStep(
                       mapper_init=self.init_get_events,
                       mapper=self.get_events,
                       mapper_final=self.final_get_events,
                       combiner = self.sum_events1,
                       reducer=self.sum_events2,
                       reducer_final=self.clean_up)]


if __name__ == '__main__':
    MRCountEvents.run()
