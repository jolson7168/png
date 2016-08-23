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


    masterList = {}
    masterList2 = {}
    eventList = []
    apiKeys = {}

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

        self.masterList = {}
        self.currentLine = 1

    def get_events(self, _, line):
        id1 = '0'
        api = 'None'
        event = 'None'
        dataDate = 'None'
        key = ('None', 'None', 'None', 'None')
        try:
            if line[0] != '#':
                if '&s=' in line:
                    id1 = self.get_between(line, '&s=', '&')
                elif ' s=' in line:
                    id1 = self.get_between(line, ' s=', '&')
                if id1[:3] == '109':
                    id1 = id1[3:]

                if '&n=' in line:
                    event = self.get_between(line, '&n=', '&')
                elif ' n=' in line:
                    event = self.get_between(line, ' n=', '&')
                dataDate = datetime.strftime(datetime.strptime(
                                self.deconstruct_filename(jobconf_from_env('mapreduce.map.input.file'))['date'], "%Y%m%d"), "%Y-%m-%d")
                api = self.deconstruct_filename(jobconf_from_env('mapreduce.map.input.file'))['api']
                key = (dataDate, api, int(id1), event)
        except KeyError as e:
            sys.stderr.write('ERROR: Missing expected key: {0} Line: {1} File: {2}{3}'.format(e, self.currentLine,jobconf_from_env('mapreduce.map.input.file'),'\n'))
            pass
        except Exception as e:
            sys.stderr.write('ERROR: {0} Line: {1} File: {2}{3}'.format(e, self.currentLine,jobconf_from_env('mapreduce.map.input.file'),'\n'))
            pass
     
        self.masterList.setdefault(key, 0)
        self.masterList[key] = self.masterList[key] + 1
        self.currentLine = self.currentLine + 1

        if self.currentLine % 100000 == 0:
            sys.stderr.write('{0}\n'.format(self.currentLine))
                 
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
        cfg.read('/tmp/event_counts.conf')
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
