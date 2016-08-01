#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import re
import base64
import json
import urllib
import os
import sys

class MRMostUsedWord(MRJob):

    def decode_data(self, line):
        try:
            retval = base64.b64decode(urllib.unquote(line).decode('utf8'))
            return retval
        except Exception as e:
            msg = 'Problem decoding {0} from base64'.format(line)
            raise TypeError(msg)

    def deconstruct_filename(self, path):
        retval={}
        part1 = path.split("_")
        if part1[1] == '136c85868b8340118112c631b9cbac80':
            retval['api'] = 'Desktop'
        elif part1[1] == '5609ac98b072469eb7b6c9b68f7441a6':
            retval['api'] = 'iOS'
        elif part1[1] == 'b5f6c16ca3314dfc9689e5def22212a9':
            retval['api'] = 'Android'
        else:
            retval['api'] = 'Unknown'
        retval['date'] = part1[2]
        part2 = part1[3].split(".")
        retval['unknown1'] = part2[0]
        retval['unknown2']=part2[2]
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
            

    def init_get_words(self):
        self.words = {}
        self.currentLine = 1

    def get_words(self, _, line):
        word = 'None'
        if ' evt ' in line:
            try:
                data = self.get_between(line, '&data=', '&')
                dataDict = self.get_json(data)
                word = self.deconstruct_filename(jobconf_from_env('mapreduce.map.input.file'))['api']+":"+self.deconstruct_filename(jobconf_from_env('mapreduce.map.input.file'))['date']+":"+dataDict['tz']
            except KeyError as e:
                sys.stderr.write('ERROR: Missing expected key: {0} Line: {1} File: {2}'.format(e, self.currentLine,jobconf_from_env('mapreduce.map.input.file')))
                pass
            except Exception as e:
                sys.stderr.write('ERROR: {0} Line: {1} File: {2}'.format(e, self.currentLine,jobconf_from_env('mapreduce.map.input.file')))
                pass
                    
            self.words.setdefault(word, 0)
            self.words[word] = self.words[word] + 1
        self.currentLine = self.currentLine + 1

    def final_get_words(self):
        for word, val in self.words.iteritems():
            yield word, val

    def sum_words(self, word, counts):
        yield word, sum(counts)

    def steps(self):
        return [MRStep(mapper_init=self.init_get_words,
                       mapper=self.get_words,
                       mapper_final=self.final_get_words,
                       combiner=self.sum_words,
                       reducer=self.sum_words)]


if __name__ == '__main__':
    MRMostUsedWord.run()
