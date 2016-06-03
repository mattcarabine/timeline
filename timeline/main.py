from __future__ import print_function

import argparse
import datetime
from glob import iglob
import io
import multiprocessing
import os
import re
import subprocess
import sys
import zipfile



import babysitter
import couchdb
import debug
import diag
import error
import info
import manager
from utils import extract_nodename, Event

MAX_BUFFER_SIZE = 1048576
LOG_MODULES = {'ns_server.couchdb.log': couchdb.CouchDBParser,
               'diag.log': diag.DiagParser,
               'ns_server.babysitter.log': babysitter.BabysitterParser,
               'ns_server.error.log': error.ErrorParser,
               'ns_server.info.log': info.InfoParser,
               'ns_server.debug.log': debug.DebugParser}


class Timeline(object):
    def __init__(self, timelines=None, input_dict=None):
        self.events = []
        self.default_node_name = None
        if timelines:
            self.events.extend(event for timeline in timelines
                               for event in timeline.events)
        elif input_dict:
            self.events = [Event(input_dict=event_dict)
                           for event_dict in input_dict['events']]

    def add_event(self, event):
        self.events.append(event)

    def add_events(self, events):
        self.events.extend(events)

    def sort(self):
        self.events = list(sorted(set(self.events)))

    def to_dict(self):
        return {'events': [event.to_dict() for event in self.events]}

    def __str__(self):
        self.sort()
        node_width = max(len(event.node_name) + 4 for event in self.events)
        for event in self.events:
            event.node_width = node_width
        return '\n'.join([str(event) for event in self.events])


def create_timeline(parsed_args):
    zips = []
    if not parsed_args.locations:
        for ci_zip in iglob('./*.zip'):
            zips.append(ci_zip)
    elif os.path.isdir(parsed_args.locations[0]):
        for ci_zip in iglob('{}/*.zip'.format(parsed_args.locations[0])):
            zips.append(ci_zip)
    else:
        for location in parsed_args.locations:
            zips.append(location)

    pool = multiprocessing.Pool(
        min(multiprocessing.cpu_count(), len(zips)))
    if len(zips) > 1:
        results = pool.map_async(multiprocessing_parse_zip_file,
                                 zips).get(9999)
        final_timeline = Timeline(results)
    else:
        final_timeline = parse_zip_file(zips[0])

    return final_timeline


def combine_timelines(timeline_dicts):
    """Used as an entry point for the CombinerManager"""
    timelines = [Timeline(input_dict=timeline_dict)
                 for timeline_dict in timeline_dicts]
    final_timeline = Timeline(timelines=timelines)
    return final_timeline


def parse_zip_file(zip_file):
    timeline = Timeline()
    try:
        ci = zipfile.ZipFile(zip_file, 'r')
    except (IOError, zipfile.BadZipfile):
        print('Could not open file: {}'.format(zip_file), file=sys.stderr)
        return

    for name in ci.namelist():
        # determine a default nodename that can be used when parsing
        # cannot otherwise determine the nodename
        m = re.search(r'couchbase\.log$', name)
        nodename = extract_nodename(name, 'unnamed_node')
        # strip cbcollect_info timestamp from nodename
        nodename = re.sub(r'_[0-9]{8}-[0-9]{6}$', '', nodename)
        timeline.default_node_name = nodename
        # determine if the file included in this zip can be parsed
        # by one of the modules. if so, add to tasks.
        logname = os.path.split(name)[-1]
        if logname == 'couchbase.log':
            timeline.collection_time = datetime.datetime(
                *ci.getinfo(name).date_time).isoformat()
        try:
            LOG_MODULES[logname](io.BufferedReader(
                ci.open(name), MAX_BUFFER_SIZE), timeline)
        except KeyError:
            pass
    ci.close()
    return timeline


def multiprocessing_parse_zip_file(zip_file):
    try:
        timeline = parse_zip_file(zip_file)
    except Exception as e:
        raise e
    else:
        return timeline


def parse_arguments(timeline_args):
    parser = argparse.ArgumentParser(description='Timeline - a tool to create'
                                     'timelines of events using cbcollects')

    parser.add_argument('locations', nargs='*', default=None,
                        help='Locations of cbcollects')
    parser.add_argument('--output', choices=['text', 'json'],
                        default='text', help='Output format to use')
    parser.add_argument('--mode', choices=['parse_only', 'combine',
                                           'default', 'convert_json'],
                        default='default', help='Mode to run nutshell in')
    return parser.parse_args(timeline_args)


def main():
    parsed_args = parse_arguments(sys.argv[1:])
    script_dir = os.path.dirname(os.path.realpath(__file__))
    git_rev = subprocess.check_output('cd ' + script_dir +
                                      '&& git describe --long 2>/dev/null'
                                      "|| echo 'g'`git rev-parse --short"
                                      ' HEAD`', shell=True).strip()

    if parsed_args.mode == 'parse_only':
        manager.ParserManager(parsed_args.locations[0], parse_zip_file,
                              git_rev)
    elif parsed_args.mode == 'combine':
        manager.CombinerManager(parsed_args.locations[0], combine_timelines,
                                git_rev)

    timeline = create_timeline(parsed_args)
    if parsed_args.output == 'json':
        print(timeline.to_dict())
    else:
        print(timeline)
    return 0


if __name__ == '__main__':
    main()
