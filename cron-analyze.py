#!/opt/local/bin/python2.4
#!/usr/bin/env python
#
# @Author: "Charlie Schluting <charlie@schluting.com>"
# @Date:   April 2012
#
# Script to analyze json blob (or a directory of) containing puppet cron resources.
#
# Parses, and outputs pickle files.
#
# Then, Analyzes and outputs various formats for visualization.
# Analysis currently highlights:
#   - the exact same cron running on various hosts
#   - duplicate crons on the same host
#   - crons that run at the same time on a host
#
# Some of these things may be normal and expected. To really parse your cron infrastructure,
# the following are available:
#   - ics (ical) output to view crons in a calendar application
#   - regex searching all crons (displays cron lines per host, and summarizes which hosts it exists on)
#
# Future: Options for displaying {day,week}-at-a-time views of all crons that will run.
# Future: for a given pair of crons (regex?), find out if they ever run at the same time.
#
'''
usage: cron-analyze.py [options] [stdin] [input file]

options:
  -h, --help            show this help message and exit
  --debug=DEBUG         enable debug output
  --output=OUTPUT       Default: stdout text-based summary. Options: [ical]
  -e, --existing-data   skip the parse step, use existing data in ./analyze-
                        output/
  -f regex, --find=regex
                        finds a cron across all hosts by regex (searches
                        command field) - use any python 're' compatible regex
'''

import sys
import os
import re
import subprocess
import logging
import simplejson as json
from optparse import OptionParser
import cronlib
import cPickle as pickle

parser = OptionParser("usage: %prog [options] [stdin] [input file]")
parser.add_option("-d", "--debug", default=None, help="enable debug output")
parser.add_option("-i", "--output", default=None, help="Default: stdout text-based summary. Options: [ical]")
parser.add_option("-e", "--existing-data", default=None, action="store_true",
        help="skip the parse step, use existing data in ./analyze-output/")
parser.add_option("-f", "--find", default=None, metavar="regex",
        help="finds a cron across all hosts by regex (searches command field) - use any python 're' compatible regex")
(options, args) = parser.parse_args()

# set up logging
if options.debug: log_level = logging.DEBUG
else:             log_level = logging.INFO

logging.basicConfig(stream=sys.stdout, level=log_level)
logging.basicConfig(stream=sys.stderr, level=(logging.ERROR,logging.CRITICAL))


# convert json puppet config back to actual cron entry line that'd appear on-disk
def cronify(cron):
    line = ""
    params = cron['parameters']
    if 'minute' in params:
        line += params['minute'] + ' '
    else:
        line += '* '

    if 'hour' in params:
        line += params['hour'] + ' '
    else:
        line += '* '

    if 'monthday' in params:
        line += params['monthday'] + ' '
    else:
        line += '* '

    if 'month' in params:
        line += params['month'] + ' '
    else:
        line += '* '

    if 'weekday' in params:
        line += params['weekday'] + ' '
    else:
        line += '* '

    if 'command' in params:
        line += params['command'] + ' '
    else:
        # you can't have a cron with no command!
        return None

    return line

def find_cron(all_crons, regex):
    ''' finds crons across all hosts by searching regex '''

    found_hosts = []
    found_sum   = 0
    for host in all_crons.iteritems():

        found_crons = [v for k,v in host[1].iteritems() if re.match(regex, k[5]) ]

        if len(found_crons) == 0: continue
        found_sum += len(found_crons)

        # re-construct how the cron looks on-disk, if any were found:
        results = []
        for cron in found_crons:
            results.append(cronify(cron))
        print "Found on host %s: " % host[0]
        print '\t', "\n\t".join(map(str, results))

        found_hosts.append(host[0])
    if len(found_hosts) >0:
        print "\n\nSummary: found %i crons on the following %i hosts: \n%s" % (found_sum, len(found_hosts), '\n'.join(map(str, found_hosts)))
    return


def find_dups_allhosts(all_crons, time_map):
    ''' the exact same cron running on various hosts (at the same times). returns dict {(host1, host2,): cron} '''
    pass

def find_sametime_crons(crons, time_map):
    ''' any crons that *ever* run at the same time on a host. returns list of full (puppet) crons. '''
    pass

def find_sameschedule_crons(crons):
    ''' crons that run at the same schedule on a host. returns list of full (puppet) crons. '''

    # ok, I better document this fucker.
    #
    # the nested comprehension just returns the time part of the key, for comparison (but skips if its
    #  full key matches the outer loop's full key)
    # loops over all crons, and compares the time part of the key (0,*,*,*,*),
    #  to all other keys (skipping itself).. then returns list of values (puppet cron json) that match.

    return [v for k,v in crons.iteritems() if k[:5] in [i[:5] for i,j in crons.iteritems() if i != k ] ]


if __name__ == '__main__':

    indir  = './parse-output/'
    outdir = './analyze-output/'

    if not os.path.exists(outdir): os.makedirs(outdir)

    if len(args) > 1:
        parser.error("only one argument allowed: file to read from")

    ''' First, find the cron json list  - stdin, a file arg, or dirlist() '''
    stdin = None
    if not sys.stdin.isatty(): # redirected from file or pipe
        stdin = sys.stdin.read()

    crons    = []
    catalogs = {}

    if stdin:
        crons = json.loads(stdin)
        catalogs.update({'single':crons})
    elif len(args) == 1:
        for cron in open(args[0], "r").readlines():
            crons += json.loads(cron)
        catalogs.update({os.path.basename(args[0]):crons})
    else:
        for catalog in os.listdir(indir):
            crons = []
            for cron in open(indir + catalog, "r").readlines():
                crons += json.loads(cron)
            catalogs.update({catalog:crons})

    ''' Next, for every catalog/blob, convert to dicts for processing: '''

    if not options.existing_data:
        all_data = {}
        for filename,crons in catalogs.iteritems():

            # create a list crons that actually run (i.e. skips ensure=>absent)
            live_crons = []

            for cron in crons:
                if 'ensure' in cron['parameters'] and cron['parameters']['ensure'] == 'absent':
                   continue
                else:
                    # these crons will actually run, ignore others:
                    live_crons.append(cron)

            #
            # Using cronlib, we'll genreate a list of timestamps all crons will run at..
            # Stores every non-duplicate cron time('0 * * * *') list of timestamps in time_map.
            # where the key is the cron entry (normalized as a tuple), and the value is a list of timestamps.
            # Dumps to pickle files, for subsequent runs where --existing-data may be used.
            #

            output = {}
            # output: {"hostname": {"(0, 0, 1, 1, 0, 'command')": PUPPET_JSON, "(0,...)": PUPPET_JSON, ... }
            time_map = {}
            # time_map: {"(0, 0, 1, 1, 0)": [98742323423.0, 29482039423.0, ... ]}

            for cron in live_crons:
                _cron = cronify(cron)

                if _cron is None:
                    continue

                norm_cron = cronlib.normalize_entry(_cron)

                if norm_cron and norm_cron[:5] not in time_map:
                    timestamps = cronlib.expand_timestamps(norm_cron)
                    time_map.update({norm_cron[:5]:timestamps})

                if filename in output and norm_cron in output[filename]:
                    logging.warn("Found duplicate cron job on host %s. Skipping: %s", (filename,_cron))

                if not filename in output:
                    output.update({filename:{norm_cron:cron}})
                else:
                    output[filename].update({norm_cron:cron})

            # Write to file:
            if stdin:
                print output, time_map
            elif len(args) == 1:
                pickle.dump(output, open(outdir + filename, 'w'))
                pickle.dump(time_map, open(outdir + "time_map.pickle", 'w'))
            else:
                pickle.dump(output, open(outdir + filename, 'w'))
                pickle.dump(time_map, open(outdir + "time_map.pickle", 'w'))

            # add to all_data
            all_data.update(output)
        # end loop: every file in indir

    ''' Next, analyze. Read all files (if we've skipped the analyze step) and analyze. '''

    if options.existing_data:
        all_data = {}
        indir = outdir
        for host in os.listdir(indir):
            if host != 'time_map.pickle':
                data = pickle.load(open(indir + host, 'r'))
                all_data.update(data)
        time_map = pickle.load(open(indir + "time_map.pickle", 'r'))

    ''' jobs that just run, and terminate '''

    # if we're just searching all crons, do it and exit:
    if options.find:
        find_cron(all_data, options.find)
        sys.exit(0)

    # if we're outting a data format, do it and exit:
    if options.output and options.output is 'ical':
        pass

    ''' full analysis '''

    # find any crons that run on the exact same schedule on a host:
    found_hosts = []
    found_sum   = 0
    for host in all_data.iteritems():
        results = []
        found_crons = find_sameschedule_crons(host[1])
        found_sum += len(found_crons)

        for cron in found_crons:
            results.append(cronify(cron))
        print "Found %i crons with the exact same run schedule on host %s: " % (len(found_crons), host[0])
        print '\t', "\n\t".join(map(str, results))

        found_hosts.append(host[0])

    if len(found_hosts) >0:
        print "\n\nSummary: found %i crons on the following %i hosts: \n%s" % (found_sum, len(found_hosts), '\n'.join(map(str, found_hosts)))

    # find any crons that ever run at the same time, on the same host:
    for host in all_data.iteritems():
        find_sametime_crons(host[1], time_map)


#    hourly = [r for r in live_crons
#            if r['parameters']['hour'] == '*']
#
#    print hourly
#



    ''' Finally, write out results and/or print summary '''




