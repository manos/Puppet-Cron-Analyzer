#!/opt/local/bin/python2.4
#!/usr/bin/env python
#
# @Author: "Charlie Schluting <charlie@schluting.com>"
# @Date:   April 2012
#
# Script to analyze json blob containing puppet cron resources.
#
# XXX: lies:
# Determines overlap, and outputs pickle file for importing into cron-viz.py.
#
# If --console is used, it prints analysis results to stdout (overlapping crons, summary of crons).

import sys
import os
import subprocess
import logging
import simplejson as json
from optparse import OptionParser
import cronlib
import cPickle as pickle

parser = OptionParser("usage: %prog [options] [input file]")
parser.add_option("--debug", default=None, help="enable debug output")
parser.add_option("--console", default=None, help="enable stdout text-based summary")
parser.add_option("--bds", default=None, help="type of 'big data store' to use: [redis|cassandra]")
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

if __name__ == '__main__':

    indir  = './parse-output/'
    outdir = './analyze-output/'

    if not os.path.exists(outdir): os.makedirs(outdir)

    if len(args) > 1:
        parser.error("only one argument allowed: file to read from")

    ''' First, find the cron json list  - stdin or a file '''
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
        catalogs.update({'single':crons})
    else:
        for catalog in os.listdir(indir):
            crons = []
            for cron in open(indir + catalog, "r").readlines():
                crons += json.loads(cron)
            catalogs.update({catalog:crons})
        sys.exit(0)

    ''' Next, for every catalog/blob, convert to dict {cron_line:[timestamps]}'''

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
        # Don't store these in memory! Writes to a file for each host... stored as json,
        # where the key is the cron entry (normalized as a tuple), and the value is a list of timestamps.
        #
        output = {}
        for cron in live_crons:
            _cron = cronify(cron)

            if _cron is None:
                continue

            norm_cron  = cronlib.normalize_entry(_cron)
            if norm_cron:
                timestamps = cronlib.expand_timestamps(norm_cron)
            else:
                continue

            output.update({norm_cron:timestamps})

            # TODO: insert into redis for live analysis?

        # Write to file:
        if stdin:
            print output
        elif len(args) == 1:
            FILE = open(outdir + os.path.basename(args[0]), 'w')
            pickle.dump(output, FILE)
            FILE.close()
        else:
            FILE = open(outdir + filename, 'w')
            pickle.dump(output, FILE)
            FILE.close()


    ''' Next, analyze. Read all files (consuming lots of memory potentially), unless --bds
        was used. Then, connect to redis or cassandra, where we've already shoved this data '''

    if not options.bds:
        all_data = {}
        indir = outdir
        for host in os.listdir(indir):
            data = pickle.load(open(indir + host, 'r'))
            all_data.update({host:data})

        print all_data


#    hourly = [r for r in live_crons
#            if r['parameters']['hour'] == '*']
#
#    print hourly
#

    # list of crons running at same hour / minute?

    # summarize crons that run at the same hour (warning), and minute (alert!)


#    print json.dumps(live_crons)

    ''' Finally, write out results and/or print summary '''




