#!/opt/local/bin/python2.4
#!/usr/bin/env python
#
# @Author: "Charlie Schluting <charlie@schluting.com>"
# @Date:   April 2012
#
# Script to analyze json blob containing puppet cron resources.
#
# Determines overlap, and outputs pickle file for importing into cron-viz.py.
# If --console is used, it prints analysis results to stdout (overlapping crons, summary of crons).

import sys, os, subprocess, logging
import simplejson as json
from optparse import OptionParser

parser = OptionParser("usage: %prog [options] [input file]")
parser.add_option("--debug", default=None, help="enable debug output")
parser.add_option("--console", default=None, help="enable stdout text-based summary")
parser.add_option("--dest", default=None, help=".pickle file to write out results")
(options, args) = parser.parse_args()

# set up logging
if options.debug: log_level = logging.DEBUG
else:             log_level = logging.INFO

logging.basicConfig(stream=sys.stdout, level=log_level)
logging.basicConfig(stream=sys.stderr, level=(logging.ERROR,logging.CRITICAL))


if __name__ == '__main__':

    if len(args) > 1:
        parser.error("only one argument allowed: file to read from")

    ''' First, find the cron json list  - stdin or a file '''
    stdin = None
    if not sys.stdin.isatty(): # redirected from file or pipe
        stdin = sys.stdin.read()

    crons = []
    if stdin:
        crons = json.loads(stdin)
    elif len(args) == 1:
        for cron in open(args[0], "r").readlines():
            print cron
            crons += json.loads(cron)

    ''' Next, analyze '''

    # create a list crons that have minute/hour specified (i.e. skips ensure=>absent)
    live_crons = []
    for cron in crons:
        runs = False
        for key,value in cron['parameters'].iteritems():
            if 'hour' or 'minute' in key:
                runs = True
        if runs:
            # these crons will actually run, ignore others:
            live_crons.append(cron)

    # sort, and organize crons by hour

    # for each matching hour, flag crons that run at exactly the same minute

    # summarize crons that run at the same hour (warning), and minute (alert!)


    print json.dumps(live_crons)

    ''' Finally, write out results and/or print summary '''


#    FILE = open(options.dest, 'w')
#    FILE.writelines(crons)
#    FILE.close()


