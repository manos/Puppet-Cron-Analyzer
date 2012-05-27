#!/opt/local/bin/python2.4
#!/usr/bin/env python
#
# @Author: "Charlie Schluting <charlie@schluting.com>"
# @Date:   April 2012
#
# Script to parse puppet catalog(s) and output json cron blobs, appropriate for
#  digestion by cron-analyze. The [input file] (or stdin) is expected to be a puppet
#  catalog in json format (see `puppet master --compile`).
#
# usage: cron-parse.py [options] [input files...] [output file]
#
# no options, no arguments, with stdin provided: reads stdin and outputs to stdout
# no options with stdin provided, and one file arguments: reads stdin, and outputs to file
# no options, one file argument: reads from file, outputs to stdout
# no options, > 1 file argument: reads all files except last one; outputs to last file, unless --stdout is used
# --dest option, reads from either stdin or all file arguments, and outputs to --dest
#
# If --generate is used, we will attempt to ask puppet to compile all catalogs (via puppet.py).
# Useful if you don't want to manage importing puppet catalogs' json into this script.
# It fetches a list of all not-revoked puppet certs, and then calls
#  'puppet master --compile' on each. Must be run on the puppet master, with sudo privs
#  for running the puppet cert and master commands.
# This can take a long time!

import sys, os, logging
import simplejson as json
from optparse import OptionParser

# parse arguments
parser = OptionParser("usage: %prog [options] [output file]")
parser.add_option("--dest", default=None, help="destination file for output (stdout if not used)")
parser.add_option("--stdout", default=False, action="store_true", help="always output to stdout")
parser.add_option("--debug", default=None, help="enable debug output")
parser.add_option("-g", "--generate", default=None, help="run puppet master --compile to generate json catalogs")
(options, args) = parser.parse_args()

# set up logging
if options.debug:
    log_level = logging.DEBUG
else:
    log_level = logging.INFO

# c'mon. don't send everything to stderr, sheesh python.
logging.basicConfig(stream=sys.stdout, level=log_level)
logging.basicConfig(stream=sys.stderr, level=(logging.ERROR,logging.CRITICAL))


def extract_crons(puppet_catalog):
    """ returns a list of dicts, and each dict contains the entire puppet resource for a cron """
    crons = []
    for resource in puppet_catalog['data']['resources']:
        for key, value in resource.iteritems():
            if key == 'type' and value == 'Cron':
                crons.append(resource)
    return crons

if __name__ == '__main__':

    stdin = None
    if not sys.stdin.isatty(): # redirected from file or pipe
        stdin = sys.stdin.read()

    if len(args) < 1 and not stdin:
        parser.error("must supply at least one file name argument, or use stdin")

    ''' Build a list of cron resources from stdin or the list of files passed as args. '''
    crons = []

    # TODO: add "host": $hostname to the dict for each cron.

    if stdin:
        crons = extract_crons(json.loads(stdin))
    elif not options.generate and len(args) == 1:
        for catalog in args:
            crons += extract_crons(json.load(open(catalog)))
    elif not options.generate and len(args) > 1:
        if options.stdout:
            _args = args
        else:
            _args = args[:-1]
        for catalog in _args:
            crons += extract_crons(json.load(open(catalog)))
    else:
        ''' TODO generate catalogs! '''

    out_file = None
    if options.dest and not options.stdout:
        out_file = options.dest
    elif not stdin and not options.stdout and len(args) == 1:
        out_file = None
    elif stdin and not options.stdout and len(args) == 1:
        out_file = args[0]
    elif not options.stdout and len(args) > 1:
        out_file = args[-1:][0]

    if out_file:
        FILE = open(out_file, 'w')
        print >>FILE, json.dumps(crons)
        FILE.close()
    else:
        print json.dumps(crons)



