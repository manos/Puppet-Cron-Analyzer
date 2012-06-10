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
# usage: cron-parse.py [--console]
#
# With no command line options, this will parse all crons in ./catalogs/ and output to ./parse-output/.
# If stdin is provided, it will output the result to stdout.
#
# If --generate is used, we will attempt to ask puppet to compile all catalogs (via puppet.py).
# Useful if you don't want to manage importing puppet catalogs' json into this script.
# It fetches a list of all not-revoked puppet certs, and then calls
#  'puppet master --compile' on each. Must be run on the puppet master, with sudo privs
#  for running the puppet cert and master commands.
# This can take a long time! (TODO: parallel compilation? Or sleeping, to not overwhelm the master?)

import sys, os, logging
import simplejson as json
from optparse import OptionParser

# parse arguments
parser = OptionParser("usage: %prog [options]")
parser.add_option("--console", default=False, action="store_true", help="output to stdout")
parser.add_option("-d", "--debug", default=None, help="enable debug output")
parser.add_option("-g", "--generate", default=None, help="run puppet master --compile to generate json catalogs")
(options, args) = parser.parse_args()

# set up logging
if options.debug: log_level = logging.DEBUG
else:             log_level = logging.INFO

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

def do_parse_and_write(catalogs_dir, outdir):
    for catalog in os.listdir(catalogs_dir):
        # parse crons out of catalog
        crons = extract_crons(json.load(open(catalogs_dir + catalog)))

        # write json of just crons
        FILE = open(outdir + catalog, 'w')
        print >>FILE, json.dumps(crons)
        FILE.close()
    return True

if __name__ == '__main__':
    outdir = './parse-output/'
    input_dir = './catalogs/'

    if not os.path.exists(outdir): os.makedirs(outdir)

    stdin = None
    if not sys.stdin.isatty(): # redirected from file or pipe
        stdin = sys.stdin.read()

    ''' Build a list of cron resources from stdin or from input_dir '''
    crons = []

    if stdin:
        crons = extract_crons(json.loads(stdin))
    elif options.generate:
        ''' TODO generate catalogs '''
        do_parse_and_write(input_dir, outdir)
    else:
        do_parse_and_write(input_dir, outdir)

    if stdin:
        print json.dumps(crons)




