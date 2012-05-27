#!/opt/local/bin/python2.4
#!/usr/bin/env python
#
# @Author: "Charlie Schluting <charlie@schluting.com>"
# @Date:   April 2012
#
# Script to generate puppet catalogs for all configured agents.
#
# It runs 'puppet master --compile' on each, so this must be run on the puppet master,
# with sudo privs for running the puppet cert and master commands.
# This can take a long time! 

import sys, os, subprocess, logging

parser = OptionParser("usage: %prog [options]")
parser.add_option("--debug", default=None, help="enable debug output")
parser.add_option("--stdout", default=None, help="print catalogs to stdout (newline delimited)")
parser.add_option("--dest", default=None, help="write to a file")
(options, args) = parser.parse_args()

# set up logging
if options.debug: log_level = logging.DEBUG
else:             log_level = logging.INFO

logging.basicConfig(stream=sys.stdout, level=log_level)
logging.basicConfig(stream=sys.stderr, level=(logging.ERROR,logging.CRITICAL))


if __name__ == '__main__':

    if len(args) > 0:
        parser.error("this script doesn't take arguments, what are you trying to do?")
    
    ''' First, gather a list of signed agents using puppet cert '''

    ''' Next, ask the puppet master to compile their catalogs and provide json '''

    ''' Finally, write these out to a file or stdout '''


    FILE = open(options.dest, 'w')
    FILE.writelines(crons)
    FILE.close()


