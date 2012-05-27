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

import sys
import os
import subprocess
import logging
from optparse import OptionParser

parser = OptionParser("usage: %prog [options]")
parser.add_option("--debug", default=None, help="enable debug output")
parser.add_option("--dest", default=None, help="write to a file")
(options, args) = parser.parse_args()

# set up logging
if options.debug: log_level = logging.DEBUG
else:             log_level = logging.INFO

logging.basicConfig(stream=sys.stdout, level=log_level)
logging.basicConfig(stream=sys.stderr, level=(logging.ERROR,logging.CRITICAL))


if __name__ == '__main__':
    outdir = './catalogs/'

    if len(args) > 0:
        parser.error("this script doesn't take arguments, what are you trying to do?")

    ''' First, gather a list of signed agents using puppet cert '''

    # --all lists all puppet certs, with a '+' starting the line for signed certs.
    cert_command = "sudo puppet cert list --all |grep '^\+' |awk '{print $2}'"

    process = subprocess.Popen(cert_command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = process.communicate()

    if stderr or not stdout:
        logging.debug("failed to get puppet catalogs. command output was: %s" % stderr)

    ''' Next, ask the puppet master to compile its catalogs and provide json, and write each file out '''

    compile_command = "sudo puppet master --compile "

    for node in stdout.split():
        process = subprocess.Popen(compile_command + node, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if stderr or not stdout:
            logging.debug("failed to get catalog for node: %s; output was: %s" % (node, stderr))
        else
            FILE = open(outdir + node, 'w')
            FILE.writelines(stdout)
            FILE.close()




