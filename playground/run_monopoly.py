#!/usr/bin/env python

##########################################################################################
#
# dbconn  -- populates postgres db with AC data
#
USAGE = """
Usage: python run.py <config_file.yml>

       where the config file named must have content in YAML format with x sections;
         one for the Agile Central,
         one for the db system,
         ......TBD............
"""
##########################################################################################

import sys
from playground.dbcon_runner import DBConRunner


PROG = 'dbconn'


##########################################################################################

def main(args):
    try:
        connector_runner = DBConRunner(args)
        connector_runner.run(args)
    except Exception as msg:
        sys.stderr.write('ERROR: %s encountered an ERROR condition.\n %s' % (PROG, msg))
        sys.exit(1)
    sys.exit(0)


##########################################################################################
##########################################################################################

if __name__ == '__main__':
    main(sys.argv[1:])