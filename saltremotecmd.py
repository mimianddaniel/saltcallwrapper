#!/usr/bin/evn python
"""This module allow user to send Salt event to salt master and can be used to
retrieve returns from redis instances"""
import argparse
import os 
import sys
import saltcallwrapper

def parse_option():
        parser = argparse.ArgumentParser(epilog="This program must be run as root (eg. via sudo)")
        parser.add_argument("--target",   "-T", type=str, action="store", \
                        help="your target regex: eg I@roles:webapp*")
        parser.add_argument("--tag",   "-N", type=str, action="store",    \
                        help="name of tag: eg name of tag that is set in the reactor")
        parser.add_argument("--timeout", "-t", type=str, action="store",  \
                        default=5, help="timeout for salt")
        parser.add_argument("--tgt_type", "-S", type=str, action="store", \
                        default="glob", help="type of regex: compound glob pcre ")
        parser.add_argument("--fun",   "-F", type=str, action="store",    \
                        help="your module that will get ran when certain tag is triggered")
        parser.add_argument("--extra",   "-E", type=str, action="store",  \
                        help="extra argument: needs to be separated by space eg: myname=john sirname=snow justanarg")
        parser.add_argument("--debug", action="store_true", default=False,\
                        help="shows debug kinda")
        parser.add_argument("--summary", action="store_true",\
                        default=False, help="show the end summary")
        parser.add_argument("--output_format",   "-O", type=str, action="store",    \
                        default='txt', help="format output type")

        args = parser.parse_args()

        # lets check to see if the user is running root equivalent
        # checking after we can print help, because asking for help and being told
        # to run with sudo is non-friendly.
        if os.geteuid() != 0:
            parser.print_help()
            sys.exit(1)

        # must have some arguments, since we aren't reading salt filters from a file
        if len(sys.argv) == 1:
            print "At least one filter argument is required."
            print
            parser.print_help()
            sys.exit(4)

        return args

def main(args):
	saltcallwrapper.startEvents(args)

if __name__ == "__main__":
        args = parse_option()
        main(args)
