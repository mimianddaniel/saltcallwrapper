#!/opt/blue-python/2.7/bin/python2.7
"""This module allow user to send Salt event to salt master and can be used to
retrieve returns from redis instances"""
import argparse
import os
import sys
import saltwrapper.saltcallwrapper

def parse_option():
    parser = argparse.ArgumentParser(epilog="This program must be run as root (eg. via sudo)")
    parser.add_argument("--target",   "-T", type=str, action="store", \
                    help="your target regex: eg I@roles:dscluster bofhcron-2*")
    parser.add_argument("--tag",   "-N", type=str, action="store",    \
                    default='salt/booking/default/cmd',
                    help="name of event: eg name of event that is set in the reactor")
    parser.add_argument("--timeout", "-t", type=str, action="store",  \
                    default=10, help="timeout for salt")
    parser.add_argument("--batchsize", "-b", type=str, action="store",  \
                    default='0', help="each run size, only useful when called for \
                    batchmode")
    parser.add_argument("--tgt_type", "-S", type=str, action="store", \
                    default="compound", help="type of regex: compound glob pcre ")
    parser.add_argument("--fun",   "-F", type=str, action="store",    \
                    help="your module that will get ran when certain tag is triggered")
    parser.add_argument("--argument",   "-E", type=str, action="store",  \
                    help="list of argument(s) for the function in --fun separated by space eg:\
                    key=value key1=value value")
    parser.add_argument("--literal", action="store_true", default=False,\
                    help="argument is literal")
    parser.add_argument("--debug", action="store_true", default=False,\
                    help="debug info are logged into ES")
    parser.add_argument("--summary", action="store_true",\
                    default=False, help="show the end summary")
    parser.add_argument("--output_format",   "-O", type=str, action="store",    \
            default='txt', help="format output type:json, pprint, txt")

    args = vars(parser.parse_args())

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
        print ("\nWhat this script does:\n- sends a salt event up to the saltmaster.\n- process the events received back from minions thru redis.\nThis mechanism is called using 'Reactor and returner' in Salt.")
        print ("\nHint on target regex:\nusing glob\n\t-T 'bc*app*ams4*'\nusing PCRE\n\t-T 'E@(bc15.*|bc10.*)'\nusing Grain or Pillar\n\t-T 'G@osmajorrelease:7'\n\t-T 'I@roles:dscluster'\nusing compound\n\t-T 'bc*app*ams4* or E@(bc15.*|bc10.*) or G@osmajorrelease:7 or I@roles:dscluster and not prodds-203*'")
        print ("\n\nExamples of event names configured")
        print ("\n* Test if the target salt minions are alive\nsudo salt_wrapper.py -N 'salt/booking/default/testping' -T 'I@roles:dscluster' --summary")
        print ("\n* Return the list of package(s)\nsudo salt_wrapper.py -N 'salt/booking/func/rpminfo' -T 'I@roles:dscluster' -F run -E 'zsh'")
        print ("\n* Run shell command\nsudo salt_wrapper.py -T 'prodds*' -E 'df -h | awk \"{print $6}\"' --literal")
        print ("\n* Run shell command in batch of 2 jobs at ALL time\nsudo salt_wrapper.py -T 'prodds*' -F 'booking_cmdmod.run' -E 'df -h | awk \"{print $6}\"' --batchsize 2 --literal -t 15")
        sys.exit(4)

    return args

def main(args):
    saltwrapper.saltcallwrapper.startEvents(args)

if __name__ == "__main__":
        args = parse_option()
        main(args)
