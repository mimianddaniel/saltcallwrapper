"""
startEvents method sends Salt event to salt master which then
retrieves the executed returns of minions via redis instances
origin -> saltmaster -> minion(s) -> redis -> origin
"""
from multiprocessing import Queue
from Queue import Empty

import signal
import saltcallutil

def startEvents(args):
    """ startEvent flow: require a dict of following keys
    - args:{
        'tag': nameoftag,
        'tgt_type': "compound|glob",
        'target': regexoftarget,
        'fun': nameoffunction,
        'arguement'= argumen to the funcion,
        'debug'= TRUE|FALSE,
        'timeout'=timeout,
        'output_format'="json|txt"
    }
    """
    # may get interrupted by the user
    signal.signal(signal.SIGINT, saltcallutil.signal_handler)

    myTasks = Queue()
    myResults = Queue()
    # limit workers to 2 for now
    numofworker = 2

    saltcallutil.setupWorker(numofworker, myTasks, myResults)
    saltcallutil.submitTask(args, myTasks)
    retMinions,actualCount, expectCount, diffSet, chan = saltcallutil.retrieveTask(
            numofworker,
            myResults,
            args.pop('output_format')
            )

    missRet = len(diffSet)

    if args.pop('summary'):
        print ("Job ID for this run was {0}".format(chan))
        print ("Number of minions returned: {0}".format(actualCount))
        print ("Expected return minions was: {0}".format(expectCount))
        if missRet > 0:
            print ("Didn't receive returns from the followin minion:")
        # print what's missing
        for minion in diffSet:
            print (minion)
    # return the number of returns
    return retMinions
