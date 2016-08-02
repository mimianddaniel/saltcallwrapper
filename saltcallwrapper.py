"""This module allow user to send Salt event to salt master and can be used to
retrieve returns from redis instances"""
import salt.client
import salt.config
import salt.utils.jid
import salt.log
import salt.output.txt
import salt.output.json_out
from salt.exceptions import SaltException
import redis
import time
import json
import sys
import os
import shlex
from multiprocessing import Queue, Process, current_process

LOGLEVEL = 'debug'
LOGFMT = 'salt-wrapper-%(processName)s:[%(process)d] %(message)s'
# logging to minion.log in format that ES understands; all salt logs in ES
salt.log.setup_logfile_logger(LOGFILE, LOGLEVEL, LOGFMT)

# salt.log.setup_logfile_logger declared prior to import logging to avoid
# overwritting
import logging  # pylint: disable=wrong-import-position
logger = logging.getLogger('salt-wrapper')
logger.setLevel(logging.INFO)


# list of REDIS that will are used as returners
REDIS1 = {
    'host': 'yourredisinstance',
    'port': 6379,
    'db': 0,
}

REDIS2 = {
    'host': 'yourredisinstance',
    'port': 6379,
    'db': 0,
}


REDIS_SITE = [REDIS1 REDIS2]
# Number of times salt call will be attempted in case it fails to be created
SALTCALL_RETRY = 3


class Saltwrapper(object):
    """Takes care of setting up basic salt settings
      - since reactor doesnt provide return of the jobs performed on the minions automatically,
      instead of doing another salt call to retrieve them, all minions can be instructed to
      publish their returns to redis and can be retrieved using the function check_redisreturns
      function
      eg:

        mysaltcall = Saltcall_wrapper(myuser, args.debug, timeout=args.timeout)
        matched_minions = mysaltcall.get_saltylist(args.target, False)
        mysaltcall.send_event(
                       tag=args.tag,
                       tgt_type=args.tgt_type,
                       target=args.target,
                       fun=args.fun,
                       extra=extra)
    """

    def __init__(
            self,
            user,
            debug,
            channel=salt.utils.jid.gen_jid(),
            master='mymsaltmasterserver',
            timeout=5):
        """set up initial salt object setting plus redis instances that will be looked up
        - user: name of the user who executed the command
        - debug: shows stage where it may be failing
        - channel: redis channel that is used for pub/sub for returns
        - master: name of the salt master
        """
        # current JID plus the process name (timestamp will be used as channel name)
        self.channel = channel
        self.opts = salt.config.minion_config('/etc/salt/minion')
        self.opts['master'] = master
        self.opts['retry_dns'] = '0'
        try:
            self.caller = salt.client.Caller(mopts=self.opts)
        except (SaltException) as err:
            logger.critical(
                'Unable to connect to the saltmaster with exception %s',
                sys.exc_info()[0])
            print "Unable to connect to the saltmaster with exception {0}".format(sys.exc_info()[0])
            raise err
        self.user = user
        self.debug = debug
        logger.info(
            'channel: %s, New Salt wrapper created by user: %s',
            self.channel, self.user)

    def send_event(self, **kwargs):
        """send the salt event up to the master
        """
        # tag, tgt_type, tgt, function_name(not modulename),
        logger.info(
            'channel: %s, About to send the event up to the master %s\n',
            self.channel, self.opts['master'])

        try:
            self.caller.sminion.functions['event.send'](
                kwargs.get('tag'),
                {
                    'tgt_type': kwargs.get('tgt_type'),
                    'tgt': kwargs.get('target'),
                    'action': kwargs.get('fun'),
                    'user_name': self.user,
                    'channel': self.channel,
                    'argument': kwargs.get('extra')})
        except (SaltException) as err:
            logger.critical(
                'channel: %s, Unable to connect to the saltmaster with exception %s',
                self.channel,
                sys.exc_info()[0])
            print "Unable to connect to the saltmaster with exception {0}".format(sys.exc_info()[0])
            raise err
        logger.info(
            'channel: %s, Tag: %s, Target: %s, Function: %s, User: %s, Argument: %s',
            self.channel,
            kwargs.get('tgt_type'),
            kwargs.get('target'),
            kwargs.get('fun'),
            self.user,
            kwargs.get('extra'))

def check_return(ret):
    """check the redis return messages per poll against the initial targeted hosts
    - ret: return from redis
    - matched_minions: list of minions that were initially matched targets
    - return_minion: minions that have published returns after the execution
    """
    rload = {}
    payload = {}

    if isinstance(ret, dict):
        if 'type' in ret and ret['type'] == 'message':
            rload = json.loads(ret['data'])
            if 'id' in rload and 'retcode' in rload and\
                rload['retcode'] == 0:
                payload[rload['id']] = [rload['retcode'], rload['return']]
                return payload

def check_redisreturns(channel, redistimeout=5):
    """check each redis instances for latestes messages
    - matched_minions: minions that were originally targeted and which we
      expected to see the returns come back from
    """
    count = 0.0
    redispubsub = setup_pubsubredis(channel)
    time.sleep(5)
    start_time = time.time()
    elapsed_time = 0
    while elapsed_time < float(redistimeout):
        for pubsub in redispubsub:
            ret = pubsub.get_message()
            elapsed_time = time.time() - start_time
            yield check_return(ret)

def setup_pubsubredis(channel):
    """set up redis pub/sub
    """
    redispubsub = []
    try:
        for site in REDIS_SITE:
            rinst = redis.StrictRedis(**site)
            pubsub = rinst.pubsub()
            pubsub.subscribe(channel)
            redispubsub.append(pubsub)
        return redispubsub
    except redis.exceptions.RedisError:
        logger.critical(
            'channel: %s, Error connecting to redis instance %s',
            channel, sys.exc_info()[0])
        print "Error connecting to redis instance {0}".format(sys.exc_info()[0])


def processResult(fun, myReturn, myRet, myFormat="txt"):
    """print out the returns from the task
    - fun: name of the function that was executed
    - myReturn: return from Redis
    - myRet: list of myreturn to be passed back
    - myFormat: format of the output
    """
    printFormat = getattr(eval('salt.output.{0}'.format(myFormat)), 'output')
    for item in myReturn:
        if item:
            if fun != 'match':
                print printFormat(item)
            myRet.append(item)


def worker(tasks, results):
    """Creates salt instance workers
    - tasks: queue that receives arguement from the user
    - results: queue that receives returns from the worker
    """
    tries = SALTCALL_RETRY
    tqueue = tasks.get()
    # creates channel id to be used pub/sub from redis
    chan = salt.utils.jid.gen_jid() + "-" + current_process().name
    # Sometimes salt cant seem to create its salt instances trying tries times
    while tries > 0:
        try:
            mysaltcall = Saltwrapper(tqueue.get('user'), tqueue.get('debug'), timeout=tqueue.get('timeout'), channel=chan)
            mysaltcall.send_event(tag=tqueue.get('tag'), tgt_type=tqueue.get('tgt_type'), target=tqueue.get('target'), fun=tqueue.get('fun'), extra=tqueue.get('extra'))
            break
        except:
            logger.critical(
                'salt-wrapper: Error creating salt-call instances, try %d with channel %s', tries, chan)
            tries -= 1
            if tries == 0:
                raise "Unable to instantiate Saltwrapper class"
            else:
                pass

    start_time = time.time()
    myret = []
    elapsed_time = 0
    # run the sub for the duration of timeout so we dont wait forever for bad minion returns
    while elapsed_time < float(tqueue.get('timeout')):
        myreturn = check_redisreturns(chan, redistimeout=tqueue.get('timeout'))
        # print out as soon as sub is showing them
        processResult(tqueue.get('fun'), myreturn, myret, myFormat=tqueue.get('output_format'))
        elapsed_time = time.time() - start_time

    # Send the result back
    results.put([current_process().name, tqueue, chan, 20, "-", myret])


def setupWorker(numofworker, myTasks, myResults):
    """Set up  mulitple workers
    - myTasks: Queue for passing arguments
    - myResults: Queue for sending returns back
    """

    workers = [Process(target=worker, args=(myTasks, myResults)) for i in range(numofworker)]
    for each in workers:
        each.start()

def submitTask(args, myTasks):
    """Send the arguments pased from the user to the worker
    - args: user arguements
    - myTasks: Queue for passing arguments
    """
    if args.extra:
        extra = shlex.split(args.extra)
    else:
        extra = ["noextra"]
    if 'SUDO_USER' in os.environ:
        myuser = os.environ['SUDO_USER']
    else:
        myuser = 'root'

    myTarget = [args.target, args.tgt_type]

    """we can possibly run more than 2 jobs but for this user case, we will always run 2 jobs
       returnlistminion job will always run once to ensure that we know how many returns are expected; One of the cons of using Syndics + 0Q
    """
    # one of the tag should be used to return the matched minions from the saltmaster and syndics
    # fun is a name of fuction this tag calls in the reactor.sls
    # second tag should be for your event you want to send to execute on the minions
    myList = [{'user': myuser, 'tag':'forreturningmatchedminion', 'tgt_type':'compound', 'target':'mysaltmasterandsyndics', 'fun':'functionname', 'extra':myTarget, 'timeout':args.timeout, 'debug':args.debug, 'output_format':args.output_format}, {'user': myuser, 'tag':args.tag, 'tgt_type':args.tgt_type, 'target':args.target, 'fun':args.fun, 'extra':extra, 'debug':args.debug, 'timeout':args.timeout, 'output_format':args.output_format}]

    # put the jobs in the queue
    for each in myList:
        myTasks.put(each)

def retrieveTask(numofworker, myResults, myFormat):
    """retrieve  the returns from the submitted jobs
    - numerofworker: counting how many jobs we submitted
    - myResults: Queue for passing on returns
    - myFormat: format in which the outputs are printed
    """
    count = 0
    funret = 0
    funName = 'default'
    retSet = set()
    actSet = set()
    while numofworker:
        result = myResults.get(timeout=60)
        if result[1]['fun'] == 'match':
            for ret in result[5]:
                for item in ret:
                    if ret[item][1]:
                        count += len(ret[item][1])
                        for minion in ret[item][1]:
                            actSet.add(minion)
        else:
            for ret in result[5]:
                if ret:
                    funret += 1
                    for minion in ret:
                        retSet.add(minion)
            channel = result[2]
            funName = result[1]['fun']
        numofworker -= 1
    if (count == 0 and funret == 0) or funret < count:
        myrets = []
        myreturn = check_redisreturns(channel, redistimeout=25)
        processResult(funName, myreturn, myrets, myFormat=myFormat)
        for myret in myrets:
            if myret:
                count += 1
                for minion in myret:
                    retSet.add(minion)

    diffSet = actSet - retSet
    return [funret, count, diffSet]

def startEvents(args):
    """start it UP!
    - args: all the goodies user has put put in
    """

    myTasks = Queue()
    myResults = Queue()
    # limit workers to 2 for now
    numofworker = 2

    setupWorker(numofworker, myTasks, myResults)
    submitTask(args, myTasks)
    actualCount, expectCount, diffSet = retrieveTask(numofworker, myResults, args.output_format)

    if args.summary:
        print "Number of minions returned: {0}".format(actualCount)
        print "Expected return minions was: {0}".format(expectCount)
        if len(diffSet) > 0:
            print "Didn't receive returns from the followin minion:"
        for minion in diffSet:
            print minion

