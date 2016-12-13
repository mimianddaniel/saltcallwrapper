"""
This module allow user to send Salt event to salt master and can be used to
retrieve returns from redis instances
"""
import salt.client
import salt.config
import salt.utils.jid
import salt.log
import salt.output
from salt.exceptions import SaltException
import redis
import time
import json
import sys
import os
import math
import shlex
from multiprocessing import Process, current_process

LOGFILE = 'file:///dev/log'
LOGLEVEL = 'info'
LOGFMT = 'salt-wrapper-%(processName)s:[%(process)d] %(message)s'
# logging to minion.log in format that ES understands; all salt logs in ES
salt.log.setup_logfile_logger(LOGFILE, LOGLEVEL, LOGFMT)

# salt.log.setup_logfile_logger declared prior to import logging to avoid
# overwritting
import logging  # pylint: disable=wrong-import-position
logger = logging.getLogger('salt-wrapper')

# list of REDIS that will are used as returners
REDISLHR = {
    'host': 'saltmaster-204.lhr4.prod.booking.com',
    'port': 6379,
    'db': 0,
}

REDISLHR2 = {
    'host': 'saltmaster-205.lhr4.prod.booking.com',
    'port': 6379,
    'db': 0,
}

REDISAMS = {
    'host': 'saltmaster-104.ams4.prod.booking.com',
    'port': 6379,
    'db': 0,
}

REDISAMS2 = {
    'host': 'saltmaster-105.ams4.prod.booking.com',
    'port': 6379,
    'db': 0,
}

REDIS_SITE = [REDISLHR, REDISAMS, REDISLHR2, REDISAMS2]
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
            master='saltmaster.prod.booking.com',
            timeout=5):
        """set up initial salt object setting plus redis instances that will be looked up
        - user: name of the user who executed the command, will be used for sending jabber message
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
            print ("Unable to connect to the saltmaster with exception {0}".format(sys.exc_info()[0]))
            raise err
        self.user = user
        self.debug = debug
        logger.debug(
            'channel %s, New Salt wrapper created by user %s',
            self.channel, self.user)

    def send_event(self, **kwargs):
        """send the salt event up to the master
        """
        # tag, tgt_type, tgt, function_name(not modulename),
        logger.debug(
            'channel %s, About to send the event up to the master %s\n',
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
                    'argument': kwargs.get('extra'),
                    'batchsize': kwargs.get('batchsize')
                    },
                    with_pillar=['roles']
                )
        except (SaltException) as err:
            logger.critical(
                'channel %s, Unable to connect to the saltmaster with exception %s',
                self.channel,
                sys.exc_info()[0])
            print ("Unable to connect to the saltmaster with exception {0}".
                    format(sys.exc_info()[0]))
            raise err
        logger.info('%s:%s',kwargs.get('tag'), 'Success')
        logger.debug(
            'Event sent succesfully with channel %s, Tag %s, Target %s, Function %s, User %s, Argument %s',
            self.channel,
            kwargs.get('tag'),
            kwargs.get('target'),
            kwargs.get('fun'),
            self.user,
            kwargs.get('extra'))

def submitTask(args, myTasks):
    """Send the arguments pased from the user to the worker
    - args: user arguements
    - myTasks: Queue for passing arguments
    """
    if args['argument'] and not args['literal']:
        argument = shlex.split(args.pop('argument'))
    # for cmdmod module, all arguments are passed as it is
    elif args['literal']:
        argument = [args.pop('argument')]
    else:
        argument = ["noargument"]

    if 'SUDO_USER' in os.environ:
        myuser = os.environ['SUDO_USER']
    else:
        myuser = 'root'

    if not args['target']:
        target = "PREDEFINED"
    else:
        target = args.pop('target')

    myTarget_type = args.pop('tgt_type')
    myDebug = args.pop('debug')
    myTimeout = args.pop('timeout')
    myFormat = args['output_format']
    myTarget = [target, myTarget_type]
    mybatchsize = args.pop('batchsize')

    """we can possibly run more than 2 jobs but for this user case, we will always run 2 jobs
       returnlistminion job will always run once to ensure that we know how many returns are expected; One of the cons of using Syndics + 0Q
    """
    myList = [ {
                'user': 'root',
                'tag':'salt/booking/func/returnlistminion',
                'tgt_type':'compound',
                'target':'pcsaltmaster-* or saltmaster-* and not saltmaster-10[12]*',
                'fun':'match',
                'argument':myTarget,
                'timeout':myTimeout,
                'debug':myDebug,
                'output_format':myFormat,
                'batchsize':mybatchsize
                },
                {'user': myuser,
                'tag':args.pop('tag'),
                'tgt_type':myTarget_type,
                'target':target,
                'fun':args.pop('fun'),
                'argument':argument,
                'debug':myDebug,
                'timeout':myTimeout,
                'output_format':myFormat,
                'batchsize':mybatchsize
                }
            ]

    # put the jobs in the queue
    for each in myList:
        myTasks.put(each)

def signal_handler(signu, frame):
    logger.critical('Saltwrapper interrupted by SIGINT')
    sys.exit(0)

def check_return(rets):
    """check the redis return messages per poll against the initial targeted hosts
    - ret: return from redis
    - matched_minions: list of minions that were initially matched targets
    - return_minion: minions that have published returns after the execution
    """
    rload = {}
    payload = {}

    if isinstance(rets, list):
        for ret in rets[0]:
            myload = json.loads(ret)
            if 'id' in myload and 'retcode' in myload:
                payload[myload['id']] = [myload['jid'], myload['retcode'], myload['return']]
        return payload

def check_redisreturns(redispubsub, channel, debug, redistimeout=10):
    """check each redis instances for latestes messages
    - matched_minions: minions that were originally targeted and which we
      expected to see the returns come back from
    """
    run_time = 0
    elapsed_time = 0
    start_time = time.time()
    exe_time = 0
    total_timelimit = (float(redistimeout) * 1.5)
    # lets wait 1 second since most of the jobs run will take ~1sec to come
    # back
    time.sleep(1)
    # retrieve messages from the redis instances
    while elapsed_time < float(redistimeout):
        for pubsub in redispubsub:
            # going to assume something big number
            rets = multi_pop(pubsub, channel, 10000)
            if debug:
                print ("return {0}".format(rets))
            # only when return list is not empty
            if rets[0]:
                # reset the run time of each returns
                # will timeout when there are no more data
                run_time = time.time()
                yield check_return(rets)
            else:
                # ensure to decrement time when there are no data
                if run_time > 0:
                    #slowly reducing seconds
                    elapsed_time = time.time() - run_time
                    run_time -= 0.01
                else:
                    # this only applies when there are no data or due to bad
                    # target regex; ensure that loop will timeout
                    exe_time = time.time() - start_time
            if debug:
                print ("run_time {0} elapsed_time {1}".format(run_time, elapsed_time))
        # If we have reached here, there was no data returned and passed normal
        # time out; forceably stopping the loop
        if debug:
            print ("exe_time {0} and redistimeout {1}".format(exe_time, redistimeout))

        if exe_time > total_timelimit:
            logger.critical('Saltwrapper running longer than timeout force\
                    exiting loop')
            break
        # no need to poll so frequently
        time.sleep(0.1)

def multi_pop(pubsub, chan, nevent):
    """get multiple items from the list
    - pubsub: redis connection
    - chan: name of the key
    - nevent: number of events to get
    """
    p = pubsub.pipeline()
    p.multi()
    # get as many as nevents
    # http://redis.io/commands/lrange
    p.lrange(chan, 0, nevent - 1)
    p.ltrim(chan, nevent, -1)
    return p.execute()

def setup_pubsubredis():
    """set up redis pub/sub
    """
    redispubsub = []
    start_time = time.time()
    #logger.debug('Starting channel %s redis subscribe time %d', channel, start_time)
    for site in REDIS_SITE:
        try:
            rinst = redis.StrictRedis(**site)
            # ensure that redis instance is pingable before adding to the list
            rinst.ping()
            redispubsub.append(rinst)
        except redis.exceptions.RedisError:
            logger.critical(
                'Error connecting to redis instance %s', sys.exc_info()[0])
            print ("Error connecting to redis instance {0}".
                    format(sys.exc_info()[0]))
            # we still want to continue
            continue
    return redispubsub


def processResult(fun, myReturn, myRet, totalRet,  myFormat="txt"):
    """print out the returns from the task
    - fun: name of the function that was executed
    - myReturn: return from Redis
    - myRet: list of myreturn to be passed back
    - myFormat: format of the output
    """
    opts = salt.config.minion_config('/etc/salt/minion')
    myJID = ""
    for item in myReturn:
        # keep all the returns
        totalRet.append(item)
        for rethost in item:
            if rethost:
                myJID, retCode, result = item[rethost]
            if fun != 'match':
                withoutJID = {rethost:[retCode, result]}
                salt.output.display_output(withoutJID, myFormat, opts)
                myRet.append(rethost)
            else:
                if type(result) == list:
                    for host in result:
                        if not host in myRet:
                            myRet.append(host)
                else:
                    if not result in myRet:
                        myRet.append(result)

    return myJID

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
            mysaltcall = Saltwrapper(
                    tqueue.get('user'),
                    tqueue.get('debug'),
                    timeout=tqueue.get('timeout'),
                    channel=chan
                    )
            mysaltcall.send_event(
                    tag=tqueue.get('tag'),
                    tgt_type=tqueue.get('tgt_type'),
                    target=tqueue.get('target'),
                    fun=tqueue.get('fun'),
                    extra=tqueue.get('argument'),
                    batchsize=tqueue.get('batchsize')
                    )
            # no need to run more than once
            break
        except KeyboardInterrupt:
            logger.critical(
                'KeyBoard interrupt while creating salt instance for channel %s', chan)

        except (SaltException) as err:
            logger.critical(
                'Saltwrapper rrror creating salt-call instances, try %d with channel %s', tries, chan)
            tries -= 1
            if tries == 0:
                raise err
            else:
                pass
        print ("Salt can not reach the masters, please retry again. Team\
        infra.dio will be alerted if there are high number of the alerts")
    # collect all the returns

    redispubsub = setup_pubsubredis()
    myreturn = check_redisreturns(
            redispubsub,
            chan,
            tqueue.get('debug'),
            redistimeout=tqueue.get('timeout')
            )
    # keep all the returns
    totalReturn = []
    # keep only the hosts
    myret = []
    # set myJID as default with channel name
    # print out as soon as sub is showing them
    myJID = processResult(
            tqueue.get('fun'),
            myreturn,
            myret,
            totalReturn,
            myFormat=tqueue.get('output_format')
            )

    if not myJID:
        myJID = chan

    # Send the result back
    results.put([current_process().name,
        tqueue,
        chan,
        myJID,
        "-",
        myret,
        totalReturn
        ])


def setupWorker(numofworker, myTasks, myResults):
    """Set up  mulitple workers
    - myTasks: Queue for passing arguments
    - myResults: Queue for sending returns back
    """

    workers = [Process(target=worker, args=(myTasks, myResults)) for i in range(numofworker)]
    for each in workers:
        each.start()

def retrieveTask(numofworker, myResults, myFormat):
    """retrieve  the returns from the submitted jobs
    - numerofworker: counting how many jobs we submitted
    - myResults: Queue for passing on returns
    - myFormat: format in which the outputs are printed
    """
    exCount = 0
    retCount = 0
    funName = 'default'
    retSet = set()
    actSet = set()

    def parseResult(fun, myresult):
        count = 0
        for item in myresult:
            count += 1
            if fun == 'match':
                actSet.add(item)
            else:
                retSet.add(item)
        return count

    while numofworker:
        try:
            result = myResults.get(timeout=300)
        except Empty:
            logger.info('No result has come back in 1 minute bailing;\
                    most likely ran into a msgpack bug on the saltmaster,\
                    please try running again')
            sys.exit(1)

        if result[1]['fun'] == 'match':
            exCount = parseResult(result[1]['fun'], result[5])
            exChannel = result[2]
            exfunName = result[1]['fun']
        else:
            actTarget = [result[1]['target'], 'compound']
            retCount = parseResult(result[1]['fun'], result[5])
            channel = result[2]
            funName = result[1]['fun']
            actJID = result[3]
            totalRet = result[6]
        numofworker -= 1

    # if exCount is zero it's most likely we are running into msgpack bug in salt
    # 'This often happens when trying to read a file not in binary mode.
    # Please open an issue and include the following error: unpack(b) received extra data.'
    # for the sake of user, run it again!
    if exCount == 0:
        logger.warn(
            'High actual value; Expected returns %d Actual return %d channel %s', exCount, retCount, actJID)
        retryChan = 'retry-' + exChannel
        mysaltcall = Saltwrapper(
                'root',
                'False',
                timeout=10,
                channel=retryChan
                )
        mysaltcall.send_event(
                tag='salt/booking/func/returnlistminion',
                tgt_type='compound',
                target='pcsaltmaster-* or saltmaster-* and not saltmaster-10[12]*',
                fun='match',
                extra=actTarget
                )
        myminRets = []
        mytotRets = []
        redispubsub = setup_pubsubredis()
        myminReturn = check_redisreturns(
                redispubsub,
                retryChan,
                None,
                redistimeout=15
                )
        processResult(
                exfunName,
                myminReturn,
                myminRets,
                mytotRets,
                myFormat=myFormat
                )
        exCount = parseResult('match', myminRets)
        logger.warn(
            'Try:2 Expected %d Actual return %d channel %s', exCount, retCount, actJID)

    # Expected - Returned results
    diffSet = actSet - retSet
    logger.debug('len of actSet %d len of retSet %d',len(actSet), len(retSet))
    # report the result to ES
    logger.info(
        'Job %s completed with %d missing returns', actJID, len(diffSet))
    return [totalRet, retCount, exCount, diffSet, actJID]
