import os, re, sys, subprocess, multiprocessing, mirror
from subprocess import Popen, PIPE
from Queue import Empty
from mirror import MirrorMsg

class Job(object):
    def __init__(self, jobId, jobContents):
        self.jobId = jobId # this must be unique across all jobs
        self.contents = jobContents
        self.done = False
        self.failed = False

    def markDone(self):
        self.done = True

    def markFailed(self):
        self.failed = True

    def checkIfDone(self):
        return self.done

    def checkIfFailed(self):
        return self.failed

class JobHandler(object):
    def init(self):
        """this should be overriden to set and args
        that you need to actually process a job"""
        pass
    
    def doJob(self, contents):
        """override this with a function that processes
        contents as desired"""
        print contents
        return False

class JobQueueHandler(multiprocessing.Process):
    def __init__(self, jQ, handler, verbose, dryrun):
        multiprocessing.Process.__init__(self)
        self.jQ = jQ
        self.handler = handler
        self.verbose = verbose
        self.dryrun = dryrun

    def run(self):
        while True:
            job = self.jQ.getJobOnQueue()
            if not job: # no jobs left, we're done
                break
            self.doJob(job)

    def doJob(self, job):
        result = self.handler.doJob(job.contents)
        if result:
            job.markFailed()
        else:
            job.markDone()
        self.jQ.notifyJobDone(job)

class JobQueue(object):
    def __init__(self, initialWorkerCount, handler, timeout, verbose, dryrun):
        """create queue for jobs, plus specified
        number of workers to read from the queue"""
        self.handler = handler
        # how long the workers will wait for a job to show up on the todo queue
        # in seconds (default 60)
        self.timeout = timeout
        if not self.timeout:
            self.timeout = 60
        self.verbose = verbose
        self.dryrun = dryrun
        # queue of jobs to be done (all the info needed, plus job id)
        self.todoQueue = multiprocessing.Queue()

        # queue to which workers write job ids of completed jobs
        self.notifyQueue = multiprocessing.Queue()

        # this 'job' on the queue means there are no more
        # jobs. we put on of these on queue for each worker
        self.endOfJobs = None

        self._initialWorkerCount = initialWorkerCount
        self._activeWorkers= []
        if not self._initialWorkerCount:
            self._initialWorkerCount = 1
        if self.verbose or self.dryrun:
            MirrorMsg.display( "about to start up %d workers:" % self._initialWorkerCount )
        for i in xrange(0, self._initialWorkerCount):
            w = JobQueueHandler(self, self.handler, self.verbose, self.dryrun)
            w.start()
            self._activeWorkers.append(w)
            if self.verbose or self.dryrun:
                MirrorMsg.display( '.', True)
        if self.verbose or self.dryrun:
            MirrorMsg.display( "done\n", True)

    def getJobOnQueue(self):
        # after self.timeout seconds of waiting around we decide that
        # no one is ever going to put stuff on the queue
        # again.  either the main process is done filling
        # the queue or it died or hung

        try:
            job = self.todoQueue.get(timeout = self.timeout)
        except Empty: 
            if self.verbose or self.dryrun:
                MirrorMsg.display( "job todo queue was empty\n" )
            return False

        if (job == self.endOfJobs):
            if self.verbose or self.dryrun:
                MirrorMsg.display( "found jobs done marker on jobs queue\n" )
            return False
        else:
            if self.verbose or self.dryrun:
                MirrorMsg.display("retrieved from the job queue: %s\n" % job.jobId)
            return job
            
    def notifyJobDone(self, job):
        self.notifyQueue.put_nowait(job)

    def addToJobQueue(self,job=None):
        if (job):
            self.todoQueue.put_nowait(job)

    def setEndOfJobs(self):
        """stuff 'None' on the queue, so that when
        a worker reads this, it will clean up and exit"""
        for i in xrange(0,self._initialWorkerCount):
            self.todoQueue.put_nowait(self.endOfJobs)

    def getJobFromNotifyQueue(self):
        """see if any job has been put on
        the notify queue (meaning that it has
        been completed)"""
        jobDone = False
        # wait up to one minute.  after that we're pretty sure
        # that if there are no active workers there are no more
        # jobs that are going to get done either.
        try:
            jobDone = self.notifyQueue.get(timeout = 60)
        except Empty:
            if not self.getActiveWorkerCount():
                return False
        return jobDone

    def getActiveWorkerCount(self):
        self._activeWorkers = [ w for w in self._activeWorkers if w.is_alive() ]
        return len(self._activeWorkers)
