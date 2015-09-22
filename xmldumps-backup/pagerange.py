import getopt
import os
import re
import sys
import time
import WikiDump
import bz2
import worker
import CommandManagement

from CommandManagement import CommandPipeline, CommandSeries, CommandsInParallel
from worker import Runner

class PageRange(object):
    """Methods for getting number of revisions per page, 
    estimating how many revisions a consecutive number of pages contains
    given a certain starting page ID,  estimating how long it will take
    to retrieve a certain number of revisions starting from a certain
    revisionID
    We use this for splitting up history runs into small chunks to be run in 
    parallel, with each job taking roughly the same length of time.
    Note that doing a straight log curve fit doesn't work; it's got to be done 
    by approximation.
    Speed of retrieval of revisions depends on revision size and whether it's
    prefetchable (in the previous dump's file) or must be retrieved from the
    external store (database query)."""
    def __init__(self, dbname, config): 
        """Arguments:
        dbname -- the name of the database we are dumping
        config -- this is the general config context used by the runner class."""
        self._dbname = dbname
        self._config = config
        self._totalPages = None
        # this is the number of pages we typically poll when we get estimated revs per page
        # at some pageID
        self._sampleSize = 500 
        self.getNumPagesInDB()

    def getNumPagesInDB(self):
        pipeline = []
        pipeline.append([ "echo", '$dbr = wfGetDB( DB_SLAVE ); $count = $dbr->selectField( "page", "max(page_id)", false ); if ( intval($count) > 0 ) { echo intval($count); }' ])
        pipeline.append([ '%s' % self._config.php, '%s/maintenance/eval.php' % config.wikiDir , '%s' % self._dbname])
        p = CommandPipeline(pipeline, quiet=True)
        p.run_pipeline_get_output()
        if not p.exited_successfully():
            print "DEBUG: serious error encountered (1)"
            return None
        output = p.output()
        output = output.rstrip('\n')
        if (output != ''):
            self._totalPages = int(output)
            return output

    def runDBQueryAndGetOutput(self,query):
        pipeline = []
        pipeline.append( query )
        pipeline.append([ '%s' % self._config.php, '%s/maintenance/eval.php' % self._config.wikiDir, '%s' % self._dbname])
        p = CommandPipeline(pipeline, quiet=True)
        p.run_pipeline_get_output()
        output = p.output().rstrip('\n')
        return(output)

    def estimateNumRevsPerPage(self, pageID):
        """Get the number of revisions for self._sampleSize pages starting at a given pageID.
        Returns an estimated number of revisions per page based on this.
        This assumes that the older pages (lower page ID) have
        generally more revisions."""

        pageRangeStart = str(pageID)
        pageRangeEnd = str(pageID + self._sampleSize)
        query = [ "echo", '$dbr = wfGetDB( DB_SLAVE ); $count = $dbr->selectField( "revision", "COUNT(distinct(rev_page))", array( "rev_page < ' + pageRangeEnd + '", "rev_page >=  ' + pageRangeStart + '" ) ); echo $count;' ]
#        query = [ "echo", '$dbr = wfGetDB( DB_SLAVE ); $result = $dbr->query( "SELECT COUNT(*) FROM ( SELECT "revision", "COUNT(*)", array( "rev_page < ' + pageRangeEnd + '", "rev_page >=  ' + pageRangeStart + '" ) ); echo $count;' ]
        print query
        rowCount = self.runDBQueryAndGetOutput(query)
        rowCount = int(rowCount)
        if (rowCount > 0):
#            limit = str(rowCount*9/10)
            limit = str(rowCount)
            print "there were %s many rows in the request, getting %s" % (rowCount, limit)
            queryString = "select avg(a.cnt) as avgcnt from (select  count(rev_page) as cnt from revision  where rev_page >=" + pageRangeStart + " and rev_page < " + pageRangeEnd + " group by rev_page order by cnt asc limit " + limit + ") as a;"
            query = [ "echo", '$dbr = wfGetDB( DB_SLAVE ); $res = $dbr->query( "' + queryString  + '" ); if ($res && $dbr->numRows( $res ) > 0) { while( $row = $dbr->fetchObject( $res ) ) { echo $row->avgcnt; } }' ]
            average = self.runDBQueryAndGetOutput(query)
            if (average):
                revsPerPage = int(round(float(average)))
                if (revsPerPage == 0):
                    revsPerPage = 1
                print "got average of %s revs per page at pageid %s" % (revsPerPage, pageID)
                print "that amounts to ", revsPerPage * self._sampleSize
                return (revsPerPage, rowCount)
        return (None, None)

    def getEstimatedRevsForIntervalFromEndpoints(self, revsPage0, revsPage1, pageID0, pageID1, pagesP0, pagesP1):
        """given the revs per page estimate at each endpoint, and the number of undeleted pages out of the sample size
        (self._sampleSize is the sample size), figure out the estimated revs for the interval"""

        estimatedPagesInInterval = self.getEstimatedUndeletedPagesInInterval(pageID0, pageID1, pagesP0, pagesP1)
        # now we can get a notion of how many revs might be in the interval

        # FIXME is this always what we want, is the average? or do we want it form the greater endpoint? or what?
        estimatedRevsInInterval =  abs((revsPage0 + revsPage1) * estimatedPagesInInterval /2)
        return(estimatedRevsInInterval)

    def getEstimatedUndeletedPagesInInterval(self, pageID0, pageID1, pagesP0, pagesP1):
        """given the number of undeleted pages at both P0 and P1 for our standard sample size self._sampleSize, 
        guesstimate the number of undeleted pages for the whole interval"""
        # guess at number of undeleted pages in the interval
        estimatedPagesInInterval = int(round((pageID1 - pageID0) * (pagesP0 + pagesP1)/(2*self._sampleSize)))
        print "estimatedPagesInInterval %s" % estimatedPagesInInterval
        return(estimatedPagesInInterval)

    def checkEstimatedRevsAgainstErrorMargin(self, revsPage0, revsPage1, pageID0, pageID1, pagesP0, pagesP1, errorMargin):
        """Decide if the estimated number of revs is within our 
        margin of error"""

        # fixme call the previous function to do this

        # guess at number of undeleted pages in the interval
        estimatedPagesInInterval = int(round((pageID1 - pageID0) * (pagesP0 + pagesP1)/self._sampleSize))
        # now we can get a notion of how many revs might be in the interval
        if abs((revsPage0 - revsPage1) * estimatedPagesInInterval) < errorMargin:
            return True
        return False

    def getErrorMarginForInterval(self, pageIDStart, pageIDEnd, maxRevs = None):
        if (not maxRevs):
            errorMargin = round((pageIDEnd - pageIDStart)*5/100)
        else:
            # get three samples, take the min revs per page, 
            # guess based on that where pageIDEnd ought to be as a max cutoff, 
            # set error margin from that
            
            intervalSize = pageIDEnd - pageIDStart
            print "%s %s %s" % (pageIDStart, pageIDStart + intervalSize/10, pageIDStart + intervalSize/5)
            (sample1, pcount1) = self.estimateNumRevsPerPage(pageIDStart)
            (sample2, pcount2) = self.estimateNumRevsPerPage(round(pageIDStart + intervalSize/10))
            (sample3, pcount3) = self.estimateNumRevsPerPage(round(pageIDStart + intervalSize/5))
            print "%s/%s, %s/%s, %s/%s" % (sample1, pcount1, sample2, pcount2, sample3, pcount3)

            if not sample1 or not sample2 or not sample3:
                errorMargin = round((pageIDEnd - pageIDStart)*5/100)
            else:
                revsPerPage = min(sample1,sample2,sample3)
                undeletedPagesPerSample = min(pcount1, pcount2, pcount3)
                newIntervalSize = (maxRevs/revsPerPage)*(undeletedPagesPerSample/self._sampleSize)
                newPageIDEnd = pageIDStart + newIntervalSize
                if newPageIDEnd < pageIDEnd:
                    newPageIDEnd = pageIDEnd
                errorMargin = round((newPageIDEnd - pageIDStart)*5/100)
            if errorMargin < self._sampleSize:
                errorMargin = sampleSize

    def estimateNumRevsForPageRange(self, pageIDStart, pageIDEnd, maxRevs = None):
        """estimate the cumulative number of revisions for a given page interval.
        if the parameter maxRevs is supplied, stop when we get to that point
        (within margin of error of it anyways).
        return (revisions, page id of upper end of interval)"""

        # error margin has to make sense given the interval size; too small and we will never
        # get an estimate that meets it, too large and our estimate will have no value

        errorMargin = self.getErrorMarginForInterval(pageIDStart, pageIDEnd, maxRevs)

        print "pageIDEnd is %s" % pageIDEnd

        if (pageIDEnd + self._sampleSize > self._totalPages):
            pageIDEnd = self._totalPages - self._sampleSize
        else:
            print "pageIDEnd + self._sampleSize < self._totalPages", pageIDEnd + self._sampleSize, self._totalPages

        if (pageIDEnd < pageIDStart):
            pageIDEnd = pageIDStart

        print "estimateNumRevsForPageRange:", pageIDStart, pageIDEnd, self._totalPages
        if (pageIDEnd - pageIDStart) < self._sampleSize:
            # just take the estimate for revs at pageIDStart, call it good
            print "estimateNumRevsForPageRange: initial pageend is close enough to pagestart to quit"
            (estimate, pages) = self.estimateNumRevsPerPage(pageIDStart)
            if (estimate):
                return (estimate*pages,pageIDStart+self._sampleSize)
            else:
                return(None, None)
        (estimateP0, pagesP0) = self.estimateNumRevsPerPage(pageIDStart)
        if (not estimateP0):
            return (None, None)
        if estimateP0 * pagesP0 > maxRevs:
            # we're already over.  too bad, report it back,
            # we just don't do fine grained enough estimates for this case whatever it is
            return (estimateP0 * pagesP0,pageIDStart+self._sampleSize)
        else:
            print "estimateP0 %s is less than maxRevs %s" % (estimateP0, maxRevs)

        (estimatePN, pagesPN) = self.estimateNumRevsPerPage(pageIDEnd)
        if (not estimatePN):
            return (None, None)

        # fixme put these comments somewhere useful
        # on the one hand we want revs per page
        # on the other hand we want pages not deleted out of the 500, these are both useful numbers

        if self.checkEstimatedRevsAgainstErrorMargin(estimateP0, estimatePN, pagesP0, pagesPN, pageIDStart, pageIDEnd, errorMargin):
            print "estimateNumRevsForPageRange: our first two estimates are close enough together to quit"
            print "they are %s and %s for page ids %s and %s respectively" % (estimateP0, estimatePN, pageIDStart, pageIDEnd)
            return (estimateP0*(pageIDEnd - pageIDStart), pageIDEnd)
        # main loop, here's where we have to do real work
        pageIDTemp = pageIDEnd
        tempMargin = errorMargin
        numintervals = 1

        i=0 # debug

        while True:
            i = i + 1 # debug
            tempMargin = tempMargin/2
            # FIXME this means that our final estimate may be outside the error margin
            if (tempMargin < 1):
                tempMargin = 1
            pageIDTemp = (pageIDTemp - pageIDStart)/2 + pageIDStart
            if pageIDTemp - self._sampleSize> self._totalPages:
                pageIDTemp = self._totalPages - self._sampleSize
            if pageIDTemp < pageIDStart:
                # FIXME we really need to do something more with this case...
                pageIDTemp = pageIDStart

            numIntervals = numintervals *2
            (estimateP0, pagesP0) = self.estimateNumRevsPerPage(pageIDStart)
            (estimatePN, pagesPN) = self.estimateNumRevsPerPage(pageIDTemp)
            if (not estimateP0 or not estimatePN):
                return (None, None)
            # the "distance less than self._sampleSize" clause is just a catchall in case the slope of the
            # curve is so steep that we can't get a good estimate within the margin of error...
            # in which case we have the absolute number (revs for 500 pages at p0) and we use it
            if self.checkEstimatedRevsAgainstErrorMargin(estimateP0, estimatePN, pageIDStart, pageIDTemp, pagesP0, pagesPN, tempMargin) or (pageIDTemp - pageIDStart < self._sampleSize):
                print "estimateNumRevsForPageRange: estimate of 1st interval close enough on %sth iteration for estimates %s and %s at %s and %s, tempmargin %s" %( i, estimateP0, estimatePN, pageIDStart, pageIDTemp, tempMargin) 
                step = pageIDTemp - pageIDStart
                if (step < self._sampleSize):
                    step = self._sampleSize
                (estimatePI1, pagesPI1) = self.estimateNumRevsPerPage(pageIDStart)
                if (not estimatePI1):
                    return (None, None)
                totalEstimate = 0
                print "have estimate %s at %s (pageIDStart)" % (estimatePI1, pageIDStart)

                pageI = pageIDStart
                while (pageI <= pageIDEnd):
                    (estimatePI2, pagesPI2) = self.estimateNumRevsPerPage(pageI+step)
                    if (not estimatePI2):
                        return None
                    print "have estimate %s at %s, step is %s and we added it to %s" % (estimatePI2, pageI+step, step, pageI)
                    print "*******estimatePI1, estimatePI2, pageI, pageI+step,  pagesPI1, pagesPI2:", estimatePI1, estimatePI2, pageI, pageI+step,  pagesPI1, pagesPI2
                    estimate = self.getEstimatedRevsForIntervalFromEndpoints(estimatePI1, estimatePI2, pageI, pageI+step,  pagesPI1, pagesPI2)

                    if (maxRevs):
                        # FIXME do we know we are within the margin of error? ummmm

                        if (totalEstimate + estimate > maxRevs):
                            print "about to return with totalEstimate %s + estimate %s > maxRevs %s" % (totalEstimate, estimate, maxRevs)
                            if (totalEstimate):
                                return (totalEstimate, pageI)
                            else:
                                return (estimate, pageI)

                    # since the number of revs decreases as the page id increases, 
                    # eventually our interval size can get larger too and still
                    # keep us within the margin of error

                    undeletedPagesInInterval = self.getEstimatedUndeletedPagesInInterval(pageI, pageI+step, pagesPI1, pagesPI2)
                    if (estimatePI2 == estimatePI1):
                        multiplier = 2
                    else:
                        multiplier = int( abs ( tempMargin/( (estimatePI2*undeletedPagesInInterval) - (estimatePI1*undeletedPagesInInterval) ) ) )

                    # check if this makes sense with the multiplier, is that really how we get it?
                    if multiplier > 1:
                        print "got multiplier %s from tempMargin %s and estimateP1 %s, estimateP2 %s, undelPagesInInterval %s, step currently %s" % (multiplier, tempMargin, estimatePI1, estimatePI2, undeletedPagesInInterval, step)
                        step = step * multiplier
                        print "step now adjusted to %s" % step
                        # FIXME is this right?
                        tempMargin = tempMargin * multiplier
                        # redo the estimate so it matches the new step size I guess
                        estimate = self.getEstimatedRevsForIntervalFromEndpoints(estimatePI1, estimatePI2, pageI, pageI+step,  pagesPI1, pagesPI2)

                    print "*******added %s to totalestimate %s for %s" % ( estimate, totalEstimate, totalEstimate + estimate )
                    totalEstimate = totalEstimate + estimate

                    estimatePI1 = estimatePI2
                    pageI = pageI + step
                    pagesPI1 = pagesPI2

                return (totalEstimate, pageIDEnd)

    def getPageEndForNumRevsFromPageStart(self, pageIDStart, numRevs):
        """given a starting pageID and the number of revs we want
        in the page range, find an ending page ID so that the cumulative number of revs 
        for the pages in that interval is around (and less than) numRevs"""
        if not self._totalPages:
            print "getPageEndForNumRevsFromPageStart: calling getNumPagesInDB"
            if not self.getNumPagesInDB():
                # something wrong with this db or the server or... anyways, we bail
                return None

        # can't have more revs than pages.  well actually
        # we can since some pages might have been deleted and then
        # their ids are no longer in the page table. But how are
        # the odds that there will be a bunch of those *and* that there 
        # will be less pages than that with more than one revision?  
        # so screw it.
        pageIDMax = pageIDStart + numRevs
        print "getPageEndForNumRevsFromPageStart: got total pages %s" % self._totalPages
        if pageIDMax > self._totalPages:
            pageIDMax = self._totalPages
        (estimatedRevs, pageIDEnd) = self.estimateNumRevsForPageRange(pageIDStart,pageIDMax,numRevs)
        return(pageIDEnd)

if __name__ == "__main__":
    
    config = WikiDump.Config()
    testchunks = PageRange('enwiki',config)
    # test with 5000 error margin
    wiki = WikiDump.Wiki(config, 'enwiki')
    date = None
    checkpoint = None
    prefetch = False
    spawn = False
    jobRequested = None
#    runner = Runner(wiki, date, checkpoint, prefetch, spawn, jobRequested)
    
    numPages = testchunks.getNumPagesInDB()
    if not numPages:
        print ">>>>>>>>>failed to retrieve number of pages for db elwikidb"
    else:
        print ">>>>>>>>>total number of pages: %s" % numPages

#    pageIDStart = 20000
#    pageIDEnd = 110000
    pageIDStart = 2000000
    pageIDEnd = 3000000
    maxNumRevs = 2500000
#    maxNumRevs = 25000
#    (revcount, pageID) = testchunks.estimateNumRevsForPageRange(pageIDStart, pageIDEnd, maxNumRevs)
#    print ">>>>>>>>>got revcount", revcount, "for range (%s, %s) ending now at %s" % ( pageIDStart, pageIDEnd, pageID)

    maxNumRevs = 30000000
    pageIDStart = 1
    pageIDEnd = 2969038
    endpageid = 0
    while (endpageid < numPages):
        endpageid = testchunks.getPageEndForNumRevsFromPageStart(pageIDStart, maxNumRevs)
        print ">>>>>>>>>we think that starting from", pageIDStart, "if you go to about ", endpageid, "you get around ", maxNumRevs, "revisions and not more than that (not more? really?)"
        pageIDStart = endpageid
        

