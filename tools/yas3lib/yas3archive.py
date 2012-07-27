import os, re, sys, time, hashlib, hmac, binascii, httplib, getopt, yas3lib, urllib, json, ConfigParser, yas3http, xml.etree.ElementTree
from yas3lib import YaS3AuthInfo, YaS3Requester, YaS3UrlBuilder, YaS3ArbitraryUrl
from yas3 import YaS3Err, YaS3Handler, YaS3HandlerFactory, YaS3SessionInfo, YaS3Args, YaS3Lib
from yas3 import  YaS3ListBucketsHandler, YaS3ListOneBucketHandler, YaS3GetObjectHandler, YaS3CreateBucketHandler, YaS3UploadObjectHandler
from yas3 import YaS3GetObjectS3MetadataHandler, YaS3GetBucketS3MetadataHandler, YaS3DeleteObjectHandler, YaS3DeleteBucketHandler
from yas3 import YaS3StartMPUploadHandler, YaS3EndMPUploadHandler, YaS3AbortMPUploadHandler
from yas3http import YaS3HTTPDate, YaS3HTTPHeaders, YaS3HTTPCookie
from utils import Err, ErrExcept, PPXML

class YaS3IAMetadata(object):
    """
    Generate archive.org metadata header names
    """

    # source of meta header information: http://archive.org/help/contrib-advanced.php
    
    # required headers:
    #     x-archive-meta01-collection
    #     x-archive-meta-title
    #     x-archive-meta-mediatype
    #     x-archive-meta-description

    # recommended by archive.org:
    #     x-archive-meta-licenseurl
    #     x-archive-meta-format 

    # recommended by us:
    #     x-archive-meta-date
    #     x-archive-meta-subject

    # common mediatypes (almost comprehensive list, stolen from http://archive.org/advancedsearch.php):
    # audio data education image movies other software texts video web

    @staticmethod
    def getHeader(fieldName):
        """
        Return archive.org metadata header name for specific field

        Arguments:
        fieldName  -- name of metadata field
        """
        if fieldName == "collection":
            return "x-archive-meta01-collection"
        else:
            return "x-archive-meta-" + fieldName

class YaS3IAAuthInfo(YaS3AuthInfo):
    """
    Methods for S3 authorization and authentication to 
    archive.org for non S3 requests

    """

    def __init__(self, accessKey, secretKey, authType, username, password):
        """
        Constructor

        Arguments:
        username    -- username for non S3 requests to archive.org
        password    -- password for non S3 requests to archive.org
        """
        super(YaS3IAAuthInfo,self).__init__(accessKey, secretKey, authType)
        self.username = username
        self.password = password

class YaS3IAObjectMetadataUrl(YaS3UrlBuilder):
    """
    Methods for producing or retrieving the base url for retrieving archive.org metadata for an object
    """

    def __init__(self, bucketName = None, remoteFileName = None, virtualHost = False):
        """
        Constructor

        Arguments:
        bucketName      -- name of bucket (item) containing object (file)
        remoteFileName  -- name of object (file)
        """
        self.bucketName = bucketName
        self.remoteFileName = remoteFileName

    # example: (http://archive.org)  /details/elwiktionary-dumps?output=json
    def buildUrl(self):
        """
        Return the base url for getting archive.org metadata for an object (file)
        """
        elts = filter(None, [self.bucketName.lstrip("/") if self.bucketName else None, self.remoteFileName])
        if not len(elts):
            return None
        else:
            return "/details/" + "/".join(elts) + "?output=json"

class YaS3IAFilesXMLUrl(YaS3UrlBuilder):
    """
    Methods for producing or retrieving the base url for retrieving the *_files.xml file from 
    archive.org for given bucket (item)
    """

    def __init__(self, bucketName = None):
        """
        Constructor

        Arguments:
        bucketName  -- name of bucket from which to get the *_files.xml file
        """
        self.bucketName = bucketName

    def buildUrl(self):
        """
        Return the base url for getting the <bucketname>_files.xml file from archive.org for
        a given bucket (item)
        """
        # fixme what happens if these filenames have utf8 in them?? check
        return "/download/%s/%s_files.xml" % (self.bucketName, self.bucketName)

class YaS3IAShowBucketStatusUrl(YaS3UrlBuilder):
    """
    Methods for producing or retrieving the base url for showing the status of jobs
    for the specified bucket on archive.org
    """

    def __init__(self, bucketName = None):
        """
        Constructor

        Arguments:
        bucketName  -- name of bucket of which to show the job status
        """
        self.bucketName = bucketName

    def buildUrl(self):
        """
        Return the base url for getting the html output listing jobs that have
        run or are scheduled to run for the specific bucket (item) on archive.org
        """
        return "/catalog.php?history=1&identifier=%s" % ( self.bucketName )

class YaS3IAHandler(YaS3Handler):
    """
    Base handler class for archive.org S3 or non-S3 operations
    """

    def getUserAgent(self):
        """
        Return a string with the value that will be put in the UserAgent HTTP header
        """
        return "yas3archive.py/0.2-pre (from yet another s3 library)"

class YaS3IAShowBucketStatusHandler(YaS3IAHandler):
    """
    Handler for requests for job status for a given bucket
    This is not an S3 request and it is specific to archive.org only
    """

    """Show the status of an item (bucket): which objects (files) are waiting
    on further action from archive.org."""
    mandatory = [ "username", "password", "host", "bucketName" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3IAShowBucketStatusHandler,self).__init__(ops, args, s3Sess)
        self.connection.resetHostAndPort(self.argDict["host"], self.argDict["port"]) # must reset host too
        self.reqType = "GET"

    def doLogin(self):
        """
        Log into archive.org using non-S3 credentials, get login cookies back
        and stash them in the session
        """
        s3 = self.ops["login"][0](self.ops, self.args, self.s3Sess)
        s3.setUp()

        result = s3.runS3()
        if self.argDict["verbose"]:
            print "login result status is", result.status
            print "login cookies are:"
            for c in self.s3Sess.cookies:
                c.displayCookie()

    def runS3(self):
        """ 
        Do a single S3 request, following Location header for 307 response codes 
        """
        self.setUrlInfo(YaS3IAShowBucketStatusUrl(self.argDict["bucketName"]))

        result = self.doS3()
        if self.mustRedoReqAfterRedir(result):
            # not sure about stash/restore here... generally check all those FIXME 
            result = self.doS3()
        elif self.mustDoGetReqAfterRedir(result):
            result = self.doS3(True)
        return result

    def doS3(self, redir = False):
        """
        Do a single request for job status for one bucket, without following Location header
        This is a non S3 request with no authentication and with html output which we parse
        """
        if not redir:  # get creds
            # fixme we should only do this if our existing creds aren't good
            # so write something to check them
            self.doLogin()

        headerInfo = self.setStandardHeaders()
        self.setOtherHeaders(headerInfo, auth = False)

        if not self.s3Req:
            self.s3Req = YaS3IARequester(self.reqType, self.argDict["verbose"], self.argDict["quiet"], self.argDict["raw"])

        reply, html = self.s3Req.makeLoggedInGetRequest(self.connection, headerInfo, self.urlInfo)
        self.s3Sess.data = html
        self.showBucketStatusFromHtml()
        self.restoreUrlInfo()
        return reply

    # fixme yas3Connection is global or local to each s3 instance? check this! should be in session
    # somehow.

    def showBucketStatusFromHtml(self):
        """
        """
        """Wade through the html output to find information 
        about each job we have requested and its status.
        THIS IS UGLY and guaranteed to break in the future
        but hey, there's no json output available, nor xml."""
        if self.argDict["quiet"]:
            return

        if not self.s3Sess.data:
            return

        if self.argDict["raw"]:
            print self.s3Sess.data
            return

        text = self.s3Sess.data

        htmlTagExpr = re.compile("(<[^>]+>)+")
        # get the headers for the table of tasks; we will find something that looks like
        #
        # <tr><th><b><a href="/catalog.php?history=1&identifier=test-bucket-atg&sort=identifier&sortOrder=1">identifier</a></b></th>
        # <th><b><a href="/catalog.php?history=1&identifier=test-bucket-atg&sort=task_id&sortOrder=1">task_id</a></b></th>
        # <th><b><a href="/catalog.php?history=1&identifier=test-bucket-atg&sort=server&sortOrder=1">server</a></b></th>
        # <th><b><a href="/catalog.php?history=1&identifier=test-bucket-atg&sort=cmd&sortOrder=1">cmd</a></b></th>
        # <th><b><a href="/catalog.php?history=1&identifier=test-bucket-atg&sort=submittime&sortOrder=1">submittime</a></b></th>
        # <th><b><a href="/catalog.php?history=1&identifier=test-bucket-atg&sort=submitter&sortOrder=1">submitter</a></b></th>
        # <th><b><a href="/catalog.php?history=1&identifier=test-bucket-atg&sort=args&sortOrder=1">args</a></b></th>
        #
        # get identifier, task_id etc out of there
        start = text.find('<tr><th><b><a href="/catalog.php?history=1&identifier=')
        if start >= 0:
            # table starts after <!--task_ids: nnnnnnnn -->  where the nnnnnnn (digits) may be missing
            end = text.find("<!--task_ids: ",start)
            content = text[start:end]
            lines = content.split("</th>")
            lines = [ re.sub(htmlTagExpr,'',line).strip() for line in lines if line.find('<th><b><a href="/catalog.php?history=1&identifier=') != -1 ]
            print " | ".join(filter(None, lines))

        # get the tasks themselves; we will find something like (this has been reformatted with addition/removal of newlines for readability):
        #
        # <tr class="evenH">
        #   <td><a href="http://archive.org/details/test-bucket-atg">test-bucket-atg</a> - <a href="/catalog.php?history=1&identifier=test-bucket-atg"><font size="1">History</font></a> <a href="http://archive.org/item-mgr.php?identifier=test-bucket-atg"><font size="1">Mgr</font></a> </td>
        #   <td><a href="http://www.us.archive.org/log_show.php?task_id=112275150">112275150</a></td>
        #   <td>ia600604.us.archive.org</td><td>archive.php</td>
        #   <td><span class="catHover2"><span class="catHidden">UTC 2012-07-11 23:39:51</span><nobr>(59.0 seconds)</nobr></span></td>
        #   <td>ariel@wikimedia.org</td>
        #   <td>          <span class="catHover"><span class="catHidden"><b>u p d a t e _ m o d e </b> = &gt; 1 <br/><b>c o m m e n t </b> = &gt; s 3 - p u t <br/><b>d e l e t e _ f r o m _ s o u r c e </b> = &gt; 2 <br/><b>f r o m _ u r l </b> = &gt; r s y n c : / / h 1 . w w w 3 7 . s 3 d n s / s 3 2 / s 3 - a d c 1 3 9 2 4 - c 4 9 f - 4 9 d 3 - a 3 f 8 - 8 4 8 8 6 7 5 e a 7 8 6 - t e s t - b u c k e t - a t g / <br/><b>n e x t _ c m d </b> = &gt; d e r i v e <br/><b>d i r </b> = &gt; / 1 / i t e m s / t e s t - b u c k e t - a t g <br/><b>d o n e </b> = &gt; d e l s r c <br/></span>from_url=rsync://h1.www37..</span></td>
        # </tr>
        #
        # get...
        
        start = text.find("<!--task_ids: ")
        if start < 0:
            raise ErrExcept("Can't locate the beginning of the item status information in the html output.")
        end = text.find("</table>",start)
        content = text[start:end]
        lines = content.split("</tr>")

        for line in lines:
            line = line.replace("\n",'')
            cells = line.split("</td>")
            cellsToPrint = [ re.sub(htmlTagExpr,'',self.stripHidden(cell)).strip() for cell in cells ]
            print " | ".join(filter(None,cellsToPrint))

    def stripHidden(self, cell):
        """
        """
        start = cell.find('<span class="catHidden">')
        if start != -1:
            index = start + 1
            openTags = 1
            spanOpenOrCloseExpr = re.compile("(<span[^>]+>|</span>)")
            # find index of first occurrence and what was matched, so we can see if it was open or close tag 
            while openTags:
                spanMatch = spanOpenOrCloseExpr.search(cell[index:])
                if spanMatch:
                    tagFound = spanMatch.group(1)
                    index = spanMatch.start(1) + index
                    if tagFound == "</span>":
                        openTags = openTags -1
                    else:
                        openTags = openTags + 1
                else:
                    # bad html. just toss the rest of the cell 
                    openTags = 0
                    index = -1
            # now we have the index where we found the close tag for us. 
            # toss everything up to that, we'll lose the actual close tag when we 
            # toss the rest of the html
            cell = cell[:start] + cell[index:]
        return cell

class YaS3IACheckBucketExistenceHandler(YaS3IAHandler):
    """
    Handler for requests to check if a given bucket exists or not
    This is a HEAD request via S3 but not an official S3 operation
    """

    mandatory = [ "s3Host", "bucketName" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3IACheckBucketExistenceHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "HEAD"

    def runS3(self):
        """ 
        Do a single S3 request, following Location header for 307 response codes 
        """
        self.setUrlInfo(YaS3UrlBuilder(self.argDict["bucketName"], virtualHost = self.argDict["virtualHost"]))

        return self.runWithRedirs()

    def doS3(self):
        """
        Do a single check that a bucket exists (by HEAD on the bucket), without following Location header
        """
        headerInfo = self.setStandardHeaders()
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3IARequester(self.reqType, self.argDict["verbose"], self.argDict["quiet"], self.argDict["raw"])

        reply, data = self.s3Req.makeAnonHeadRequest(self.connection, headerInfo, self.urlInfo)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

class YaS3IAGetFilesXMLHandler(YaS3IAHandler):
    """
    Handler for requests to get the <bucketname>_files.xml file which for
    the given bucket, which has nifty things like the md5 of each object
    (file) in the bucket
    This is not an S3 request
    """
    mandatory = [ "host", "bucketName" ]
        
    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3IAGetFilesXMLHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "GET"
        self.connection.resetHostAndPort(self.argDict["host"], self.argDict["port"])

    def runS3(self):
        """ 
        Do a single S3 request, following redirects
        """
        self.setUrlInfo(YaS3IAFilesXMLUrl(self.argDict["bucketName"]))

        result =  self.runWithRedirs()
        self.displayXmlData()
        return result

    def displayXmlData(self):
        """
        Dig out the relevant bits from (successful) XML response body and
        display to stdout
        """
        if self.argDict["quiet"]:
            return

        if not self.s3Sess.data:
            return

        if self.argDict["raw"]:
            print self.s3Sess.data
            return

        tree =  xml.etree.ElementTree.fromstring(self.s3Sess.data)
        elts = tree.findall('file')

        for i in elts:
            print "%s   " % "file",
            print "name:%s|%s" % ( i.attrib["name"], "|".join([ "%s:%s" % (j.tag, j.text) for j in list(i) ]))

    def doS3(self):
        """
        """
        headerInfo = self.setStandardHeaders()
        self.setOtherHeaders(headerInfo, auth = False)

        if not self.s3Req:
            self.s3Req = YaS3IARequester(self.reqType, self.argDict["verbose"], self.argDict["quiet"], self.argDict["raw"])

        reply, data = self.s3Req.makeAnonGetRequest(self.connection, headerInfo, self.urlInfo)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

class YaS3IAVerifyObjectHandler(YaS3IAHandler):
    """
    Handler for requests to verify an object in the specified bucket against the 
    local copy of the file, usin the md5 sum to compare.
    This is not an S3 request, it relies on both an S3 request and potentially a 
    non-S3 request to get the information
    """
    mandatory = [ "s3Host", "bucketName", "localFileName", "remoteFileName" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3IAVerifyObjectHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "HEAD"

    def runS3(self):
        """ 
        Do a single S3 request, following Location header for 307 response codes 
        """
        self.setUrlInfo(YaS3UrlBuilder(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["virtualHost"]))

        return self.runWithRedirs()

    def doS3(self):
        """
        Do a single verification of one object (using its md5) against a local copy, without following Location header
        """
        headerInfo = self.setStandardHeaders()
        self.setOtherHeaders(headerInfo, auth = False)

        if not self.s3Req:
            self.s3Req = YaS3IARequester(self.reqType, self.argDict["verbose"], self.argDict["quiet"], self.argDict["raw"])

        reply, data = self.s3Req.makeAnonHeadRequest(self.connection, headerInfo, self.urlInfo)
        self.s3Sess.data = data
        # if there is a location header, return right here, let the caller do something about the redirect
        if "location" in [ h for h, v in reply.getheaders() ]:
            return reply

        md5sumFromEtag = self.getEtagValue(reply.getheaders())
        if not md5sumFromEtag:
            if self.argDict["verbose"]:
                print("no Etag in server response")

            # try another approach specific to archive.org, to get the md5s. 
            # retrieve the <bucketname>_files.xml file and poke around in there
            quiet = self.argDict["quiet"] # save the old value
            self.argDict["quiet"] = True # verbose overrides this, but otherwise (normal run) we don't want the output
            s3 = self.ops["getfilesxml"][0](self.ops, self.args, self.s3Sess)
            s3.setUp()
            reply = s3.runS3()
            self.argDict["quiet"] = quiet # and restore the old value after the sub-operation runs
            if self.s3Sess.data:
                tree =  xml.etree.ElementTree.fromstring(self.s3Sess.data)
                flist = tree.findall('file')
                for f in flist:
                    if f.get("name") == self.argDict["remoteFileName"]:
                        md5elt = f.find("md5")
                        if md5elt is not None:
                            md5sumFromEtag = md5elt.text
        md5sumFromLocalFile = self.localFile.getMd5()
        if self.argDict["verbose"]:
            print "Etag: ", md5sumFromEtag, "md5 of local file: ", md5sumFromLocalFile
        if md5sumFromEtag == md5sumFromLocalFile:
            if self.argDict["verbose"]:
                print "File verified ok."
        else:
            print("File verification FAILED.")

        self.restoreUrlInfo()
        return reply

    def getEtagValue(self, headers):
        """
        """
        # specific to archive.org only!
        for h, v in headers:
            if h == "etag":
                etag = v.strip('"')
            elif h == "x-ias3-multipart-upload-id":
                # this means the etag isn't the md5 so we try something else
                return None
        return None

class YaS3IAGetBucketIAMetadataHandler(YaS3IAHandler):
    """
    Handler for requests to get the archive.org metadata for
    the specific bucket (item)
    This is not an S3 request
    """

    mandatory = [ "host", "bucketName" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3IAGetBucketIAMetadataHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "GET"
        self.connection.resetHostAndPort(self.argDict["host"], self.argDict["port"])

    def showBucketMetadataFromJson(self):
        """
        Grab the metadata for a bucket (item) from the json for the bucket details
        (contains lost of other cruft) and display to stdout
        """
        if self.argDict["quiet"]:
            return

        if not self.s3Sess.data:
            return

        if self.argDict["raw"]:
            print self.s3Sess.data
            return

        jsonString = self.s3Sess.data
        # sample output
        #"metadata":{ 
        #   "identifier":["elwiktionary-dumps"],
        #   "description":["Dumps of el.wiktionary created by the Wikimedia Foundation and downloadable from http:\/\/dumps.wikimedia.\org\/elwiktionary\/"],
        #   "language":["el (Modern Greek)"],
        #   "licenseurl":["http:\/\/wikimediafoundation.org\/wiki\/Terms_of_Use"],
        #   "mediatype":["web"],
        #   "subject":["xml,dump,wikimedia,el,wiktionary"],
        #   "title":["Wikimedia database dumps of el.wiktionary, format:xml and sql"],
        #   "publicdate":["2012-02-17 11:03:45"],
        #   "collection":["opensource"],
        #   "addeddate":["2012-02-17 11:03:45"]},
        details = json.loads(jsonString)
        if "metadata" in details.keys():
            metadata = details["metadata"]
            print "Bucket metadata for", self.argDict["bucketName"]
            for k in details["metadata"].keys():
                print "%s:" % k,
                print " | ".join(details["metadata"][k])
        else:
            print "No metadata for", self.bucketName, "is available."

    def runS3(self):
        """ 
        Do a single S3 request, following Location header for 307 response codes 
        """
        self.setUrlInfo(YaS3IAObjectMetadataUrl(self.argDict["bucketName"], self.argDict["remoteFileName"], virtualHost = self.argDict["virtualHost"]))

        return self.runWithRedirs()

    def doS3(self):
        """
        Do a single retrieval of metadata for one bucket, without following Location header
        This is not an S3 request.  No authentication is required, and the data returned
        is json output.
        """
        headerInfo = self.setStandardHeaders()
        self.setOtherHeaders(headerInfo, auth = False)

        if not self.s3Req:
            self.s3Req = YaS3IARequester(self.reqType, self.argDict["verbose"], self.argDict["quiet"], self.argDict["raw"])

        reply, json = self.s3Req.makeAnonGetRequest(self.connection, headerInfo, self.urlInfo)
        self.s3Sess.data = json
        self.showBucketMetadataFromJson()
        self.restoreUrlInfo()
        return reply

class YaS3IACreateBucketWithIAMetadataHandler(YaS3IAHandler):
    """
    Handler for S3 requests to create buckets (items) with metadata specific
    to archive.org (via x-archive-X headers), also adding the archive.org
    size hint header if the estimated bucketsize in bytes is provided.
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "collectionHdr", "titleHdr", "mediatypeHdr", "descriptionHdr" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops             -- list of predefined operations from a HandlerFactory
        args            -- YaS3Args object
        s3Sess          -- YaS3SessonInfo object
        """
        super(YaS3IACreateBucketWithIAMetadataHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "PUT"

    def runS3(self):
        """ 
        Do a single S3 request, following Location header for 307 response codes 
        """
        self.setUrlInfo(YaS3UrlBuilder(self.argDict["bucketName"], virtualHost = self.argDict["virtualHost"]))

        return self.runWithRedoRedirsOnly("302/303 redirect encountered on create bucket with ia metadata, giving up")

    def doS3(self):
        """
        Do a single S3 create bucket request, adding in special metadata headers for 
        archive.org (x-archive-X), without following Location header.
        """
        headerInfo = self.setStandardHeaders()
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3IARequester(self.reqType, self.argDict["verbose"], self.argDict["quiet"], self.argDict["raw"])

        reply, data = self.s3Req.makeRequest(self.connection, headerInfo, self.urlInfo)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

    def setOtherHeaders(self, headerInfo, auth = True):
        """
        """
#        self.setAwsAuthHeader(headerInfo)
        for (name, value) in args.getIAMetadataArgs():
            if value:
                headerInfo.addHeader(YaS3IAMetadata.getHeader(name), value)
        if self.argDict["bucketSize"]:
            headerInfo.addHeader("x-archive-size-hint", self.argDict["bucketSize"])
        super(YaS3IACreateBucketWithIAMetadataHandler,self).setOtherHeaders(headerInfo, auth)

class YaS3IAUpdateBucketIAMedataHandler(YaS3IACreateBucketWithIAMetadataHandler):
    """
    Handler for S3 requests to update already existing buckets (items) with metadata specific
    to archive.org (via x-archive-X headers). Note that all archive.org metadata that exists
    is replaced by the new metadata; if a fild is not updated it will bre removed. (Don't
    blame the library, blame archive.org :-P)
    """

    def setOtherHeaders(self, headerInfo, auth = True):
        """
        """
        headerInfo.addHeader("x-archive-ignore-preexisting-bucket", "1")
        super(YaS3IAUpdateBucketIAMedataHandler,self).setOtherHeaders(headerInfo, auth)

class YaS3IALoginHandler(YaS3IAHandler):
    """
    Handler for requests to log in to archive.org and get login cookies for
    non-S3 requests.
    """

    mandatory = [ "username", "password", "host" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3IALoginHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "POST"
        self.connection.resetHostAndPort(self.argDict["host"], self.argDict["port"]) # must reset host too

    # form elements: username (actual account name), password (actual account password)
    # remember   ('CHECKED')  submit ('Log in')
    # want to pass cooike  'test-cookie=1' and toss all output that is not headers
    # (if successful)

    def httpencode(self, paramsdict):
        """
        """
        params = []
        for name in paramsdict.keys():
            params.append("%s=%s" % (urllib.quote(name), urllib.quote(paramsdict[name])))
        return "&".join(params)

    def runS3(self):
        """ 
        Do a single S3 request, following Location header for 307 response codes 
        """
        # change this to suit your derived class 
        self.setUrlInfo(YaS3ArbitraryUrl("/account/login.php"))

        result = self.doS3()
        if self.mustRedoReqAfterRedir(result):
            # not sure about stash/restore here... generally check all those FIXME 
            result = self.doS3()
        elif self.mustDoGetReqAfterRedir(result):
            # it so happens that archive.org will 302 us to the index.php page
            # which we don't actually want/need for anything, so ignore
            pass
        return result

    def doS3(self):
        """
        Log in to archive.org using credentials for non-S3 requests and retrieve and store in
        session the login cookies for future requests, without following Location header
        This is not an S3 request.
        """
        headerInfo = self.setStandardHeaders()
        postBody = self.httpencode({"username": self.argDict["username"], "password": self.argDict["password"], "remember": "CHECKED", "submit": "Log in"})
        self.setOtherHeaders(headerInfo, len(postBody))
        
        if not self.s3Req:
            self.s3Req = YaS3IARequester(self.reqType, self.argDict["verbose"], self.argDict["quiet"], self.argDict["raw"])

        reply, data = self.s3Req.makeAnonPostRequest(self.connection, headerInfo, self.urlInfo, postBody)
        self.s3Sess.data = data
        cookies = self.getLoginCookies(reply)
        if not len(cookies) or str(reply.status).startswith("4") or str(reply.status).startswith("5"):
            Err.whine("Login failed.")
        self.s3Sess.setCookies(cookies)
        self.restoreUrlInfo()
        return reply

    def setOtherHeaders(self, headerInfo, contentLength, auth = False):
        """
        """
        headerInfo.addHeader("Cookie", "test-cookie=1")
        headerInfo.addHeader("Content-Type", "application/x-www-form-urlencoded")
        headerInfo.addHeader("Content-Length", contentLength)
        super(YaS3IALoginHandler,self).setOtherHeaders(headerInfo, auth)

    def getLoginCookies(self, reply):
        """
        """
        cookies = []
        lines = reply.msg.getallmatchingheaders("Set-cookie")
        for l in lines:
            if (not len(l)) or (l[0] == '#'):
                continue
            # example cookie: 'Set-Cookie: PHPSESSID=jixxxjctopm35cfortx06xx8x0; path=/; domain=.archive.org\r\n'
            cookie = YaS3HTTPCookie(l.rstrip("\r\n"), self.connection.host, self.connection.protocol, self.urlInfo.buildUrl())
            if cookie.name == "logged-in-user" or cookie.name == "logged-in-sig":
                    cookies.append(cookie)
        return cookies

class YaS3IAUploadObjectHandler(YaS3UploadObjectHandler):
    """
    Handler for S3 requests to upload objects (files) to archive.org.
    This handler sets the special 'no-derive' header if requested, which 
    tells archive.org that it should not try to produce other formats of
    the uploaded data for download.  Text, tarballs and source code
    should use no-derive.  Sound and video files probably could use the
    conversion; see http://archive.org/about/faqs.php#235 and
    http://archive.org/about/faqs.php#236
    """

    def setOtherHeaders(self, headerInfo, md5, contentLength, auth = True):
        """
        """
        if self.argDict["noderive"]:
            headerInfo.addHeader("x-archive-queue-derive", "0")
        super(YaS3IAUploadObjectHandler,self).setOtherHeaders(headerInfo, md5, contentLength, auth)

class YaS3IAEndMPUploadHandler(YaS3EndMPUploadHandler):
    """
    Handler for S3 requests to complete multi-part uploads of objects (files) 
    to archive.org. This handler sets the special 'no-derive' header if requested, 
    which tells archive.org that it should not try to produce other formats of
    the uploaded data for download.  Text, tarballs and source code
    should use no-derive.  Sound and video files probably could use the
    conversion; see http://archive.org/about/faqs.php#235 and
    http://archive.org/about/faqs.php#236
    """

    # FIXME check that thi really stops the derive proceess from starting up
    # for mp uploads
    def setOtherHeaders(self, headerInfo, auth = True):
        """
        """
        if self.argDict["noderive"]:
            headerInfo.addHeader("x-archive-queue-derive", "0")
        super(YaS3IAEndMPUploadHandler,self).setOtherHeaders(headerInfo, auth)

class YaS3IAHandlerFactory(YaS3HandlerFactory):
    """
    Produce the right Handler object based on the S3 or non-S3 operation
    """

    introduction = [ "This library implements a subset of the S3 REST api for storage and",
                     "retrieval of objects in buckets, with additional non-s3",
                     "operations supported by the Internet Archive.  The Internet Archive",
                     "documentation and other interfaces use the terminology 'item' for 'bucket',",
                     "i.e. the container that holds content, and 'file' for 'object',",
                     "i.e. the content itself.  This library uses the standard S3 bucket/object",
                     "terminology but for archive.org-specific operations or extensions the",
                     "corresponding archive.org name will be given in parentheses."]

    # operation name for command line, handler name
    ops = YaS3HandlerFactory.ops
    ops["uploadobject"] = [ YaS3IAUploadObjectHandler, "upload an object (file) to the specified bucket (item)" ]
    ops["endmpupload"] = [ YaS3IAEndMPUploadHandler, "complete a multi-part upload of an object (file) to the specified bucket (item)" ]
    ops["login"] = [ YaS3IALoginHandler, "log in to archive.org using username and password, get associated login cookies" ]
    ops["getbucketiametadata"] = [ YaS3IAGetBucketIAMetadataHandler, "get the archive.org metadata (x-archive-meta-X) of a the specified bucket (item)" ]
    ops["showbucketstatus"] = [ YaS3IAShowBucketStatusHandler, "show archive.org most recent jobs (bup, derive, etc.) associated with specifed bucket (item)" ]
    ops["checkbucketexistence"] = [ YaS3IACheckBucketExistenceHandler, "check if the specified bucket (item) exists or not" ]
    ops["getfilesxml"] = [ YaS3IAGetFilesXMLHandler, "get the archive.org X_files.xml file associated with the specified bucket (item)" ]
    ops["verifyobject"] = [ YaS3IAVerifyObjectHandler, "check the md5sum of the specified object (file) against the specified local copy" ]
    for o in ["copyobject", "deletebucket"]:
        del ops[o]

    def __new__(cls, op, args, s3Sess):
        """
        Given the specific operation, return a new Handler object for that operation
        """
        if op in YaS3IAHandlerFactory.ops:
            return YaS3IAHandlerFactory.ops[op][0](YaS3IAHandlerFactory.ops, args, s3Sess)
        return None
        
class IAMetadataHandlerFactory(YaS3IAHandlerFactory):
    """
    Produce the right Handler object based on the S3 or non-S3 operation
    """

    introduction = [ "This library implements a subset of the S3 REST api for storage and",
                     "retrieval of objects in buckets, with additional non-s3",
                     "operations supported by the Internet Archive.  The Internet Archive",
                     "documentation and other interfaces use the terminology 'item' for 'bucket',"
                     "i.e. the container that holds content, and 'file' for 'object',"
                     "i.e. the content itself.  This library uses the standard S3 bucket/object",
                     "terminology but for archive.org-specific operations or extensions the",
                     "corresponding archive.org name will be given in parentheses.",
                     "Additional extensions include the maipulation of special metadata",
                     "information for archive.org buckets (items)."]

    # operation name for command line, handler name
    ops = YaS3IAHandlerFactory.ops
    ops["createbucketwithiametadata"] = [ YaS3IACreateBucketWithIAMetadataHandler, "create specified bucket (item) providing archive.org (x-archive-X) metadata" ]
    ops["updatebucketiametadata"] = [ YaS3IAUpdateBucketIAMedataHandler, "update the archive.org metadata (x-archive-X) for the specified bucket (item)" ]

    def __new__(cls, op, args, s3Sess):
        """
        Given the specific operation, return a new Handler object for that operation
        """
        if op in IAMetadataHandlerFactory.ops:
            return IAMetadataHandlerFactory.ops[op][0](IAMetadataHandlerFactory.ops, args, s3Sess)
        return None
        
class YaS3IAArgs(YaS3Args):
    """
    Manages arguments passed on the command line or in a config file
    for S3 and non-S3 requests to archive.org
    """

    def __init__(self):
        """
        Constructor

        Each list entry consists of: 
             variable name
             option name for command line/configfile
             True if the option name takes a string value, False if it's a boolean
             config file section name for option, or None for options that won't be read from the config file
             short description (used for help messages)
             default value, or None if there is no default
             short form of option (one letter) if any
        If the option name is "", the variable name is used as the option name as well
        """
        super(YaS3IAArgs,self).__init__()
        self.removeArgs(["sourceBucketName"])  # archive.org doesn't support COPY so his arg is useless
        self.args.extend([
                [ "username", "", True, "auth", "username for non-s3 requests", None, None ],
                [ "password", "", True, "auth", "password for non-s3 requests", None, None ],
                [ "host", "", True, "host", "host name for non s3 requests", "archive.org", None ],
                [ "noderive", "", False, "flags", "archive.org should not try to derive other formats from uploaded objects (files)", False, None ],
                [ "bucketSize", "bucketsize", True, "misc", "guess at ultimate bucket (item) size (used to set special archive.org header)", None, None ],
                # If you change the config file section for these from iametadata to something
                # else, change the method getIAMetadataArgs() below.
                [ "collectionHdr", "collection", True, "iametadata", "archive.org collection name for bucket (item)", None, None ],
                [ "titleHdr", "title", True, "iametadata", "archive.org title of bucket (item)", None, None ],
                [ "mediatypeHdr", "mediatype", True, "iametadata", "archive.org mediatype of contents of bucket (item)", None, None ],
                [ "descriptionHdr", "description", True, "iametadata", "archive.org description of contents of bucket (item)", None, None ],
                [ "licenseUrlHdr", "licenseurl", True, "iametadata", "archive.org url of license for contents of bucket (item)", None, None ],
                [ "formatHdr", "format", True, "iametadata", "archive.org file format of contents of bucket (item)", None, None ],
                [ "dateHdr", "date", True, "iametadata", "archive.org date of contents of bucket (item)", None, None ],
                [ "subjectHdr", "subject", True, "iametadata", "archive.org subject of contents of bucket (item)", None, None ]
                ])

    def getIAMetadataArgs(self):
        """
        Return a list of opt names (as they appear on command line or in config file)
        and values for allpredefined arguments that can appear in the 'iametadata'
        section of the config file.

        These are all turned into x-meta-headername and appear in the bucket x_meta.xml file
        on archive.org; they are also visible to the archive.org search engine.  For these arguments,
        the second field is not only the option name but also the name that appears in the 
        header.x-meta-headername.
        """
        return [ (self.getOptName(a), self.mergedDict[self.getVarName(a)]) for a in self.args if self.getConfSection(a) == "iametadata" ]

class YaS3IARequester(YaS3Requester):
    """
    Methods for making S3 and non-S3 requests to archive.org
    """

    def makeAnonGetRequest(self, c, headerInfo, urlInfo, contentLength=None):
        """
        Make a single anonymous (non authenticated) GET request to
        archive.org, and get headers and response back

        Arguments:
        c              -- YaS3Connection object
        headerInfo     -- YaS3HTTPHeaders object
        urlInfo        -- YaS3UrlBuilder object
        contentLength  -- content length of body to be sent, if any
        """
        self.sendReqFirstLine(c, urlInfo.buildUrl())
        self.putHeaders(c, headerInfo)

        reply = c.getresponse()
        headersReceived = reply.getheaders()
        data = self.getResponse(reply, headersReceived)
        return reply, data

    def makeAnonHeadRequest(self, c, headerInfo, urlInfo):
        """
        Make a single anonymous (non authenticated) HEAD request to
        archive.org, and get headers and response back

        Arguments:
        c              -- YaS3Connection object
        headerInfo     -- YaS3HTTPHeaders object
        urlInfo        -- YaS3UrlBuilder object
        """
        self.sendReqFirstLine(c, urlInfo.buildUrl())
        self.putHeaders(c, headerInfo)

        reply = c.getresponse()
        headersReceived = reply.getheaders()
        data = self.getResponse(reply, headersReceived)
        return reply, data

    def makeAnonPostRequest(self, c, headerInfo, urlInfo, postBody):
        """
        Make a single anonymous (non authenticated) POST request to archive.org,
        and get headers and response back

        Arguments:
        c              -- YaS3Connection object
        headerInfo     -- YaS3HTTPHeaders object
        urlInfo        -- YaS3UrlBuilder object
        postBody       -- text to be sent in request body
        """
        self.sendReqFirstLine(c, urlInfo.buildUrl())
        self.putHeaders(c, headerInfo)

        if postBody and self.reqType == "POST":
            if self.verbose:
                print "POST of >>>%s<<<" % postBody
            c.send(postBody)

        reply = c.getresponse()
        headersReceived = reply.getheaders()
        data = self.getResponse(reply, headersReceived)
        return reply, data

    def makeLoggedInGetRequest(self, c, headerInfo, urlInfo):
        """
        Make a single logged in (using archive.org session cookies)
        GET request to archive.org, and get headers and response back

        Arguments:
        c              -- YaS3Connection object
        headerInfo     -- YaS3HTTPHeaders object
        urlInfo        -- YaS3UrlBuilder object
        cookies        -- login session cookies
        """
        self.sendReqFirstLine(c, urlInfo.buildUrl())
        self.putHeaders(c, headerInfo)

        reply = c.getresponse()
        headersReceived = reply.getheaders()
        data = self.getResponse(reply, headersReceived)
        return reply, data

class YaS3IALib(YaS3Lib):
    """
    Set up YaS3IA library (S3 and non-S3 archive.org library) for
    processing a request, reading args from the command line
    and config file
    """

    def __init__(self, args, errors):
        """
        Constructor

        Arguments:
        args      -- YaS3Args object (contains predefined list of arguments)
        errors    -- YsS#Err object (for usage messages)
        """
        super(YaS3IALib,self).__init__(args, errors)

if __name__ == "__main__":
    """
    Command line client that will run any specified S3 or non S3 operation connecting to archive.org
    """

    args = YaS3IAArgs()
    s3lib = YaS3IALib(args, YaS3Err(args, IAMetadataHandlerFactory.ops, IAMetadataHandlerFactory.introduction))
    argDict = args.mergedDict

    s3lib.checkMissing([ "operation" ]) # the rest will be checked in the handlers

    s3Sess = YaS3SessionInfo(s3lib.errors)

    s3 = IAMetadataHandlerFactory(argDict["operation"], args, s3Sess)
    if not s3:
        s3lib.errors.usage("Unknown operation %s" % argDict["operation"])

    s3.setUp()

    result = s3.runS3()
    s3.tearDown()
    if result.status < 200 or result.status >= 400:
        print "result status is", result.status
        sys.exit(1)
