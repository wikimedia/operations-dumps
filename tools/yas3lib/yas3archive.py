import os, re, sys, time, hashlib, hmac, binascii, httplib, getopt, yas3lib, urllib, json, ConfigParser, yas3http, xml.etree.ElementTree
from yas3lib import YaS3AuthInfo, YaS3Requester, YaS3UrlInfo, YaS3ArbitraryUrl
from yas3 import YaS3Err, YaS3Handler, YaS3HandlerFactory, YaS3SessionInfo, YaS3RequestInfo, YaS3Args, YaS3Lib
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

class YaS3IASessionInfo(YaS3SessionInfo):
    """
    Session info and methods for archive.org sessions
    """

    def __init__(self, errors, s3Host, host):
        """
        Constructor

        Arguments:
        errors     -- YaS3Err object (for usage messages)
        s3Host     -- fqdn for host for s3 operations
        host       -- fqdn for hos for non-S3 operations
        """
        super(YaS3IASessionInfo,self).__init__(errors)
        self.connType = None # 's3' for s3 requests, or 'other' for everything else

        # need these member vars here so we can switch between them for setting the
        # host in the YaS3RequestInfo object
        self.host = host # for non s3 requests
        self.s3Host = s3Host # for s3 requess

    def setConnType(self, ctype):
        """
        Set connection type as specified, change name of remote host accordingly
        
        Arguments:
        ctype     -- "s3" for s3 operations, or "other" for everything else
        """
        # FIXME what about virtual host stuff?
        if ctype == "s3":
            self.connType = ctype
            if self.request:
                self.request.host = self.s3Host
        elif ctype == "other":
            self.connType = ctype
            if self.request:
                self.request.host = self.host
        else:
            raise ErrExcept("unknown connection type requested, %s " % ctype)

    def setHost(self, host):
        """
        Set session host information
        
        Arguments:
        host     -- fqdn of remote server
        """
        if self.connType == "s3":
            self.s3Host = host
        else:
            self.host = host
        self.request.host = host

    def getHost(self):
        """
        Return name of host in for current session connection type
        """
        if self.connType == "s3":
            return self.s3Host
        else:
            return self.host

class YaS3IAAuthInfo(YaS3AuthInfo):
    """
    Methods for S3 authorization and authentication to 
    archive.org for non S3 requests

    """

    def __init__(self, accessKey, secretKey, username, password):
        """
        Constructor

        Arguments:
        username    -- username for non S3 requests to archive.org
        password    -- password for non S3 requests to archive.org
        """
        super(YaS3IAAuthInfo,self).__init__(accessKey, secretKey)
        self.username = username
        self.password = password

class YaS3IAObjectMetadataUrl(YaS3UrlInfo):
    """
    Methods for producing or retrieving the base url for retrieving archive.org metadata for an object
    """

    def __init__(self, bucketName = None, remoteFileName = None):
        """
        Constructor

        Arguments:
        bucketName      -- name of bucket (item) containing object (file)
        remoteFileName  -- name of object (file)
        """
        self.bucketName = bucketName
        self.remoteFileName = remoteFileName
        self.url = self.getUrlBase()

    # example: (http://archive.org)  /details/elwiktionary-dumps?output=json
    def getUrlBase(self):
        """
        Return the base url for getting archive.org metadata for an object (file)
        """
        elts = filter(None, [self.bucketName.lstrip("/") if self.bucketName else None, self.remoteFileName])
        if not len(elts):
            return None
        else:
            return "/details/" + "/".join(elts) + "?output=json"

class YaS3IAFilesXMLUrl(YaS3UrlInfo):
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
        self.url = self.getUrlBase()

    def getUrlBase(self):
        """
        Return the base url for getting the <bucketname>_files.xml file from archive.org for
        a given bucket (item)
        """
        # fixme what happens if these filenames have utf8 in them?? check
        return "/download/%s/%s_files.xml" % (self.bucketName, self.bucketName)

class YaS3IAShowBucketStatusUrl(YaS3UrlInfo):
    """
    Methods for producing or retrieving the base url for showing the staatus of jobs
    for the specified bucket on archive.org
    """

    def __init__(self, bucketName = None):
        """
        Constructor

        Arguments:
        bucketName  -- name of bucket of which to show the job status
        """
        self.bucketName = bucketName
        self.url = self.getUrlBase()

    # example: (http://archive.org)  /details/elwiktionary-dumps?output=json
    def getUrlBase(self):
        """
        Return the base url for getting the html output listing jobs that have
        run or are scheduled to run for the specific bucket (item) on archive.org
        """
        return "/catalog.php?history=1&identifier=%s" % ( self.bucketName )

class YaS3IAHandler(YaS3Handler):
    """
    Base handler class for archive.org S3 or non-S3 operations
    """

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        # FIXME what did I need this class for again??
        super(YaS3IAHandler,self).__init__(ops, argDict, s3Sess)

class YaS3IAShowBucketStatusHandler(YaS3IAHandler):
    """
    Handler for requests for job status for a given bucket
    This is not an S3 request and it is specific to archive.org only
    """

    """Show the status of an item (bucket): which objects (files) are waiting
    on further action from archive.org."""
    mandatory = [ "username", "password", "host", "bucketName" ]

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3IAShowBucketStatusHandler,self).__init__(ops, argDict, s3Sess)
        self.s3Sess.setConnType("other") # this is not an s3 request
        self.connection.resetHostAndPort(self.s3Sess.getHost(), self.s3Sess.getPort()) # must reset host too
        self.reqType = "GET"

    def doLogin(self):
        """
        Log into archive.org using non-S3 credentials, get login cookies back
        and stash them in the session
        """
        s3 = self.ops["login"][0](self.ops, self.argDict, self.s3Sess)
        s3.setUp()

        result = s3.runS3()
        if self.argDict["verbose"]:
            print "login result status is", result.status
            print "login cookies are:"
            for c in self.s3Sess.cookies:
                c.displayCookie()
        if result.status == 307:
            s3.setupForNewLocation(result)
            result = s3.runS3()
            if self.argDict["verbose"]:
                print "cookies are now:"
                for c in self.s3Sess.cookies:
                    c.displayCookie()

    def doS3(self):
        """
        """
        """ordinary http request, no authentication, expect json output"""
        self.setUrlInfo(YaS3IAShowBucketStatusUrl(self.argDict["bucketName"]))
        self.doLogin()
        headerInfo = self.setHeadersWithNoAuth()
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3IARequester(self.reqType, self.argDict["verbose"], self.argDict["quiet"])

        reply, html = self.s3Req.makeLoggedInGetRequest(self.connection, headerInfo, self.s3Sess.urlInfo, self.s3Sess.cookies)
        self.s3Sess.data = html
        if html:
            self.showBucketStatusFromHtml(html)
        self.restoreUrlInfo()
        return reply

    # fixme yas3Connection is global or local to each s3 instance? check this! should be in session
    # somehow.

    def showBucketStatusFromHtml(self, text):
        """
        """
        """Wade through the html output to find information 
        about each job we have requested and its status.
        THIS IS UGLY and guaranteed to break in the future
        but hey, there's no json output available, nor xml."""
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
            if self.argDict["verbose"]:
                print "content is", content
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
    This is a HEAD request, not an official S3 operation
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName" ]

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3IACheckBucketExistenceHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "HEAD"

    def doS3(self):
        """
        """
        self.setUrlInfo(YaS3UrlInfo(self.argDict["bucketName"], virtualHost = self.argDict["virtualHost"]))
        headerInfo = self.setHeadersWithNoAuth()
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3IARequester(self.reqType, self.argDict["verbose"], self.argDict["quiet"])

        reply, data = self.s3Req.makeAnonHeadRequest(self.connection, headerInfo, self.s3Sess.urlInfo)
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
        
    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3IAGetFilesXMLHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "GET"
        self.s3Sess.setConnType("other") # this is not an s3 request
        self.connection.resetHostAndPort(self.s3Sess.getHost(), self.s3Sess.getPort())

    def doS3(self):
        """
        """
        self.setUrlInfo(YaS3IAFilesXMLUrl(self.argDict["bucketName"]))
        headerInfo = self.setHeadersWithNoAuth()
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3IARequester(self.reqType, self.argDict["verbose"], self.argDict["quiet"])

        reply, data = self.s3Req.makeAnonGetRequest(self.connection, headerInfo, self.s3Sess.urlInfo)
        if reply.status == 302:
            self.setupForNewLocation(reply)
            # have to redo these, host or something may be different
            headerInfo = self.setHeadersWithNoAuth()
            reply, data = self.s3Req.makeAnonGetRequest(self.connection, headerInfo, self.s3Sess.urlInfo)

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
    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "localFileName", "remoteFileName" ]

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3IAVerifyObjectHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "HEAD"

    def doS3(self):
        """
        """
        self.setUrlInfo(YaS3UrlInfo(argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["virtualHost"]))
        headerInfo = self.setHeadersWithNoAuth()
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3IARequester(self.reqType, self.argDict["verbose"], self.argDict["quiet"])

        reply, data = self.s3Req.makeAnonHeadRequest(self.connection, headerInfo, self.s3Sess.urlInfo)
        self.s3Sess.data = data

        md5sumFromEtag = self.getEtagValue(reply.getheaders())
        if not md5sumFromEtag:
            if self.argDict["verbose"]:
                print("no Etag in server response")

            # try another approach specific to archive.org, to get the md5s. 
            # retrieve the <bucketname>_files.xml file and poke around in there
            s3 = self.ops["getfilesxml"][0](self.ops, self.argDict, self.s3Sess)
            s3.setUp()
            reply = s3.runS3()
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
            raise ErrExcept("File verification FAILED.")

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

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3IAGetBucketIAMetadataHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "GET"
        self.s3Sess.setConnType("other") # this is not an s3 request
        self.connection.resetHostAndPort(self.s3Sess.getHost(), self.s3Sess.getPort())

    def showBucketMetadataFromJson(self, jsonString):
        """
        Grab the metadata for a bucket (item) from the json for the bucket details
        (contains lost of other cruft) and display to stdout
        """
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

    def doS3(self):
        """
        Do a single retrieval of metadata for one bucket, without following Location header
        This is not an S3 request.  No authentication is required, and the data returned
        is json output.
        """
        self.setUrlInfo(YaS3IAObjectMetadataUrl(self.argDict["bucketName"], self.argDict["remoteFileName"]))
        headerInfo = self.setHeadersWithNoAuth()
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3IARequester(self.reqType, self.argDict["verbose"], self.argDict["quiet"])

        reply, json = self.s3Req.makeAnonGetRequest(self.connection, headerInfo, self.s3Sess.urlInfo)
        self.s3Sess.data = json
        if json:
            self.showBucketMetadataFromJson(json)
        self.restoreUrlInfo()
        return reply

class YaS3IACreateBucketWithIAMetadataHandler(YaS3IAHandler):
    """
    Handler for S3 requests to create buckets (items) with metadata specific
    to archive.org (via x-archive-X headers), also adding the archive.org
    size hint header if the estimated bucketsize in bytes is provided.
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "collectionHdr", "titleHdr", "mediatypeHdr", "descriptionHdr" ]

    def __init__(self, ops, argDict, s3Sess, iAMetadataArgs):
        """
        Constructor

        Arguments:
        ops             -- list of predefined operations from a HandlerFactory
        argDict         -- dictionary of args (from command line/config file/defaults) and values
        s3Sess          -- YaS3SessonInfo object
        iAMetadataArgs  -- list of args from predefined argument list that are in config section 'iametadata'
                           (these would be used to construct headers to set archive.org metadata for a specified
                           bucket (item) on archive.org
        """
        super(YaS3IACreateBucketWithIAMetadataHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "PUT"
        self.iAMetadataArgs = iAMetadataArgs

    def doS3(self):
        """
        Do a single S3 create bucket request, adding in special metadata headers for 
        archive.org (x-archive-X), without following Location header.
        """
        self.setUrlInfo(YaS3UrlInfo(self.argDict["bucketName"], virtualHost = self.argDict["virtualHost"]))
        headerInfo = self.setHeadersWithAwsAuth()
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3IARequester(self.reqType, self.argDict["verbose"], self.argDict["quiet"])

        reply, data = self.s3Req.makeRequest(self.connection, headerInfo, self.s3Sess.urlInfo)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

    def setOtherHeaders(self, headerInfo):
        """
        """
        super(YaS3IACreateBucketWithIAMetadataHandler,self).setOtherHeaders(headerInfo)
        for (name, value) in self.iAMetadataArgs:
            if value:
                headerInfo.addHeader(YaS3IAMetadata.getHeader(name), value)
        if self.argDict["bucketSize"]:
            headerInfo.addHeader("x-archive-size-hint", self.argDict["bucketSize"])

class YaS3IAUpdateBucketIAMedataHandler(YaS3IACreateBucketWithIAMetadataHandler):
    """
    Handler for S3 requests to update already existing buckets (items) with metadata specific
    to archive.org (via x-archive-X headers). Note that all archive.org metadata that exists
    is replaced by the new metadata; if a fild is not updated it will bre removed. (Don't
    blame the library, blame archive.org :-P)
    """

    def setOtherHeaders(self, headerInfo):
        """
        """
        super(YaS3IAUpdateBucketIAMedataHandler,self).setOtherHeaders(headerInfo)
        headerInfo.addHeader("x-archive-ignore-preexisting-bucket", "1")

class YaS3IALoginHandler(YaS3IAHandler):
    """
    Handler for requests to log in to archive.org and get login cookies for
    non-S3 requests.
    """

    mandatory = [ "username", "password", "host" ]

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3IALoginHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "POST"
        self.s3Sess.setConnType("other") # this is not an s3 request
        self.connection.resetHostAndPort(self.s3Sess.getHost(), self.s3Sess.getPort()) # must reset host too

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

    def doS3(self):
        """
        Log in to archive.org using credentials for non-S3 requests and retrieve and store in
        session the login cookies for future requests, without following Location header
        This is not an S3 request.
        """
        self.setUrlInfo(YaS3ArbitraryUrl("/account/login.php"))
        headerInfo = self.setHeadersWithNoAuth()
        self.setOtherHeaders(headerInfo)
        postBody = self.httpencode({"username": self.argDict["username"], "password": self.argDict["password"], "remember": "CHECKED", "submit": "Log in"})
        headerInfo.addHeader("Content-Length", len(postBody))
        
        if not self.s3Req:
            self.s3Req = YaS3IARequester(self.reqType, self.argDict["verbose"], self.argDict["quiet"])

        reply, data = self.s3Req.makeAnonPostRequest(self.connection, headerInfo, self.s3Sess.urlInfo, postBody)
        self.s3Sess.data = data
        cookies = self.getLoginCookies(reply)
        if not len(cookies) or str(reply.status).startswith("4") or str(reply.status).startswith("5"):
            Err.whine("Login failed.")
        self.s3Sess.setCookies(cookies)
        self.restoreUrlInfo()
        return reply

    def setOtherHeaders(self, headerInfo):
        """
        """
        super(YaS3IALoginHandler,self).setOtherHeaders(headerInfo)
        headerInfo.addHeader("Cookie", "test-cookie=1")
        headerInfo.addHeader("Content-Type", "application/x-www-form-urlencoded")

    def getLoginCookies(self, reply):
        """
        """
        cookies = []
        lines = reply.msg.getallmatchingheaders("Set-cookie")
        for l in lines:
            if (not len(l)) or (l[0] == '#'):
                continue
            # example cookie: 'Set-Cookie: PHPSESSID=jixxxjctopm35cfortx06xx8x0; path=/; domain=.archive.org\r\n'
            cookie = YaS3HTTPCookie(l.rstrip("\r\n"), self.s3Sess.getHost(), self.s3Sess.getProtocol(), self.s3Sess.getUrl())
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

    def setOtherHeaders(self, headerInfo):
        """
        """
        super(YaS3IAUploadObjectHandler,self).setOtherHeaders(headerInfo)
        if self.argDict["noderive"]:
            headerInfo.addHeader("x-archive-queue-derive", "0")

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
    def setOtherHeaders(self, headerInfo):
        """
        """
        super(YaS3IAEndMPUploadHandler,self).setOtherHeaders(headerInfo)
        if self.argDict["noderive"]:
            headerInfo.addHeader("x-archive-queue-derive", "0")

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

    def __new__(cls, op, argDict, s3Sess):
        """
        Given the specific operation, return a new Handler object for that operation
        """
        if op in YaS3IAHandlerFactory.ops:
            return YaS3IAHandlerFactory.ops[op][0](YaS3IAHandlerFactory.ops, argDict, s3Sess)
        return None
        
class IAMetadataHandlerFactory(YaS3IAHandlerFactory):
    """
    Produce the right Handler object based on the S3 or non-S3 operation, passing in extra
    args for archive.org metadata to the few Handlers that need it
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

    def __new__(cls, op, argDict, s3Sess, iAMetadataArgs):
        """
        Given the specific operation, return a new Handler object for that operation,
        passing in args for archive.org metadata to the few Handlers that need it
        """
        if op in IAMetadataHandlerFactory.ops:
            if op == "createbucketwithiametadata" or op == "updatebucketiametadata":
                return IAMetadataHandlerFactory.ops[op][0](IAMetadataHandlerFactory.ops, argDict, s3Sess, iAMetadataArgs)
            else:
                return IAMetadataHandlerFactory.ops[op][0](IAMetadataHandlerFactory.ops, argDict, s3Sess)
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
        If the option name is "", the variable name is used as the option name as well
        """
        super(YaS3IAArgs,self).__init__()
        self.args.extend([
                [ "username", "", True, "auth", "username for non-s3 requests", None ],
                [ "password", "", True, "auth", "password for non-s3 requests", None ],
                [ "host", "", True, "host", "host name for non s3 requests", "archive.org" ],
                [ "noderive", "", False, "flags", "archive.org should not try to derive other formats from uploaded objects (files)", False ],
                [ "bucketSize", "", True, "misc", "guess at ultimate bucket (item) size (used to set special archive.org header)", None ],
                # If you change the config file section for these from iametadata to something
                # else, change the method getIAMetadataArgs() below.
                [ "collectionHdr", "collection", True, "iametadata", "archive.org collection name for bucket (item)", None ],
                [ "titleHdr", "title", True, "iametadata", "archive.org title of bucket (item)", None ],
                [ "mediatypeHdr", "mediatype", True, "iametadata", "archive.org mediatype of contents of bucket (item)", None ],
                [ "descriptionHdr", "description", True, "iametadata", "archive.org description of contents of bucket (item)", None ],
                [ "licenseUrlHdr", "licenseurl", True, "iametadata", "archive.org url of license for contents of bucket (item)", None ],
                [ "formatHdr", "format", True, "iametadata", "archive.org file format of contents of bucket (item)", None ],
                [ "dateHdr", "date", True, "iametadata", "archive.org date of contents of bucket (item)", None ],
                [ "subjectHdr", "subject", True, "iametadata", "archive.org subject of contents of bucket (item)", None ]
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
        urlInfo        -- YaS3UrlInfo object
        contentLength  -- content length of body to be sent, if any
        """
        self.sendReqFirstLine(c, urlInfo.url)
        self.putHeaders(c, headerInfo)

        reply = c.conn.getresponse()
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
        urlInfo        -- YaS3UrlInfo object
        """
        self.sendReqFirstLine(c, urlInfo.url)
        self.putHeaders(c, headerInfo)

        reply = c.conn.getresponse()
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
        urlInfo        -- YaS3UrlInfo object
        postBody       -- text to be sent in request body
        """
        self.sendReqFirstLine(c, urlInfo.url)
        self.putHeaders(c, headerInfo)

        if postBody and self.reqType == "POST":
            if self.verbose:
                print "POST of >>>%s<<<" % postBody
            c.conn.send(postBody)

        reply = c.conn.getresponse()
        headersReceived = reply.getheaders()
        data = self.getResponse(reply, headersReceived)
        return reply, data

    def makeLoggedInGetRequest(self, c, headerInfo, urlInfo, cookies):
        """
        Make a single logged in (using archive.org session cookies)
        GET request to archive.org, and get headers and response back

        Arguments:
        c              -- YaS3Connection object
        headerInfo     -- YaS3HTTPHeaders object
        urlInfo        -- YaS3UrlInfo object
        cookies        -- login session cookies
        """
        self.sendReqFirstLine(c, urlInfo.url)
        headerInfo.addHeader("Cookie", "; ".join([ "%s=%s" % (cookie.name, cookie.value) for cookie in cookies if cookie.checkIfCookieValid() ]))
        self.putHeaders(c, headerInfo)

        reply = c.conn.getresponse()
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
        self.iAMetadataArgs = args.getIAMetadataArgs()
        if self.argDict["verbose"]:
            print self.iAMetadataArgs

if __name__ == "__main__":
    """
    Command line client that will run any specified S3 or non S3 operation connecting to archive.org
    """

    args = YaS3IAArgs()
    s3lib = YaS3IALib(args, YaS3Err(args, IAMetadataHandlerFactory.ops, IAMetadataHandlerFactory.introduction))
    argDict = s3lib.argDict
    iAMetadataArgs = s3lib.iAMetadataArgs

    s3lib.checkMissing([ "operation" ]) # the rest will be checked in the handlers

    s3Sess = YaS3IASessionInfo(s3lib.errors, argDict["s3Host"], argDict["host"])

    s3Sess.setConnType("s3") # default, specific handlers will override this. must be before setRequestInfo
    s3Sess.setRequestInfo(YaS3RequestInfo(s3Sess.getHost(), argDict["port"], argDict["protocol"]))

    s3 = IAMetadataHandlerFactory(argDict["operation"], argDict, s3Sess, iAMetadataArgs)
    if not s3:
        s3lib.errors.usage("Unknown operation %s" % argDict["operation"])

    s3.setUp()

    result = s3.runS3()
    s3.tearDown()
