import os, re, sys, time, hashlib, hmac, binascii, httplib, urllib, getopt, ConfigParser, xml.etree.ElementTree
from yas3http import YaS3HTTPDate, YaS3HTTPHeaders, YaS3HTTPCookie
from yas3lib import YaS3UrlInfo, YaS3ArbitraryUrl, YaS3ListMPUploadsUrl, YaS3StartMPUploadUrl, YaS3MPUploadUrl, YaS3UploadMPPartUrl
from yas3lib import YaS3AuthInfo, YaS3LocalFile, YaS3Requester, YaS3Connection
from utils import Err, ErrExcept, PPXML

# FIXME this seems a bit useless as it is, maybe this should be moved into the sessioninfo object? 
# or somewhere else?
class YaS3RequestInfo(object):
    """
    A place to stash information about the remote server
    """

    def __init__(self, host, port, protocol):
        """
        Constructor

        Arguments:
        host       -- fqdn of remote server
        port       -- port for connection to remote server
        protocol   -- http or https
        """
        self.host = host
        self.port = port
        self.protocol = protocol

class YaS3SessionInfo(object):
    """
    Everything the caller needs to know and do for a session (across several S3 requests)
    """

    def __init__(self, errors):
        """
        Constructor

        Arguments:
        errors       -- YAS3Err object
        """
        self.errors = errors
        self.request = None # YaS3RequestInfo
        self.cookies = [] # list of YaS3HTTPCookies
        self.auth = None    # YaS3AuthInfo
        self.urlInfo = None # YaS3UrlInfo
        self.data = None

    def setRequestInfo(self, req):
        """
        Set the request attribute for the session
        This also updates any cookies stored in the session
        with the appropriate host/protocol information
        
        Arguments:
        req     -- YaS3RequestInfo object
        """
        self.request = req
        # update any cookies with the new remote host/protocol
        for c in self.cookies:
            c.setRemoteHostInfo(self.getHost(), self.getProtocol(), self.getUrl())

    def setAuthInfo(self, auth):
        """
        Set the auth attribute for the session

        Arguments:
        auth     -- YaS3AuthInfo object
        """
        self.auth = auth

    def setCookies(self, cookies):
        """
        Set the cookies attribute for the session

        Arguments:
        cookies   -- list of YaS3HTTPCookies
        """
        self.cookies = cookies

    def setUrlInfo(self, url):
        """
        Set the url attribute of the object
        This also updates any cookies stored in the session
        with the appropriate url information
        
        Arguments:
        url    -- YaS3UrlInfo object
        """
        self.urlInfo = url
        # update any cookies with the new remote url
        for c in self.cookies:
            c.setRemoteHostInfo(self.getHost(), self.getProtocol(), self.getUrl())

    def setHost(self, host):
        """
        Set remote host for session

        Arguments:
        host    -- fqdn of server for session
        """
        if self.request:
            self.request.host = host

    def setPort(self, port):
        """
        Set port for session

        Arguments:
        port    -- port of server for session
        """
        if self.request:
            self.request.port = port

    def setRemoteFileName(self, remoteFileName):
        """
        Set name of S3 object for session

        Arguments:
        remoteFileName  -- name of object (file) for the session
        """
        if self.urlInfo:
            self.urlInfo.resetUrl(remoteFileName = remoteFileName)

    def getHost(self):
        """
        Return name of remote host for session
        """
        if self.request:
            return self.request.host
        else:
            return None

    def getProtocol(self):
        """
        Return protocol used for session (http or https)
        """
        if self.request:
            return self.request.protocol
        else:
            return None

    def getUrl(self):
        """
        Return url (without host/port/protocol) in use for session
        """
        if self.urlInfo:
            return self.urlInfo.getUrl()
        else:
            return None

    def getPort(self):
        """
        Return port for remote server for session (as string)
        """
        if self.request:
            return self.request.port
        else:
            return None

class YaS3Args(object):
    """
    Manages arguments passed on the command line or in a config file
    """

    def __init__(self):
        """
        Constructor
        Derived classes should extend the list of args below.
        Each list entry consists of: 
             variable name
             option name for command line/configfile
             True if the option name takes a string value, False if it's a boolean
             config file section name for option, or None for options that won't be read from the config file
             short description (used for help messages)
             default value, or None if there is no default
        If the option name is "", the variable name is used as the option name as well
        
        The varname configFile and the varnames help and helpops should be present if you want 
        config file handling or help messages for the user.
        """
        self.args = [ 
            [ "help", "", False, None, "display this help message", False ],
            [ "helpop", "", True, None, "display help for the specified operation", False ],
            [ "accessKey", "accesskey", True, "auth", "access key for s3 requests", None ],
            [ "secretKey", "secretkey", True, "auth", "secret key for s3 requests", None ],
            [ "s3Host", "s3host", True, "host", "hostname for s3 requests", None ],
            [ "port", "", True, "host", "port number for requests", "80" ],
            [ "protocol", "", True, "host", "protocol for requests", "http" ],
            [ "virtualHost", "virtualhost", False, "flags", "use virtual host style requests built from the bucket name", False ],
            [ "dryrun", "", False, None, "don't save/upload but describe what would be done", False ],
            [ "verbose", "", False, "flags", "print headers and other data from the request", False ],
            [ "quiet", "", False, "flags", "suppress normal output and error output (verbose overrides this)", False ],
            [ "bucketName", "bucket", True, None, "bucket name for uploads/downloads/creation etc.", None ],
            [ "sourceBucketName", "sourcebucket", True, None, "source bucket name for copy", None ],
            [ "remoteFileName", "remotefile", True, None, "object name in bucket to get/put", None ],
            [ "localFileName", "localfile", True, None, "path to local file to upload/save", None ],
            [ "mpUploadId", "mpuploadid", True, None, "id of multipart upload for end/abort etc.", None ],
            [ "mpPartNum", "mppartnum", True, None, "part number (in ascending order) of multipart upload", None ],
            [ "mpPartsAndEtags", "mppartsetags", True, None, "comma-separated list of part number:etag of multipart upload", None ],
            [ "mpChunkSize", "mpchunksize", True, "misc", "max size of file pieces in multipart upload", None ],
            [ "mpFileOffset", "mpfileoffset", True, None, "offset into a local file for uploading it as multipart upload", "0" ],
            [ "operation", "", True, None, "operation to perform", None ],
            [ "configFile", "configfile", True, None, "full path to config file", None ]
            ]
        self.defaultsDict = None  # default values for all args
        self.configDict = {}      # values of args from config file
        self.optDict = {}         # values of args from command line
        self.mergedDict = {}      # final dict of args with preference: command line, then config file, then default

    def getArgs(self):
        """
        Returns the list of predefined argument entries
        """
        return self.args

    def getOptName(self, arg):
        """
        Given an entry in the predefined argument list, return the option name

        Arguments:
        arg   -- entry in the list of predefined arguments
        """
        return arg[1] if arg[1] else arg[0]

    def getVarName(self, arg):
        """
        Given an entry in the predefined argument list, return the internal variable name

        Arguments:
        arg   -- entry in the list of predefined arguments
        """
        return arg[0]

    def getConfSection(self, arg):
        """
        Given an entry in the predefined argument list, return the config file section

        Arguments:
        arg   -- entry in the list of predefined arguments
        """
        return arg[3]

    def OptTakesStringValue(self, arg):
        """
        Given an entry in the predefined argument list, return True if the arg takes a string value,
        False if it is a flag (boolean) that takes no value on the command line

        Arguments:
        arg   -- entry in the list of predefined arguments
        """
        return arg[2]

    def getDefault(self, arg):
        """
        Given an entry in the predefined argument list, return the default value

        Arguments:
        arg   -- entry in the list of predefined arguments
        """
        return arg[5]

    def getVarNamesList(self):
        """
        Return a list of the internal variable names for all items in the
        predefined argument list
        """
        return [ self.getVarName(a) for a in self.args ]

    def getOptNamesList(self):
        """
        Return a list of the command line / config file option names for all items in the
        predefined argument list
        """
        return [  self.getOptName(a) for a in self.args ]

    def getOptNameFromVarName(self, varName):
        """
        Given the internal variable name of a predefined argument, return the
        command line / config file option name
        
        Arguments:
        varName  -- internal variable name of argument
        """
        for a in self.args:
            if self.getVarName(a) == varName:
                return self.getOptName(a)
        return None

    def getConfSectionFromOptName(self, optName):
        """
        Given the command line / config file option name of a predefined
        argument, return the section in the config file where it can be 
        listed

        Arguments:
        optName  -- command line / config file option name of argument
        """
        for a in self.args:
            if self.getOptName(a) == optName:
                return self.getConfSection(a)
        return None

    # returns a list like [ "accesskey=", "verbose", .... ]
    def getOptListForGetopt(self):
        """
        Return a list of all of the command line / config file option
        names of all prdfined arguments
        """
        return [ self.getOptName(a) + ( "=" if self.OptTakesStringValue(a)  else "") for a in self.args ]

    def mergeDicts(self):
        """
        From the defaults, the config file values and the command line values, put toether
        the final set of values for all predefined args
        This method will initialize the default values dict if it has not been done already
        """
        if not self.defaultsDict:
            self.getDefaultsDict()
        for i in self.getVarNamesList():
            self.mergedDict[i] = None
        for i in self.defaultsDict:
            if self.defaultsDict[i] != None:
                self.mergedDict[i] = self.defaultsDict[i]
        for i in self.configDict:
            if self.configDict[i] != None:
                self.mergedDict[i] = self.configDict[i]
        for i in self.optDict:
            if self.optDict[i] != None:
                self.mergedDict[i] = self.optDict[i]

    def setOptDictVal(self, optName, val):
        """
        Set the value of an argument in the dict of values derived from the
        command line / config file entries

        Arguments:
        optName    -- name of argument as specified on command line or in config file
        val        -- value to assign
        """
        for a in self.args:
            name = self.getOptName(a)
            if name == optName:
                self.optDict[self.getVarName(a)] = val if val else True
                return
        return

    def getDefaultsDict(self):
        """
        From the predefined list of arguments, generate a dict of internal
        variable names and their default values
        """
        if not self.defaultsDict:
            self.defaultsDict = {}
            for a in self.args:
                self.defaultsDict[self.getVarName(a)] = self.getDefault(a)

    def getConfigSections(self):
        """
        Return list of all known config sections as specified in list of predefined args
        """
        return list(set([ self.getConfSection(a) for a in self.args if self.getConfSection(a) ]))

    def getValueFromVarName(self, varName):
        """
        Given the internal variable name for an argument, get the value for it
        Priority: first check command line values, fall back to config file, 
        and finally to default value
        
        Arguments:
        varName    -- internal variable name
        """
        if self.optDict and varName in self.optDict:
            return self.optDict[varName]
        elif self.configDict and varName in self.configDict:
            return self.configDict[varName]
        elif self.defaultsDict and varName in self.defaultsDict:
            return self.defaultsDict[varName]
        elif varName in self.getOptNamesList():
            return None
        else:
            # fixme maybe an exception here?
            return None
    
    def readConfigFile(self):
        """
        Read and stash the args and values from the specified config file, if any
        checking current working directory,  home directory and /etc in that
        order, for the file
        Call this after optDict is filled in from command line
        (so we had a chance to read the config file name from there :-P)
        """
        if not self.defaultsDict:
            self.getDefaultsDict()
        configFile = self.getValueFromVarName('configFile')
        home = os.path.dirname(sys.argv[0])
        if configFile:
            if not os.path.exists(os.path.join(home,configFile)):
                Err.whine("Specified config file '%s' does not exist" % configFile)
        if (not configFile):
            configFile = self.getDefaultConfFileName()
        self.files = [
            os.path.join(home,configFile),
            "/etc/" + self.getDefaultConfFileName(),
            os.path.join(os.getenv("HOME"), '.' + self.getDefaultConfFileName())]
        self.conf = ConfigParser.SafeConfigParser()
        self.conf.read(self.files)
        self._parseConfFile()

    def getDefaultConfFileName(self):
        """
        Return the default config file name in case none is specified
        on the command line
        """
        return "yas3.conf"

    def _parseConfFile(self):
        """
        Read and stash the args and values from the specified config file, if any
        """
        knownSections = self.getConfigSections()

        # if config file has sections we didn't define, whine
        for s in self.conf.sections():
            if not s in knownSections:
                Err.whine("Unknown config section '%s'" % s)

        for s in knownSections:
            if not self.conf.has_section(s):
                self.conf.add_section(s)

        # if config file has options we didn't specify in a given section, whine
        for s in knownSections:
            for o in self.conf.options(s):
                section = self.getConfSectionFromOptName(o)
                if not o in self.getOptNamesList() or not section:
                    Err.whine("Unknown config option '%s'" % o)
                elif section != s:
                    Err.whine("Config option '%s' specified in wrong section %s, should be %s" % (o,s, section))

        # and now get each arg that is permitted to be present in the config file
        for a in self.getArgs():
            if self.getConfSection(a):
                # if it's not there we want to not fail
                try:
                    if self.OptTakesStringValue(a):
                        self.configDict[self.getVarName(a)] = self.conf.get(self.getConfSection(a), self.getOptName(a))
                    else:
                        self.configDict[self.getVarName(a)] = self.conf.getboolean(self.getConfSection(a), self.getOptName(a))
                except ConfigParser.NoOptionError:
                    pass
        
class YaS3Handler(object):
    """
    Base class for all S3 operation handlers
    Override the init method and set the reqType (but call the parent
    init method in your derived class)
    """

    mandatory = [ "accessKey", "secretKey", "s3Host" ] # list of mandatory options, override this in your subclass

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        self.ops = ops
        self.s3Sess = s3Sess
        self.argDict = argDict

        if self.argDict["virtualHost"] and self.argDict["bucketName"]:
            self.s3Sess.setHost("%s.%s" % (self.argDict["bucketName"], self.argDict["s3Host"]))

        self.s3Sess.setAuthInfo(YaS3AuthInfo(argDict["accessKey"], argDict["secretKey"]))
        self.connection = YaS3Connection(self.s3Sess.getHost(), self.s3Sess.getPort())

        self.localFile = YaS3LocalFile(self.argDict["localFileName"])
        self.reqType = None
        self.s3Req = None
        self.oldUrlInfo = None
        # put the right call in your init()
#        self.setUrlInfo(YaS3UrlInfo(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["virtualHost"]))

    def setUrlInfo(self, urlInfo, saveOld = True):
        """
        Set url information abnd optionally stash a copy of the current
        information for restoral later
        Since one handler can call others; the others will save and restore the parent
        handler's url info insead of blithely overwriting it with their own

        Arguments:
        urlInfo   -- YAS3UrlInfo object
        saveOld   -- set to False if a copy of the current urlInfo should not be 
                     stashed; this might happen if the handler is following a
                     redirect in the case of a single S3 request
        """
        if saveOld:
            self.oldUrlInfo = self.s3Sess.urlInfo if self.s3Sess.urlInfo else None
        self.s3Sess.urlInfo = urlInfo

    def restoreUrlInfo(self):
        """
        Restore url information from stashed copy
        """
        self.s3Sess.setUrlInfo(self.oldUrlInfo)

    def checkMandatory(self):
        """
        Check and complain if arguments mandatory to the operation were not specified
        and do not have default values
        """
        missing = []
        for m in self.__class__.mandatory:
            if self.argDict[m] is None:
                missing.append(self.s3Sess.errors.args.getOptNameFromVarName(m)) # display the command line var name, not the internal one
        if len(missing):
            self.s3Sess.errors.usage("This operation is missing one or more mandatory arguments: %s" % ', '.join(missing))

    def getDefaultPort(self, protocol):
        """
        Return the default port for a protocol

        Arguments:
        protocol   -- "http" or "https"
        """
        if protocol == "http":
            return "80"
        elif protocol == "https":
            return "443"
        else:
            return None

    def getHostAndPortFromUrl(self,url):
        """
        Extract and return the host and the port from a url (to be used for redirection handling, when given a Location: header, for example)

        Arguments:
        url    -- full url string including protocol, host, port
        """
        found = re.match("http(s?)+://([^:/]+)(:[0-9]+)?(.*)$", url)
        if not found:
            return None, None

        host = found.group(2)
        if found.group(3):
            return host, found.group(3)[1:], found.group(4)
        else:
            return host, self.getDefaultPort("http"+found.group(1)), found.group(4)

    def checkArgs(self):
        """
        Do all argument checking (call before trying to process an operation)
        Right now this only does check mandatory but maybe it will check for
        excess arguments one day
        """
        self.checkMandatory()

    def setUp(self):
        """
        Do various things related to setting up the S3 session
        Call this right after getting your operation handler instance.
        There's nothing else we do here but check arguments for the operation
        but maybe we will at some point
        """
        self.checkArgs()

    def tearDown(self):
        """
        Do various things related to tearing down the S3 session.
        Call this as the last thing you do before moving on to non-S3-related
        work or exiting.
        Right now the only thing this does is close the connection to the
        remote server but it might do more in the future.
        """
        self.connection.close()

    def getUserAgent(self):
        """
        Return a string with the value that will be put in the UserAgent HTTP header
        """
        return "yas3lib.py/0.1 (yet another s3 library)"

    def getAcceptTypes(self):
        """
        Return a string with the mime types this client will accept
        """
        return "*/*"

    def _setHeadersNoAuth(self, amzheaders = []):
        """
        Create and return some standard HTTP headers for requests, with
        no Authorization header
        This returns the headers and the date string (formatted as it appears in the
        HTTP Date header).  I guess one could dig the date out from the Date: header
        for later use but whatever.  We need it later in order to sign things.
        """
        date = YaS3HTTPDate.getHTTPDate()
        headerInfo = YaS3HTTPHeaders()
        headerInfo.addHeader("Host", self.s3Sess.getHost())
        headerInfo.addHeader("Date", date)
        headerInfo.addHeader("User-Agent", self.getUserAgent())
        headerInfo.addHeader("Accept", self.getAcceptTypes())
        if len(amzheaders):
            for (h,v) in amzheaders:
                headerInfo.addHeader(h, v)
        return headerInfo, date

    def setHeadersWithAwsAuth(self, md5 = "", contentType = "", amzheaders = []):
        """
        Create and return some standard HTTP headers for requests, with the AWS
        (S3 signature-based) Authorization header for S3 -- use this auth method when possible
        Returns just the headers
        """
        headerInfo, date = self._setHeadersNoAuth(amzheaders)
        # fixme  -- if only I remembered what was wrong with it that it needs to be fixed.
        headerInfo.addHeader("Authorization", self.s3Sess.auth.getAWSHeader(self.reqType, md5, contentType, date, self.s3Sess.getUrl(), amzheaders))
        return headerInfo

    def setHeadersWithLOWAuth(self):
        """
        Create and return some standard HTTP headers for requests, with the 'LOW'
        (archive.org low security) Authorization header for S3 -- avoid when possible
        Returns just the headers
        """
        headerInfo, date = self._setHeadersNoAuth()
        headerInfo.addHeader("Authorization", self.s3Sess.auth.getLOWHeader())
        return headerInfo

    def setHeadersWithNoAuth(self, amzheaders = []):
        """
        Create and return some standard HTTP headers for requests, with no
        Authorization header for S3
        Returns just the headers
        """
        headerInfo, date = self._setHeadersNoAuth(amzheaders = [])
        return headerInfo

    def setOtherHeaders(self, headerInfo):
        """
        Set additional non-standard headers; override this in derived handlers as needed
        """
        pass

    def doS3(self):
        """
        Do a single S3 request, without following Location header
        This will restore stashed url information (so be sure that
        it was stashed before you get here, if you needed it)

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        headerInfo = self.setHeadersWithAwsAuth()
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3Requester(self.reqType, self.argDict["verbose"], self.argDict["quiet"])

        reply, data = self.s3Req.makeRequest(self.connection, headerInfo, self.s3Sess.urlInfo)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        result = self.doS3()
        if result.status == 307:
            # not sure about stash/restore here... generally check all those FIXME
            self.setupForNewLocation(result)
            result = self.doS3()
        return result

    def setupForNewLocation(self, reply):
        """
        Set session info for new host port and url from Location header
        """
        # by the time this is called the old urlinfo has already been restored. uhhhh
        headersReceived = reply.getheaders()
        newUrl = None
        for (header, value) in headersReceived:
            if header == "location":
                newUrl = value
                break
        if not newUrl:
            Err.whine("Could not retrieve url from Location header, giving up")
        host, port, restOfUrl = self.getHostAndPortFromUrl(newUrl)
        self.s3Sess.setHost(host)
        self.s3Sess.setPort(port)
        self.connection.resetHostAndPort(self.s3Sess.getHost(), self.s3Sess.getPort())

        # the one place we don't save the old url, since it's ours and not from a
        # previous caller
        self.setUrlInfo(YaS3ArbitraryUrl(restOfUrl), saveOld = False)

        if self.argDict["verbose"]:
            print "closing connection, preparing for new request attempt"

        # FIXME test if it is a new host or not before doing this
        self.tearDown() # must close connection, since we request from a new host

class YaS3ListBucketsHandler(YaS3Handler):
    """
    Handler for S3 list buckets requests
    """

    mandatory = [ "accessKey", "secretKey" ] # mandatory arguments for this operation

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3ListBucketsHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "GET"

    def doS3(self):
        """
        Do a single S3 list buckets request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3UrlInfo(virtualHost = self.argDict["virtualHost"]))
        reply = super(YaS3ListBucketsHandler,self).doS3()
        return reply

class YaS3CreateBucketHandler(YaS3Handler):
    """
    Handler for S3 create bucket requests
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
        super(YaS3CreateBucketHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "PUT"

    def doS3(self):
        """
        Do a single S3 create bucket request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3UrlInfo(self.argDict["bucketName"], virtualHost = self.argDict["virtualHost"]))
        reply = super(YaS3CreateBucketHandler,self).doS3()
        return reply

class YaS3UploadObjectHandler(YaS3Handler):
    """
    Handler for S3 upload object requests
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
        super(YaS3UploadObjectHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "PUT"
        if not self.argDict["remoteFileName"]:
            self.argDict["remoteFileName"] = self.argDict["localFileName"]
            s3Sess.setRemoteFileName(self.argDict["remoteFileName"])

    def doS3(self):
        """
        Do a single S3 upload object request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3UrlInfo(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["virtualHost"]))
        # this is cached so it's fine to call multiple times
        md5 = self.localFile.getMd5B64()
        md5ToPrint = self.localFile.getMd5()

        if self.argDict["verbose"]:
            print "md5 of file %s: %s and %s" % (self.localFile.name, md5, md5ToPrint)
        contentLength = self.localFile.getSize()
        contentType = ""
        headerInfo = self.setHeadersWithAwsAuth(md5, contentType)
        headerInfo.addHeader("Content-MD5", md5)
        headerInfo.addHeader("Content-Length", contentLength)
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3Requester(self.reqType, self.argDict["verbose"], self.argDict["quiet"])

        reply, data = self.s3Req.makeUploadObjectRequest(self.connection, headerInfo, self.s3Sess.urlInfo, contentLength, md5, self.localFile.name)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

class YaS3UploadObjectAsMPHandler(YaS3Handler):
    """
    Handler for S3 upload object as multi-part uploadrequests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "localFileName", "remoteFileName", "mpChunkSize" ]

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3UploadObjectAsMPHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "PUT"
        if not self.argDict["remoteFileName"]:
            self.argDict["remoteFileName"] = self.argDict["localFileName"]
            s3Sess.setRemoteFileName(self.argDict["remoteFileName"])
        # no setUrlInfo because this call consists of a set of calls to handlers which will do their own

    def doS3(self):
        """
        Upload a local file as an S3 multi-part upload, splitting it up into appropriate
        sized pieces and uploading each part in turn

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        fileSize =  self.localFile.getSize()
        if fileSize < int(self.argDict["mpChunkSize"]):
            # do a regular upload
            s3 = self.ops["uploadobject"][0](self.ops, self.argDict, self.s3Sess)
            s3.setUp()
            reply = s3.runS3()
            return reply
        else:
            s3 = self.ops["startmpupload"][0](self.ops, self.argDict, self.s3Sess)
            s3.setUp()
            reply = s3.runS3()

            # if the reply is bad, bail

            # dig the upload id out of the data

            # <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            #   <Bucket>test-bucket-atg</Bucket>
            #   <Key>mptest</Key>
            #   <UploadId>f63cb1e1-ff3c-4ce1-af11-b13d3ebfbfbc</UploadId>
            # </InitiateMultipartUploadResult>
            mpUploadId = None
            if self.s3Sess.data:
                tree =  xml.etree.ElementTree.fromstring(self.s3Sess.data)
                f = tree.find("{http://s3.amazonaws.com/doc/2006-03-01/}UploadId")
                if f is not None:
                    mpUploadId = f.text
            self.argDict["mpUploadId"] = mpUploadId
            if not mpUploadId:
                Err.whine("Failed to start multi part upload, no uploadId in response %s" % self.s3Sess.data)

            mpPartNum = 1
            mpFileOffset = 0
            mpPartsAndEtagsList = []
            errors = 0
            while mpFileOffset < fileSize:
                self.argDict["mpPartNum"] = str(mpPartNum)
                self.argDict["mpFileOffset"] = str(mpFileOffset)
                s3 = self.ops["uploadmppart"][0](self.ops, self.argDict, self.s3Sess)
                s3.setUp()
                reply = s3.runS3()
                # if the reply is bad, note it and continue
                if reply.status != 200:
                    errors += 1
                    print "reply status was ", reply.status
                else:
                    # dig the etag out of the reply headers
                    mpEtag= None
                    for h, v in reply.getheaders():
                        if h == "etag":
                            mpEtag = v
                            break

                    # save the partnum and etag in our list for end
                    mpPartsAndEtagsList.append("%s:%s" % (mpPartNum, mpEtag))
                mpPartNum += 1
                mpFileOffset += int(self.argDict["mpChunkSize"])
            mpPartsAndEtags = ",".join(mpPartsAndEtagsList)
            if self.argDict["verbose"]:
                print "parts and etags for multipart upload: ", mpPartsAndEtags
            if errors:
                Err.whine("%s parts failed to upload.  Successful uploads: %s. Uploadid: %s, giving up." % (errors, mpPartsAndEtags, mpUploadId))
            # if there were no upload errors we do the end mp
            self.argDict["mpPartsAndEtags"] = mpPartsAndEtags
            s3 = self.ops["endmpupload"][0](self.ops, self.argDict, self.s3Sess)
            s3.setUp()
            reply = s3.runS3()
            return reply

class YaS3GetObjectHandler(YaS3Handler):
    """
    Handler for S3 get object requests
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
        super(YaS3GetObjectHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "GET"
        if not argDict["remoteFileName"]:
            argDict["remoteFileName"] = argDict["localFileName"]
            s3Sess.setRemoteFileName(self.argDict["remoteFileName"])

    def doS3(self):
        """
        Do a single S3 get objct request, without following Location header

        Returns HTTPResponse object and saves any data *that is not the file* from the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3UrlInfo(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["virtualHost"]))
        headerInfo = self.setHeadersWithAwsAuth()
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3Requester(self.reqType, self.argDict["verbose"], self.argDict["quiet"])

        reply, data = self.s3Req.makeGetObjectRequest(self.connection, headerInfo, self.s3Sess.urlInfo, self.localFile.name)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

class YaS3ListOneBucketHandler(YaS3Handler):
    """
    Handler for S3 list given bucket requests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName" ]

    # FIXME amazon for example limits results to 1000 per bucket, need to figure out how to retrieve the rest
    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3ListOneBucketHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "GET"

    def doS3(self):
        """
        Do a single S3 list given bucket request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3UrlInfo(self.argDict["bucketName"], virtualHost = self.argDict["virtualHost"]))
        reply = super(YaS3ListOneBucketHandler,self).doS3()
        return reply 

class YaS3DeleteObjectHandler(YaS3Handler):
    """
    Handler for S3 delete object requests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "remoteFileName" ]

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3DeleteObjectHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "DELETE"

    def doS3(self):
        """
        Do a single S3 delete object request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3UrlInfo(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["virtualHost"]))
        reply = super(YaS3DeleteObjectHandler,self).doS3()
        return reply

class YaS3DeleteBucketHandler(YaS3Handler):
    """
    Handler for S3 delete buckets requests
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
        super(YaS3DeleteBucketHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "DELETE"

    def doS3(self):
        """
        Do a single S3 delete bucket request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3UrlInfo(self.argDict["bucketName"], virtualHost = self.argDict["virtualHost"]))
        reply = super(YaS3DeleteBucketHandler,self).doS3()
        return reply

class YaS3GetObjectS3MetadataHandler(YaS3Handler):
    """
    Handler for S3 get object metadata requests (this is "head object')
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "remoteFileName" ]

    # thisis a head request, it might return very little
    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3GetObjectS3MetadataHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "HEAD"

    def doS3(self):
        """
        Do a single S3 get object S3 metadata request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3UrlInfo(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["virtualHost"]))
        headerInfo = self.setHeadersWithAwsAuth()
        self.setOtherHeaders(headerInfo)
        if not self.s3Req:
            self.s3Req = YaS3Requester(self.reqType, self.argDict["verbose"], self.argDict["quiet"])

        reply, data = self.s3Req.makeRequest(self.connection, headerInfo, self.s3Sess.urlInfo)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

class YaS3GetBucketS3MetadataHandler(YaS3Handler):
    """
    Handler for S3 get bucket S3 metadata requests (this is 'Head bucket')
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
        super(YaS3GetBucketS3MetadataHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "HEAD"

    def doS3(self):
        """
        Do a single S3 get bucket S3 metadata request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3UrlInfo(self.argDict["bucketName"], virtualHost = self.argDict["virtualHost"]))
        reply = super(YaS3GetBucketS3MetadataHandler,self).doS3()
        return reply

class YaS3CopyObjectHandler(YaS3Handler):
    """
    Handler for S3 copy object requests (copies an object from one bucket to another without local download)
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "sourceBucketName", "remoteFileName" ]

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3CopyObjectHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "PUT"

    def doS3(self):
        """
        Do a single S3 copy object request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3UrlInfo(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["virtualHost"]))
        # fixme what about virtual host with this, I guess we don't want it right? should turn off
        amzheaders = [ ( "x-amz-copy-source", urllib.quote("%s/%s" % ( self.argDict[ "sourceBucketName" ], self.argDict[ "remoteFileName" ] ) ) ) ]
        headerInfo = self.setHeadersWithAwsAuth(amzheaders = amzheaders)
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3Requester(self.reqType, self.argDict["verbose"], self.argDict["quiet"])

        reply, data = self.s3Req.makeRequest(self.connection, headerInfo, self.s3Sess.urlInfo)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

class YaS3ListMPUploadsHandler(YaS3Handler):
    """
    Handler for S3 list multi-part uploads requests (lists all uncompleted
    multi-part uploads for a given bucket)
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
        super(YaS3ListMPUploadsHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "GET"

    def doS3(self):
        """
        Do a single S3 list multipart uploads (for a given bucket) request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3ListMPUploadsUrl(argDict["bucketName"]))
        reply = super(YaS3ListMPUploadsHandler,self).doS3()
        return reply

class YaS3StartMPUploadHandler(YaS3Handler):
    """
    Handler for S3 start multi-part upload requests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "remoteFileName" ]

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3StartMPUploadHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "POST"

    def doS3(self):
        """
        Do a single S3 start multi-part upload request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        Useful data: the UploadId from the XML body from the remote server
        """
        self.setUrlInfo(YaS3StartMPUploadUrl(argDict["bucketName"], argDict["remoteFileName"]))
        reply = super(YaS3StartMPUploadHandler,self).doS3()
        return reply

class YaS3EndMPUploadHandler(YaS3Handler):
    """
    Handler for S3 end multi-part upload requests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName",  "remoteFileName", "mpUploadId", "mpPartsAndEtags" ]

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3EndMPUploadHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "POST"

    #<CompleteMultipartUpload>
    #  <Part>
    #    <PartNumber>PartNumber</PartNumber>
    #    <ETag>ETag</ETag>
    #  </Part>
    #  ...
    #</CompleteMultipartUpload>
    def getMpUploadReqXML(self):
        """
        Prepare and return XML body to be sent to remote server for
        an S3 end multi-part upload request
        """
        xml = ""
        if self.argDict["mpPartsAndEtags"]:
            # fix me should prolly do some try catches here and return None if bad
            xml = "<CompleteMultipartUpload>"
            if "," in self.argDict["mpPartsAndEtags"]:
                peList = self.argDict["mpPartsAndEtags"].split(',')
            else:
                peList = [ self.argDict["mpPartsAndEtags"] ]
            for pe in peList:
                p,e = pe.split(':')
                xml = xml + '<Part><PartNumber>%s</PartNumber><ETag>%s</ETag></Part>' % (p, e)
            xml = xml + "</CompleteMultipartUpload>"
        return xml

    def doS3(self):
        """
        Do a single S3 end multi-part upload request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3MPUploadUrl(argDict["bucketName"], argDict["remoteFileName"], argDict["mpUploadId"]))
        xml = self.getMpUploadReqXML()
        contentLength = len(xml)
        headerInfo = self.setHeadersWithAwsAuth()
        headerInfo.addHeader("Content-Length", contentLength)
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3Requester(self.reqType, self.argDict["verbose"], self.argDict["quiet"])

        reply, data = self.s3Req.makePostRequestWithXML(self.connection, headerInfo, self.s3Sess.urlInfo, xml)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

class YaS3AbortMPUploadHandler(YaS3Handler):
    """
    Handler for S3 abort multi-part upload requests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "remoteFileName", "mpUploadId" ]

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3AbortMPUploadHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "DELETE"

    def doS3(self):
        """
        Do a single S3 abort multi-part upload request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3MPUploadUrl(argDict["bucketName"], argDict["remoteFileName"], argDict["mpUploadId"]))
        reply = super(YaS3AbortMPUploadHandler,self).doS3()
        return reply

class YaS3ListOneMPUploadHandler(YaS3Handler):
    """
    Handler for S3 list given multi-part upload requests (list is based on UploadId returned
    from start multi-part upload request or from listing all incomplete multi-part uploads
    for the given bucket)
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "remoteFileName", "mpUploadId" ]

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3ListOneMPUploadHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "GET"

    def doS3(self):
        """
        Do a single S3 list given multi-part upload (that was not completed) request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3MPUploadUrl(argDict["bucketName"], argDict["remoteFileName"], argDict["mpUploadId"]))
        reply = super(YaS3ListOneMPUploadHandler,self).doS3()
        return reply

class YaS3UploadMPPartHandler(YaS3Handler):
    """
    Handler for S3 upload one part of multi-part upload requests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "remoteFileName", "mpPartNum", "mpUploadId", "localFileName" ]

    def __init__(self, ops, argDict, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        argDict     -- dictionary of args (from command line/config file/defaults) and values
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3UploadMPPartHandler,self).__init__(ops, argDict, s3Sess)
        self.reqType = "PUT"

    def doS3(self):
        """
        Do a single S3 upload part (of a multi-part upload) request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        self.setUrlInfo(YaS3UploadMPPartUrl(argDict["bucketName"], argDict["remoteFileName"], argDict["mpPartNum"], argDict["mpUploadId"]))
        chunkSize = int(self.argDict["mpChunkSize"]) if self.argDict["mpChunkSize"] else None
        md5 = self.localFile.getMd5B64(int(self.argDict["mpFileOffset"]), chunkSize)
        md5ToPrint = self.localFile.getMd5(int(self.argDict["mpFileOffset"]), chunkSize)
        if self.argDict["verbose"]:
            print "md5 of file %s: %s and %s" % (self.localFile.name, md5, md5ToPrint)
        contentLength = self.localFile.getSize(int(self.argDict["mpFileOffset"]), chunkSize)
        contentType = ""
        headerInfo = self.setHeadersWithAwsAuth(md5, contentType)
        headerInfo.addHeader("Content-MD5", md5)
        headerInfo.addHeader("Content-Length", contentLength)
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3Requester(self.reqType, self.argDict["verbose"], self.argDict["quiet"])

        reply, data = self.s3Req.makeUploadObjectRequest(self.connection, headerInfo, self.s3Sess.urlInfo, contentLength, md5, self.localFile.name, int(self.argDict["mpFileOffset"]), chunkSize)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

class YaS3HandlerFactory(object):
    """
    Produce the right Handler object based on the S3 operation specified
    """

    introduction = [ "This library implements a subset of the S3 REST api for storage and",
                     "retrieval of objects (files) in buckets." ]

    # operation name for command line, handler name
    ops = { "listbuckets" : [ YaS3ListBucketsHandler, "list all buckets owned by the authenticated user" ],
            "createbucket": [ YaS3CreateBucketHandler, "create a bucket of the specified name" ],
            "uploadobject" : [ YaS3UploadObjectHandler, "upload an object (file) into the specified bucket" ],
            "getobject" : [ YaS3GetObjectHandler, "get an object (file) from the specified bucket" ],
            "listonebucket" : [ YaS3ListOneBucketHandler, "list the contents of the specified bucket" ],
            "deleteobject" : [ YaS3DeleteObjectHandler, "delete an object (file) from the specified bucket" ],
            "copyobject" : [ YaS3CopyObjectHandler, "copy an object (file) from one bucket to another" ],
            "startmpupload" : [ YaS3StartMPUploadHandler, "start a multi-part upload of an object (file) to the specified bucket" ],
            "uploadmppart" : [ YaS3UploadMPPartHandler, "upload one part of a multi-part object (file) to the specified bucket" ],
            "endmpupload" : [ YaS3EndMPUploadHandler, "complete a multi-part upload of an object (file) to the specified bucket" ],
            "abortmpupload" : [ YaS3AbortMPUploadHandler, "abort the specific multi-part upload for the specified bucket" ],
            "listmpuploads" : [ YaS3ListMPUploadsHandler, "list all multi-part uploads in progress for the specified bucket" ],
            "listonempupload" : [ YaS3ListOneMPUploadHandler, "list all the parts of a specific multi-part upload for the specified bucket" ],
            "uploadobjectasmp" : [ YaS3UploadObjectAsMPHandler, "upload an object (file) in multiple parts to the specified bucket" ],
            "deletebucket" : [ YaS3DeleteBucketHandler, "delete the specified bucket" ],
            "getobjects3metadata" : [ YaS3GetObjectS3MetadataHandler, "get the s3 metadata (x-amz-meta-X) of an object in the specified bucket" ],
            "getbuckets3metadata" : [ YaS3GetBucketS3MetadataHandler, "get the s3 metadata of the specified bucket" ] }

    def __new__(cls, op, argDict, s3Sess):
        """
        Given the specific operation, return a new Handler object for that operation
        """
        if op in YaS3HandlerFactory.ops:
            return YaS3HandlerFactory.ops[op][0](YaS3HandlerFactory.ops, argDict, s3Sess)
        return None

class YaS3Err(object):
    """
    Various usage messages
    """

    def __init__(self, args, ops, introduction):
        """
        Constructor

        Arguments:
        args          -- YaS3SArgs object (contains the list of predefined args)
        ops           -- list of predefined operations from a HandlerFactory
        introduction  -- introductory text to be printed before the args and operations in
                         the usage message, consisting of a list of lines of text without
                         newlines included in the lines
        """
        self.args = args # YaS3Args
        self.ops = ops
        self.introduction = introduction

    def opusage(self, op):
        """
        Display usage information about a given operation to stderr

        Arguments:
        op  -- operation name
        """
        if op in self.ops:
            sys.stderr.write("Help for operation %s:\n" % op)
            sys.stderr.write("%s\n" % self.ops[op][1])
            mandatoryArgs = [ self.args.getOptNameFromVarName(m) for m in self.ops[op][0].mandatory ]
            if len(mandatoryArgs):
                sys.stderr.write("Mandatory arguments: %s\n" % ", ".join(mandatoryArgs))
            sys.stderr.write("\n")
            sys.exit(1)
        else:
            self.usage("Unknown operation specified: %s" % op)
        
    def usage(self, message = None):
        """
        Display usage message to stdout
        
        Arguments:
        message -- error message to be displayed before usage message, without newline
                   (e.g. "missing argument: blah")
        """
        if message:
            sys.stderr.write("Error: %s\n\n" % message)
        sys.stderr.write("Usage: python %s [options/flags]\n" % sys.argv[0])
        sys.stderr.write("\n")

        if len(self.introduction):
            for l in self.introduction:
                sys.stderr.write(l + "\n")
            sys.stderr.write("\n")

        sys.stderr.write("Options:\n")

        maxOptionLen = max([ len(a) for a in self.args.getOptNamesList() ])
        argList = self.args.getArgs()
        for a in argList:
            if a[2]: # takes a value
                name = a[1] if a[1] else a[0]
                spaces = ' ' *(2 + maxOptionLen - len(name))
                default = " (default %s)" % a[5] if a[5] != None else ""
                sys.stderr.write("  --%s: %s%s%s\n" % (name, spaces, a[4], default))
        sys.stderr.write("\n")

        sys.stderr.write("Flags:\n")
        for a in argList:
            if not a[2]: # doesn't want a value
                name = a[1] if a[1] else a[0]
                spaces = ' ' *(2 + maxOptionLen - len(name))
                default = " (default %s)" % a[5] if a[5] != None else ""
                sys.stderr.write("  --%s: %s%s%s\n" % (name, spaces, a[4], default))
        sys.stderr.write("\n")

        opsList = self.ops.keys()
        opsList.sort()
        if len(opsList):
            sys.stderr.write("Operations:\n")
            self.printNCols(opsList, 3)
        sys.stderr.write("\n")

        configList = [ a[1] if a[1] else a[0] for a in argList if a[3] ]
        if len(configList):
            sys.stderr.write("The following options may be specified in a configuration file and can be\n")
            sys.stderr.write("overriden on the command line:\n")
            self.printNCols(configList,3)
        sys.exit(1)

    def printNCols(self, items, numCols):
        """
        Display a list of items in the specified number of columns with
        columns lined up nicely

        Arguments:
        items     -- list of items to display
        numCols   -- number of columns in the table
        """
        if len(items):
            maxLen = max([ len(i) for i in items ])
            ind = 0
            for i in items:
                spaces = ' ' *(2 + maxLen - len(i))
                ind += 1
                sys.stderr.write("  %s%s" % (i, spaces))
                if ind == numCols:
                    sys.stderr.write("\n")
                    ind = 0
            if ind:
                sys.stderr.write("\n")
            
class YaS3Lib(object):
    """
    Set up YaS3 library for processing a request, reading args from the command line
    and config file
    """

    def __init__(self, args, errors):
        """
        Constructor

        Arguments:
        args      -- YaS3Args object (contains predefined list of arguments)
        errors    -- YsS#Err object (for usage messages)
        """
        self.args = args
        self.errors = errors

        try:
            options, remainder = getopt.gnu_getopt(sys.argv[1:], "", self.args.getOptListForGetopt())
        except getopt.GetoptError, err:
            print str(err)
            self.errors.usage("Unknown option specified")

        if not len(options) or not len(options[0]):
            self.errors.usage("No options specified.")
        if len(remainder):
            self.errors.usage("Unknown option specified: %s" % remainder)
        
        for opt, val in options:
            self.args.setOptDictVal(opt[2:], val) # all args passed in on command line

        if self.args.getValueFromVarName('help'):
            self.errors.usage()

        if self.args.getValueFromVarName("helpop"):
            self.errors.opusage(self.args.getValueFromVarName("helpop"))

        # default options from config file
        # by now we have read in everything ecxept conf so we can do the check...
        self.args.readConfigFile()
        self.args.mergeDicts()
        self.argDict = self.args.mergedDict

        if self.argDict["verbose"]:
            print "default dict", self.args.defaultsDict
            print "config dict", self.args.configDict
            print "opt dict", self.args.optDict
            print "final dict: ", self.args.mergedDict

    def checkMissing(self, varsToCheck):
        """
        Check that each variablee in the list has a value, if not whine about the ones that don't

        Arguments:
        varsToCheck  -- list of variables which need to have a value (supply internal variable name as it appears
                        in list of predefined arguments)
        """
        missing = []
        for m in varsToCheck:
            if not self.argDict[m]:
                missing.append(self.args.getOptNameFromVarName(m))
        if len(missing):
            self.errors.usage("Missing one or more mandatory arguments: %s" % ", ".join(missing))

if __name__ == "__main__":
    """
    Command line client that will run any specified S3 operation
    """
    args = YaS3Args()
    s3lib = YaS3Lib(args, YaS3Err(args, YaS3HandlerFactory.ops, YaS3HandlerFactory.introduction))
    argDict = s3lib.argDict

    s3lib.checkMissing([ "operation" ]) # other checks will happen in the handlers

    s3Sess = YaS3SessionInfo(s3lib.errors)
    s3Sess.setRequestInfo(YaS3RequestInfo(argDict["s3Host"], argDict["port"], argDict["protocol"]))

    s3 = YaS3HandlerFactory(argDict["operation"], argDict, s3Sess)
    if not s3:
        s3lib.errors.usage("Unknown operation %s" % argDict["operation"])
        
    s3.setUp()

    result = s3.runS3()
    print "result status is", result.status
    s3.tearDown()
