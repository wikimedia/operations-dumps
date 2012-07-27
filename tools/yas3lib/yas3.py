import os, re, sys, time, hashlib, hmac, binascii, httplib, urllib, getopt, ConfigParser, xml.etree.ElementTree
from yas3http import YaS3HTTPDate, YaS3HTTPHeaders, YaS3HTTPCookie
from yas3lib import YaS3UrlBuilder, YaS3ArbitraryUrl, YaS3ListMPUploadsUrl, YaS3StartMPUploadUrl, YaS3MPUploadUrl, YaS3UploadMPPartUrl
from yas3lib import YaS3AuthInfo, YaS3LocalFile, YaS3Requester, YaS3Connection
from utils import Err, ErrExcept, PPXML

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
        self.cookies = [] # list of YaS3HTTPCookies
        self.auth = None    # YaS3AuthInfo
        self.data = None

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
             short form of option (one letter) if any
        If the option name is "", the variable name is used as the option name as well
        
        The varname configFile and the varnames help and helpops should be present if you want 
        config file handling or help messages for the user.
        """
        self.args = [ 
            [ "help", "", False, None, "display this help message", False, None ],
            [ "helpop", "", True, None, "display help for the specified operation", False, None ],
            [ "accessKey", "accesskey", True, "auth", "access key for s3 requests", None, None ],
            [ "secretKey", "secretkey", True, "auth", "secret key for s3 requests", None, None ],
            [ "authType", "auth", True, "auth", "type of authntication (aws or low)", "aws", 'a' ],
            [ "s3Host", "s3host", True, "host", "hostname for s3 requests", None, None ],
            [ "port", "", True, "host", "port number for requests", "80", None ],
            [ "protocol", "", True, "host", "protocol for requests", "http", None ],
            [ "virtualHost", "virtualhost", False, "flags", "use virtual host style requests built from the bucket name", False, None ],
            [ "dryrun", "", False, None, "don't save/upload but describe what would be done", False, None ],
            [ "verbose", "", False, "flags", "print headers and other data from the request", False, 'v' ],
            [ "quiet", "", False, "flags", "suppress normal output and error output except usage messages (verbose overrides this)", False, 'q' ],
            [ "raw", "", False, "flags", "display output from server as is, without prettyprinting or other formatting", False, None ],
            [ "bucketName", "bucket", True, None, "bucket name for uploads/downloads/creation etc.", None, 'b' ],
            [ "sourceBucketName", "sourcebucket", True, None, "source bucket name for copy", None, None ],
            [ "remoteFileName", "remotefile", True, None, "object name in bucket to get/put", None, 'r' ],
            [ "localFileName", "localfile", True, None, "path to local file to upload/save", None, 'l' ],
            [ "mpUploadId", "mpuploadid", True, None, "id of multipart upload for end/abort etc.", None, None ],
            [ "mpPartNum", "mppartnum", True, None, "part number (in ascending order) of multipart upload", None, None ],
            [ "mpPartsAndEtags", "mppartsetags", True, None, "comma-separated list of part number:etag of multipart upload", None, None ],
            [ "mpChunkSize", "mpchunksize", True, "misc", "max size of file pieces in multipart upload", None, None ],
            [ "mpFileOffset", "mpfileoffset", True, None, "offset into a local file for uploading it as multipart upload", "0", None ],
            [ "operation", "", True, None, "operation to perform", None, 'o' ],
            [ "configFile", "configfile", True, None, "full path to config file", None, 'c' ]
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

    def getShortOption(self, arg):
        """
        Given an entry in the predefined argument list, return the short form
        (one letter) of the option

        Arguments:
        arg   -- entry in the list of predefined arguments
        """
        return arg[6]

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

    # returns a string like "l:c:br"
    def getShortOptsForGetopt(self):
        """
        Return a string of all of the short command line options
        of all predefined arguments
        """
        return "".join([ self.getShortOption(a) + ( ":" if self.OptTakesStringValue(a)  else "")for a in self.args if self.getShortOption(a) ])

    def getOptNameFromShortOpt(self, shortOpt):
        """
        Given the short option of an argument, return the OptName
        (the long form) of the argument as it appears on the command line
        or in config files

        Arguments:
        shortOpt  -- the one letter name of the option
        """
        if not shortOpt:
            return None
        for a in self.args:
            if self.getShortOption(a) == shortOpt:
                return self.getOptName(a)
        return None

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

    def removeArgs(self, varNames):
        """
        Remove specified arguments from the predefined arg list
        (might want this for derived classes)

        Arguments:
        list of internal variable names for which the arg entries will
        be removed from self.args
        """
        self.args = [ a for a in self.args if self.getVarName(a) not in varNames ]

class YaS3Handler(object):
    """
    Base class for all S3 operation handlers
    Override the init method and set the reqType (but call the parent
    init method in your derived class)
    """

    mandatory = [ "accessKey", "secretKey", "s3Host" ] # list of mandatory options, override this in your subclass

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        self.ops = ops
        self.s3Sess = s3Sess
        self.args = args
        self.argDict = self.args.mergedDict

        self.s3Sess.setAuthInfo(YaS3AuthInfo(self.argDict["accessKey"], self.argDict["secretKey"], self.argDict['authType']))
        self.connection = YaS3Connection(self.argDict["s3Host"], self.argDict["port"], self.argDict["protocol"])
        # any other changes of connectin.host during the course of the handler
        # (handlers called by other handlers don't count, they have their own connection)
        # will be: to non-s3 hostname (no virtual host there) or to host from redirect (no virtual
        # host in that either)
        if self.argDict["virtualHost"] and self.argDict["bucketName"]:
            self.connection.host = "%s.%s" % (self.argDict["bucketName"], self.argDict["s3Host"])
            
        self.localFile = YaS3LocalFile(self.argDict["localFileName"])
        self.reqType = None
        self.s3Req = None
        self.oldUrlInfo = None
        self.urlInfo = None

    def setUrlInfo(self, urlInfo, saveOld = True, virtualHost = False):
        """
        Set url information abnd optionally stash a copy of the current
        information for restoral later
        Since one handler can call others; the others will save and restore the parent
        handler's url info insead of blithely overwriting it with their own

        Arguments:
        urlInfo   -- YaS3UrlBuilder object
        saveOld   -- set to False if a copy of the current urlInfo should not be 
                     stashed; this might happen if the handler is following a
                     redirect in the case of a single S3 request
        """
        if saveOld:
            self.oldUrlInfo = self.urlInfo if self.urlInfo else None
        self.urlInfo = urlInfo

    def restoreUrlInfo(self):
        """
        Restore url information from stashed copy
        """
        self.urlInfo = self.oldUrlInfo

    def setRemoteFileName(self, remoteFileName):
        """
        Set name of S3 object for handler

        Arguments:
        remoteFileName  -- name of object (file) for the session
        """
        if self.urlInfo:
            self.urlInfo.reset(remoteFileName = remoteFileName)

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

    def getHostAndPortFromUrl(self,url):
        """
        Extract and return the host and the port from a url (to be used for redirection handling, when given a Location: header, for example)

        Arguments:
        url    -- full url string including protocol, host, port
        """
        if not url:
            return None, None, None
        found = re.match("http(s?)+://([^:/]+)(:[0-9]+)?(.*)$", url)
        if not found:
            return None, None, url

        host = found.group(2)
        if found.group(3):
            return host, found.group(3)[1:], found.group(4)
        else:
            return host, None, found.group(4)

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
        return "yas3lib.py/0.2-pre (yet another s3 library)"

    def getAcceptTypes(self):
        """
        Return a string with the mime types this client will accept
        """
        return "*/*"

    def setStandardHeaders(self, amzheaders = []):
        """
        Create and return some standard HTTP headers for requests
        Includes setting cookie headers for session cookies we
        may have accumulated
        Does *not* include any auth headers, the caller should
        add these separately
        Returns a pipulated YaS3HTTPHeader objct
        """
        date = YaS3HTTPDate.getHTTPDate()
        headerInfo = YaS3HTTPHeaders()
        headerInfo.addHeader("Host", self.connection.host)
        headerInfo.addHeader("Date", date)
        headerInfo.addHeader("User-Agent", self.getUserAgent())
        headerInfo.addHeader("Accept", self.getAcceptTypes())
        if len(amzheaders):
            for (h,v) in amzheaders:
                headerInfo.addHeader(h, v)
        if len(self.s3Sess.cookies):
            headerInfo.addHeader("Cookie", "; ".join([ "%s=%s" % (cookie.name, cookie.value) for cookie in self.s3Sess.cookies if cookie.checkIfCookieValid(self.connection.host, self.connection.protocol, self.urlInfo.buildUrl()) ]))
        return headerInfo

    def setS3AuthHeader(self, headerInfo):
        """
        Add the AWS (S3 signature-based) Authorization header for S3
        to the headers passed in -- prefer this auth type (instead of
        'LOW') whenever possible
        
        Arguments:
        headerInfo   -- YaS3HTTPHeaders object
        """
        # if the creds weren't set, don't try to set the header
        if not self.argDict["accessKey"] and self.argDict["secretKey"]:
            return

        date = headerInfo.findHeader("Date")
        contentType = headerInfo.findHeader("Content-Type")
        if not contentType:
            contentType = ""
        md5 = headerInfo.findHeader("Content-MD5")
        if not md5:
            md5 = ""
        amzheaders = headerInfo.findAmzHeaders()
        headerInfo.addHeader("Authorization", self.s3Sess.auth.getS3AuthHeader(self.reqType, md5, contentType, date, self.urlInfo.buildUrl(), amzheaders))

    def setOtherHeaders(self, headerInfo, auth = True):
        """
        Set additional non-standard headers; override this in derived handlers as needed
        """
        if auth:
            self.setS3AuthHeader(headerInfo)

    def doS3(self):
        """
        Do a single S3 request, without following Location header
        This will restore stashed url information (so be sure that
        it was stashed before you get here, if you needed it)

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        headerInfo = self.setStandardHeaders()
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3Requester(self.reqType, self.argDict["verbose"], self.argDict["quiet"], self.argDict["raw"])

        reply, data = self.s3Req.makeRequest(self.connection, headerInfo, self.urlInfo)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

    def setHostPortUrlForRedir(self, reply):
        """
        From Location header set new host, port in connection
        and new url in urlinfo
        This will close the connection if the hostname has changed

        Arguments:
        reply  -- HTTPReponse object
        """
        headersReceived = reply.getheaders()
        newUrl = None
        for (header, value) in headersReceived:
            if header == "location":
                newUrl = value
                break
        if not newUrl:
            Err.whine("Could not retrieve url from Location header, giving up")
        host, port, restOfUrl = self.getHostAndPortFromUrl(newUrl)
        if not host and not port and not restOfUrl:
            Err.whine("Could not retrieve url from Location header, giving up")
        if not host:
            host = self.connection.host
        if not port:
            port = self.connection.port
        self.connection.resetHostAndPort(host, port)
        self.setUrlInfo(YaS3ArbitraryUrl(restOfUrl))

    def mustRedoReqAfterRedir(self, reply):
        """
        Prepare for redoing a request after 301/307 redirect, if any
        
        Returns True if there is a redirect to be done, False otherwise
        
        Arguments:
        reply  -- HTTPResponse Object
        """
        if reply.status == 301 or reply.status == 307:
            self.setHostPortUrlForRedir(reply)
            return True
        else:
            return False

    def mustDoGetReqAfterRedir(self, reply):
        """
        Prepare for doing a GET request after 302/303 redirect, if any
        
        Returns True if there is a redirect to be done, False otherwise
        
        Arguments:
        reply  -- HTTPResponse Object
        """
        if reply.status == 302 or reply.status == 303:
            self.setHostPortUrlForRedir(reply)
            return True
        else:
            return False

    def runWithRedirs(self):
        result = self.doS3()
        # change redirection handling if doGetReqAfterRedir returns true, 
        # to suit your request. This is a 302/303 redirect, which means 
        # to do a new request as GET (or HEAD if the original was a HEAD
        # request).  For most PUT/POST/DELET requests this isn't
        # going to make much sense; for most GET/HEAD requests, you should
        # be able to just rerun the request with the new url (set 
        # automatically)
        if self.mustRedoReqAfterRedir(result) or self.mustDoGetReqAfterRedir(result):
            # not sure about stash/restore here... generally check all those FIXME
            result = self.doS3()
        return result

    def runWithRedoRedirsOnly(self, err):
        """
        Arguments:
        err    -- message to be displayed if 302/303 redir is encountered
        """
        # here we will properly set up for 301/307 redirs but whine if
        # we see a 302/303 redir (i.e. a nudge to redo the same request
        # but as a GET to the new url).  PUT/POST/DELETE requests
        # that get a 302/303 back liley want to respond with an error
        # so they would use this method.
        result = self.doS3()
        if self.mustRedoReqAfterRedir(result):
            # not sure about stash/restore here... generally check all those FIXME
            result = self.doS3()
        elif self.mustDoGetReqAfterRedir(result):
            Err.whine(err)
        return result


    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        # change this to suit your derived class
        self.setUrlInfo(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["virtualHost"])

        return self.runWithRedirs()

    def removeNs(self, text, ns):
        if text.startswith(ns):
            return text[len(ns):]
        else:
            return text

    def displayXmlData(self, treeTagNames):
        """
        Dig out the relevant bits from (successful) XML response body and
        display to stdout

        Arguments:
        list of tags in order from top level to tag of the element we
        want to display. For example, if the XML has <Buckets><bucket>...
        and we are interested in printing out the details of each bucket,
        we would pass [ 'Buckets', 'Bucket' ]
        """
        if self.argDict["quiet"]:
            return

        if not self.s3Sess.data:
            return

        if self.argDict["raw"]:
            print self.s3Sess.data
            return

        # ok now we are set to print a formatted version
        tree =  xml.etree.ElementTree.fromstring(self.s3Sess.data)

        ns = ""
        if tree.tag[0] == '{':
            ns = tree.tag[:tree.tag.find('}') +1]
        if tree.tag == treeTagNames[-1] or tree.tag == ns + treeTagNames[-1]:
            elts = [ tree ]
        else:
            pathNs = "/".join([ ns + t for t in treeTagNames ])
            elts = tree.findall(pathNs)
            if not elts:
                path = "/".join(treeTagNames)
                elts = tree.findall(path)

        for i in elts:
            print "%s   " % treeTagNames[-1],
            # here if we have 'Initiator' or 'Ownder' and the value is none, we want to look up
            # 'DisplayName'
            print "|".join([ "%s:%s" % (self.removeNs(j.tag, ns), self.displayXmlFixup(j, ns)) for j in list(i) ])

    def displayXmlFixup(self, elt, ns):
        """
        Some tags have nested elements (Initiator, Owner) so we dig out the value
        we want from those (DisplayName)
        Also Part (want PartNumber, Etag)
        Arguments:
        elt       -- xml element as produced e.g. by a find() from etree
        ns        -- the xml namespace if any of the elt (including '{}')
        """
        if elt.text == None and (elt.tag == ns + "Initiator" or elt.tag == ns + "Owner"):
            elt = elt.find(ns + "DisplayName")
            if elt is not None:
                return elt.text
            else:
                return None
        elif elt.text == None and elt.tag == ns + "Part":
            num = elt.find(ns + "PartNumber")
            numText = num.text if num is not None else ""
            etag = elt.find(ns + "ETag")
            etagText = etag.text if etag is not None else ""
            return "('PartNumber':%s)('ETag':%s)" % (numText, etagText)
        else:
            return elt.text

class YaS3ListBucketsHandler(YaS3Handler):
    """
    Handler for S3 list buckets requests
    """

    mandatory = [ "accessKey", "secretKey" ] # mandatory arguments for this operation

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3ListBucketsHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "GET"

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3UrlBuilder(virtualHost = self.argDict["virtualHost"]))

        result = self.runWithRedirs()
        self.displayXmlData([ 'Buckets', 'Bucket' ])
        return result

    def doS3(self):
        """
        Do a single S3 list buckets request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        reply = super(YaS3ListBucketsHandler,self).doS3()
        return reply

class YaS3CreateBucketHandler(YaS3Handler):
    """
    Handler for S3 create bucket requests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3CreateBucketHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "PUT"

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3UrlBuilder(self.argDict["bucketName"], virtualHost = self.argDict["virtualHost"]))

        return self.runWithRedoRedirsOnly("302/303 redirect encountered on create bucket, giving up")

    def doS3(self):
        """
        Do a single S3 create bucket request, without following Location header
        If we are told to redirect and use GET, that's a different and weird request
        but ok, it will be handled transparently by the parent doS3(), we do 
        nothing special

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        reply = super(YaS3CreateBucketHandler,self).doS3()
        return reply

class YaS3UploadObjectHandler(YaS3Handler):
    """
    Handler for S3 upload object requests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "localFileName", "remoteFileName" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3UploadObjectHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "PUT"
        if not self.argDict["remoteFileName"]:
            self.argDict["remoteFileName"] = self.argDict["localFileName"]
            self.setRemoteFileName(self.argDict["remoteFileName"])

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3UrlBuilder(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["virtualHost"]))

        return self.runWithRedoRedirsOnly("302/303 redirect encountered on upload object, giving up")

    def doS3(self):
        """
        Do a single S3 upload object request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        # this is cached so it's fine to call multiple times
        md5 = self.localFile.getMd5B64()
        if self.argDict["verbose"]:
            md5ToPrint = self.localFile.getMd5()
            print "md5 of file %s: %s and %s" % (self.localFile.name, md5, md5ToPrint)
        contentLength = self.localFile.getSize()
        contentType = ""
        headerInfo = self.setStandardHeaders()
        self.setOtherHeaders(headerInfo, md5, contentLength)

        if not self.s3Req:
            self.s3Req = YaS3Requester(self.reqType, self.argDict["verbose"], self.argDict["quiet"], self.argDict["raw"])

        reply, data = self.s3Req.makeUploadObjectRequest(self.connection, headerInfo, self.urlInfo, contentLength, md5, self.localFile.name)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

    def setOtherHeaders(self, headerInfo, md5, contentLength, auth = True):
        headerInfo.addHeader("Content-MD5", md5)
        headerInfo.addHeader("Content-Length", contentLength)
        headerInfo.addHeader("Expect", "100-continue")
        super(YaS3UploadObjectHandler,self).setOtherHeaders(headerInfo, auth)

class YaS3UploadObjectAsMPHandler(YaS3Handler):
    """
    Handler for S3 upload object as multi-part uploadrequests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "localFileName", "remoteFileName", "mpChunkSize" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3UploadObjectAsMPHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "PUT"
        if not self.argDict["remoteFileName"]:
            self.argDict["remoteFileName"] = self.argDict["localFileName"]
            self.setRemoteFileName(self.argDict["remoteFileName"])

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        # no setUrlInfo because this call consists of a set of calls to handlers which will do their own

        return self.runWithRedoRedirsOnly("302/303 redirect encountered on upload object as mp, giving up")

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
            s3 = self.ops["uploadobject"][0](self.ops, self.args, self.s3Sess)
            s3.setUp()
            reply = s3.runS3()
            return reply
        else:
            s3 = self.ops["startmpupload"][0](self.ops, self.args, self.s3Sess)
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
                s3 = self.ops["uploadmppart"][0](self.ops, self.args, self.s3Sess)
                s3.setUp()
                reply = s3.runS3()
                # if the reply is bad, note it and continue
                if reply.status != 200:
                    errors += 1
                    if not self.argDict["quiet"]:
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
            s3 = self.ops["endmpupload"][0](self.ops, self.args, self.s3Sess)
            s3.setUp()
            reply = s3.runS3()
            return reply

class YaS3GetObjectHandler(YaS3Handler):
    """
    Handler for S3 get object requests
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
        super(YaS3GetObjectHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "GET"
        if not self.argDict["remoteFileName"]:
            self.argDict["remoteFileName"] = self.argDict["localFileName"]
            self.setRemoteFileName(self.argDict["remoteFileName"])

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3UrlBuilder(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["virtualHost"]))

        return self.runWithRedirs()

    def doS3(self):
        """
        Do a single S3 get objct request, without following Location header

        Returns HTTPResponse object and saves any data *that is not the file* from the body of the
        response in the YaS3SessionInfo object
        """
        headerInfo = self.setStandardHeaders()
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3Requester(self.reqType, self.argDict["verbose"], self.argDict["quiet"], self.argDict["raw"])

        reply, data = self.s3Req.makeGetObjectRequest(self.connection, headerInfo, self.urlInfo, self.localFile.name)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

class YaS3ListOneBucketHandler(YaS3Handler):
    """
    Handler for S3 list given bucket requests
    """

    mandatory = [ "s3Host", "bucketName" ]

    # FIXME amazon for example limits results to 1000 per bucket, need to figure out how to retrieve the rest
    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3ListOneBucketHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "GET"

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3UrlBuilder(self.argDict["bucketName"], virtualHost = self.argDict["virtualHost"]))

        result = self.runWithRedirs()
        self.displayXmlData([ 'Contents' ])
        return result

class YaS3DeleteObjectHandler(YaS3Handler):
    """
    Handler for S3 delete object requests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "remoteFileName" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3DeleteObjectHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "DELETE"

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3UrlBuilder(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["virtualHost"]))

        return self.runWithRedoRedirsOnly("302/303 redirect encountered on delete object, giving up")

class YaS3DeleteBucketHandler(YaS3Handler):
    """
    Handler for S3 delete buckets requests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3DeleteBucketHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "DELETE"

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3UrlBuilder(self.argDict["bucketName"], virtualHost = self.argDict["virtualHost"]))

        return self.runWithRedoRedirsOnly("302/303 redirect encountered on delete bucket, giving up")

class YaS3GetObjectS3MetadataHandler(YaS3Handler):
    """
    Handler for S3 get object metadata requests (this is "head object')
    """

    mandatory = [ "s3Host", "bucketName", "remoteFileName" ]

    # thisis a head request, it might return very little
    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3GetObjectS3MetadataHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "HEAD"

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3UrlBuilder(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["virtualHost"]))

        return self.runWithRedirs()

    def doS3(self):
        """
        Do a single S3 get object S3 metadata request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        headerInfo = self.setStandardHeaders()
        self.setOtherHeaders(headerInfo)
        if not self.s3Req:
            self.s3Req = YaS3Requester(self.reqType, self.argDict["verbose"], self.argDict["quiet"], self.argDict["raw"])

        reply, data = self.s3Req.makeRequest(self.connection, headerInfo, self.urlInfo)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

class YaS3GetBucketS3MetadataHandler(YaS3Handler):
    """
    Handler for S3 get bucket S3 metadata requests (this is 'Head bucket')
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
        super(YaS3GetBucketS3MetadataHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "HEAD"

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3UrlBuilder(self.argDict["bucketName"], virtualHost = self.argDict["virtualHost"]))

        return self.runWithRedirs()

class YaS3CopyObjectHandler(YaS3Handler):
    """
    Handler for S3 copy object requests (copies an object from one bucket to another without local download)
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "sourceBucketName", "remoteFileName" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3CopyObjectHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "PUT"

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3UrlBuilder(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["virtualHost"]))

        result = self.runWithRedoRedirsOnly("302/303 redirect encountered on copy object, giving up")
        self.displayXmlData([ 'CopyObjectResult' ])
        return result

    def doS3(self):
        """
        Do a single S3 copy object request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        amzheaders = [ ( "x-amz-copy-source", urllib.quote("%s/%s" % ( self.argDict[ "sourceBucketName" ], self.argDict[ "remoteFileName" ] ) ) ) ]
        headerInfo = self.setStandardHeaders(amzheaders = amzheaders)
        self.setOtherHeaders(headerInfo)

        if not self.s3Req:
            self.s3Req = YaS3Requester(self.reqType, self.argDict["verbose"], self.argDict["quiet"], self.argDict["raw"])

        reply, data = self.s3Req.makeRequest(self.connection, headerInfo, self.urlInfo)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

class YaS3ListMPUploadsHandler(YaS3Handler):
    """
    Handler for S3 list multi-part uploads requests (lists all uncompleted
    multi-part uploads for a given bucket)
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3ListMPUploadsHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "GET"

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3ListMPUploadsUrl(self.argDict["bucketName"], virtualHost = self.argDict["virtualHost"]))

        result = self.runWithRedirs()
        self.displayXmlData([ "Upload" ])
        return result

class YaS3StartMPUploadHandler(YaS3Handler):
    """
    Handler for S3 start multi-part upload requests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "remoteFileName" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3StartMPUploadHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "POST"

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3StartMPUploadUrl(self.argDict["bucketName"], self.argDict["remoteFileName"], virtualHost = self.argDict["virtualHost"]))

        result = self.runWithRedoRedirsOnly("302/303 redirect encountered on start mp upload, giving up")
        self.displayXmlData([ "InitiateMultipartUploadResult" ])
        return result

class YaS3EndMPUploadHandler(YaS3Handler):
    """
    Handler for S3 end multi-part upload requests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName",  "remoteFileName", "mpUploadId", "mpPartsAndEtags" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3EndMPUploadHandler,self).__init__(ops, args, s3Sess)
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

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3MPUploadUrl(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["mpUploadId"], virtualHost = self.argDict["virtualHost"]))

        result = self.runWithRedoRedirsOnly("302/303 redirect encountered on end mp upload, giving up")
        self.displayXmlData([ "CompleteMultipartUploadResult" ])
        return result

    def doS3(self):
        """
        Do a single S3 end multi-part upload request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        xml = self.getMpUploadReqXML()
        contentLength = len(xml)
        headerInfo = self.setStandardHeaders()
        self.setOtherHeaders(headerInfo, contentLength)

        if not self.s3Req:
            self.s3Req = YaS3Requester(self.reqType, self.argDict["verbose"], self.argDict["quiet"], self.argDict["raw"])

        reply, data = self.s3Req.makePostRequestWithData(self.connection, headerInfo, self.urlInfo, xml)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

    def setOtherHeaders(self, headerInfo, contentLength, auth = True):
        headerInfo.addHeader("Content-Length", contentLength)
        super(YaS3EndMPUploadHandler,self).setOtherHeaders(headerInfo, auth)

class YaS3AbortMPUploadHandler(YaS3Handler):
    """
    Handler for S3 abort multi-part upload requests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "remoteFileName", "mpUploadId" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3AbortMPUploadHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "DELETE"

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3MPUploadUrl(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["mpUploadId"], virtualHost = self.argDict["virtualHost"]))

        return self.runWithRedoRedirsOnly("302/303 redirect encountered on abort mp upload, giving up")

class YaS3ListOneMPUploadHandler(YaS3Handler):
    """
    Handler for S3 list given multi-part upload requests (list is based on UploadId returned
    from start multi-part upload request or from listing all incomplete multi-part uploads
    for the given bucket)
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "remoteFileName", "mpUploadId" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3ListOneMPUploadHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "GET"

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3MPUploadUrl(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["mpUploadId"], virtualHost = self.argDict["virtualHost"]))

        result = self.runWithRedirs()
        self.displayXmlData([ "ListPartsResult" ])
        return result

class YaS3UploadMPPartHandler(YaS3Handler):
    """
    Handler for S3 upload one part of multi-part upload requests
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "remoteFileName", "mpPartNum", "mpUploadId", "localFileName" ]

    def __init__(self, ops, args, s3Sess):
        """
        Constructor

        Arguments:
        ops         -- list of predefined operations from a HandlerFactory
        args        -- YaS3Args object
        s3Sess      -- YaS3SessonInfo object
        """
        super(YaS3UploadMPPartHandler,self).__init__(ops, args, s3Sess)
        self.reqType = "PUT"

    def runS3(self):
        """
        Do a single S3 request, following Location header for 307 response codes
        """
        self.setUrlInfo(YaS3UploadMPPartUrl(self.argDict["bucketName"], self.argDict["remoteFileName"], self.argDict["mpPartNum"], self.argDict["mpUploadId"], virtualHost = self.argDict["virtualHost"]))

        return self.runWithRedoRedirsOnly("302/303 redirect encountered on upload mp part, giving up")

    def doS3(self):
        """
        Do a single S3 upload part (of a multi-part upload) request, without following Location header

        Returns HTTPResponse object and saves any data in the body of the
        response in the YaS3SessionInfo object
        """
        chunkSize = int(self.argDict["mpChunkSize"]) if self.argDict["mpChunkSize"] else None
        md5 = self.localFile.getMd5B64(int(self.argDict["mpFileOffset"]), chunkSize)
        if self.argDict["verbose"]:
            md5ToPrint = self.localFile.getMd5(int(self.argDict["mpFileOffset"]), chunkSize)
            print "md5 of file %s: %s and %s" % (self.localFile.name, md5, md5ToPrint)
        contentLength = self.localFile.getSize(int(self.argDict["mpFileOffset"]), chunkSize)
        contentType = ""
        headerInfo = self.setStandardHeaders()
        self.setOtherHeaders(headerInfo, md5, contentLength)

        if not self.s3Req:
            self.s3Req = YaS3Requester(self.reqType, self.argDict["verbose"], self.argDict["quiet"], self.argDict["raw"])

        reply, data = self.s3Req.makeUploadObjectRequest(self.connection, headerInfo, self.urlInfo, contentLength, md5, self.localFile.name, int(self.argDict["mpFileOffset"]), chunkSize)
        self.s3Sess.data = data
        self.restoreUrlInfo()
        return reply

    def setOtherHeaders(self, headerInfo, md5, contentLength, auth = True):
        headerInfo.addHeader("Content-MD5", md5)
        headerInfo.addHeader("Content-Length", contentLength)
        headerInfo.addHeader("Expect", "100-continue")
        super(YaS3UploadMPPartHandler,self).setOtherHeaders(headerInfo, auth)

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

    def __new__(cls, op, args, s3Sess):
        """
        Given the specific operation, return a new Handler object for that operation
        """
        if op in YaS3HandlerFactory.ops:
            return YaS3HandlerFactory.ops[op][0](YaS3HandlerFactory.ops, args, s3Sess)
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
                shortOpt = "(-%s) " % self.args.getShortOption(a) if self.args.getShortOption(a) else ""
                sys.stderr.write("  --%s: %s%s%s%s\n" % (name, spaces, shortOpt, a[4], default))
        sys.stderr.write("\n")

        sys.stderr.write("Flags:\n")
        for a in argList:
            if not a[2]: # doesn't want a value
                name = a[1] if a[1] else a[0]
                spaces = ' ' *(2 + maxOptionLen - len(name))
                default = " (default %s)" % a[5] if a[5] != None else ""
                shortOpt = "(-%s) " % self.args.getShortOption(a) if self.args.getShortOption(a) else ""
                sys.stderr.write("  --%s: %s%s%s%s\n" % (name, spaces, shortOpt, a[4], default))
        sys.stderr.write("\n")

        opsList = self.ops.keys()
        opsList.sort()
        if len(opsList):
            sys.stderr.write("Operations:\n")
            self.printNCols(opsList, 3)
        sys.stderr.write("\n")

        configList = [ a[1] if a[1] else a[0] for a in argList if a[3] ]
        configList.sort()
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

            numItems = len(items)
            numLines = numItems / numCols
            if numItems % numCols:
                numLines += 1

            for i in range(0, numLines):
                for j in [items[k] for k in range(i, numItems, numLines)]:
                    spaces = ' ' *(2 + maxLen - len(j))
                    sys.stderr.write("  %s%s" % (j, spaces))
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
            options, remainder = getopt.gnu_getopt(sys.argv[1:], self.args.getShortOptsForGetopt(), self.args.getOptListForGetopt())
        except getopt.GetoptError, err:
            print str(err)
            self.errors.usage("Unknown option specified")

        if not len(options) or not len(options[0]):
            self.errors.usage("No options specified.")
        if len(remainder):
            self.errors.usage("Unknown option specified: %s" % remainder)
        
        for opt, val in options:
            if opt.startswith("--"):
                self.args.setOptDictVal(opt[2:], val) # all args passed in on command line
            elif opt[0] == '-':
                self.args.setOptDictVal(self.args.getOptNameFromShortOpt(opt[1:]), val) # all args passed in on command line
            else:
                self.errors.usage("Unknown option specified: %s" % opt)

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
    argDict = args.mergedDict

    s3lib.checkMissing([ "operation" ]) # other checks will happen in the handlers

    s3Sess = YaS3SessionInfo(s3lib.errors)

    s3 = YaS3HandlerFactory(argDict["operation"], args, s3Sess)
    if not s3:
        s3lib.errors.usage("Unknown operation %s" % argDict["operation"])
        
    s3.setUp()

    result = s3.runS3()
    s3.tearDown()
    if result.status < 200 or result.status >= 400:
        print "result status is", result.status
        sys.exit(1)
    
        
