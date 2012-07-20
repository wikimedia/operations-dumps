import os, re, sys, time, hashlib, hmac, binascii, httplib, getopt, yas3lib, urllib, json, yas3http, wmfmw
from yas3lib import YaS3Requester, YaS3AuthInfo, YaS3UrlInfo
from yas3 import YaS3Err, YaS3Handler, YaS3HandlerFactory, YaS3SessionInfo, YaS3RequestInfo
from yas3 import  YaS3ListBucketsHandler, YaS3ListOneBucketHandler, YaS3GetObjectHandler, YaS3CreateBucketHandler, YaS3UploadObjectHandler
from yas3 import YaS3GetObjectS3MetadataHandler, YaS3GetBucketS3MetadataHandler, YaS3DeleteObjectHandler, YaS3DeleteBucketHandler
from yas3http import YaS3HTTPDate, YaS3HTTPHeaders, YaS3HTTPCookie
from wmfmw import MWSiteMatrix, WikiMatrixInfo
from yas3archive import YaS3IAArgs, YaS3IARequester, YaS3IASessionInfo, YaS3IAAuthInfo, YaS3IAHandler
from yas3archive import YaS3IACheckBucketExistenceHandler, YaS3IAGetFilesXMLHandler, YaS3IAVerifyObjectHandler
from yas3archive import YaS3IAGetBucketIAMetadataHandler, YaS3IACreateBucketWithIAMetadataHandler, YaS3IAUpdateBucketIAMedataHandler
from yas3archive import YaS3IALoginHandler, YaS3IAUploadObjectHandler, YaS3IAEndMPUploadHandler
from yas3archive import YaS3IAShowBucketStatusHandler
from yas3archive import YaS3IAHandlerFactory, YaS3IALib, IAMetadataHandlerFactory, YaS3IAMetadata
from utils import Err, ErrExcept, PPXML

class WMFIAArgs(YaS3IAArgs):
    """
    Manages arguments passed on the command line or in a config file
    for S3 and non-S3 requests to archive.org, including special args
    for generating archive.org metadata automatically for Wikimedia XML
    dump files being uploaded
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
        super(WMFIAArgs,self).__init__()
        self.args.extend([
                [ "wiki", "", True, "wmfmeta", "name of wiki database for dump to be uploaded", None ],
                [ "matrixCache", "matrixcache", True, "wmfmeta", "location of cache of sitematrix file", None ],
                [ "apiUrl", "apiurl", True, "wmfmeta", "full url to api.php for the site matrix of known wikis", "http://en.wikipedia.org/w/api.php" ],
                [ "creator", "", True, "wmfmeta", "creator that will appear in the bucket (item) description", "the Wikimedia Foundation" ],
                [ "downloadUrl", "downloadurl", True, "wmfmeta", "url for dump downloads that will appear in the bucket (item) description", "http://dumps.wikimedia.org" ]
#                [ "licenseUrl", "licenseurl", True, "wmfmeta", "url for copyright license of dump that will appear in the bucket metadata", "http://wikimediafoundation.org/wiki/Terms_of_Use" ],
                ])

    # FIXME do we really need this??
    def getWMFMetadataArgs(self):
        """
        Return a list of opt names (as they appear on command line or in config file)
        and values for allpredefined arguments that can appear in the 'wmfmeta'
        section of the config file.

        These are all used for the creation of archive.org metadata values for Wikimedia
        XML dumps for wiki projects.
        """
        return [ (self.getOptName(a), self.mergedDict[self.getVarName(a)]) for a in self.args if self.getConfSection(a) == "wmfmeta" ]

class WMFIACreateBucketWithIAMetadataHandler(YaS3IACreateBucketWithIAMetadataHandler):
    """
    Handler for S3 requests to create buckets (items) with metadata specific
    to archive.org (via x-archive-X headers), also adding the archive.org
    size hint header if the estimated bucketsize in bytes is provided.
    This Handler generates metadata for a bucket assuming that the bucket will hold Wikimedia
    XML dumps of wiki projects.
    """

    mandatory = [ "accessKey", "secretKey", "s3Host", "bucketName", "wiki", "collectionHdr", "licenseUrlHdr", "downloadUrl", "creator" ]

    def __init__(self, ops, argDict, s3Sess, iAMetadataArgs, wMFMetadataArgs):
        """
        Constructor

        Arguments:
        ops             -- list of predefined operations from a HandlerFactory
        argDict         -- dictionary of args (from command line/config file/defaults) and values
        s3Sess          -- YaS3SessonInfo object
        iAMetadataArgs  -- list of args from predefined argument list that are in config section 'iametadata'
                           (these would be used to construct headers to set archive.org metadata for a specified
                           bucket (item) on archive.org
        wMFMetadataArgs -- list of args from predefined argument list that are in config section 'wmfmeta'
                           (these would be used to generate values for archive.org metadata for Wikimedia
                           XML dumps of wiki projects
        """
        super(WMFIACreateBucketWithIAMetadataHandler,self).__init__(ops, argDict, s3Sess, iAMetadataArgs)
        self.reqType = "PUT"
        self.iAMetadataArgs = iAMetadataArgs
        # fixme really do we need this as a separate arg list? 
        self.wMFMetadataArgs = wMFMetadataArgs

    def setOtherHeaders(self, headerInfo):
        """
        Generate all the headers containing metadata for buckets (items) on archive.org
        assuming that the buckets will contain Wikimedia XML dumps of wiki projects
        
        Sample headers for el wiktionary:
          x-archive-meta-title:        Wikimedia database dumps of el.wiktionary'
          x-archive-meta-mediatype:    web
          x-archive-meta-description:: Dumps of el.wiktionary created by the Wikimedia Foundation and downloadable 
                                       from http://dumps.wikimedia.org
          x-archive-meta-format:       xml and sql
          x-archive-meta-licenseurl:   http://wikimediafoundation.org/wiki/Terms_of_Use
          x-archive-meta-language:     el (Modern Greek)
          x-archive-meta-subject:      xml,dump,wikimedia,el,wiktionary
        
        Arguuments:
        headerInfo  -- YaS3HTTPHeaders object
        """
        
        headers =  [ ("title", "Wikimedia database dumps of %s" %  self.argDict["wiki"]), 
                     ("mediatype", "web"),
                     ("description", "Dumps of %s created by %s and downloadable from %s" % (self.argDict["wiki"], self.argDict["creator"], self.argDict["downloadUrl"])),
                     ("format", "xml and sql"),
                     ("licenseurl", self.argDict["licenseUrlHdr"]) ]

        for (name, value) in headers:
            headerInfo.addHeader(YaS3IAMetadata.getHeader(name), value)

        wmi = WikiMatrixInfo(self.argDict["wiki"], self.argDict["apiUrl"], self.argDict["matrixCache"], self.argDict["verbose"], self.argDict["dryrun"])
        langCode = wmi.getLangCode()
        localLangName = wmi.getLocalLangName()
        project = wmi.getProject()

        if langCode:
            headerInfo.addHeader("language", "%s (%s)" % (langCode, localLangName))
            headerInfo.addHeader("subject", "xml,dump,wikimedia,%s,%s" %(langCode, project))
        else:
            headerInfo.addHeader("subject", "xml,dump,wikimedia,%s" % project)

        # wow is this sketchy, don't like it. better approach?
        super(YaS3IACreateBucketWithIAMetadataHandler,self).setOtherHeaders(headerInfo)
        wmfheaders = [ name for (name, value) in headers ]
        for (name, value) in self.iAMetadataArgs:
            if name not in wmfheaders and value is not None:
                headerInfo.addHeader(YaS3IAMetadata.getHeader(name), value)

class WMFIAUpdateBucketIAMetadataHandler(WMFIACreateBucketWithIAMetadataHandler):
    """
    Handler for S3 requests to update already existing buckets (items) with metadata specific
    to archive.org (via x-archive-X headers). Note that all archive.org metadata that exists
    is replaced by the new metadata; if a fild is not updated it will bre removed. (Don't
    blame the library, blame archive.org :-P)
    This Handler generates metadata for a bucket assuming that the bucket will hold Wikimedia
    XML dumps of wiki projects.
    """

    def setOtherHeaders(self, headerInfo):
        """
        Set non-standard headers (all archive.org metadata handlers and 
        the special header that tells archive.org to update the metadata only
        ignoring the fact that there is already a bucket of the specified name)
        """
        super(WMFIAUpdateBucketIAMetadataHandler,self).setOtherHeaders(headerInfo)
        headerInfo.addHeader("x-archive-ignore-preexisting-bucket", "1")

class WMFIAHandlerFactory(IAMetadataHandlerFactory):
    """
    Produce the right Handler object based on the S3 or non-S3 operation, passing in extra
    args for archive.org metadata to the few Handlers that need it

    """

    introduction = [ "This library implements a subset of the S3 REST api for storage and",
                     "retrieval of objects in buckets, with additional non-s3",
                     "operations supported by the Internet Archive.  The Internet Archive",
                     "documentation and other interfaces use the terminology 'item' for 'bucket',",
                     "i.e. the container that holds content, and 'file' for 'object',",
                     "i.e. the content itself.  This library uses the standard S3 bucket/object",
                     "terminology but for archive.org-specific operations or extensions the",
                     "corresponding archive.org name will be given in parentheses.",
                     "This extension to the library is intended to be used for bulk uplaods",
                     "of Wikimedia XML data dumps (see http://download.wikimedia.org) to",
                     "archive.org, setting the special metadata information according to the",
                     "specific dump.  It probably isn't useful for anyone else." ]

    # operation name for command line, handler name
    ops = IAMetadataHandlerFactory.ops
    ops["wmfcreatebucketwithiametadata"] = [ WMFIACreateBucketWithIAMetadataHandler, "create bucket (item) with specified archive.org metadata (x-archive-X) for the given Wikimedia wiki dump" ]
    ops["wmfupdatebucketiametadata"] = [ WMFIAUpdateBucketIAMetadataHandler, "update archive.org metadata (x-archive-X) for the specified bucket (item) for the given Wikimedia wiki dump" ]

    # sketchy, maybe do this differently? FIXME
    def __new__(cls, op, argDict, s3Sess, iAMetadataArgs, wMFMetadataArgs):
        """
        """
        if op in WMFIAHandlerFactory.ops:
            if op == "wmfcreatebucketwithiametadata" or op == "wmfupdatebucketiametadata":
                return WMFIAHandlerFactory.ops[op][0](WMFIAHandlerFactory.ops, argDict, s3Sess, iAMetadataArgs, wMFMetadataArgs)
            else:
                return WMFIAHandlerFactory.ops[op][0](WMFIAHandlerFactory.ops, argDict, s3Sess)
        return None

class WMFIAMetadataLib(YaS3IALib):
    """
    Set up WMFIAMetadata library (S3 and non-S3 archive.org with WMF metadata library)
    for processing a request, reading args from the command line and config file
    """
    
    def __init__(self, args, errors):
        """
        Constructor

        Arguments:
        args      -- YaS3Args object (contains predefined list of arguments)
        errors    -- YsS#Err object (for usage messages)
        """
        super(WMFIAMetadataLib,self).__init__(args, errors)
        self.wMFMetadataArgs = args.getWMFMetadataArgs()
        if self.argDict["verbose"]:
            print self.wMFMetadataArgs

if __name__ == "__main__":
    """
    Command line client that will run any specified S3 or non S3 operation connecting to archive.org,
    filling in archive.org metadata for object uploads, expecting these objects to be Wikimedia
    XML dumps of wiki projects.
    """
    args = WMFIAArgs()
    s3lib = WMFIAMetadataLib(args, YaS3Err(args, WMFIAHandlerFactory.ops, WMFIAHandlerFactory.introduction))
    argDict = s3lib.argDict
    iAMetadataArgs = s3lib.iAMetadataArgs
    wMFMetadataArgs = s3lib.wMFMetadataArgs

    s3lib.checkMissing([ "operation" ]) # the rest will be checked in the handlers

    s3Sess = YaS3IASessionInfo(s3lib.errors, argDict["s3Host"], argDict["host"])

    s3Sess.setConnType("s3") # default, specific handlers will override this. must be before setRequestInfo
    s3Sess.setRequestInfo(YaS3RequestInfo(s3Sess.getHost(), argDict["port"], argDict["protocol"]))
    s3 = WMFIAHandlerFactory(argDict["operation"], argDict, s3Sess, iAMetadataArgs, wMFMetadataArgs)
    if not s3:
        s3lib.errors.usage("Unknown operation %s" % argDict["operation"])

    s3.setUp()

    result = s3.runS3()
    s3.tearDown()
