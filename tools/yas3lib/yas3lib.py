import os, re, sys, time, hashlib, hmac, binascii, httplib, urllib, getopt, ConfigParser, xml.etree.ElementTree
from yas3http import YaS3HTTPDate, YaS3HTTPHeaders, YaS3HTTPCookie
from utils import Err, ErrExcept, PPXML

class YaS3UrlInfo(object):
    """
    Methods for producing or retrieving the base url for some S3 request
    """

    def __init__(self, bucketName = None, remoteFileName = None, virtualHost = False):
        """
        Constructor

        Arguments:
        bucketName      -- name of bucket, if any
        remoteFileName  -- name of object in bucket, if any
        virtualHost     -- whether we are using a S3 virtual host based on the bucket name
        """
        self.bucketName = bucketName
        self.remoteFileName = remoteFileName
        self.virtualHost = virtualHost
        self.url = self._getUrlBase()

    def _getUrlBase(self):
        """
        Return the base url for S3 requests with the specified bucket and object
        If neither bucket nor object name were set in the constructor, the url / will be returned
        """
        if self.virtualHost:
            elts = filter(None, [self.remoteFileName.lstrip("/") if self.remoteFileName else None])
        else:
            elts = filter(None, [self.bucketName.lstrip("/") if self.bucketName else None, self.remoteFileName])
        if not len(elts):
            return '/'
        else:
            return '/' + '/'.join(elts)

    def resetUrl(self, bucketName = None, remoteFileName = None):
        """
        Reset the url attributes outside of the constructor

        Arguments:
        bucketName      -- name of bucket, if any
        remoteFileName  -- name of object in bucket, if any
        """
        if bucketName:
            self.bucketName = bucketName
        if remoteFileName:
            self.remoteFileName = remoteFileName
        self.url = self._getUrlBase()

    def getUrl(self):
        """
        Return the url for S3 requests corrsponding to the specified bucket/object
        """
        return self.url

class YaS3ArbitraryUrl(YaS3UrlInfo):
    """
    Methods for producing or retrieving an arbitrary url
    """

    def __init__(self, url):
        """
        Constructor

        Arguments:
        url -- the url (not the host/port/protocol) to be stored/retrieved
        """
        self.url = url

class YaS3ListMPUploadsUrl(YaS3UrlInfo):
    """
    Methods for producing or retrieving the S3 url for listing multipart uploads
    """

    def __init__(self, bucketName):
        """
        Constructor

        Arguments:
        bucketName      -- name of bucket to list
        """
        self.bucketName = bucketName
        self.url = self._getUrlBase()

    # FIXME if we have virtual hostname then what?
    def _getUrlBase(self):
        """
        Return the base url for listing S3 multipart uploads with the specified bucket
        """
        return "/%s?uploads" % ( self.bucketName )

class YaS3StartMPUploadUrl(YaS3UrlInfo):
    """
    Methods for producing or retrieving the base url for starting S3 multipart uploads
    """

    def __init__(self, bucketName, remoteFileName ):
        """
        Constructor

        Arguments:
        bucketName       -- name of bucket to which to upload
        remoteFileName   -- name of object to be uploaded
        """
        self.bucketName = bucketName
        self.remoteFileName = remoteFileName
        self.url = self._getUrlBase()

    # FIXME if we have virtual hostname then what?
    def _getUrlBase(self):
        """
        Return the base url for starting an S3 multipart upload of the given object to the specified bucket
        """
        return "/%s/%s?uploads" % ( self.bucketName, self.remoteFileName )

class YaS3MPUploadUrl(YaS3UrlInfo):
    """
    Methods for producing or retrieving the base url for ending/aborting S3 multipart uploads
    """

    def __init__(self, bucketName, remoteFileName, mpUploadId ):
        """
        Constructor

        Arguments:
        bucketName       -- name of bucket to which to upload
        remoteFileName   -- name of object to be uploaded
        mpUploadId       -- uploadId as returned from server when starting multipart upload
        """
        self.bucketName = bucketName
        self.remoteFileName = remoteFileName
        self.mpUploadId = mpUploadId
        self.url = self._getUrlBase()

    # FIXME if we have virtual hostname then what?
    def _getUrlBase(self):
        """
        Return the base url for ending/aborting S3 multipart uploads of the given object to the specified bucket
        """
        return "/%s/%s?uploadId=%s" % ( self.bucketName, self.remoteFileName, self.mpUploadId )

class YaS3UploadMPPartUrl(YaS3UrlInfo):
    """
    Methods for producing or retrieving the base url for doing one part of an S3 multipart upload
    """

    def __init__(self, bucketName, remoteFileName, mpPartNum, mpUploadId ):
        """
        Constructor

        Arguments:
        bucketName       -- name of bucket to which to upload
        remoteFileName   -- name of object to be uploaded
        mpPartNum        -- part number of part to be uploaded
        mpUploadId       -- uploadId as returned from server when starting multipart upload
        """
        self.bucketName = bucketName
        self.remoteFileName = remoteFileName
        self.mpUploadId = mpUploadId
        self.mpPartNum = mpPartNum
        self.url = self._getUrlBase()

    # FIXME if we have virtual hostname then what?
    def _getUrlBase(self):
        """
        Return the base url for uploading one part of an S3 multipart upload to the given object and bucket
        """
        return "/%s/%s?partNumber=%s&uploadId=%s" % ( self.bucketName, self.remoteFileName, self.mpPartNum, self.mpUploadId )

class YaS3AuthInfo(object):
    """
    Methods for S3 authoriazation
    """

    def __init__(self, accessKey, secretKey):
        """
        Constructor

        Argumnents:
        accessKey -- the account name
        secretKey -- the secret key used for signing requests
        """
        self.accessKey = accessKey
        self.secretKey = secretKey

    def getSig(self, reqType, contentMd5, contentType, date, url, amzheaders = []):
        """
        return S3 signature for request based on the parameters that should be signed
        
        Arguments:
        reqType     -- type of request (GET/PUT/POST/HEAD/DELETE)
        contentMd5  -- md5 of body to be sent, if there is a text body, or ""
        contentType -- value of Content-Type header to be sent, or "" if there is none
        date        -- value of the HTTP Date header for the request
        url         -- url (excludes host/port/protocol) of the request
        amzheaders  -- any headers starting with x-amz to be sent with request
        """
        # fixme does the url need some special encoding??
        if len(amzheaders):
            amzheaders.sort(key = lambda tup: tup[0])
            amzheaderString = "".join([ "%s:%s\n" % (h.lower(),v) for (h,v) in amzheaders ])
        else:
            amzheaderString = ""
        self.stringForSigning =  "%s\n%s\n%s\n%s\n%s%s" % (reqType, contentMd5, contentType, date, amzheaderString, url)
        hasher = hmac.new(self.secretKey, self.stringForSigning, hashlib.sha1)
        result = hasher.digest()
        return binascii.b2a_base64(result).rstrip("\n")

    def getAWSHeader(self, reqType, contentMd5, contentType, date, url, amzheaders = []):
        """
        Return value of HTTP Authorization header using standard S3 auth for given request parameters

        Arguments:
        reqType     -- type of request (GET/PUT/POST/HEAD/DELETE)
        contentMd5  -- md5 of body to be sent, if there is a text body, or ""
        contentType -- value of Content-Type header to be sent, or "" if there is none
        date        -- value of the HTTP Date header for the request
        url         -- url (excludes host/port/protocol) of the request
        amzheaders  -- any headers starting with x-amz to be sent with request
        """
        return ("AWS %s:%s" %( self.accessKey, self.getSig(reqType, contentMd5, contentType, date, url, amzheaders)))

    # FIXME allow this to be set by something :-D
    def getLOWHeader(self):
        """
        Return value of HTTP Authorization header for archive.org of type 'LOW' (risky!!)
        """
        return ("LOW %s:%s" %( self.accessKey, self.secretKey ))

class YaS3LocalFile(object):
    """
    Methods for operating on local files for upload/retrieval
    """

    def __init__(self, localFileName = None):
        """
        Constructor

        Arguments:
        localFileName -- full path of local file which will be uploaded/saved
        """
        # not that this stuff expects a fixed size file. if it
        # grows during the call you are going to be SOL
        self.name = localFileName
        self.hasher = None
        self.offset = None
        self.byteCount = None
        self.buffSize = 65536

    def doMd5(self, offset = None, byteCount = None):
        """
        Compute md5 of file from given offset for given byteCount
        If the md5 has already been computed for the specified offset/byteCount, 
        return it and don't recompute

        Arguments:
        offset     -- seek to this point in file before starting md5, default: 0
        byteCount  -- do md5 of this many bytes of file, default: through eof
        """
        if not self.name:
            return None

        if self.hasher and self.offset == offset and self.byteCount == byteCount:
            result = self.hasher.digest()
            return binascii.b2a_base64(result).rstrip("\n")

        self.hasher = hashlib.md5()
        self.offset = offset if offset else 0
        self.byteCount = byteCount if byteCount else os.path.getsize(self.name)

        infd = file(self.name, "rb")
        infd.seek(offset)
        bytesRead = 0

        data = infd.read(self.byteCount if self.byteCount < self.buffSize else self.buffSize)
        while data:
            bytesRead += len(data)
            self.hasher.update(data)
            if bytesRead == byteCount:
                break
            data = infd.read(self.byteCount - bytesRead if self.byteCount - bytesRead < self.buffSize else self.buffSize)
        infd.close()

    def getMd5B64(self, offset = 0, byteCount = None):
        """
        Return base 64 encded md5 of file from given offset for given byteCount
        If the md5 has already been computed for the specified offset/byteCount, 
        return it and don't recompute

        Arguments:
        offset     -- seek to this point in file before starting md5, default: 0
        byteCount  -- do md5 of this many bytes of file, default: through eof
        """
        self.doMd5(offset, byteCount)
        result = self.hasher.digest()
        return binascii.b2a_base64(result).rstrip("\n")

    def getMd5(self, offset = 0, byteCount = None):
        """
        Return hex string of md5 of file from given offset for given byteCount
        If the md5 has already been computed for the specified offset/byteCount, 
        return it and don't recompute

        Arguments:
        offset     -- seek to this point in file before starting md5, default: 0
        byteCount  -- do md5 of this many bytes of file, default: through eof
        """
        self.doMd5(offset, byteCount)
        return self.hasher.hexdigest()

    def getSize(self, offset = 0, byteCount = None):
        """
        Return size of file from offset and up to byteCount bytes

        offset     -- skip this many bytes of file when computing size, default: 0
        byteCount  -- max size to be returned, default: size through eof
        """
        if not self.name:
            return None

        size = os.path.getsize(self.name)
        # really? check and fixme
        if not size:
            return None

        if offset:
            # size is what's left after seek to the offset
            size = size - offset if size > offset else 0

        # if we were given a max number of bytes to process, 
        # return that if the bytes left in the file from the offset to the end are enough
        return byteCount if byteCount and size > byteCount else size

class YaS3Requester(object):
    """
    Methods for making S3 requests
    """

    def __init__(self, reqType, verbose, quiet):
        """
        Constructor

        Arguments:
        reqType   -- Type of HTTP request (GET/HEAD/PUT/POST/DELETE)
        verbose   -- print lots of messages about what's being processed including HTTP headers
        quiet     -- suppress all extra messages, even XML bodies returned by requests
        """
        # FIXME what does quiet really suppress? not clear
        self.reqType = reqType
        self.verbose = verbose
        if self.verbose:
            self.quiet = False
        else:
            self.quiet = quiet
        self.buffSize = 65536 # wonder what a good r/w buffer size is anyways

    def putHeaders(self, c, headerInfo):
        """
        Get headers ready to send to an HTTPConnection
        This does not actually send the headers to the remote server,
        it just queues them up

        Arguments:
        c           -- YaS3Connection object
        headerInfo  -- YaS3HTTPHeaders object
        """
        for h,v in headerInfo.headers:
            c.conn.putheader(h,v)
        if self.verbose:
            headerInfo.printAllHeaders()
        c.conn.endheaders()

    def makeRequest(self, c, headerInfo, urlInfo, contentLength=None, md5=None):
        """
        Send an HTTP request to the remote server and get headers and response back

        Arguments:

        c              -- YaS3Connection object
        headerInfo     -- YaS3HTTPHeaders object
        urlInfo        -- YaS3UrlInfo object
        contentLength  -- content length of body to be sent, if any
        md5            -- base 64 of md5 of body to be sent, if any
        """
        self.sendReqFirstLine(c, urlInfo.getUrl())
        self.putHeaders(c, headerInfo)

        reply = c.conn.getresponse()
        headersReceived = reply.getheaders()
        data = self.getResponse(reply, headersReceived)
        return reply, data

    def getResponse(self, reply, headersReceived):
        """
        From reply to HTTP request, get and potentially display HTTP 
        response and reason codes from server; potentially display 
        HTTP headers received from server

        Arguments:
        reply            --
        headeersReceived --
        """
        reason = reply.reason
        status = reply.status
        if not self.quiet and (not str(status).startswith('2') or self.verbose):
            print status, reason

        if self.verbose:
            for (header, value) in headersReceived:
                print "%s: %s" % (header, value)

        data = None
        cl = self.getContentLengthFromHeaders(headersReceived)
        if cl or status == 200 or status >= 300:
            # FIXME needs timeouts
            data = reply.read(cl) if cl else reply.read()
            if self.verbose:
                print "Response body from server:", data
            if not self.quiet:
                PPXML.cheapPrettyPrintXML(data)
        return data

    def getContentLengthFromHeaders(self, headers):
        """
        Find the content length header if any, retrieve and return the value, or 0 if none found

        Arguments:
        headers -- list of headers in format [ (headername, value), ( hname2, value2)...]
        """
        if not len(headers):
            return 0
        for (header, value) in headers:
            if header == "content-length":
                return int(value)
        return 0

    def makeUploadObjectRequest(self, c, headerInfo, urlInfo, contentLength, md5, localFileName, mpFileOffset = None, mpChunkSize = None):
        """
        Upload an object (file) to the remote server and get headers and response back

        Arguments:
        c              -- YaS3Connection object
        headerInfo     -- YaS3HTTPHeaders object
        urlInfo        -- YaS3UrlInfo object
        contentLength  -- content length of body to be sent, if any
        md5            -- base 64 of md5 of body to be sent, if any
        localFileName  -- full path of local file to upload
        mpFileOffset   -- upload from this point in the file
        mpChunkSize    -- upload at most this many bytes of file
        """
        self.sendReqFirstLine(c, urlInfo.getUrl())
        self.putHeaders(c, headerInfo)

        self.sendFile(c, localFileName, mpFileOffset, mpChunkSize)

        reply = c.conn.getresponse()
        headersReceived = reply.getheaders()
        data = self.getResponse(reply, headersReceived)
        return reply, data

    def makeGetObjectRequest(self, c, headerInfo, urlInfo, localFileName):
        """
        Get an object (file) from the remote server and get headers and response back

        Arguments:
        c              -- YaS3Connection object
        headerInfo     -- YaS3HTTPHeaders object
        urlInfo        -- YaS3UrlInfo object
        localFileName  -- full path of local file where object will be saved
        """
        self.sendReqFirstLine(c, urlInfo.getUrl())
        self.putHeaders(c, headerInfo)

        reply = c.conn.getresponse()

        headersReceived = reply.getheaders()
        if str(reply.status).startswith('2'):
            self.getFile(reply, localFileName)
            data = None
        else:
            data = self.getResponse(reply, headersReceived)
        return reply, data

    def sendReqFirstLine(self, c, url):
        """
        Send the first line of an HTTP request to the remote server

        Arguments:
        c             -- YaS3Connection object
        url           -- url of request without host/port/protocol
        """
        c.conn.putrequest(self.reqType, url, skip_host = True, skip_accept_encoding = True)
        if self.verbose:
            print "%s %s HTTP/1.1" %( self.reqType, url )

    def sendFile(self, c, path, offset = None, byteCount = None):
        """
        Send a local file to the remote server
        It is assumed that the caller has already sent the first line
        of the HTTP request as well as all HTTP headers, and that this is 
        a straight 'PUT' request with no fancy encoding needed

        Arguments:
        path       -- full path of local file to upload
        offset     -- upload from this point in the file
        byteCount  -- upload at most this many bytes of file
        """
        infd = file(path, "rb")
        if offset:
            infd.seek(offset)
        bytesRead = 0
        data = infd.read(byteCount if byteCount and byteCount < self.buffSize else self.buffSize)
        while data:
            bytesRead += len(data)
            c.conn.send(data)
            if bytesRead == byteCount:
                break
            data = infd.read(byteCount - bytesRead if byteCount and byteCount - bytesRead < self.buffSize else self.buffSize)
        infd.close()

    def getFile(self, reply, path):
        """
        Get data from the remote server and save it as a local file
        It is assumed that the caller has already sent the first line
        of the HTTP request as well as all HTTP headers

        Arguments:
        reply      -- HTTPResponse object
        path       -- full path of local file to which to save the data
        """
        outfd = file(path, "wb")
        data = reply.read(self.buffSize)
        while data:
            outfd.write(data) 
            data = reply.read(self.buffSize)
        outfd.close()

    # fixme why is 'XML' in the name? oh well
    def makePostRequestWithXML(self, c, headerInfo, urlInfo, xml):
        """
        Do an HTTP POST request to remote server, sending body text in the post
        It is assumed that the caller has already sent the first line
        of the HTTP request as well as all HTTP headers; additionally
        no encoding is done on the text to be sent.

        Arguments:
        c           -- YaS3Connection object
        headerInfo  -- YaS3HTTPHeaders object
        urlInfo     -- YaS3UrlInfo object
        xml         -- text to be sent in body
        """
        self.sendReqFirstLine(c, urlInfo.getUrl())
        self.putHeaders(c, headerInfo)

        # FIXME is it possible for the xml to be larger than a standard buffer size of 64k?
        c.conn.send(xml)

        reply = c.conn.getresponse()
        headersReceived = reply.getheaders()
        data = self.getResponse(reply, headersReceived)
        return reply, data

class YaS3Connection(object):
    """
    Methods for managing the HTTP connection to a remote server
    """

    def __init__(self, host, port):
        """
        Constructor

        Arguments:
        host   -- fqdn of remote server
        port   -- port number of connection
        """
        self.conn = httplib.HTTPConnection(host, port)

    def open(self):
        """
        Initiate the connection
        """
        self.conn.connect()

    def close(self):
        """
        Close the connection
        """
        self.conn.close()

    def resetHostAndPort(self, host, port):
        """
        Set the host and port outside of the constructor
        """
        self.conn.host = host
        self.conn.port = port
