import socket
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

import os, re, sys, time, hashlib, hmac, binascii, httplib, urllib, getopt, ConfigParser, xml.etree.ElementTree
from yas3http import YaS3HTTPDate, YaS3HTTPHeaders, YaS3HTTPCookie
from utils import Err, ErrExcept, PPXML

class YaS3UrlBuilder(object):
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

    def buildUrl(self):
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

    def reset(self, bucketName = None, remoteFileName = None):
        """
        Reset the attributes outside of the constructor

        Arguments:
        bucketName      -- name of bucket, if any
        remoteFileName  -- name of object in bucket, if any
        """
        if bucketName:
            self.bucketName = bucketName
        if remoteFileName:
            self.remoteFileName = remoteFileName

class YaS3ArbitraryUrl(YaS3UrlBuilder):
    """
    Methods for producing or retrieving an arbitrary url
    """

    def __init__(self, url):
        """
        Constructor

        Arguments:
        url -- the url (not the host/port/protocol) to be stored/retrieved
        """
        self.arbitraryString = url

    def buildUrl(self):
        """
        Return the base url for requests which have an arbitrary url
        """
        return self.arbitraryString

class YaS3ListMPUploadsUrl(YaS3UrlBuilder):
    """
    Methods for producing or retrieving the S3 url for listing multipart uploads
    """

    def __init__(self, bucketName, virtualHost = False):
        """
        Constructor

        Arguments:
        bucketName      -- name of bucket to list
        """
        self.bucketName = bucketName
        self.virtualHost = virtualHost

    # FIXME if we have virtual hostname then what?
    def buildUrl(self):
        """
        Return the base url for listing S3 multipart uploads with the specified bucket
        """
        if self.virtualHost:
            return "/?uploads"
        else:
            return "/%s?uploads" % ( self.bucketName )

class YaS3StartMPUploadUrl(YaS3UrlBuilder):
    """
    Methods for producing or retrieving the base url for starting S3 multipart uploads
    """

    def __init__(self, bucketName, remoteFileName, virtualHost = False):
        """
        Constructor

        Arguments:
        bucketName       -- name of bucket to which to upload
        remoteFileName   -- name of object to be uploaded
        """
        self.bucketName = bucketName
        self.remoteFileName = remoteFileName
        self.virtualHost = virtualHost

    def buildUrl(self):
        """
        Return the base url for starting an S3 multipart upload of the given object to the specified bucket
        """
        if self.virtualHost:
            return "/%s?uploads" % self.remoteFileName
        else:
            return "/%s/%s?uploads" % ( self.bucketName, self.remoteFileName )

class YaS3MPUploadUrl(YaS3UrlBuilder):
    """
    Methods for producing or retrieving the base url for ending/aborting S3 multipart uploads
    """

    def __init__(self, bucketName, remoteFileName, mpUploadId, virtualHost = False):
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
        self.virtualHost = virtualHost

    def buildUrl(self):
        """
        Return the base url for ending/aborting S3 multipart uploads of the given object to the specified bucket
        """
        if self.virtualHost:
            return "/%s?uploadId=%s" % ( self.remoteFileName, self.mpUploadId )
        else:
            return "/%s/%s?uploadId=%s" % ( self.bucketName, self.remoteFileName, self.mpUploadId )

class YaS3UploadMPPartUrl(YaS3UrlBuilder):
    """
    Methods for producing or retrieving the base url for doing one part of an S3 multipart upload
    """

    def __init__(self, bucketName, remoteFileName, mpPartNum, mpUploadId, virtualHost = False):
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
        self.virtualHost = virtualHost

    def buildUrl(self):
        """
        Return the base url for uploading one part of an S3 multipart upload to the given object and bucket
        """
        if self.virtualHost:
            return "/%s?partNumber=%s&uploadId=%s" % ( self.remoteFileName, self.mpPartNum, self.mpUploadId )
        else:
            return "/%s/%s?partNumber=%s&uploadId=%s" % ( self.bucketName, self.remoteFileName, self.mpPartNum, self.mpUploadId )

class YaS3AuthInfo(object):
    """
    Methods for S3 authoriazation
    """

    def __init__(self, accessKey, secretKey, authType):
        """
        Constructor

        Argumnents:
        accessKey -- the account name
        secretKey -- the secret key used for signing requests
        authType  -- "aws" or "low" depending on which s3 auth type we want
        """
        self.accessKey = accessKey
        self.secretKey = secretKey
        self.authType = authType

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

    def getS3AuthHeader(self, reqType, contentMd5, contentType, date, url, amzheaders = []):
        if self.authType == "aws":
            if self.accessKey and self.secretKey:
                return self.getAWSHeader(reqType, contentMd5, contentType, date, url, amzheaders)
            else:
                return None
        elif self.authType == "low":
            if self.accessKey and self.secretKey:
                return self.getAWSHeader(reqType, contentMd5, contentType, date, url, amzheaders)
            else:
                return self.getLOWHeader()
        else:
            return None

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

    def __init__(self, reqType, verbose, quiet, raw):
        """
        Constructor

        Arguments:
        reqType   -- Type of HTTP request (GET/HEAD/PUT/POST/DELETE)
        verbose   -- print lots of messages about what's being processed including HTTP headers
        quiet     -- suppress all extra messages, even XML bodies returned by requests
        raw       -- don't prettyprint xml output, display exactly what the server sent (for verbose)
        """
        self.reqType = reqType
        self.verbose = verbose
        if self.verbose:
            self.quiet = False
        else:
            self.quiet = quiet
        self.raw = raw
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
            c.putheader(h,v)
        if self.verbose:
            print "headers sent:"
            print headerInfo.printAllHeaders()
        c.endheaders()

    def makeRequest(self, c, headerInfo, urlInfo, contentLength=None, md5=None):
        """
        Send an HTTP request to the remote server and get headers and response back

        Arguments:

        c              -- YaS3Connection object
        headerInfo     -- YaS3HTTPHeaders object
        urlInfo        -- YaS3UrlBuilder object
        contentLength  -- content length of body to be sent, if any
        md5            -- base 64 of md5 of body to be sent, if any
        """
        self.sendReqFirstLine(c, urlInfo.buildUrl())
        self.putHeaders(c, headerInfo)

        reply = c.getresponse()
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
        if self.verbose:
            print status, reason
            for (header, value) in headersReceived:
                print "%s: %s" % (header, value)

        data = None
        cl = self.getContentLengthFromHeaders(headersReceived)
        if cl or status == 200 or status >= 300:
            # FIXME needs timeouts
            data = reply.read(cl) if cl else reply.read()
            if self.verbose:
                if self.raw:
                    print "Response body from server:", data
                else:
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
        urlInfo        -- YaS3UrlBuilder object
        contentLength  -- content length of body to be sent, if any
        md5            -- base 64 of md5 of body to be sent, if any
        localFileName  -- full path of local file to upload
        mpFileOffset   -- upload from this point in the file
        mpChunkSize    -- upload at most this many bytes of file
        """
        self.sendReqFirstLine(c, urlInfo.buildUrl())
        self.putHeaders(c, headerInfo)
        # if we did the expect 100-continue trick, make sure we got the 100 status code back
        # otherwise we will return what we got (should be a redir) and let the caller deal
        if headerInfo.findHeader("Expect") == "100-continue":
            oldTimeout = c.timeout
            c.timeout = 2
            c.callback = self.sendFile
            c.callbackArgs = ( c, localFileName, mpFileOffset, mpChunkSize )
            reply = c.getresponse()
            c.timeout = oldTimeout
            if reply.status != 100:
                headersReceived = reply.getheaders()
                data = self.getResponse(reply, headersReceived)
                return reply, data
                
        else:
            self.sendFile(c, localFileName, mpFileOffset, mpChunkSize)
            reply = c.getresponse()

        headersReceived = reply.getheaders()
        data = self.getResponse(reply, headersReceived)
        return reply, data

    def makeGetObjectRequest(self, c, headerInfo, urlInfo, localFileName):
        """
        Get an object (file) from the remote server and get headers and response back

        Arguments:
        c              -- YaS3Connection object
        headerInfo     -- YaS3HTTPHeaders object
        urlInfo        -- YaS3UrlBuilder object
        localFileName  -- full path of local file where object will be saved
        """
        self.sendReqFirstLine(c, urlInfo.buildUrl())
        self.putHeaders(c, headerInfo)

        reply = c.getresponse()

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
        c.putrequest(self.reqType, url, skip_host = True, skip_accept_encoding = True)
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
            c.send(data)
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

    def makePostRequestWithData(self, c, headerInfo, urlInfo, data):
        """
        Do an HTTP POST request to remote server, sending body text in the post
        It is assumed that the caller has already sent the first line
        of the HTTP request as well as all HTTP headers; additionally
        no encoding is done on the text to be sent.

        Arguments:
        c           -- YaS3Connection object
        headerInfo  -- YaS3HTTPHeaders object
        urlInfo     -- YaS3UrlBuilder object
        xml         -- text to be sent in body
        """
        self.sendReqFirstLine(c, urlInfo.buildUrl())
        self.putHeaders(c, headerInfo)

        # FIXME is it possible for the data to be larger than a standard buffer size of 64k?
        c.send(data)

        reply = c.getresponse()
        headersReceived = reply.getheaders()
        data = self.getResponse(reply, headersReceived)
        return reply, data

class YaS3Response(httplib.HTTPResponse):
    """
    hack for HTTPResponse so it isn't stupid about 100-Continue
    The below code taken from httplib.HTTPResponse, 78261:d56306b78b6,
    http://hg.python.org/cpython/file/2.7/Lib/httplib.py, modified for Expect: 100-Continue
    """

    def begin(self, version, status, reason):
        if self.msg is not None:
            # we've already started reading the response
            return

        # read until we get a non-100 response
        while True:
            if status != 100:
                break
            # skip the header from the 100 response
            while True:
                skip = self.fp.readline(httplib._MAXLINE + 1)
                if len(skip) > httplib._MAXLINE:
                    raise LineTooLong("header line")
                skip = skip.strip()
                if not skip:
                    break
                if self.debuglevel > 0:
                    print "header:", skip
            # move this to the end since we have moved the 100 continue check
            # and subsequent read of status outside of this method... and this is
            # why we have to include the entire method. grrr
            version, status, reason = self._read_status()

        self.status = status
        self.reason = reason.strip()
        if version == 'HTTP/1.0':
            self.version = 10
        elif version.startswith('HTTP/1.'):
            self.version = 11 # use HTTP/1.1 code for HTTP/1.x where x>=1
        elif version == 'HTTP/0.9':
            self.version = 9
        else:
            raise UnknownProtocol(version)

        if self.version == 9:
            self.length = None
            self.chunked = 0
            self.will_close = 1
            self.msg = httplib.HTTPMessage(StringIO())
            return

        self.msg = httplib.HTTPMessage(self.fp, 0)
        if self.debuglevel > 0:
            for hdr in self.msg.headers:
                print "header:", hdr,

        # don't let the msg keep an fp
        self.msg.fp = None

        # are we using the chunked-style of transfer encoding?
        tr_enc = self.msg.getheader('transfer-encoding')
        if tr_enc and tr_enc.lower() == "chunked":
            self.chunked = 1
            self.chunk_left = None
        else:
            self.chunked = 0

        # will the connection close at the end of the response?
        self.will_close = self._check_close()

        # do we have a Content-Length?
        # NOTE: RFC 2616, S4.4, #3 says we ignore this if tr_enc is "chunked"
        length = self.msg.getheader('content-length')
        if length and not self.chunked:
            try:
                self.length = int(length)
            except ValueError:
                self.length = None
            else:
                if self.length < 0: # ignore nonsensical negative lengths
                    self.length = None
        else:
            self.length = None

        # does the body have a fixed length? (of zero)
        if (status == httplib.NO_CONTENT or status == httplib.NOT_MODIFIED or
            100 <= status < 200 or # 1xx codes
            self._method == 'HEAD'):
            self.length = 0

        # if the connection remains open, and we aren't using chunked, and
        # a content-length was not provided, then assume that the connection
        # WILL close.
        if not self.will_close and \
                not self.chunked and \
                self.length is None:
            self.will_close = 1

    def check100Continue(self, method, args):
        if self.msg is not None:
            # we've already started reading the response
            return

        version, status, reason = self._read_status()
        if status == 100:
            line = self.fp.readline() # skip the blank line marking end of (non-existent) headers, ugh
            if line != "\r\n":
                Err.whine("Unexpected content received after 100 Continue: >>%s<<" % line)
            if method:
                method(*args)
            version, status, reason = self._read_status()
        
        return (version, status, reason)

class YaS3Connection(httplib.HTTPConnection):
    """
    Methods for managing the HTTP connection to a remote server
    The below code taken from HTTPConnection, 78261:d56306b78b6,
    http://hg.python.org/cpython/file/2.7/Lib/httplib.py, modified for Expect: 100-Continue
    """

    def __init__(self, host, port, protocol, strict=None,
                 timeout=socket._GLOBAL_DEFAULT_TIMEOUT, source_address=None):
        """
        Constructor

        Arguments:
        host     -- fqdn of remote server
        port     -- port number of connection
        protocol -- http or https
        """
        # grrr, old-style class
        httplib.HTTPConnection.__init__(self, host, port, strict, timeout, source_address)
        self.response_class = YaS3Response
        self.protocol = protocol
        # these get set later just before any request you would
        # do with expect: 100-continue; since they are connection-specific
        # and you probably don't want to use the same callbacks for all requests
        # but only e.g. upload objects
        self.callback = None
        self.callbackArgs = None

    def resetHostAndPort(self, host, port):
        """
        Set the host and port outside of the constructor
        This will close the connection if the new host is different
        than the old one.
        """
        if self.host != host or self.port != port:
            # this can be called multiple times (at elast with the existing httplib code:-P)
            self.close()

        self.host = host
        self.port = port

    def getresponse(self, buffering=False):
        """Code stolen right out ot HTTPResponse. Need it so we can 
        add code to check for the 100 continue response before we hit
        begin()"""

        "Get the response from the server."

        # if a prior response has been completed, then forget about it.
        if self._HTTPConnection__response and self._HTTPConnection__response.isclosed():
            self._HTTPConnection__response = None

        #
        # if a prior response exists, then it must be completed (otherwise, we
        # cannot read this response's header to determine the connection-close
        # behavior)
        #
        # note: if a prior response existed, but was connection-close, then the
        # socket and response were made independent of this HTTPConnection
        # object since a new request requires that we open a whole new
        # connection
        #
        # this means the prior response had one of two states:
        # 1) will_close: this connection was reset and the prior socket and
        # response operate independently
        # 2) persistent: the response was retained and we await its
        # isclosed() status to become true.
        #
        if self._HTTPConnection__state != httplib._CS_REQ_SENT or self._HTTPConnection__response:
            raise httplib.ResponseNotReady()

        args = (self.sock,)
        kwds = {"strict":self.strict, "method":self._method}
        if self.debuglevel > 0:
            args += (self.debuglevel,)
        if buffering:
            #only add this keyword if non-default, for compatibility with
            #other response_classes.
            kwds["buffering"] = True;
        response = self.response_class(*args, **kwds)

        # here's the reason we had to include this whole function :-/
        (v, s, r) = response.check100Continue(self.callback, self.callbackArgs)
        response.begin(v, s, r)

        assert response.will_close != httplib._UNKNOWN
        self._HTTPConnection__state = httplib._CS_IDLE

        if response.will_close:
            # this effectively passes the connection to the response
            self.close()
        else:
            # remember this, so we can tell when it is complete
            self._HTTPConnection__response = response

        return response


