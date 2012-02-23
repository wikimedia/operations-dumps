import getopt
import sys
import json
import xml.sax
import os
import codecs
import traceback
import re
import hashlib
import subprocess
from subprocess import Popen, PIPE
import ConfigParser

# todo: 
# progress bar for large file uploads, or other way the user can figure
#   out how much of the file upload has been done.
# support multipart uploads for the really huge files
# support the size hints for items that are going to be > 10gb
# sure wish we could check item and contrib history in any other way
#   than log in via icky old web interface and screen scraping
# md5sum or sha1 of uploaded object??

class ArchiveUploaderConfig(object):
    """Read contents of config file, if any.
    If no filename is provided, the default name 'archiveuploader.conf' will
    be checked.  If it is not present, the files /etc/archiveuploader.conf and
    .archiveuploader.conf will be checked, in that order."""

    def __init__(self, configFile=False):
        """Constructor. Args:
        configFile -- path to configuration file. If not passed,
                      the default 'archiveuploader.conf' will be checked."""

        self.projectName = False

        home = os.path.dirname(sys.argv[0])
        if (not configFile):
            configFile = "archiveuploader.conf"
        self.files = [
            os.path.join(home,configFile),
            "/etc/archiveuploader.conf",
            os.path.join(os.getenv("HOME"), ".archiveuploader.conf")]
        defaults = {
            #"auth": {
            "accesskey": "",
            "secretkey": "",
            #"output": {
            "sitematrixfile": "",
            #"web": {
            "apiurl": "http://en.wikipedia.org/w/api.php",
            "curl" : "/usr/bin/curl",
            "itemnameformat" : "%s",
            "licenseurl" : "http://wikimediafoundation.org/wiki/Terms_of_Use",
            "creator" : "the Wikimedia Foundation",
            "downloadurl" : "http://dumps.wikimedia.org"
            }

        self.conf = ConfigParser.SafeConfigParser(defaults)
        self.conf.read(self.files)
        self.parseConfFile()

    def parseConfFile(self):
        """Get contents of config file, using new values to overwrite
        corresponding defaults."""
        self.accessKey = self.conf.get("auth", "accesskey")
        self.secretKey = self.conf.get("auth", "secretkey")
        self.siteMatrixFile = self.conf.get("output", "sitematrixfile")
        self.apiUrl = self.conf.get("web", "apiurl")
        self.curl = self.conf.get("web", "curl")
        self.itemNameFormat = self.conf.get("web", "itemnameformat")
        self.licenseUrl = self.conf.get("web", "licenseurl")
        self.creator = self.conf.get("web", "creator")
        self.downloadurl = self.conf.get("web", "downloadurl")

class SiteMatrix(object):
    """Get and/or update the SiteMatrix (list of MediaWiki sites
    with projct name, database name and language name) via the api,
    saving it to a cache file if requested.
    If no filename is supplied in the config we will use only the api 
    to load and update.
    If a filename is supplied in the config we will load from it 
    initially and save to it after every update from the api."""

    def __init__(self, config, dontSaveFile = False, verbose = False):
        """Constructor. Arguments:
        config       -- populated ArchiveUploaderConfig object
        sourceUrl    -- url to the api.php script. For example:"
                        http://en.wikipedia.org/w/api.php
        fileName     -- full path to a cache file for the site matrix information
        dontSaveFile -- load form cache file but never update it (used primarily for
                        doing a dry run)"""
        self.config = config
        self.sourceUrl = self.config.apiUrl + "?action=sitematrix&format=json"
        self.fileName = self.config.siteMatrixFile
        self.dontSaveFile = dontSaveFile
        self.verbose = verbose
        self.matrixJson = None
        if self.fileName and os.path.exists(self.fileName):
            try:
                self.matrixJson = self.loadMatrixJsonFromFile()
                self.matrix = json.loads(self.matrixJson)
            except:
                self.matrixJson = None
        if self.matrixJson == None:
            self.matrixJson = self.loadMatrixJsonFromApi()
            self.matrix = json.loads(self.matrixJson)
            if not self.dontSaveFile:
                self.saveMatrixJsonToFile()

    def updateMatrix(self):
        """Update the copy of the sitematrix in memory via the MW api.
        Write the results to a cache file if requested/enabled."""
        newMatrixJson = self.loadMatrixJsonFromApi()
        newMatrix = json.loads(newMatrixJson)
        # We may wind up with wikis that have been renamed, or removed, so that
        # the old name is no longer valid; it will take up space but otherwise
        # is harmless, so ignore this case.
        self.matrix = self.matrix.update(newMatrix)
        self.matrixJson = json.dumps(self.matrix, ensure_ascii = False)
        if not self.dontSaveFile:
            self.saveMatrixJsonToFile()

    def loadMatrixJsonFromApi(self):
        """Fetch the sitematrix information via the MW api. Get rid 
        of the extra columns and convert the rest to a dict for our use."""
        apiMatrixJson = self.loadApiMatrixJsonFromApi()
        matrix = self.apiMatrixJsonToDict(apiMatrixJson)
        matrixJson = json.dumps(matrix, ensure_ascii = False)
        return matrixJson

    def loadApiMatrixJsonFromApi(self):
        """Fetch the sitematrix information via the MW api."""
        command = [ self.config.curl, "--location", self.sourceUrl ]

        if self.verbose:
            commandString = " ".join(command)
            print "about to run " + commandString

        proc = Popen(command, stdout = PIPE, stderr = PIPE)
        output, error = proc.communicate()
        if proc.returncode:
            commandString = " ".join(command)
            raise ArchiveUploaderError("command '" + commandString + ( "' failed with return code %s " % proc.returncode ) + " and error '" + error + "'")
        return output

    def apiMatrixJsonToDict(self, jsonString):
        """Convert the sitematrix json string to a dict for our use,
        keeping only the information we want: dbname, project name, lang code."""
        matrixJson = json.loads(jsonString)
        matrix = {}
        #{ u'localname': u'Aromanian', 
        #  u'code': u'roa-rup', 
        #  u'name': u'Arm\xe3neashce', 
        #  u'site': 
        #  [ {u'url': u'http://roa-rup.wikipedia.org', u'code': u'wiki', u'dbname': u'roa_rupwiki'}, 
        #    {u'url': u'http://roa-rup.wiktionary.org', u'code': u'wiktionary', u'dbname': u'roa_rupwiktionary'}
        #  ] }
        for k in matrixJson['sitematrix'].keys():
            if k == 'count':
                continue
            if k == 'specials':
                for s in range(0,len((matrixJson['sitematrix'][k]))):
                    sitename = matrixJson['sitematrix'][k][s]['dbname']
                    matrix[sitename] = {}
                    matrix[sitename]['project'] = matrixJson['sitematrix'][k][s]['code']
                    # special hack
                    if matrix[sitename]['project'] == 'wiki':
                        matrix[sitename]['project'] = 'wikipedia'
                    matrix[sitename]['locallangname'] = None
                    matrix[sitename]['lang'] = None
            else:
                for s in range(0,len((matrixJson['sitematrix'][k]['site']))):
                    sitename = matrixJson['sitematrix'][k]['site'][s]['dbname']
                    matrix[sitename] = {}
                    matrix[sitename]['project'] = matrixJson['sitematrix'][k]['site'][s]['code']
                    # special hack
                    if matrix[sitename]['project'] == 'wiki':
                        matrix[sitename]['project'] = 'wikipedia'
                    matrix[sitename]['locallangname'] = matrixJson['sitematrix'][k]['localname']
                    matrix[sitename]['lang'] = matrixJson['sitematrix'][k]['code']
        return matrix

    def saveMatrixJsonToFile(self):
        """Write the site matrix information to a cache file
        in json format."""
        if self.fileName:
            outfile = codecs.open(self.fileName,"w","UTF-8")
            json.dump(self.matrix, outfile, ensure_ascii = False)
            outfile.close()
        
    def loadMatrixJsonFromFile(self):
        """Load the json-formatted site matrix information from a 
        cache file, converting it to a dict for our use."""
        if self.fileName and os.path.exists(self.fileName):
            infile = open(self.fileName,"r")
            self.matrixJson= json.load(infile)
            infile.close()

class ArchiveUploaderError(Exception):
    """Exception class for the Archive Uploader and all of
    its related classes.  Doesn't do much :-P"""
    pass

class ArchiveUploader(object):
    """Use the archive.org s3 api to create and update items (buckets)
    and to upload files (objects) into a bucket.  Relies on curl."""

    def __init__(self, config, archiveKey, itemName, verbose = False, dryrun = False, getmatrix = False):
        """Constructor. Args:
        config     -- populated ArchiveUploadedConfig object
        archiveKey -- populated ArchiveKey object (contains access and secret keys)
        itemName   -- name of item tp be created, updated, or uploaded into
        verbose    -- if set, produce extra output; default False
        dryrun     -- if set, don't actually do update/creation/upload, show what
                      would be run; default False
        """
        self.config = config
        self.archiveKey = archiveKey
        self.itemName = itemName
        self.verbose = verbose
        self.dryrun = dryrun
        self.existence = False # will hold return code of an existence check via curl, on demand
        if self.dryrun:
            self.dontSaveFile = True
        else:
            self.dontSaveFile = False
        self.matrix = None
        self.dbName = self.itemName
        if self.config.itemNameFormat:
            self.itemName = self.config.itemNameFormat % self.dbName

    def getArchiveBaseS3Url(self):
        """Returns location of the base url for archive.org S3 requests"""
        return "http://s3.us.archive.org/"

    def getArchiveBaseUrl(self):
        """Returns location of the base url for regular archive.org requests"""
        return "http://www.archive.org/"

    def getArchiveItemUrl(self):
        """Returns location of the item as an S3-style url"""
        return "%s%s" % (self.getArchiveBaseS3Url(), self.itemName)

    def getArchiveItemDetailsUrl(self):
        """Returns location of item details, sadly as a regular url, but
        happily with json output."""
        return "%sdetails/%s?output=json" % (self.getArchiveBaseUrl(), self.itemName)

    def getObjectUrl(self, objectName, fileName):
        """Returns the curl arguments needed for the url of an object (file) S3-style"""
        return "%s/%s" % ( self.getArchiveItemUrl(), objectName )

    def getLocationCurlArg(self):
        """Returns the argument that causes curl to follow all redirects"""
        return [ "--location" ]

    def getS3AuthCurlArgs(self):
        """Returns the arguments needed for auth to the archive.org S3 api"""
        return [ "--header", self.archiveKey.getAuthHeader() ]

    def getObjectUploadCurlArgs(self, objectName, fileName):
        """Returns the curl arguments needed for upload of a file S3-style:
        the authentication header with accesskey and secret key, and
        the url to the object (file) as an S3 url."""
        args = self.getS3AuthCurlArgs()
        args.extend( ['--upload-file', fileName, self.getObjectUrl() ] )
        return args

    def getQuietCurlArg(self):
        return [ "-s" ]

    def getNoDeriveCurlArg(self):
        """This tag tells archive.org not to try to derive a bunch of other
        formats from this file (which it would do for videos, for example).
        We've been requested to add this since our files have no derivative
        formats."""
        return [ "--header", "x-archive-queue-derive:0" ]

    def getHeadReqCurlArgs(self):
        """Returns the curl arguments needed to do head request and write 
        out just the http return code"""
        args = self.getQuietCurlArg()
        args.extend([ "--write-out", "%{http_code}", "-X", "HEAD" ] )
        return args

    def getHeadWithOutputCurlArgs(self):
        """Returns the curl arguments needed to do head request and write 
        out everything"""
        return [ "--head" ]
 
    def getItemCreationCurlArgs(self):
        """Returns the curl arguments needed to put an empty file; 
        this is used for updating or creating an item (bucket)."""
        return [ "-X", "PUT", "--header", "Content-Length: 0" ]

    def getItemMetaHeaderArgs(self):
        """Get the curl arguments needed to generate all the headers containing
        metadata for objects (files) on archive.org.
        Sample headers for el wiktionary:
          --header 'x-archive-meta-title:Wikimedia database dumps of el.wiktionary' 
          --header 'x-archive-meta-mediatype:web' 
          --header 'x-archive-meta-language:el (Modern Greek)' 
          --header 'x-archive-meta-description:Dumps of el.wiktionary created by the Wikimedia Foundation and downloadable from http://dumps.wikimedia.org' 
          --header 'x-archive-meta-format:xml and sql' 
          --header 'x-archive-meta-licenseurl:http://wikimediafoundation.org/wiki/Terms_of_Use' 
          --header 'x-archive-meta-subject:xml,dump,wikimedia,el,wiktionary'"""
        headers = [ '--header', 'x-archive-meta-title:Wikimedia database dumps of %s' %  self.dbName, 
                 '--header', 'x-archive-meta-mediatype:web',
                 '--header', 'x-archive-meta-description:Dumps of %s created by %s and downloadable from %s' % (self.dbName, self.config.creator, self.config.downloadurl), 
                 '--header', 'x-archive-meta-format:xml and sql', 
                 '--header', 'x-archive-meta-licenseurl:%s' % self.config.licenseUrl ]

        lang = self.getLang()
        if lang:
            headers.extend([ '--header', 'x-archive-meta-language:%s (%s)' % ( lang, self.getLocalLangName() ),
                             '--header', 'x-archive-meta-subject:xml,dump,wikimedia,%s,%s' %( lang, self.getProject() ) ])
        else:
            headers.extend([ '--header', 'x-archive-meta-subject:xml,dump,wikimedia,%s' %( self.getProject() ) ])
        return headers

    def getIgnoreExistingBucketCurlArg(self):
        """Return the curl argument required for overwriting the metadata
        of an existing item (bucket)."""
        return [ "--header", "x-archive-ignore-preexisting-bucket:1" ]

    def doCurlCommand(self, curlCommand, getOutput=False):
        """Given a list containing curl command with all the args and run it.
        If getOutput is True, return any output.
        Raises ArchiveUploaderError on error fron curl."""
        if self.verbose:
            commandString = " ".join(curlCommand)
            print "about to run " + commandString

        try:
            proc = Popen(curlCommand, stdout = PIPE, stderr = PIPE)
        except:
            commandString = " ".join(curlCommand)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
            raise ArchiveUploaderError("curlCommand '" + commandString +  "' failed'" )
                                       
        output, error = proc.communicate()
        if proc.returncode:
            # curl has this annoying idea that when you specifically do a HEAD
            # request it should return an error code anyways indicating a
            # partial file transfer
            if not (proc.returncode == 18 and 'HEAD' in curlCommand):
                commandString = " ".join(curlCommand)
                raise ArchiveUploaderError("curlCommand '" + commandString + ( "' failed with return code %s " % proc.returncode ) + " and error '" + error + "'")
        if verbose:
            print "Command successful."
            if getOutput:
                if output:
                    print output
                else:
                    print "No output returned."

        if getOutput:
            return output

    def showCommand(self,command):
        """Print the supplied command (a list consisting of a command
        and any arguments) to stdout."""
        commandString = " ".join(command)
        print "would run: command " + commandString
        
    def uploadObject(self, objectName, fileName):
        """Upload an object (file) to the bucket (item). Args:
        objectName -- name of the object as it will appear in the S3-style url
        fileName   -- path to file to be uploaded"""

        # note that someone could remove the item in between the
        # time we check for one upload and the time we check for another
        # upload, in the case of multiple uploads via this script.
        # we're not expecting to beat race conditions, just to warn
        # the user if they try uploading to a bucket they never set up
        self.checkIfItemExists()
        if self.existence != "200":
            raise ArchiveUploaderError("No such item " + self.itemName + " exists, http error code " + self.existence + ", giving up.")
        curlCommand = [ self.config.curl ];
        curlCommand.extend(self.getLocationCurlArg())
        curlCommand.extend(self.getNoDeriveCurlArg())
        curlCommand.extend(self.getObjectUploadCurlArgs(objectName, fileName))
        if (self.dryrun):
            self.showCommand(curlCommand)
        else:
            self.doCurlCommand(curlCommand)

    def verifyObject(self, objectName, fileName):
        """Verify an object (file) in a given bucket (item) by checking etag
        from server and md5sum of local file. Args:
        objectName -- name of the object as it appears in the S3-style url
        fileName   -- path to corresponding local file"""

        self.checkIfItemExists()
        if self.existence != "200":
            raise ArchiveUploaderError("No such item " + self.itemName + " exists, http error code " + self.existence + ", giving up.")
        curlCommand = [ self.config.curl ];
        curlCommand.extend(self.getLocationCurlArg())
        curlCommand.extend(self.getHeadWithOutputCurlArgs())
        curlCommand.append(self.getObjectUrl(objectName, fileName))
        if (self.dryrun):
            self.showCommand(curlCommand)
        else:
            result = self.doCurlCommand(curlCommand, True)
            md5sumFromEtag = self.getEtagValue(result)
            if not md5sumFromEtag:
                print "no Etag in server output, received:"
                print result
                sys.exit(1)
            md5sumFromLocalFile = self.getMd5sumOfFile(fileName)
            if verbose:
                print "Etag: ", md5sumFromEtag, "md5 of local file: ", md5sumFromLocalFile
            if md5sumFromEtag == md5sumFromLocalFile:
                if verbose:
                    print "File verified ok."
            else:
                raise ArchiveUploaderError("File verification FAILED.")

    def getEtagValue(self, text):
        # format: ETag: "8ea7c3551a74098b49fbfea49b1ee9e1"
        lines = text.split('\n')
        etagExpr = re.compile('^ETag:\s+"([abcdef0-9]+)"')
        for l in lines:
            etagMatch = etagExpr.match(l)
            if etagMatch:
                return etagMatch.group(1)
        return None

    def getMd5sumOfFile(self, fileName):
        summer = hashlib.md5()
        infile = file(fileName, "rb")
        # really? could this be bigger?? consider 20GB files.
        bufsize = 4192 * 32
        buffer = infile.read(bufsize)
        while buffer:
            summer.update(buffer)
            buffer = infile.read(bufsize)
        infile.close()
        return summer.hexdigest()

    def checkIfItemExists(self):
        """Check it the item (bucket) exists, returning True if it exists
        and False otherwise."""
        if not self.existence:
            curlCommand = [ self.config.curl ];
            curlCommand.extend(self.getLocationCurlArg())
            curlCommand.extend(self.getHeadReqCurlArgs())
            curlCommand.append(self.getArchiveItemUrl())
            result = self.doCurlCommand(curlCommand,getOutput=True)
            self.existence = result

    # FIXME we should really check once to see if the project name
    # is valid and then refuse to work on it otherwise, instead
    # of scattering the retry throughout all these functions
    def getLang(self):
        """Get the language code corresponding to the dbname
        of the dump we are creating/uploading"""
        if not self.matrix:
            self.matrix = SiteMatrix(self.config, self.dontSaveFile, self.verbose)

        if self.dbName in self.matrix.matrix.keys():
            return self.matrix.matrix[self.dbName]['lang']
        self.matrix.updateMatrix()
        # one more try
        if self.dbName in self.matrix.matrix.keys():
            return self.matrix.matrix[self.dbName]['lang']
        else:
            return None

    def getLocalLangName(self):
        """From the dbname, get the translation of the name of the language 
        for the lang code of the dump we are creating/uploading.  The translation
        is into the content language of the site from which we retrieve
        the sitematrix information; typically this should be English, since we are
        uploading to archive.org and the description keywords used there
        are generally English."""
        if not self.matrix:
            self.matrix = SiteMatrix(self.config, self.dontSaveFile, self.verbose)

        if self.dbName in self.matrix.matrix.keys():
            return self.matrix.matrix[self.dbName]['locallangname']
        self.matrix.updateMatrix()
        # one more try
        if self.dbName in self.matrix.matrix.keys():
            return self.matrix.matrix[self.dbName]['locallangname']
        else:
            return None

    def getProject(self):
        """From the dbname, get the project name of the dump we are
        creating/uploading."""
        if not self.matrix:
            self.matrix = SiteMatrix(self.config, self.dontSaveFile, self.verbose)

        if self.dbName in self.matrix.matrix.keys():
            return self.matrix.matrix[self.dbName]['project']
        self.matrix.updateMatrix()
        # one more try
        if self.dbName in self.matrix.matrix.keys():
            return self.matrix.matrix[self.dbName]['project']
        else:
            return None

    def updateItem(self):
        """Update an item (bucket); this entails a full update of the metadata. The
        objects (files) it contains are not touched in any way."""
        self.createItem(True)

    def createItem(self, rewrite = False):
        """Create an item (bucket) S3-style.  Args:
        rewrite -- if true, we are updating the metadata of an item that 
                   already exists; default false"""
        curlCommand = [ self.config.curl ];
        curlCommand.extend(self.getLocationCurlArg())
        curlCommand.extend(self.getS3AuthCurlArgs())
        if (rewrite):
            curlCommand.extend(self.getIgnoreExistingBucketCurlArg())
        else:
            self.checkIfItemExists()
            if self.existence == "200":
                raise ArchiveUploaderError("Item " + self.itemName + " already exists, giving up.")
        curlCommand.extend(self.getItemMetaHeaderArgs())
        curlCommand.extend(self.getItemCreationCurlArgs())
        curlCommand.append(self.getArchiveItemUrl())
        if (self.dryrun):
            self.showCommand(curlCommand)
        else:
            self.doCurlCommand(curlCommand)

    def listAllItems(self):
        """List all items for the user associated with the accesskey/secretkey."""
        curlCommand = [ self.config.curl ];
        curlCommand.extend(self.getLocationCurlArg())
        curlCommand.extend(self.getS3AuthCurlArgs())
        if (not self.verbose):
            curlCommand.extend(self.getQuietCurlArg())
        curlCommand.append(self.getArchiveBaseS3Url())
        if (self.dryrun):
            self.showCommand(curlCommand)
        else:
            output = self.doCurlCommand(curlCommand, True)
            if (self.verbose):
                print "About to parse output"
            xml.sax.parseString(output, ListAllItemsCH())

    def listObjects(self):
        """List all objects (files) contained in a specific item (bucket)."""
        curlCommand = [ self.config.curl ];
        curlCommand.extend(self.getLocationCurlArg())
        curlCommand.extend(self.getS3AuthCurlArgs())
        if (not self.verbose):
            curlCommand.extend(self.getQuietCurlArg())
        curlCommand.append(self.getArchiveItemUrl())
        if (self.dryrun):
            self.showCommand(curlCommand)
        else:
            output = self.doCurlCommand(curlCommand, True)
            if (self.verbose):
                print "About to parse output"
            xml.sax.parseString(output, ListObjectsCH())

    def showItem(self):
        """Show metadata associated with a particular item (bucket)."""
        curlCommand = [ self.config.curl ];
        curlCommand.extend(self.getLocationCurlArg())
        if (not self.verbose):
            curlCommand.extend(self.getQuietCurlArg())
        curlCommand.append(self.getArchiveItemDetailsUrl())
        if (self.dryrun):
            self.showCommand(curlCommand)
        else:
            output = self.doCurlCommand(curlCommand, True)
            if (self.verbose):
                print "About to parse output"
            self.showItemMetadataFromJson(output)

    def showItemMetadataFromJson(self,jsonString):
        """Grab the metadata for an item from the json for the item details
        (contains lost of other cruft) and display to stdout"""
        # sample output:
        #"metadata":{
        #   "identifier":["elwiktionary-dumps"],
        #   "description":["Dumps of el.wiktionary created by the Wikimedia Foundation and downloadable from http:\/\/dumps.wikimedia.org\/elwiktionary\/"],
        #   "language":["el (Modern Greek)"],
        #   "licenseurl":["http:\/\/wikimediafoundation.org\/wiki\/Terms_of_Use"],
        #   "mediatype":["web"],
        #   "subject":["xml,dump,wikimedia,el,wiktionary"],
        #   "title":["Wikimedia database dumps of el.wiktionary, format:xml and sql"],
        #   "publicdate":["2012-02-17 11:03:45"],
        #   "collection":["opensource"],
        #   "addeddate":["2012-02-17 11:03:45"]},
        details = json.loads(jsonString)
        if 'metadata' in details.keys():
            metadata = details['metadata']
            print "Item metadata for", self.itemName
            for k in details['metadata'].keys():
                print "%s:" % k,
                print " | ".join(details['metadata'][k])
        else:
            print "No metadata for", self.itemName, "is available."

class ListObjectsCH(xml.sax.ContentHandler):
    """Read contents from a request to list all objects (files)
    in a given item (bucket)"""
    NONE = 0x0
    CONTENTS = 0x1
    KEY = 0x2
    LASTMODIFIED = 0x3
    SIZE = 0x4

    # sample output:
    #<?xml version='1.0' encoding='UTF-8'?>
    #<ListBucketResult>
    #    <Name>elwiktionary-dumps</Name>
    #    <Contents>
    #        <Key>elwiktionary-20060703.tar</Key>
    #        <LastModified>2012-02-17T11:22:21.000Z</LastModified>
    #        <ETag>2012-02-17T11:22:21.000Z</ETag>
    #        <Size>10076160</Size>
    #        <StorageClass>STANDARD</StorageClass>
    #        <Owner>
    #            <ID>OpaqueIDStringGoesHere</ID>
    #            <DisplayName>Readable ID Goes Here</DisplayName>
    #        </Owner>
    #    </Contents>
    #</ListBucketResult>

    def __init__(self):
        xml.sax.ContentHandler.__init__(self)
        self.Key = ""
        self.LastModified = ""
        self.Size = ""
        self.state = ListObjectsCH.NONE

    def startElement(self, name, attrs):
        if name == "Contents":
            self.state = ListObjectsCH.CONTENTS
        elif name == "Key" and self.state == ListObjectsCH.CONTENTS:
            self.state = ListObjectsCH.KEY
        elif name == "LastModified" and self.state == ListObjectsCH.CONTENTS:
            self.state = ListObjectsCH.LASTMODIFIED
        elif name == "Size" and self.state == ListObjectsCH.CONTENTS:
            self.state = ListObjectsCH.SIZE

    def endElement(self, name):
        if name == "Contents":
            self.state = ListObjectsCH.NONE
            # FIXME really, a print? Do better.
            print "Object: %s, last modified: %s, size: %s" % (self.key, self.lastModified, self.size)
            self.itemName = ""
            self.itemCreationDate = ""

        elif name == "Key" and self.state == ListObjectsCH.KEY:
            self.state = ListObjectsCH.CONTENTS
        elif name == "LastModified" and self.state == ListObjectsCH.LASTMODIFIED:
            self.state = ListObjectsCH.CONTENTS
        elif name == "Size" and self.state == ListObjectsCH.SIZE:
            self.state = ListObjectsCH.CONTENTS
 
    def characters(self, content):
        if self.state == ListObjectsCH.KEY:
            self.key = content
        elif self.state == ListObjectsCH.LASTMODIFIED:
            self.lastModified = content
        elif self.state == ListObjectsCH.SIZE:
            self.size = content

class ListAllItemsCH(xml.sax.ContentHandler):
    """Read contents from a request to list all items (buckets)"""
    NONE = 0x0
    BUCKET = 0x1
    NAME = 0x2
    CREATIONDATE = 0x3

    # sample output:

    #<?xml version='1.0' encoding='UTF-8'?>
    #<ListAllMyBucketsResult>
    #    <Owner>
    #        <ID>OpaqueIDStringGoesHere</ID>
    #        <DisplayName>atglenn</DisplayName>
    #    </Owner>
    #    <Buckets>
    #        <Bucket>
    #            <Name>elwiktionary-dumps</Name>
    #            <CreationDate>1970-01-01T00:00:00.000Z</CreationDate>
    #        </Bucket>
    #    </Buckets>
    #</ListAllMyBucketsResult>

    def __init__(self):
        xml.sax.ContentHandler.__init__(self)
        self.itemName = ""
        self.itemCreationDate = ""
        self.state = ListAllItemsCH.NONE

    def startElement(self, name, attrs):
        if name == "Bucket":
            self.state = ListAllItemsCH.BUCKET
        elif name == "Name" and self.state == ListAllItemsCH.BUCKET:
            self.state = ListAllItemsCH.NAME
        elif name == "CreationDate" and self.state == ListAllItemsCH.BUCKET:
            self.state = ListAllItemsCH.CREATIONDATE

    def endElement(self, name):
        if name == "Bucket":
            self.state = ListAllItemsCH.NONE
            # FIXME really, a print? Do better.
            print "Item: %s, created: %s" % (self.itemName, self.itemCreationDate)
            self.itemName = ""
            self.itemCreationDate = ""

        elif name == "Name" and self.state == ListAllItemsCH.NAME:
            self.state = ListAllItemsCH.BUCKET
        elif name == "CreationDate" and self.state == ListAllItemsCH.CREATIONDATE:
            self.state = ListAllItemsCH.BUCKET
 
    def characters(self, content):
        if self.state == ListAllItemsCH.NAME:
            self.itemName = content
        elif self.state == ListAllItemsCH.CREATIONDATE:
            self.itemCreationDate = content


class ArchiveKey(object):
    """Authentication to the archive.org api, S3-style."""

    def __init__(self, config):
        """Constructor. Args:
        config -- a populated ArchiveUploaderConfig object."""
        self.config = config
        self.accessKey = self.config.accessKey
        self.secretKey = self.config.secretKey

    def getAuthHeader(self):
        """Returns the http header needed for authentication to the archive.org
        api."""
        return "authorization: LOW %s:%s" % ( self.accessKey, self.secretKey )

def usage(message = None):
    """Print comprehensive help information to stdout, including a specified message
    if any, and then error exit."""
    if message:
        print message
        print

    print "Usage: python archiveuploader.py [options]"
    print "Mandatory options: --accesskey, --secretkey"
    print "--accesskey <key>:     The access key from archive.org used to create items and upload objects."
    print "--secretkey <key>:     The secret key corresponding to the access key described above."
    print "Action options (choose one):"
    print "--createitem <item>:   The item specified will be created. Fails if item already exists."
    print "--updateitem <item>:   The metadata for the specified item will be updated."
    print "--uploadobject <item>: An object will be created by uploading to the specified item the file"
    print "                       given by --filename.  Requires the --objectName option."
    print "--verifyobject <item>: An object in an item will be verified by checking its md5sum locally and on"
    print "                       the server.  Requires the --objectName and the --filename options."
    print "--listitems:           List all items belonging to the account identified by the --accesskey"
    print "                       and --secretkey options."
    print "--showitem <item>:     Show metadata about the specified item."
    print "--listobjects <item>:  List all objects in the specified item."
    print "Other options:"
    print "--configfile <file>:   Name of optional configuration file with access keys, etc."
    print "--dryrun:              Don't create or update items or objects but show the commands that would"
    print "                       be run.  This option also means that updates to the sitematrix cache file"
    print "                       will not be done, although it will be read from if it exists, and the"
    print "                       MediaWiki instance will be queried via the api as well, if needed."
    print "--filename <file>:     The full path to the file to upload, when --uploadobject is specified."
    print "--objectname <object>: The name of an object as it is to appear in a aurl."
    print "--verbose:             Display progress bars and other output."
    sys.exit(1)

if __name__ == "__main__":
    verbose = False
    accessKey = None
    secretKey = None
    itemName = None
    objectName = None
    createItem = False
    updateItem = False
    uploadObject = False
    verifyObject = False
    configFile = None
    fileName = None
    listItems = False
    listObjects = False
    showItem = False
    dryrun = False

    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "",
            ['accesskey=', 'secretkey=', 'createitem=', 'updateitem=', 'uploadobject=', 'objectname=', 'filename=', 'configfile=', 'listitems', 'showitem=', 'listobjects=', 'verifyobject=', 'dryrun', 'verbose' ])
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
        usage("Unknown option or other error encountered")

    for (opt, val) in options:
        if opt == "--accesskey":
            accessKey = val
        elif opt == "--secretkey":
            secretKey = val
        elif opt == '--uploadobject':
            itemName = val
            uploadObject = True
        elif opt == '--verifyobject':
            itemName = val
            verifyObject = True
        elif opt == '--objectname':
            objectName = val
        elif opt == '--createitem':
            itemName = val
            createItem = True
        elif opt == '--updateitem':
            itemName = val
            updateItem = True
        elif opt == '--listitems':
            listItems = True
        elif opt == '--listobjects':
            itemName = val
            listObjects = True
        elif opt == '--showitem':
            itemName = val
            showItem = True
        elif opt == "--dryrun":
            dryrun = True
        elif opt == "--filename":
            fileName = val
        elif opt == "--configfile":
            configFile = val
        elif opt == "--verbose":
            verbose = True

    if len(remainder):
        usage("Error: unknown option specified.")

    if (uploadObject or verifyObject) and not fileName:
        usage("Error: a filename for upload or verification must be specified with --uploadobject/--verifyobject.")

    if (uploadObject or verifyObject) and not objectName:
        usage("Error: the option --objectname must be specified with --uploadobject/--verifyobject.")

    actionOptsCount = len(filter(None, [ createItem, updateItem, uploadObject, verifyObject, listItems, listObjects, showItem ]))

    if actionOptsCount > 1:
        usage("Error: conflicting action options specified.")
    elif actionOptsCount < 1:
        usage("Error: no action option specified.")

    config = ArchiveUploaderConfig(configFile)

    if not config.accessKey:
        config.accessKey = accessKey
    if not config.secretKey:
        config.secretKey = secretKey

    if (not config.accessKey or not config.secretKey):
        usage("Error: one of the mandatory options was not specified.")

    archiveKey = ArchiveKey(config)
    archiveUploader = ArchiveUploader(config, archiveKey, itemName, verbose, dryrun)
    
    if uploadObject:
        result = archiveUploader.uploadObject(objectName, fileName)
    elif verifyObject:
        result = archiveUploader.verifyObject(objectName, fileName)
    elif createItem:
        result = archiveUploader.createItem()
    elif updateItem:        
        result = archiveUploader.updateItem()
    elif listItems:
        result = False
        archiveUploader.listAllItems()
    elif listObjects:
        result = False
        archiveUploader.listObjects()
    elif showItem:
        result = False
        archiveUploader.showItem()
    if result:
        print "Failed."
    else:
        print "Successful."

