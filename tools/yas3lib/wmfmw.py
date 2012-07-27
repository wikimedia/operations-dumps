import getopt, sys, json, os, codecs, urllib

class ErrExcept(Exception):
    pass

class MWSiteMatrix(object):
    """Get and/or update the SiteMatrix (list of MediaWiki sites
    with projct name, database name and language name) via the api,
    saving it to a cache file if requested.
    If no siteMatrixFile is supplied we will use only the api 
    to load and update.
    If a filename is supplied we will load from it 
    initially and save to it after every update from the api."""

    def __init__(self, apiUrl, siteMatrixFile, dryrun = False, verbose = False):
        """Constructor. Arguments:
        apiUrl          -- url to the api.php script. For example:"
                           http://en.wikipedia.org/w/api.php
        siteMatrixFile  -- full path to a cache file for the site matrix information
        dryrun          -- load from cache file but don't update the cache
        verbose         -- if set, produce extra output; default False"""

        self.apiUrl = apiUrl
        self.sourceUrl = self.apiUrl + "?action=sitematrix&format=json"
        self.fileName = siteMatrixFile
        self.dryrun = dryrun
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
            if not self.dryrun:
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
        if not self.dryrun:
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

        if self.verbose:
            print "about to retrieve", self.sourceUrl

        ufd = urllib.urlopen(self.sourceUrl)
        if str(ufd.getcode()).startswith("2"):
            output = ufd.read()
            ufd.close()
            return output
        else:
            ufd.close()
            raise ErrExcept("failed to retrieve %s, error code %s" % (self.sourceUrl, ufd.getcode() ))

    def apiMatrixJsonToDict(self, jsonString):
        """Convert the sitematrix json string to a dict for our use,
        keeping only the information we want: wikiname, project name, lang code."""
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

class WikiMatrixInfo(object):
    """retrieve language, project etc from site matrix
    for the specified wiki"""
    def __init__(self, wikiName, apiUrl, siteMatrixFile, verbose = False, dryrun = False):
        """Constructor. Args:
        wikiName        -- name of wiki for which to get info (dbname)
        apiUrl          -- url to the api.php script. For example:"
                           http://en.wikipedia.org/w/api.php
        siteMatrixFile  -- full path to a cache file for the site matrix information
        verbose         -- if set, produce extra output; default False
        dryrun          -- if set, don't actually do update/creation/upload, show what
                           would be run; default False"""
        self.wikiName = wikiName
        self.apiUrl = apiUrl
        self.siteMatrixFile = siteMatrixFile
        self.verbose = verbose
        self.dryrun = dryrun

        self.langCode = None
        self.localLangName = None
        self.project = None

        self.matrix = MWSiteMatrix(self.apiUrl, self.siteMatrixFile, self.dryrun, self.verbose)

        if not self.wikiName in self.matrix.matrix.keys():
            # one more try
            self.matrix.updateMatrix()

        if self.wikiName in self.matrix.matrix.keys():
            self.langCode = self.matrix.matrix[self.wikiName]['lang']
            self.localLangName = self.matrix.matrix[self.wikiName]['locallangname']
            self.project = self.matrix.matrix[self.wikiName]['project']

    def getLangCode(self):
        """Get the language code corresponding to the wikiname
        of the dump we are creating/uploading"""
        return self.langCode

    def getLocalLangName(self):
        """From the wikiname, get the translation of the name of the language 
        for the lang code of the dump we are creating/uploading.  The translation
        is into the content language of the site from which we retrieve
        the sitematrix information; typically this should be English, since we are
        uploading to archive.org and the description keywords used there
        are generally English."""
        return self.localLangName

    def getProject(self):
        """From the wikiname, get the project name of the dump we are
        creating/uploading."""
        return self.project

def wmfmwUsage(message = None):
    if message:
        sys.stderr.write("Error: %s\n\n" % message)
        sys.stderr.write("Usage: python wmfmw.py --wiki [--matrixcache] [--apiurl]\n")
        sys.stderr.write("                    [--dryrun] [--verbose]\n")
        sys.stderr.write("\n")
        sys.stderr.write("Retrieve language code, projectname and local language name for specified wiki,\n")
        sys.stderr.write("by checking a local cache file if specified and/or reading site matrix\n")
        sys.stderr.write("via a MediaWiki api request\n")
        sys.stderr.write("\n")
        sys.stderr.write("Required arguments:")
        sys.stderr.write("wiki:         name of the wiki for which to retrieve information\n")
        sys.stderr.write("\n")
        sys.stderr.write("Optional arguments:")
        sys.stderr.write("matrixcache:  name of a cache file from which to read/write the information\n")
        sys.stderr.write("apiurl:       full url to api.php on the specified wiki\n")
        sys.stderr.write("dryrun:       don't save a cache file, just display what would be done\n")
        sys.stderr.write("verbse:       display messages about what the script is doing\n")
        sys.stderr.write("\n")
        sys.stderr.write("Example run:\n")
        sys.stderr.write("python ./wmfmw.py --dryrun --wiki elwiki --apiurl 'http://el.wikipedia.org/w/api.php'\n")
        sys.exit(1)

if __name__ == "__main__":
    """
    Command line client to retrieve language code, language name and project name of the specified wiki
    """
    # get the wiki name
    siteMatrixFile = None
    apiUrl = "http://en.wikipedia.org/w/api.php"
    verbose = False
    dryrun = False
    wikiName = None

    if len(sys.argv) < 2:
        wmfmwUsage("No options specified.")
        
    try:
        options, remainder = getopt.gnu_getopt(sys.argv[1:], "", [ "apiurl=", "matrixcache=", "wiki=", "dryrun", "verbose" ])
    except getopt.GetoptError, err:
        print str(err)
        wmfmwUsage("Unknown option specified")

    if len(remainder):
        wmfmwUsage("Unknown option specified")

    for (opt, val) in options:
        if opt == "--apiurl":
            apiUrl = val
        elif opt == "--matrixcache":
            siteMatrixFile = val
        elif opt == "--wiki":
            wikiName = val
        elif opt == "--dryrun":
           dryrun = True
        elif opt == "--verbose":
            verbose = True
        else:
            wmfmwUsage("Unknown option specified")

        
    wmi = WikiMatrixInfo(wikiName, apiUrl, siteMatrixFile, verbose, dryrun)
    print "language code: %s, project: %s, local language name: %s" % (wmi.getLangCode(), wmi.getProject(), wmi.getLocalLangName())
