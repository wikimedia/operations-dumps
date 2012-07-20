import os, re, sys, time, httplib, getopt

class YaS3HTTPCookie(object):
    """
    Simple cookie manager
    """

    def __init__(self, cookieString, host = None, protocol = None, url = None):
        """
        Arguments:
        cookieString -- the Set-Cookie header including the header name
        host         -- fqdn of the cookie source
        protocol     -- protocol of the cookie source (http or https)
        url          -- url of the cookie source (excluding host, port, protocol)

        Note that host, protocol and url must be set if you want to
        do basic checks on th cookie such as "do we store a cookie that
        the remote host sent us' or 'do we send this cookie to th remote host
        for the specified url'.  These values do not default, *including the
        protocol*.
        """
        self.name = None
        self.value = None
        self.path = None
        self.domain = None
        self.expires = None
        self.maxage = None
        self.secure = False
        self.httponly = False
        self.text = cookieString
        self.host = host
        self.protocol = protocol
        self.url = url
        self.convertStringToCookie()
        # FIXME if there is no expiry header what is the right behavior?
        if self.expires:
            cookieExpiry = time.strptime(self.expires, "%a, %d-%b-%Y %H:%M:%S GMT")
            self.cookieExpiryString = "%s%s%s%s%s%s" % (cookieExpiry.tm_year, cookieExpiry.tm_mon, cookieExpiry.tm_mday, cookieExpiry.tm_hour, cookieExpiry.tm_min, cookieExpiry.tm_sec)
        else:
            self.cookieExpiryString = None
        # FIXME deal with max-age attribute sometime

    def convertStringToCookie(self):
        """
        Convert Set-Cookie text string to our internal format
        """
        if self.text.startswith("Set-Cookie: "):
            pieces = self.text[12:].split("; ")
            for p in pieces:
                name, val = p.split("=")
                if name == 'path':
                    self.path = val
                elif name == 'domain':
                    self.domain = val
                elif name == 'expires':
                    self.expires = val
                elif name == 'max-age':
                    self.maxage = val
                elif name == 'secure':
                    self.secure = True
                elif name == 'httponly':
                    self.httponly = True
                else:
                    self.name = name
                    self.value = val
            if self.name:
                # fill in the defaults
                if not self.path:
                    self.path = self.url
                if not self.domain:
                    self.domain = self.host
                elif self.domain[0] != '.':
                    # doesn't start with a period, force one
                    # this is not according to spec BUT it is
                    # according to usual behavior :-/
                    self.domain = '.' + self.domain
                # check to see if we like the cookie
                if not self.checkIfCookieToBeStored():
                    self.name = None # indicates a bad or empty cookie


    def displayCookie(self):
        """
        Display the cookie to stdout
        """
        print "%s: %s" % (self.name, self.value)
        print "path: %s, domain: %s, expires: %s" % (self.path, self.domain, self.expires)

    def checkDomain(self):
        """
        Check if the domain specified in the cokoie is the same as the host from which
        the cookie was sent
        """
        # the domain is the host that set the cookie. exact matches only
        if self.domain[0] != '.':
            if self.domain == self.host:
                return True
            else:
                return False
        # direct match (after adding the leading dot)
        if "." + self.host == self.domain:
            return True
        # no domain match at all
        if not self.host.endswith(self.domain):
            return False
        # end of host matches domain but there is a subdomain (a.b.x.y host, .x.y domain)
        # FIXME this is for FQDN only, not ips so we are overly exclusive here
        if self.host[:len(self.host) - len(self.domain)].find('.') != -1:
            return False
        return True

    def checkPath(self):
        """
        Check that the path specified in the cookie properly matches the url
        for which it was stored
        """
        # exact match
        if self.url == self.path:
            return True
        # no match at all
        if not self.url.startswith(self.path):
            return False
        # check for / starting next component
        if self.path[-1] != '/' and self.url[len(self.path)] != '/':
            return False
        return True

    def checkSecure(self):
        """
        Check that the protocol specified in the cookie matches the protocol
        in use when it was stored
        """
        if self.secure and not self.protocol == "https":
            return False
        return True

    def checkExpires(self):
        """
        Check that the cookie is not expired
        """
        currentDate = time.gmtime()
        currentDateString = "%s%s%s%s%s%s" % (currentDate.tm_year, currentDate.tm_mon, currentDate.tm_mday, currentDate.tm_hour, currentDate.tm_min, currentDate.tm_sec)
        if currentDateString > self.cookieExpiryString:
            return False
        return True

    def checkIfCookieToBeStored(self):
        """
        Check if the cookie received should be stored or rejteced as bad

        Returns True if it should be stored, False otherwise
        """
        # this expects: the domain of the cookie to have been set with whatever is in the 'Domain"
        # attribute, unmodified.

        if not self.host or not self.protocol or not self.path:
            # no point in making this check since we don't have the sender host info
            # let the caller figure it out
            return True

        # no embedded periods, discard
        if self.domain.strip('.').find('.') == -1:
            return False

        if not self.checkDomain():
            return False
        if not self.checkPath():
            return False
        return True

    def setRemoteHostInfo(self, host, protocol, url):
        """
        Set information about the remote host to which the cookie might be sent

        Arguments:
        host      -- fqdn of the host
        protocol  -- http or https
        url       -- the url (without host, port, protocol)
        """
        self.host = host
        self.prptocol = protocol
        self.url = url

    def checkIfCookieValid(self):
        """
        Check if the cookie should be sent to the remote location or not
        
        Call this after setRemoteHostInfo(...)

        Returns True if ok to send the cookie, False otherwise
        """
        # if this cookie is not expired and the path matches and the domain matches etc 
        # this is a valid cookie (and we would want to send the corresponding cookie header)
        if not self.name or not self.domain or not self.path: # bad or not saved cookie
            return False

        if not self.host or not self.protocol or not self.path:
            # no point in making this check since we don't have the remote host info
            # let the caller figure it out
            return True

        if not self.checkDomain():
            return False
        if not self.checkPath():
            return False
        if not self.checkSecure():
            return False
        if self.cookieExpiryString:
            if not self.checkExpires():
                return False
        return True

    def getCookieHeader(self):
        """
        Return the cookie in the format of an HTTP Cookie header
        """
        return "%s=%s" % (self.name, self.value)

class YaS3HTTPDate(object):
    """
    methods for working with dates as set in HTTP Date headers 
    """

    @staticmethod
    def getHTTPDate():
        """
        Format and return the current time as a string ready for Date header
        """
        return time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())

class YaS3HTTPHeaders(object):
    """
    HTTP headers and methods for manipulating them
    """

    def __init__(self):
        """
        Initialize an empty list of headers
        """
        self.headers = []

    def addHeader(self, header, value):
        """
        Add a header to the list of headers

        Argumnents;
        header  -- header name
        value   -- text string for the header value
        """
        self.headers.append([ header, value ])

    def printHeader(self, header, value):
        """
        Display the header/value pair properly formatted to stdout

        Arguments:
        header  -- header name
        value   -- text string for the header value
        """
        print "%s: %s" %(header, value)

    def printAllHeaders(self):
        """
        Display all headers in our list
        """
        if len(self.headers):
            for h, v in self.headers:
                self.printHeader(h,v)

if __name__ == "__main__":
    """
    Test suite for the cookie validation checks
    """
    print "set 1, expect: T F T F T T F T F T T F T F"
    print "set 2, expect: T F T F T T F T F T T F T F"
    for (i, domain) in [ (1, ".archive.org"), (2, "archive.org") ]:
        print "Doing set", i

        # check to see if stored cookie would be sent to new host/url/etc
        c = YaS3HTTPCookie("Set-Cookie: domain=%s; path=/; testcookie=1" % domain, "archive.org", "http", "/account.php")
        c.setRemoteHostInfo("archive.org", "http", "/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid()
        c.setRemoteHostInfo("a.b.archive.org", "http", "/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid()
        c.setRemoteHostInfo("a.archive.org", "http", "/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid()
        c.setRemoteHostInfo("abarchive.org", "http", "/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid()

        c = YaS3HTTPCookie("Set-Cookie: domain=%s; path=/a/; testcookie=1" % domain, "archive.org", "http", "/a/account.php")
        c.setRemoteHostInfo("ab.archive.org", "http", "/a/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid()

        c = YaS3HTTPCookie("Set-Cookie: domain=%s; path=/a; testcookie=1" % domain, "archive.org", "http", "/a/account.php")
        c.setRemoteHostInfo("ab.archive.org", "http", "/a/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid()

        c = YaS3HTTPCookie("Set-Cookie: domain=%s; path=/abc; testcookie=1" % domain, "archive.org", "http", "/abc/account.php")
        c.setRemoteHostInfo("ab.archive.org", "http", "/abcd/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid()

        # check to see if cookie is stored or rejected
        c = YaS3HTTPCookie("Set-Cookie: domain=%s; path=/abc; testcookie=1" % domain, "archive.org", "http", "/abc/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid()

        c = YaS3HTTPCookie("Set-Cookie: domain=%s; path=/ab; testcookie=1" % domain, "archive.org", "http", "/abc/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid()

        c = YaS3HTTPCookie("Set-Cookie: domain=%s; path=/abc/; testcookie=1" % domain, "archive.org", "http", "/abc/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid(), "\n"

    print "Doing set 3"
    c = YaS3HTTPCookie("Set-Cookie: domain=archive.org; path=/abc/; testcookie=1", "archive.org", "https", "/abc/account.php")
    print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid()
    c.setRemoteHostInfo("ab.archive.org", "http", "/a/account.php")
    print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid()
    
    c = YaS3HTTPCookie("Set-Cookie: path=/abc/; testcookie=1", "archive.org", "https", "/abc/account.php")
    print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid()
    c.setRemoteHostInfo("ab.archive.org", "https", "/a/account.php")
    print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid(), "\n"
