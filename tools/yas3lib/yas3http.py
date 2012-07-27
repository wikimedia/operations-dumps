import os, re, sys, time, httplib, getopt

class YaS3HTTPCookie(object):
    """
    Simple cookie manager
    """

    def __init__(self, cookieString, host, protocol, url):
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
        self.path = None     # as specified in cookie
        self.domain = None   # as specified in cookie
        self.expires = None  # as specified in cookie
        self.maxage = None   # as specified in cookie
        self.secure = False  # as specified in cookie
        self.httponly = False
        self.text = cookieString
        self.host = host           # host that sent the cookie
        self.protocol = protocol   # protocol used when we got the cookie
        self.url = url             # url (without host/port/protocol) from which cookie was set

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

    def checkDomain(self, host = None):
        """
        Check if the domain specified in the cokoie is the same as the host from which
        the cookie was sent, or from which we are about to make a request

        Arguments:
        host     -- fqdn of the host from which we will be making the remote request
                    if not set, check against the host which sent the cookie
                    (for verification that the cookie should be stored)
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

    def checkPath(self, url = None):
        """
        Check that the path specified in the cookie properly matches the url
        for which it was stored or which we are about to request

        Arguments:
        url      -- url (without host/port/protocol) that will be used for the remote request
                    if not set, check against the url for which the cookie was set
                    (for verification that the cookie should be stored)
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

    def checkSecure(self, protocol = None):
        """
        Check that the protocol specified in the cookie matches the protocol
        in use when it was stored, or which we are about to use for a request

        Arguments:
        protocol    -- protocol that will be used for the remote request
                       if not set, check against the protocol used when the cookie was set
                       (for verification that the cookie should be stored)
        """
        if not protocol:
            protocol = self.protocol
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

    def checkIfCookieValid(self, host, protocol, url):
        """
        Check if the cookie should be sent to the remote location or not
        Returns True if ok to send the cookie, False otherwise

        Arguments:
        host       -- fqdn of the host from which we will be making the remote request
        protocol   -- protocol that will be used for the remote request
        url        -- url (without host/port/protocol) that will be used for the remote request
        """
        # if this cookie is not expired and the path matches and the domain matches etc 
        # this is a valid cookie (and we would want to send the corresponding cookie header)
        if not self.name or not self.domain or not self.path: # bad or not saved cookie
            return False

        if not self.host or not self.protocol or not self.path:
            # no point in making this check since we don't have the remote host info
            # let the caller figure it out
            return True

        if not self.checkDomain(host):
            return False
        if not self.checkPath(url):
            return False
        if not self.checkSecure(protocol):
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
        if header and value is not None:
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

    def findHeader(self, name):
        """
        Find and return value of header for specified
        header name
        
        Arguments:
        name   -- header name
        """
        lcaseName = name.lower()
        if not len(self.headers):
            return None
        for h, v in self.headers:
            if h.lower() == lcaseName:
                return v
        return None

    def findAmzHeaders(self):
        """
        Find and return a list of all x-amz headers
        """
        if not len(self.headers):
            return []
        result = []
        for h, v in self.headers:
            if h.lower().startswith("x-amz-"):
                result.append((h,v))
        return result

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
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid("archive.org", "http", "/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid("a.b.archive.org", "http", "/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid("a.archive.org", "http", "/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid("abarchive.org", "http", "/account.php")

        c = YaS3HTTPCookie("Set-Cookie: domain=%s; path=/a/; testcookie=1" % domain, "archive.org", "http", "/a/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid("ab.archive.org", "http", "/a/account.php")

        c = YaS3HTTPCookie("Set-Cookie: domain=%s; path=/a; testcookie=1" % domain, "archive.org", "http", "/a/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid("ab.archive.org", "http", "/a/account.php")

        c = YaS3HTTPCookie("Set-Cookie: domain=%s; path=/abc; testcookie=1" % domain, "archive.org", "http", "/abc/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid("ab.archive.org", "http", "/abcd/account.php")

        # check to see if cookie is stored or rejected
        c = YaS3HTTPCookie("Set-Cookie: domain=%s; path=/abc; testcookie=1" % domain, "archive.org", "http", "/abc/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid("archive.org", "http", "/abc/account.php")

        c = YaS3HTTPCookie("Set-Cookie: domain=%s; path=/ab; testcookie=1" % domain, "archive.org", "http", "/abc/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid("archive.org", "http", "/abc/account.php")

        c = YaS3HTTPCookie("Set-Cookie: domain=%s; path=/abc/; testcookie=1" % domain, "archive.org", "http", "/abc/account.php")
        print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid("archive.org", "http", "/abc/account.php"), "\n"

    print "Doing set 3"
    c = YaS3HTTPCookie("Set-Cookie: domain=archive.org; path=/abc/; testcookie=1", "archive.org", "https", "/abc/account.php")
    print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid("archive.org", "https", "/abc/account.php")
    print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid("ab.archive.org", "http", "/a/account.php")
    
    c = YaS3HTTPCookie("Set-Cookie: path=/abc/; testcookie=1", "archive.org", "https", "/abc/account.php")
    print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid("archive.org", "https", "/abc/account.php")
    print "domain:", c.domain, "host:", c.host, "would send: ", c.checkIfCookieValid("ab.archive.org", "https", "/a/account.php"), "\n"
