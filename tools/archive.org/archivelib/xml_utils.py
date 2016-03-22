import xml.sax


class ListObjectsCH(xml.sax.ContentHandler):
    """
    Read contents from a request to list all objects (files)
    in a given item (bucket)

    Sample output:
    <?xml version='1.0' encoding='UTF-8'?>
    <ListBucketResult>
        <Name>elwiktionary-dumps</Name>
        <Contents>
            <Key>elwiktionary-20060703.tar</Key>
            <LastModified>2012-02-17T11:22:21.000Z</LastModified>
            <ETag>2012-02-17T11:22:21.000Z</ETag>
            <Size>10076160</Size>
            <StorageClass>STANDARD</StorageClass>
            <Owner>
                <ID>OpaqueIDStringGoesHere</ID>
                <DisplayName>Readable ID Goes Here</DisplayName>
            </Owner>
        </Contents>
    </ListBucketResult>
    """

    NONE = 0x0
    CONTENTS = 0x1
    KEY = 0x2
    LASTMODIFIED = 0x3
    SIZE = 0x4

    def __init__(self):
        xml.sax.ContentHandler.__init__(self)
        self.key = ""
        self.last_modified = ""
        self.size = ""
        self.state = ListObjectsCH.NONE
        self.item_name = ""
        self.item_creation_date = ""

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
            print ("Object: %s, last modified: %s, size: %s"
                   % (self.key, self.last_modified, self.size))
            self.item_name = ""
            self.item_creation_date = ""

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
            self.last_modified = content
        elif self.state == ListObjectsCH.SIZE:
            self.size = content


class ListAllItemsCH(xml.sax.ContentHandler):
    """
    Read contents from a request to list all items (buckets)

    Sample output:

    <?xml version='1.0' encoding='UTF-8'?>
    <ListAllMyBucketsResult>
        <Owner>
            <ID>OpaqueIDStringGoesHere</ID>
            <DisplayName>atglenn</DisplayName>
        </Owner>
        <Buckets>
            <Bucket>
                <Name>elwiktionary-dumps</Name>
                <CreationDate>1970-01-01T00:00:00.000Z</CreationDate>
            </Bucket>
        </Buckets>
    </ListAllMyBucketsResult>
    """

    NONE = 0x0
    BUCKET = 0x1
    NAME = 0x2
    CREATIONDATE = 0x3

    def __init__(self):
        xml.sax.ContentHandler.__init__(self)
        self.item_name = ""
        self.item_creation_date = ""
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
            print "Item: %s, created: %s" % (self.item_name, self.item_creation_date)
            self.item_name = ""
            self.item_creation_date = ""

        elif name == "Name" and self.state == ListAllItemsCH.NAME:
            self.state = ListAllItemsCH.BUCKET
        elif name == "CreationDate" and self.state == ListAllItemsCH.CREATIONDATE:
            self.state = ListAllItemsCH.BUCKET

    def characters(self, content):
        if self.state == ListAllItemsCH.NAME:
            self.item_name = content
        elif self.state == ListAllItemsCH.CREATIONDATE:
            self.item_creation_date = content


class ArchiveKey(object):
    """Authentication to the archive.org api, S3-style."""

    def __init__(self, config):
        """Constructor. Args:
        config -- a populated ArchiveUploaderConfig object."""
        self.config = config
        self.access_key = self.config.settings['access_key']
        self.secret_key = self.config.settings['secret_key']

    def get_auth_header(self):
        """Returns the http header needed for authentication to the archive.org
        api."""
        return "authorization: LOW %s:%s" % (self.access_key, self.secret_key)

    def get_s3_auth_curl_args(self):
        """Returns the arguments needed for auth to the archive.org S3 api"""
        return ["--header", self.get_auth_header()]
