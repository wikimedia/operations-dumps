What is this? 

YaS3Lib (Yet Another S3 Library) is a very scrappy minimal s3 library for creation/listing/downloading
content from a service which supports the S3 REST api.  

VERSION

0.2-pre

MANIFEST

yas3.py         -- library of S3 operation handlers, basic command line client
yas3archive.py  -- library of archive.org operations, command line client extending the
                   yas3 functions
wmfarchive.py   -- library of archive.org operations for working with Wikimedia XML dumps, 
                   command line client, extending the yas3archive and yas3 functions
yas3lib.py      -- s3 utilities needed by the yas3 operation handlers
yas3http.py     -- http utilities needed by the yas3 operation handlers
utils.py        -- misc utilities needed by the yas3 operation handlers
wmfmw.py        -- utilities to interact with MediaWiki to retrieve information about
                   wikis for which the Wikimedia XML dump will be uploaded or updated
                   to archive.org
COPYING         -- license information for this package
README.txt      -- this file
TODO.txt        -- things not done yet; maybe someday

MISSING features

This is a specialized library meant for interacting with archive.org and other services
as needed by Wikimedia for uploading public datasets to public locations. As such there
is a lot of the standard S3 api that is not implemented and for which there are no
implentation plans:

* everything involving acls, policies, locations, lifecycles of buckets
* requests involving bucket logging
* anything involving payment
* anything related to bucket website configuration

Missing but perhaps to be implemented: 

* versioning

TODOs

See the file TODO.txt in this directory.

WARNINGS

This is 0.1, subject to a good deal of change. Also, it could eat all of your data for
breakfast, and then burp loudly during your afternoon nap. You Have Been Warned.

LICENSE

This package is Copyright (c) 2012 Ariel T. Glenn. 
See the file COPYING in this directory for more information.
