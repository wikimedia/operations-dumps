wikiqueries.py is a short little script that will run the same sql query
ovr all the wikis in your wikifarm, writing the output in files with
a standard naming scheme provided by the user.  You might use this to
generate a daily list of all page titles in the main namespace, of
all media files uploaded in the last 24 hours, or whatever else you
find useful.

Installation

You need to set up the config file first.  See README.config for details.

Put wikiquery.py, your config file, and a copy of WikiDump.py (from the
XML data dumps repo where you should have gotten a copy of this file as well)
in a directory from which the script will run.

Try a test: run

python wikiqueries.py --verbose --configfile path-to-your-config-here \
  --query 'select page_title from page where page_namespace=0;' wikidbname

where wiki-dbname should be the name of one wiki database in your farm.

Does it run successfully?  If so you could try leaving off the wikidbname and seeing if it
runs across all the wikis in your farm.

