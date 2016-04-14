The adds/changes dumps are a supplementary set of dumps intended to accompany
the regular XML dump files.

The adds/changes dumps are produced in one pass.

For each project:

First the max rev_id value for the current date minus the configured delay
is written out to a file.
Then a stub file is produced containing all revisions from the previous
adds/changes dump through the max rev_id just written.  This file is sorted
by page id, just as the regular XML stubs files are.
Next a history file containing metadata and page text for those
revisions is written, in the same format as the pages-meta-history file
generated for the regular XML dumps.
A status file is then written to indicate that the job is done, and the
md5sums of the stub and revision text files are written to a file as well.

There is a configured 'delay' which controls how recent the revisions recorded
may be, for example you might set it to 28800 to write out only revisions
between the last adds-changes dump and those at least 8 hours old.  This is
so that you can give editors time to delete or hide sensitive or offensive
material newly entered, before it winds up in a dump that's publically accessible
and which will sit around on your website for some days or potentially be archived
elsewhere.  This field is configurable in the configuration file; see README.config
for information on that.

Installation:

Seriously?  You want to install this already?  This is version 0.0.1.  Know
what that means? It's buggy, risky, and could eat your data.

However, if you just want to play around with it on your laptop, fine.
* Put the files generateincrementals.py and IncrDumpLib.py together with
  the sample configuration file dumpincr.conf into a directory from which the
  job will run.
  Make sure you have a copy or a symlink of WikiDump.py from the regular XML
  dumps in this same directory.
  Also make sure you have a template for the top level index.html file, called
  "incrs-index.html" in the same directory with these scripts.  See the existing
  incrs-index.html file for the format; the key here is that you want the
  string "%(items)s" in between <ul> and </ul> tags.  The status of the dump
  for each wiki, along with links to the stub and revisions files, will be
  included as a list item in that spot in the file.
* See README.config for information on the various options in the config file.
* Create the top level directory underneath which there will be a directory
  for each project you want to generate additions/changes. You needn't create
  the subdirectories, this will be done for you at run time.
* Do a test run.  Then look in the top level directory you created earlier.
  Is there a directory for each project? Is there a subdirectory under each
  of these with the date, in YYYYMMDD format?
  In the date subdirectory are there a file maxrevid.txt containing a positive
  integer, a stubs files and a pages file, and a status file containing the
  text 'done'?  The file listing should look like the following:
    maxrevid.txt
    mywiki-yyyymmdd-md5sums.txt
    mywiki-yyyymmdd-pages-meta-hist-incr.xml.bz2
    mywiki-yyyymmdd-stubs-meta-hist-incr.xml.gz
    maxrevid.txt
    status.txt
* If the runs look like they are producing the right files, check the html
  file.  In the top level directory for the adds/changes dumps, do you see
  the file index.html?  If you view that file in a browser, do the contents
  look reasonable?
* If that looks good, put the script into a cron job, setting it to run
  at a little bit before midnight. (This allows you to run missing dates
  later and have the produced files contain more or less the same amount
  or edit activity, since maxrevid for a non-current date is calculated
  by checking for the most recent revision with 'delay' seconds older
  than the specified date at 23:59:00.)
