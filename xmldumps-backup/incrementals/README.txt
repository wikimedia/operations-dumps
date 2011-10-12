The adds/changes dumps are a supplementary set of dumps intended to accompany
the regular XML dump files.

The adds/changes dumps are produced in two stages. 

In stage one, the max rev_id value at the time of the run is written out to a file for each project for the given date.  Script name: generatemaxrevids.py

In stage two, intended to be run at a later time, a stub file containing all 
revisions from the previous adds/changes dump through the max rev_id just 
written.  This file is sorted by page id, just as the regular XML stubs files 
are.  Next a history file containing metadata and page text for those 
revisions is written, in the same format as the pages-meta-history file 
generated for the regular XML dumps.  A status file is written to indicate
that the job is done, and the md5sums of the stub and revision text files
is written to a file as well.  Script name: generateincrementals.py

The reason that there are two stages run via two separate scripts is that
you may want to allow editors time to delete or hide sensitive or offensive
material newly entered.  A delay of an arbitrary number of seconds between
the recording of the max rev_id to dump and the start of the stub and 
revision text dump is configurable in the configuration file; see 
README.config for information on that. 

Installation: 

Seriously?  You want to install this already?  This is version 0.0.1.  Know
what that means? It's buggy, risky, and could eat your data.  

However, if you just want to play around with it on your laptop, fine.  
* Put the files generateincrementals.py, generatemaxrevids.py, incrmonitor.py,
  incrmonitor and IncrDumpLib.py together with the sample configuration file 
  dumpincr.conf into a directory from which the job will run.  
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
* Do a test run; run generatemaxrevids.py by hand.  Then look in the top level
  directory you created earlier.  Is there a directory for each project? Is
  there a subdirectory under each of these with the date, in YYYYMMDD format?
  In the date subdirectory are there a file maxrevid.txt containing a positive
  integer?
* Do the phase 2 test run: run generateincrementals.py by hand.  If you have 
  configured a large delay, you will need to wait at least that amount of time
  before running this script.  When it has completed, check the subdirectory
  from phase 1; are there files analogous to the following?
    mywiki-yyyymmdd-md5sums.txt                   
    mywiki-yyyymmdd-pages-meta-hist-incr.xml.bz2  
    mywiki-yyyymmdd-stubs-meta-hist-incr.xml.gz
    maxrevid.txt
    status.txt
  Does the status.txt file contain "done"?
* If the runs look like they are producing the right files, do the html
  generation by hand; run monitor.py.  In the top level directory for the
  adds/changes dumps, do you see the file index.html?  If you view that
  file in a browser, do the contents look reasonable?
* If that looks good, put phase 1 and phase 2 into separate cron jobs, 
  spacing them out as appropriate.

