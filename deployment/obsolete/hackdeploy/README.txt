So you don't want to package yer stuff and you don't need a
heavy weight deployment tool, you just want to shovel stuff
out to the hosts relatively quickly?  This tool is (maybe)
for you.

Requirements:
* needs salt, runs from salt master as root.

What this does:
* prep stage: copy files from your local repo to a staging/prep
  directory in a subdir with the date
* deploy stage: copy files from your staging area to the remote
  location, adding '_tmp' to all the filenames
* check the md5s of the _tmp files against the local md5s
* if those pass, move the _tmp files into place
* create or update hackdeploy_RELEASE.txt with the date

Setup:
* Check out your repo and put it somewhere on the salt master. Get
  on the right branch and commit.
* Decide where you want your prep/staging area on the salt master.
  Files are staged there in a subdirectory with the specified date
  or the date of the deployment.
* Write your config file that says what files to deploy to which
  directories (see README.config for a description and a sample).
* Run the script for the 'prep' stage. (--help gives a help message). 
* Check that the prep/staginarea has a subdir with the files you want.
* Run the script for the 'deploy' stage on a sample host.
* Check that the remote dir on the remote host has sane contents.
* Enjoy, bug reports to https://phabricator.wikimedia.org/

This is a very minimal deployment script.  Things it won't do and
aren't planned:
- anything but copy text files like scripts or config files.
- handle multiple files with the same name in the same service
  for deployment.
- keep track of multiple deployments on the same day; all those
  are considered one deployment
  with several attempts to squash a bug :-P
- run a bunch of pre/post scripts.  That's what packages are for.
- have fancy rollback.  Just redeploy from the previous prep.
- have fancy logging  That's what eyeballs are for.
- remove files you no longer want/need on remote hosts
- remove old prep/staging directories on the salt master

Seriously, this is just to keep you from having to manually copy
stuff from your git repo into some dir, use salt to shovel it
around, and then use salt to make sure everything worked ok.
If you need more than that, see rgist[1] or scap or debian
packaging or etc.

[1] Ryan-GIt-Sartoris-Trebuchet deploy
