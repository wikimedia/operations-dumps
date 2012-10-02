Deployment of xml dump producing scripts to th snapshot hosts

0) Be root on a bastion host (with ssh key forwarding) in the directory
   /home/w/downloadserver/snapshothosts/dumps   

1) Make a copy of the previously deployed code, config files, etc:
     python scripts/prep-dumps-deploy.py
   This will create /home/wikipedia/downloadserver/snapshothosts/dumps/deploy/mon-dd-yyyy

2) Edit or update files in the above directory

3) Copy the directory to one of the snapshot hosts for testing
     bash scripts/copy-dir.sh --hosts specific-snap-host
   This sets the permissions correctly for configuration files so they can be
   read by the backup user, which runs the dumps

4) Enable the directory as production on that host
     bash scripts/set-symlink.sh --hosts specific-snap-host
   This creates a symlink of the newly deployed dir to 'production'

5) Test over there 
   -- note that the next dump for a wiki run on that host will use the new production
      directory.  If dumps are active on that host, you may wish to simply watch
      one run if the current wiki dump is nearing completion.
   -- if tests fail you can reset the symlink to the old production directory
      either manually or by running on the bastion host
      set-symlink.sh --hosts specific-snap-host --deploydir mon-dd-yyyy

3) Copy the directory to all the snapshot hosts
     bash scripts/copy-dir.sh
   This will scp the directory to all hosts; if you prefer you can specify a comma-separated
    list of selected hosts via the --hosts option

4) Activate the directory as production on all hosts:
     bash scripts/set-symlink.sh
