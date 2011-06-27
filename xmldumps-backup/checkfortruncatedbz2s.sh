#!/bin/bash
rm -f /backups-atg/truncated.files
find /mnt/data/xmldatadumps/public -name \*bz2 -a -type f -exec sh -c '/backups-atg/checkforbz2footer {}; if [ $? -eq 0 ]; then echo {} >> /backups-atg/truncated.files; fi' \;
