To use this script to set up your interwiki links for your downloaded and
imported XML file for one of the Wikimedia projects, you need the following:

python-cdb, see http://pypi.python.org/pypi/python-cdb/
You will need to follow the build and install instructions for the package if
it is not included in your *nix distribution.

This script was tested with python-cdb version 0.34.

Known issues:

This script is designed to add links for one wiki, it does not handle wiki-farms.

If the interwiki cdb file that you are modifiying has multiple values for the same key
(as the Wikimedia interwiki cdb file does for language codes dk and no), the 'extra'
keys will be dropped from the new cdb file.  This will be fixed or classified as
not an issue as soon as the author finds out if the multiple values for one key is
a bug in the original file or a feature.



