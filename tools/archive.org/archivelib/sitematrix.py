import os
import json
import codecs
from subprocess import Popen, PIPE
from archivelib.error import ArchiveUploaderError


def set_site_entries(site_info, lang_info):
    """
    given pointers to project info, extract the
    project type and language, and return
    a little dict with this information
    """
    site_entries = {}
    site_entries['project'] = site_info['code']
    # special hack
    if site_entries['project'] == 'wiki':
        site_entries['project'] = 'wikipedia'
    if lang_info is None:
        site_entries['locallangname'] = None
        site_entries['lang'] = None
    else:
        site_entries['locallangname'] = lang_info['localname']
        site_entries['lang'] = lang_info['code']
    return site_entries


def api_matrix_json_to_dict(json_string):
    """
    Convert the sitematrix json string to a dict for our use,
    keeping only the information we want: dbname, project name, lang code.

    Sample input:
    { u'localname': u'Aromanian',
      u'code': u'roa-rup',
      u'name': u'Arm\xe3neashce',
      u'site':
      [
       {u'url': u'http://roa-rup.wikipedia.org', \
        u'code': u'wiki', u'dbname': u'roa_rupwiki'},
       {u'url': u'http://roa-rup.wiktionary.org', \
        u'code': u'wiktionary', u'dbname': u'roa_rupwiktionary'}
      ]
    }
    """
    matrix_json = json.loads(json_string)
    matrix = {}

    for key in matrix_json['sitematrix'].keys():
        if key == 'count':
            continue
        if key == 'specials':
            for site in range(0, len((matrix_json['sitematrix'][key]))):
                sitename = matrix_json['sitematrix'][key][site]['dbname']
                matrix[sitename] = set_site_entries(matrix_json['sitematrix'][key][site], None)
        else:
            for site in range(0, len((matrix_json['sitematrix'][key]['site']))):
                sitename = matrix_json['sitematrix'][key]['site'][site]['dbname']
                matrix[sitename] = set_site_entries(matrix_json['sitematrix'][key]['site'][site],
                                                    matrix_json['sitematrix'][key])
    return matrix


class SiteMatrix(object):
    """
    Get and/or update the SiteMatrix (list of MediaWiki sites
    with projct name, database name and language name) via the api,
    saving it to a cache file if requested.
    If no filename is supplied in the config we will use only the api
    to load and update.
    If a filename is supplied in the config we will load from it
    initially and save to it after every update from the api.
    """

    def __init__(self, config, debugging):
        """
        Constructor. Arguments:
        config         -- populated ArchiveUploaderConfig object
        source_url     -- url to the api.php script. For example:"
                          http://en.wikipedia.org/w/api.php
        file_name      -- full path to a cache file for the site matrix information
        debugging      -- list of debugging flags
                          * dont_save_file -- load from cache file but never update it
                            (used primarily for doing a dry run)
                          * verbose -- print out various progress messages
                          all other entries are ignored
        """
        self.config = config
        self.source_url = self.config.settings['api_url'] + "?action=sitematrix&format=json"
        self.file_name = self.config.settings['site_matrix_file']
        self.debugging = debugging
        self.matrix_json = None
        if self.file_name and os.path.exists(self.file_name):
            try:
                self.matrix_json = self.load_matrix_json_from_file()
                self.matrix = json.loads(self.matrix_json)
            except Exception:
                self.matrix_json = None
        if self.matrix_json is None:
            self.matrix_json = self.load_matrix_json_from_api()
            self.matrix = json.loads(self.matrix_json)
            if 'dont_save_file' not in self.debugging:
                self.save_matrix_json_to_file()

    def update_matrix(self):
        """Update the copy of the sitematrix in memory via the MW api.
        Write the results to a cache file if requested/enabled."""
        new_matrix_json = self.load_matrix_json_from_api()
        new_matrix = json.loads(new_matrix_json)
        # We may wind up with wikis that have been renamed, or removed, so that
        # the old name is no longer valid; it will take up space but otherwise
        # is harmless, so ignore this case.
        self.matrix = self.matrix.update(new_matrix)
        self.matrix_json = json.dumps(self.matrix, ensure_ascii=False)
        if 'dont_save_file' not in self.debugging:
            self.save_matrix_json_to_file()

    def load_matrix_json_from_api(self):
        """
        Fetch the sitematrix information via the MW api. Get rid
        of the extra columns and convert the rest to a dict for our use.
        """
        api_matrix_json = self.load_api_matrix_json_from_api()
        matrix = api_matrix_json_to_dict(api_matrix_json)
        matrix_json = json.dumps(matrix, ensure_ascii=False)
        return matrix_json

    def load_api_matrix_json_from_api(self):
        """Fetch the sitematrix information via the MW api."""
        command = [self.config.settings['curl'], "--location", self.source_url]

        if 'verbose' in self.debugging:
            command_string = " ".join(command)
            print "about to run " + command_string

        proc = Popen(command, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        if proc.returncode:
            command_string = " ".join(command)
            raise ArchiveUploaderError(
                "command '" + command_string +
                ("' failed with return code %s " % proc.returncode) +
                " and error '" + error + "'")
        return output

    def save_matrix_json_to_file(self):
        """Write the site matrix information to a cache file
        in json format."""
        if self.file_name:
            outfile = codecs.open(self.file_name, "w", "UTF-8")
            json.dump(self.matrix, outfile, ensure_ascii=False)
            outfile.close()

    def load_matrix_json_from_file(self):
        """Load the json-formatted site matrix information from a
        cache file, converting it to a dict for our use."""
        if self.file_name and os.path.exists(self.file_name):
            infile = open(self.file_name, "r")
            self.matrix_json = json.load(infile)
            infile.close()
