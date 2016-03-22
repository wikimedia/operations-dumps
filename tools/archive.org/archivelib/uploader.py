import sys
import json
import xml.sax
import traceback
import re
import hashlib
from subprocess import Popen, PIPE
from archivelib.curlargs import get_location_curl_arg, get_rest_of_login_curl_args
from archivelib.curlargs import get_quiet_curl_arg, get_no_derive_curl_arg, get_head_req_curl_args
from archivelib.curlargs import get_head_with_output_curl_args
from archivelib.curlargs import get_item_creation_curl_args, get_ign_exist_bucket_curl_arg
from archivelib.urls import get_archive_base_s3_url, get_login_form_url
from archivelib.urls import get_archive_item_details_url, get_archive_item_url
from archivelib.urls import get_object_url, get_archive_item_status_url
from archivelib.html_utils import get_login_cookies, show_item_status_from_html
from archivelib.error import ArchiveUploaderError
from archivelib.sitematrix import SiteMatrix
from archivelib.xml_utils import ListObjectsCH, ListAllItemsCH


def show_command(command):
    """Print the supplied command (a list consisting of a command
    and any arguments) to stdout."""
    command_string = " ".join(command)
    print "would run: command " + command_string


def get_etag_value(text):
    # format: ETag: "8ea7c3551a74098b49fbfea49b1ee9e1"
    lines = text.split('\n')
    etag_expr = re.compile(r'^ETag:\s+"([abcdef0-9]+)"')
    for line in lines:
        etag_match = etag_expr.match(line)
        if etag_match:
            return etag_match.group(1)
    return None


def get_md5sum_of_file(file_name):
    summer = hashlib.md5()
    infile = file(file_name, "rb")
    # really? could this be bigger?? consider 20GB files.
    bufsize = 4192 * 32
    inbuffer = infile.read(bufsize)
    while inbuffer:
        summer.update(inbuffer)
        inbuffer = infile.read(bufsize)
    infile.close()
    return summer.hexdigest()


class ArchiveUploader(object):
    """Use the archive.org s3 api to create and update items (buckets)
    and to upload files (objects) into a bucket.  Relies on curl."""

    def __init__(self, config, archive_key, item_name, debugging):
        """Constructor. Args:
        config     -- populated ArchiveUploadedConfig object
        archiveKey -- populated ArchiveKey object (contains access and secret keys)
        itemName   -- name of item tp be created, updated, or uploaded into
        debugging  -- if 'verbose' is in this list, produce extra output; default False
                      if 'dryrun' is in this list don't actually do update/creation/upload,
                      show what would be run; default False
        """
        self.config = config
        self.archive_key = archive_key
        self.item_name = item_name
        self.debugging = debugging
        if 'dryrun' in self.debugging:
            self.debugging.append('dont_save_file')
        self.matrix = None
        self.db_name = self.item_name
        if self.config.settings['item_name_format']:
            self.item_name = self.config.settings['item_name_format'] % self.db_name
        self.session_cookies = None

    def get_login_form_curl_args(self):
        """Returns the arguments needed for auth to the archive.org S3 api"""
        return ["--data-urlencode", "username=%s" % self.config.settings['username'],
                "--data-urlencode", "password=%s" % self.config.settings['password'],
                '--data-urlencode', 'referer=https://archive.org/',
                '--data-urlencode', 'action=login',
                '--data-urlencode', 'remember=CHECKED',
                '--data-urlencode', 'submit=Log in']

    def get_object_upload_curl_args(self, object_name, file_name):
        """Returns the curl arguments needed for upload of a file S3-style:
        the authentication header with accesskey and secret key, and
        the url to the object (file) as an S3 url."""
        args = self.archive_key.get_s3_auth_curl_args()
        args.extend(['--upload-file', file_name, get_object_url(object_name, self.item_name)])
        return args

    def get_cookie_curl_args(self):
        return ["-b", self.session_cookies]

    def get_item_meta_header_args(self):
        """
        Get the curl arguments needed to generate all the headers containing
        metadata for objects (files) on archive.org.
        Sample headers for el wiktionary:
          --header 'x-archive-meta-title:Wikimedia database dumps of el.wiktionary'
          --header 'x-archive-meta-mediatype:web'
          --header 'x-archive-meta-language:el (Modern Greek)'
          --header 'x-archive-meta-description:Dumps of el.wiktionary \
                    created by the Wikimedia Foundation and downloadable \
                    from http://dumps.wikimedia.org'
          --header 'x-archive-meta-format:xml and sql'
          --header 'x-archive-meta-licenseurl:http://wikimediafoundation.org/wiki/Terms_of_Use'
          --header 'x-archive-meta-subject:xml;dump;wikimedia;el;wiktionary'"""
        headers = [
            '--header',
            'x-archive-meta-title:Wikimedia database dumps of %s' % self.db_name,
            '--header',
            'x-archive-meta-mediatype:web',
            '--header',
            ('x-archive-meta-description:Dumps of %s created by %s and downloadable from %s'
             % (self.db_name, self.config.settings['creator'],
                self.config.settings['downloadurl'])),
            '--header',
            'x-archive-meta-format:xml and sql',
            '--header',
            'x-archive-meta-licenseurl:%s' % self.config.settings['licenseUrl']]

        lang = self.get_lang()
        if lang:
            headers.extend([
                '--header',
                'x-archive-meta-language:%s (%s)' % (lang, self.get_local_lang_name()),
                '--header',
                ('x-archive-meta-subject:xml;dump;wikimedia;%s;%s'
                 % (lang, self.get_project()))])
        else:
            headers.extend([
                '--header',
                ('x-archive-meta-subject:xml,dump,wikimedia,%s'
                 % (self.get_project()))])
        return headers

    def do_curl_command(self, curl_command, get_output=False):
        """Given a list containing curl command with all the args and run it.
        If getOutput is True, return any output.
        Raises ArchiveUploaderError on error fron curl."""
        if 'verbose' in self.debugging:
            command_string = " ".join(curl_command)
            print "about to run " + command_string

        try:
            proc = Popen(curl_command, stdout=PIPE, stderr=PIPE)
        except:
            command_string = " ".join(curl_command)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
            raise ArchiveUploaderError("curl_command '" + command_string + "' failed'")

        output, error = proc.communicate()
        if proc.returncode:
            # curl has this annoying idea that when you specifically do a HEAD
            # request it should return an error code anyways indicating a
            # partial file transfer
            if not (proc.returncode == 18 and 'HEAD' in curl_command):
                command_string = " ".join(curl_command)
                raise ArchiveUploaderError(
                    "curl_command '" + command_string +
                    ("' failed with return code %s " % proc.returncode) +
                    " and error '" + error + "'")
        if 'verbose' in self.debugging:
            print "Command successful."
            if get_output:
                if output:
                    print output
                else:
                    print "No output returned."

        if get_output:
            return output

    def upload_object(self, object_name, file_name):
        """Upload an object (file) to the bucket (item). Args:
        object_name -- name of the object as it will appear in the S3-style url
        file_name   -- path to file to be uploaded"""

        # note that someone could remove the item in between the
        # time we check for one upload and the time we check for another
        # upload, in the case of multiple uploads via this script.
        # we're not expecting to beat race conditions, just to warn
        # the user if they try uploading to a bucket they never set up
        exists = self.check_if_item_exists()
        if exists != "200":
            raise ArchiveUploaderError(
                "No such item " + self.item_name +
                " exists, http error code " + exists + ", giving up.")
        curl_command = [self.config.settings['curl']]
        curl_command.extend(get_location_curl_arg())
        curl_command.extend(get_no_derive_curl_arg())
        curl_command.extend(self.get_object_upload_curl_args(object_name, file_name))
        if 'dryrun' in self.debugging:
            show_command(curl_command)
        else:
            self.do_curl_command(curl_command)

    def verify_object(self, object_name, file_name):
        """Verify an object (file) in a given bucket (item) by checking etag
        from server and md5sum of local file. Args:
        object_name -- name of the object as it appears in the S3-style url
        file_name   -- path to corresponding local file"""

        exists = self.check_if_item_exists()
        if exists != "200":
            raise ArchiveUploaderError(
                "No such item " + self.item_name +
                " exists, http error code " + exists + ", giving up.")
        curl_command = [self.config.settings['curl']]
        curl_command.extend(get_location_curl_arg())
        curl_command.extend(get_head_with_output_curl_args())
        curl_command.append(get_object_url(object_name, self.item_name))
        if 'dryrun' in self.debugging:
            show_command(curl_command)
        else:
            result = self.do_curl_command(curl_command, True)
            md5sum_from_etag = get_etag_value(result)
            if not md5sum_from_etag:
                print "no Etag in server output, received:"
                print result
                sys.exit(1)
            md5sum_from_local_file = get_md5sum_of_file(file_name)
            if 'verbose' in self.debugging:
                print "Etag: ", md5sum_from_etag, "md5 of local file: ", md5sum_from_local_file
            if md5sum_from_etag == md5sum_from_local_file:
                if 'verbose' in self.debugging:
                    print "File verified ok."
            else:
                raise ArchiveUploaderError("File verification FAILED.")

    def check_if_item_exists(self):
        """Check it the item (bucket) exists, returning True if it exists
        and False otherwise."""
        curl_command = [self.config.settings['curl']]
        curl_command.extend(get_location_curl_arg())
        curl_command.extend(get_head_req_curl_args())
        curl_command.append(get_archive_item_url(self.item_name))
        result = self.do_curl_command(curl_command, get_output=True)
        return result

    # FIXME we should really check once to see if the project name
    # is valid and then refuse to work on it otherwise, instead
    # of scattering the retry throughout all these functions
    def get_lang(self):
        """Get the language code corresponding to the dbname
        of the dump we are creating/uploading"""
        if not self.matrix:
            self.matrix = SiteMatrix(self.config, self.debugging)

        if self.db_name in self.matrix.matrix.keys():
            return self.matrix.matrix[self.db_name]['lang']
        self.matrix.update_matrix()
        # one more try
        if self.db_name in self.matrix.matrix.keys():
            return self.matrix.matrix[self.db_name]['lang']
        else:
            return None

    def get_local_lang_name(self):
        """From the dbname, get the translation of the name of the language
        for the lang code of the dump we are creating/uploading.  The translation
        is into the content language of the site from which we retrieve
        the sitematrix information; typically this should be English, since we are
        uploading to archive.org and the description keywords used there
        are generally English."""
        if not self.matrix:
            self.matrix = SiteMatrix(self.config, self.debugging)

        if self.db_name in self.matrix.matrix.keys():
            return self.matrix.matrix[self.db_name]['locallangname']
        self.matrix.update_matrix()
        # one more try
        if self.db_name in self.matrix.matrix.keys():
            return self.matrix.matrix[self.db_name]['locallangname']
        else:
            return None

    def get_project(self):
        """From the dbname, get the project name of the dump we are
        creating/uploading."""
        if not self.matrix:
            self.matrix = SiteMatrix(self.config, self.debugging)

        if self.db_name in self.matrix.matrix.keys():
            return self.matrix.matrix[self.db_name]['project']
        self.matrix.update_matrix()
        # one more try
        if self.db_name in self.matrix.matrix.keys():
            return self.matrix.matrix[self.db_name]['project']
        else:
            return None

    def update_item(self):
        """Update an item (bucket); this entails a full update of the metadata. The
        objects (files) it contains are not touched in any way."""
        self.create_item(True)

    def create_item(self, rewrite=False):
        """Create an item (bucket) S3-style.  Args:
        rewrite -- if true, we are updating the metadata of an item that
                   already exists; default false"""
        curl_command = [self.config.settings['curl']]
        curl_command.extend(get_location_curl_arg())
        curl_command.extend(self.archive_key.get_s3_auth_curl_args())
        if rewrite:
            curl_command.extend(get_ign_exist_bucket_curl_arg())
        else:
            exists = self.check_if_item_exists()
            if exists == "200":
                raise ArchiveUploaderError("Item " + self.item_name + " already exists, giving up.")
        curl_command.extend(self.get_item_meta_header_args())
        curl_command.extend(get_item_creation_curl_args())
        curl_command.append(get_archive_item_url(self.item_name))
        if 'dryrun' in self.debugging:
            show_command(curl_command)
        else:
            self.do_curl_command(curl_command)

    def list_all_items(self):
        """List all items for the user associated with the accesskey/secretkey."""
        curl_command = [self.config.settings['curl']]
        curl_command.extend(get_location_curl_arg())
        curl_command.extend(self.archive_key.get_s3_auth_curl_args())
        if 'verbose' not in self.debugging:
            curl_command.extend(get_quiet_curl_arg())
        curl_command.append(get_archive_base_s3_url())
        if 'dryrun' in self.debugging:
            show_command(curl_command)
        else:
            output = self.do_curl_command(curl_command, True)
            if 'verbose' in self.debugging:
                print "About to parse output (list items)"
            xml.sax.parseString(output, ListAllItemsCH())

    def list_objects(self):
        """List all objects (files) contained in a specific item (bucket)."""
        curl_command = [self.config.settings['curl']]
        curl_command.extend(get_location_curl_arg())
        curl_command.extend(self.archive_key.get_s3_auth_curl_args())
        if 'verbose' not in self.debugging:
            curl_command.extend(get_quiet_curl_arg())
        curl_command.append(get_archive_item_url(self.item_name))
        if 'dryrun' in self.debugging:
            show_command(curl_command)
        else:
            output = self.do_curl_command(curl_command, True)
            if 'verbose' in self.debugging:
                print "About to parse output (list objects)"
            xml.sax.parseString(output, ListObjectsCH())

    def show_item(self):
        """Show metadata associated with a particular item (bucket)."""
        curl_command = [self.config.settings['curl']]
        curl_command.extend(get_location_curl_arg())
        if 'verbose' not in self.debugging:
            curl_command.extend(get_quiet_curl_arg())
        curl_command.append(get_archive_item_details_url(self.item_name))
        if 'dryrun' in self.debugging:
            show_command(curl_command)
        else:
            output = self.do_curl_command(curl_command, True)
            if 'verbose' in self.debugging:
                print "About to parse output (show item)"
            self.show_item_metadata_from_json(output)

    def show_item_metadata_from_json(self, json_string):
        """
        Grab the metadata for an item from the json for the item details
        (contains lost of other cruft) and display to stdout
        Sample output:

        "metadata":{
           "identifier":["elwiktionary-dumps"],
           "description":["Dumps of el.wiktionary created by the \
              Wikimedia Foundation and downloadable from \
              http://dumps.wikimedia.org/elwiktionary/"],
           "language":["el (Modern Greek)"],
           "licenseurl":["http://wikimediafoundation.org/wiki/Terms_of_Use"],
           "mediatype":["web"],
           "subject":["xml,dump,wikimedia,el,wiktionary"],
           "title":["Wikimedia database dumps of el.wiktionary, format:xml and sql"],
           "publicdate":["2012-02-17 11:03:45"],
           "collection":["opensource"],
           "addeddate":["2012-02-17 11:03:45"]
        }
        """
        details = json.loads(json_string)
        if 'metadata' in details.keys():
            print "Item metadata for", self.item_name
            for key in details['metadata'].keys():
                print "%s:" % key,
                print " | ".join(details['metadata'][key])
        else:
            print "No metadata for", self.item_name, "is available."

    def log_in(self):
        if not self.session_cookies:
            curl_command = [self.config.settings['curl']]
        curl_command.extend(self.get_login_form_curl_args())
        curl_command.extend(get_rest_of_login_curl_args())
        curl_command.append(get_login_form_url())
        if 'dryrun' in self.debugging:
            show_command(curl_command)
        else:
            output = self.do_curl_command(curl_command, True)
            if 'verbose' in self.debugging:
                print "About to dig cookie out of login response:"
                print output
                self.session_cookies = get_login_cookies(output)
                if not self.session_cookies:
                    raise ArchiveUploaderError("Login failed.")

    def show_item_status(self):
        """Show the status of an item (bucket): which objects (files) are waiting
        on further action from archive.org."""
        self.log_in()
        curl_command = [self.config.settings['curl']]
        curl_command.extend(get_location_curl_arg())
        if 'verbose' not in self.debugging:
            curl_command.extend(get_quiet_curl_arg())
        curl_command.extend(self.get_cookie_curl_args())
        curl_command.append(get_archive_item_status_url(self.item_name))
        if 'dryrun' in self.debugging:
            show_command(curl_command)
        else:
            output = self.do_curl_command(curl_command, True)
            show_item_status_from_html(output)


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
