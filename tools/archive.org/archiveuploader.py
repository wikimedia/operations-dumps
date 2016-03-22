import getopt
import sys
import traceback
from archivelib.config import ArchiveUploaderConfig
from archivelib.uploader import ArchiveUploader, ArchiveKey


# todo:
# progress bar for large file uploads, or other way the user can figure
#   out how much of the file upload has been done.
# support multipart uploads for the really huge files
# support the size hints for items that are going to be > 10gb
# sure wish we could check item and contrib history in any other way
#   than log in via icky old web interface and screen scraping
# md5sum or sha1 of uploaded object??


def usage(message=None):
    """Print comprehensive help information to stdout, including a specified message
    if any, and then error exit."""
    if message:
        sys.stderr.write(message + "\n")

    usage_message = """
Usage: python archiveuploader.py [options]

Mandatory options: --action --accesskey, --secretkey
    --accesskey <key>:       The access key from archive.org used to
                             create items and upload objects.
    --secretkey <key>:       The secret key corresponding to the access
                             key described above.
    --action <action>:       See below for the list of actions
Action options (choose one):
      create_item:           The item specified will be created. Fails
                             if item already exists.
      update_item:           The metadata for the specified item will
                             be updated.
      upload_object:         An object will be created by uploading to
                             the specified item the file given by
                             --filename.  Requires the --objectname option.
      verify_object:         An object in an item will be verified by
                             checking its md5sum locally and on the server.
                             Requires the --objectname and the --filename
                             options.
      show_item:             Show metadata about the specified item.
      show_item_status:      Show pending tasks related to specified item.
      list_objects:          List all objects in the specified item.

The above actions all require the --itemname argument be specified.

      list_items:            List all items belonging to the account
                             identified by the --accesskey and --secretkey
                             options.

Other options:
    --configfile <file>:     Name of optional configuration file with
                             access keys, etc.
    --dryrun:                Don't create or update items or objects but
                             show the commands that would be run. This
                             option also means that updates to the
                             sitematrix cache file will not be done,
                             although it will be read from if it exists,
                             and the MediaWiki instance will be queried
                             via the api as well, if needed.
    --filename <file>:       The full path to the file to upload, when
                             --uploadobject is specified.
    --objectname <object>:   The name of an object as it is to appear in
;                            a url.
    --verbose:               Display progress bars and other output.
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def get_opt_vals(options):
    """
    get and return values of options, with
    appropriate defaults
    """
    opt_vals = {
        'action': None,
        'verbose': False,
        'access_key': None,
        'secret_key': None,
        'item_name': None,
        'object_name': None,
        'file_name': None,
        'config_file': None,
        'dryrun': False
        }

    for (opt, val) in options:
        if opt == "--accesskey":
            opt_vals['access_key'] = val
        elif opt == "--secretkey":
            opt_vals['secret_key'] = val
        elif opt == '--action':
            opt_vals['action'] = val
        elif opt == "--dryrun":
            opt_vals['dryrun'] = True
        elif opt == "--itemname":
            opt_vals['item_name'] = val
        elif opt == "--objectname":
            opt_vals['object_name'] = val
        elif opt == "--filename":
            opt_vals['file_name'] = val
        elif opt == "--configfile":
            opt_vals['config_file'] = val
        elif opt == "--verbose":
            opt_vals['verbose'] = True
    return opt_vals


def do_action(archive_uploader, action, object_name, file_name):
    """
    do the specified action and display success or failure
    """
    result = False
    if action == 'upload_object':
        result = archive_uploader.upload_object(object_name, file_name)
    elif action == 'verify_object':
        result = archive_uploader.verify_object(object_name, file_name)
    elif action == 'create_item':
        result = archive_uploader.create_item()
    elif action == 'update_item':
        result = archive_uploader.update_item()
    elif action == 'list_items':
        archive_uploader.list_all_items()
    elif action == 'list_objects':
         archive_uploader.list_objects()
    elif action == 'show_item':
        archive_uploader.show_item()
    elif action == 'show_item_status':
        archive_uploader.show_item_status()
    if result:
        print "Failed."
    else:
        print "Successful."


def do_main():
    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "",
            ['accesskey=', 'secretkey=', 'action=', 'objectname=', 'filename=',
             'itemname=', 'configfile=', 'dryrun', 'verbose'])
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
        usage("Unknown option or other error encountered")

    opt_vals = get_opt_vals(options)

    if len(remainder):
        usage("Error: unknown option specified.")

    if opt_vals['action'] is None:
        usage("Error: no action option specified.")

    if opt_vals['action'] not in ['upload_object', 'verify_object', 'create_item', 'update_item',
                                  'list_items', 'list_objects', 'show_item', 'show_item_status']:
        usage("Error: unknown action " + opt_vals['action'])

    if (opt_vals['action'] in ['upload_object', 'verify_object']):
        if not opt_vals['file_name']:
            usage("Error: a filename for upload or verification must "
                  "be specified with uploadobject/verifyobject action.")
        if not opt_vals['object_name']:
            usage("Error: the option --objectname must be specified "
                  "with uploadobject/verifyobject action.")

    config = ArchiveUploaderConfig(opt_vals['config_file'])

    if not config.settings['access_key']:
        config.settings['access_key'] = opt_vals['access_key']
    if not config.settings['secret_key']:
        config.settings['secret_key'] = opt_vals['secret_key']

    if not config.settings['access_key'] or not config.settings['secret_key']:
        usage("Error: one of the mandatory options was not specified.")

    archive_key = ArchiveKey(config)
    debugging = []
    if opt_vals['verbose']:
        debugging.append('verbose')
    if opt_vals['dryrun']:
        debugging.append('verbose')
    archive_uploader = ArchiveUploader(config, archive_key, opt_vals['item_name'], debugging)

    do_action(archive_uploader, opt_vals['action'], opt_vals['object_name'], opt_vals['file_name'])


if __name__ == "__main__":
    do_main()
