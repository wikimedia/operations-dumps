def get_archive_base_s3_url():
    """Returns location of the base url for archive.org S3 requests"""
    return "http://s3.us.archive.org/"


def get_archive_base_url():
    """Returns location of the base url for regular archive.org requests"""
    return "https://archive.org/"


def get_login_form_url():
    """Returns the url of the login form for archive.org"""
    return "%saccount/login.php" % get_archive_base_url()


def get_archive_item_details_url(item_name):
    """Returns location of item details, sadly as a regular url, but
    happily with json output."""
    return "%sdetails/%s?output=json" % (get_archive_base_url(), item_name)


def get_archive_item_url(item_name):
    """Returns location of the item as an S3-style url"""
    return "%s%s" % (get_archive_base_s3_url(), item_name)


def get_object_url(object_name, item_name):
    """Returns the curl arguments needed for the url of an object (file) S3-style"""
    return "%s/%s" % (get_archive_item_url(item_name), object_name)


def get_archive_item_status_url(item_name):
    """Returns the url of the status of an item (whether there are
    any related things in the job queue) for archive.org"""
    return ('%scatalog.php?history=1&identifier=%s'
            % (get_archive_base_url(), item_name))
