def get_location_curl_arg():
    """Returns the argument that causes curl to follow all redirects"""
    return ["--location"]


def get_rest_of_login_curl_args():
    """Returns some bizarre arguments needed for archive.org login
    partly cause we want to get thecookies without all the html, partly
    cause login failes without this 'test cookie', how is that possible? >_<"""
    return ['-s', '-c', '-', '-b', 'test-cookie=1', '-o', '/dev/null']


def get_quiet_curl_arg():
    return ["-s"]


def get_no_derive_curl_arg():
    """This tag tells archive.org not to try to derive a bunch of other
    formats from this file (which it would do for videos, for example).
    We've been requested to add this since our files have no derivative
    formats."""
    return ["--header", "x-archive-queue-derive:0"]


def get_head_req_curl_args():
    """Returns the curl arguments needed to do head request and write
    out just the http return code"""
    args = get_quiet_curl_arg()
    args.extend(["--write-out", "%{http_code}", "-X", "HEAD"])
    return args


def get_head_with_output_curl_args():
    """Returns the curl arguments needed to do head request and write
    out everything"""
    return ["--head"]


def get_show_headers_curl_arg():
    """Returns the curl argument needed to do a normal (post or
    get) request and show the headers along with the output"""
    return ["--include"]


def get_item_creation_curl_args():
    """Returns the curl arguments needed to put an empty file;
    this is used for updating or creating an item (bucket)."""
    return ["-X", "PUT", "--header", "Content-Length: 0"]


def get_ign_exist_bucket_curl_arg():
    """Return the curl argument required for overwriting the metadata
    of an existing item (bucket)."""
    return ["--header", "x-archive-ignore-preexisting-bucket:1"]
