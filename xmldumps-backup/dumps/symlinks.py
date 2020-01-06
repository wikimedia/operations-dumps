#!/usr/bin/python3
"""
Handle creation of symlinks from the 'latest' directory for a wiki,
pointing to current dump content files, plus rss feed files for these
"""
import os
from os.path import exists
import re
import time

from dumps.exceptions import BackupError
from dumps.fileutils import DumpFilename, FileUtils


def xml_escape(text):
    """
    do minimal conversion of text for use in xml files
    """
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class SymLinks():
    """
    management of links in the 'latest' directory to dump content files
    from the latest run, so that downloaders can download these links directly;
    the names never change except for page range files.
    """
    NAME = "symlinks"

    def __init__(self, wiki, dump_dir, logfn, debugfn, enabled):
        self.wiki = wiki
        self.dump_dir = dump_dir
        self._enabled = enabled
        self.logfn = logfn
        self.debugfn = debugfn

    def make_dir(self, dirname):
        """
        make the specified directory if needed,
        with a message to console (or wherever) about
        what we did
        """
        if SymLinks.NAME in self._enabled:
            if exists(dirname):
                self.debugfn("Checkdir dir %s ..." % dirname)
            else:
                self.debugfn("Creating %s ..." % dirname)
                os.makedirs(dirname)

    def save_symlink(self, dumpfile):
        """
        given the base filename of a dump content file,
        make a symlink to it in the 'latest' directory, removing any
        existing link, only if the existing link points to an older
        file.
        """
        if SymLinks.NAME in self._enabled:
            self.make_dir(self.dump_dir.latest_dir())
            realfilepath = self.dump_dir.filename_public_path(dumpfile)
            latest_filename = dumpfile.new_filename(dumpfile.dumpname, dumpfile.file_type,
                                                    dumpfile.file_ext, 'latest',
                                                    dumpfile.partnum, dumpfile.checkpoint,
                                                    dumpfile.temp)
            link = os.path.join(self.dump_dir.latest_dir(), latest_filename)
            if exists(link) or os.path.islink(link):
                if os.path.islink(link):
                    oldrealfilepath = os.readlink(link)
                    # format of these links should be...
                    # ../20110228/elwikidb-20110228-templatelinks.sql.gz
                    rellinkpattern = re.compile(r'^\.\./(20[0-9]+)/')
                    dateinlink = rellinkpattern.search(oldrealfilepath)
                    if dateinlink:
                        dateoflinkedfile = dateinlink.group(1)
                        dateinterval = int(self.wiki.date) - int(dateoflinkedfile)
                    else:
                        dateinterval = 0
                    # no file or it's older than ours... *then* remove the link
                    if not exists(os.path.realpath(link)) or dateinterval > 0:
                        self.debugfn("Removing old symlink %s" % link)
                        try:
                            os.remove(link)
                        except FileNotFoundError:
                            pass
                else:
                    self.logfn("What the hell dude, %s is not a symlink" % link)
                    raise BackupError("What the hell dude, %s is not a symlink" % link)
            relative = FileUtils.relative_path(realfilepath, os.path.dirname(link))
            # if we removed the link cause it's obsolete, make the new one
            if exists(realfilepath) and not exists(link):
                self.debugfn("Adding symlink %s -> %s" % (link, relative))
                os.symlink(relative, link)

    def cleanup_symlinks(self):
        """
        toss all symlinks in the 'latest' directory
        that point to non-existent files
        """
        if SymLinks.NAME in self._enabled:
            latest_dir = self.dump_dir.latest_dir()
            files = os.listdir(latest_dir)
            for filename in files:
                link = os.path.join(latest_dir, filename)
                if os.path.islink(link):
                    realfilepath = os.readlink(link)
                    if not exists(os.path.join(latest_dir, realfilepath)):
                        try:
                            os.remove(link)
                        except FileNotFoundError:
                            pass

    def remove_symlinks_from_old_runs(self, date_string, dump_name=None, partnum=None,
                                      checkpoint=None, onlyparts=False):
        """
        Remove symlinks from the 'latest' directory for (some) links that point to
        files from other runs than the current one (of 'date_string').
        If dump_name, part_num, checkpoint are False or None, we remove all the old symlinks
        for all values of the arg in the filename.
        example: if partnum is False or None then we remove all old values for all file parts

        This needs to do more work if there are file parts or checkpoint files linked in here from
        earlier dates. checkpoint ranges change, and configuration of parallel jobs for file parts
        changes too, so maybe old files still exist and the links need to be removed because we
        have newer files for the same phase of the dump. So we keep symlinks to files from
        one older run only, and clean up the rest. We do this because here at WMF we do partial
        and full runs alternating, and we like to keep the links to files from the full runs around
        until a new full run is in place. Really the number of keeps should be configurable
        (FIXME later I guess).
        """
        if SymLinks.NAME in self._enabled:
            latest_dir = self.dump_dir.latest_dir()
            files = os.listdir(latest_dir)
            dates = []

            files_for_cleanup = []
            for filename in files:
                link = os.path.join(latest_dir, filename)
                if os.path.islink(link):
                    realfilepath = os.readlink(link)
                    dfname = DumpFilename(self.dump_dir._wiki)
                    dfname.new_from_filename(os.path.basename(realfilepath))
                    files_for_cleanup.append({'link': link, 'dfname': dfname, 'path': realfilepath})
                    dates.append(dfname.date)
            try:
                index = dates.index(date_string)
                prev_run_date = dates[index - 1]
            except Exception:
                if len(dates) >= 2:
                    prev_run_date = dates[-2]
                else:
                    prev_run_date = None
            for item in files_for_cleanup:
                if item['dfname'].date < date_string:
                    if dump_name and (item['dfname'].dumpname != dump_name):
                        continue
                    if prev_run_date is None or item['dfname'].date == prev_run_date:
                        # for the previous run, or the only existing run, if different
                        # from the current one, we are very careful. For all older runs
                        # we pretty much want to toss everything

                        # fixme check that these are ok if the value is None
                        if (partnum or onlyparts) and (item['dfname'].partnum != partnum):
                            continue
                        if checkpoint and (item['dfname'].checkpoint != checkpoint):
                            continue
                    self.debugfn("Removing old symlink %s -> %s" % (item['link'], item['path']))
                    try:
                        os.remove(item['link'])
                    except FileNotFoundError:
                        pass


class Feeds():
    """
    manage rss feed files
    """
    NAME = "feeds"

    def __init__(self, wiki, dump_dir, dbname, debugfn, enabled):
        self.wiki = wiki
        self.dump_dir = dump_dir
        self.db_name = dbname
        self.debugfn = debugfn
        self._enabled = enabled

    def make_dir(self, dirname):
        """
        make the specified directory if needed,
        with a message to console (or wherever) about
        what we did
        """
        if Feeds.NAME in self._enabled:
            if exists(dirname):
                self.debugfn("Checkdir dir %s ..." % dirname)
            else:
                self.debugfn("Creating %s ..." % dirname)
                os.makedirs(dirname)

    def feed_newer_than_file(self, feed_path, dfname):
        '''
        given the path to a possibly nonexistent feed file,
        return True if the file exists and the dump output file
        described within is more recent (from the date of the
        dump run directory) than the date of dfname (from the date
        in dfname filename)
        in all other cases including various errors, return False
        '''
        try:
            lines = open(feed_path).read().splitlines()
            links = [line for line in lines if '<link>' in line]
            # <link>http://download.wikimedia.org/wikidatawiki/20180420</link>
            datepattern = r"<link>.*/([0-9]{8})</link>"
            match = re.search(datepattern, links[0])
            feed_date = match.group(1)
            if feed_date > dfname.date:
                return True
        except Exception:
            pass
        return False

    def save_feed(self, dfname):
        """
        produce an rss feed file for the specified dump output file
        (dfname)

        If there is already such a feed, update it only if
        the date of the dump output file in the feed is not older
        than the date of dfname, as indicated in the dump dirs/filenames
        themselves, NOT via stat

        args:
            DumpFilename
        """
        if Feeds.NAME in self._enabled:
            rss_path = os.path.join(self.dump_dir.latest_dir(),
                                    self.db_name + "-latest-" + dfname.basename +
                                    "-rss.xml")

            self.make_dir(self.dump_dir.latest_dir())
            filename_and_path = self.dump_dir.web_path(dfname)
            web_path = os.path.dirname(filename_and_path)
            if self.feed_newer_than_file(rss_path, dfname):
                return
            rss_text = self.wiki.config.read_template("feed.xml") % {
                "chantitle": dfname.basename,
                "chanlink": web_path,
                "chandesc": "Wikimedia dump updates for %s" % self.db_name,
                "title": web_path,
                "link": web_path,
                "description": xml_escape("<a href=\"%s\">%s</a>" % (
                    filename_and_path, dfname.filename)),
                "date": time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
            }
            self.debugfn("adding rss feed file %s " % rss_path)
            FileUtils.write_file(
                FileUtils.wiki_tempdir(self.wiki.db_name, self.wiki.config.temp_dir),
                rss_path,
                rss_text, self.wiki.config.fileperms)

    def cleanup_feeds(self):
        """
        Remove rss feed files in the 'latest' dir for which there are no
        corresponding symlinks to dump content files.
        This must be called *after* sym links in the 'latest' dir have been cleaned up.
        we should probably fix this so there is no such dependency,
        but it would mean parsing the contents of the rss file, bleah
        """
        if Feeds.NAME in self._enabled:
            latest_dir = self.dump_dir.latest_dir()
            files = os.listdir(latest_dir)
            for fname in files:
                if fname.endswith("-rss.xml"):
                    filename = fname[:-8]
                    link = os.path.join(latest_dir, filename)
                    if not exists(link):
                        self.debugfn("Removing old rss feed %s for link %s" % (
                            os.path.join(latest_dir, fname), link))
                        try:
                            os.remove(os.path.join(latest_dir, fname))
                        except FileNotFoundError:
                            # a separate cleanup job might have removed it.
                            pass
