"""
test suite for index.html generation
"""
import os
from unittest.mock import patch
from test.basedumpstest import BaseDumpsTestCase
from dumps.utils import FilePartInfo
from dumps.dumpitemlist import DumpItemList
from dumps.report import Report
from dumps.runnerutils import DumpRunJobData, RunSettings


class TestIndexHtml(BaseDumpsTestCase):
    """
    test generation of index.html files
    """

    def setup_dump_jobs_info(self):
        '''
        set up fake output files, fake checksum files, and a dumpruninfo txt file
        so that we can get file sizes and download links for all dump jobs
        '''
        fullpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, self.wd['wiki'].db_name,
                                self.today, 'dumpruninfo.txt')
        contents = []
        jobs = ['articlesmultistreamdumprecombine', 'articlesmultistreamdump',
                'metahistory7zdump', 'metahistorybz2dump',
                'xmlpagelogsdumprecombine', 'xmlpagelogsdump',
                'metacurrentdumprecombine', 'metacurrentdump',
                'articlesdumprecombine', 'articlesdump',
                'xmlstubsdumprecombine', 'xmlstubsdump',
                'abstractsdumprecombine', 'abstractsdump',
                'allpagetitlesdump', 'pagetitlesdump',
                'namespaces', 'wbcentityusagetable',
                'sitestable', 'wbchangessubscriptiontable',
                'wbpropertyinfotable', 'wbtermstable']

        for jobname in jobs:
            contents.append('name:{job}; status:done; updated:2020-02-14 13:44:13'.format(
                job=jobname))

        with open(fullpath, "w") as outfile:
            outfile.write('\n'.join(contents))

        # now set up a couple fake checksum files
        outfiles = [
            'abstract1.xml.gz', 'abstract2.xml.gz', 'abstract.xml.gz'
            'all-titles.gz', 'all-titles-in-ns0.gz',
            'pages-articles1.xml-p1p100.bz2', 'pages-articles2.xml-p101p300.bz2',
            'pages-articles3.xml-p301p600.bz2', 'pages-articles4.xml-p1601p2600.bz2',
            'pages-articles4.xml-p2601p3600.bz2', 'pages-articles4.xml-p3601p4450.bz2',
            'pages-articles-multistream1.xml-p1p100.bz2',
            'pages-articles-multistream2.xml-p101p300.bz2',
            'pages-articles-multistream-index1.txt-p1p100.bz2',
            'pages-articles-multistream-index2.txt-p101p300.bz2',
            'pages-articles-multistream-index.txt.bz2', 'pages-articles-multistream.xml.bz2',
            'pages-articles.xml.bz2',
            'pages-logging1.xml.gz', 'pages-logging.xml.gz',
            'pages-meta-current1.xml-p1p100.bz2', 'pages-meta-current2.xml-p101p300.bz2',
            'pages-meta-current.xml.bz2',
            'pages-meta-history1.xml-p1p100.7z', 'pages-meta-history1.xml-p1p100.bz2',
            'pages-meta-history2.xml-p101p300.7z',
            'siteinfo-namespaces.json.gz', 'sites.sql.gz',
            'stub-articles1.xml.gz', 'stub-articles2.xml.gz', 'stub-articles.xml.gz',
            'stub-meta-current1.xml.gz', 'stub-meta-current2.xml.gz', 'stub-meta-current.xml.gz',
            'stub-meta-history1.xml.gz', 'stub-meta-history2.xml.gz', 'stub-meta-history.xml.gz',
            'wbc_entity_usage.sql.gz', 'wb_changes_subscription.sql.gz',
            'wb_items_per_site.sql.gz', 'wb_property_info.sql.gz', 'wb_terms.sql.gz']

        contents = []
        for outfile in outfiles:
            contents.append('53249215f76fce821f571ba481f4ef702b63e8e9  '
                            '{wiki}-{date}-{outfile}'.format(
                                wiki=self.wd['wiki'].db_name, date=self.today, outfile=outfile))
        fullpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, self.wd['wiki'].db_name, self.today,
                                '{name}-{date}-sha1sums.txt'.format(
                                    name=self.wd['wiki'].db_name, date=self.today))
        with open(fullpath, "w") as outfile:
            outfile.write('\n'.join(contents))

        contents = []
        for outfile in outfiles:
            contents.append('94d842ab965a70b1a805c10783f30a97  {wiki}-{date}-{outfile}'.format(
                wiki=self.wd['wiki'].db_name, date=self.today, outfile=outfile))
        fullpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, self.wd['wiki'].db_name, self.today,
                                '{name}-{date}-md5sums.txt'.format(
                                    name=self.wd['wiki'].db_name, date=self.today))
        with open(fullpath, "w") as outfile:
            outfile.write('\n'.join(contents))

        # make fake output files, it's ok if they contain crap
        for outfile in outfiles:
            fullpath = os.path.join(
                BaseDumpsTestCase.PUBLICDIR, self.wd['wiki'].db_name, self.today,
                '{name}-{date}-{outfile}'.format(
                    name=self.wd['wiki'].db_name, date=self.today, outfile=outfile))
            with open(fullpath, "w") as outfile:
                outfile.write("junk\n")

    @patch('dumps.wikidump.Wiki.get_known_tables')
    def test_update_index_html(self, mock_get_known_tables):
        '''
        check index.html generated from dumpruninfo file with various
        dump output files
        '''
        self.setup_dump_jobs_info()

        known_tables = [
            'site_stats', 'wb_items_per_site', 'wb_terms'
            'wb_entity_per_page', 'wb_property_info'
            'wb_changes_subscription', 'sites', 'wbc_entity_usage']

        mock_get_known_tables.return_value = known_tables

        filepart_info = FilePartInfo(self.wd['wiki'], self.wd['wiki'].db_name)
        filepart_info._pages_per_filepart_abstract = [100, 1000, 1000, 1000]
        filepart_info._logitems_per_filepart_pagelogs = [100, 1000, 1000, 1000]

        dumpjobdata = DumpRunJobData(self.wd['wiki'], self.wd['dump_dir'], notice="",
                                     enabled=[RunSettings.NAME])

        dump_item_list = DumpItemList(self.wd['wiki'], prefetch=True, prefetchdate=None,
                                      spawn=True, partnum_todo=None, checkpoint_file=None,
                                      singleJob='noop', skip_jobs=[],
                                      filepart=filepart_info, page_id_range=None,
                                      dumpjobdata=dumpjobdata, dump_dir=self.wd['dump_dir'],
                                      verbose=False)
        items = dump_item_list.dump_items

        reporter = Report(wiki=self.wd['wiki'], enabled=True, dump_dir=self.wd['dump_dir'],
                          items=items, dumpjobdata=dumpjobdata)

        status_items = [Report.report_dump_step_status(self.wd['dump_dir'], item)
                        for item in items]

        reporter.update_index_html(status_items, dump_status="done")

        fullpath = os.path.join(BaseDumpsTestCase.PUBLICDIR, self.wd['wiki'].db_name,
                                self.today, 'index.html')
        with open(fullpath, "r") as infile:
            produced_contents = infile.read()

        with open(os.path.join('./test/files', 'report_test_output.txt'), "r") as infile:
            contents = infile.read()
        expected_contents = contents.format(date=self.today)
        self.assertEqual(produced_contents, expected_contents)
