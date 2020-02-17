#!/usr/bin/python3
"""
test suite for command management module
"""
from io import StringIO
import unittest
from unittest.mock import patch
from test.basedumpstest import BaseDumpsTestCase
from dumps.utils import MiscUtils
from dumps.commandmanagement import CommandPipeline
from dumps.runner import Runner


class TestCommandManagement(BaseDumpsTestCase):
    """
    a few tests for command execution and error
    handling
    """
    @staticmethod
    def debugging_output(output, return_ok, failed_commands,
                         failed_with_sigpipe, uncompress):
        """
        convenience for debugging the pipeline tests if needed
        """
        print('output:', output)
        print('return_ok', return_ok)
        print('failed_commands', failed_commands)
        print('failed_with_sigpipe', failed_with_sigpipe)
        print([[[pipevalue, uncompress]] for pipevalue in MiscUtils.get_sigpipe_values()])

    @staticmethod
    def do_pipeline_test(uncompress, grep):
        """
        run various simple pipelines and check that
        we get sigpipe where we should, get output
        from the commands where we should, and report
        real failures where we should
        """
        pipeline = [uncompress]
        pipeline.append(['/usr/bin/head', "-50"])
        pipeline.append(grep)
        pipeline.append(['/usr/bin/head', "-1"])

        proc = CommandPipeline(pipeline, quiet=True)
        proc.run_pipeline_get_output()

        output = proc.output()
        return_ok = bool(proc.exited_successfully())
        failed_commands = proc.get_failed_cmds_with_retcode()
        failed_with_sigpipe = bool(
            proc.get_failed_cmds_with_retcode() in [
                [[pipevalue, uncompress]] for pipevalue in MiscUtils.get_sigpipe_values()])
        return output, return_ok, failed_commands, failed_with_sigpipe

    def test_command_pipeline(self):
        """
        run a simple zcat | grep, discard sigpipe
        errors, flag any others
        """
        filepath = './test/files/stub-articles-sample.xml.gz'
        uncompress = ['/usr/bin/zcat', filepath]
        grep = ['/usr/bin/grep', "-n", '<page>']

        output, return_ok, failed_commands, failed_with_sigpipe = self.do_pipeline_test(
            uncompress, grep)

        with self.subTest('no failed commands'):
            self.assertEqual(output, b'37:  <page>\n')
            self.assertEqual(return_ok, True)
            self.assertEqual(failed_commands, None)
            self.assertEqual(failed_with_sigpipe, False)

        grep = ['/usr/bin/grep', "-n", '<nosuchtag>']

        output, return_ok, failed_commands, failed_with_sigpipe = self.do_pipeline_test(
            uncompress, grep)

        with self.subTest('grep fails'):
            self.assertEqual(output, b'')
            self.assertEqual(return_ok, False)
            self.assertEqual(failed_commands, [[1, ['/usr/bin/grep', '-n', '<nosuchtag>']]])
            self.assertEqual(failed_with_sigpipe, False)

        uncompress = ['/usr/bin/zcat'] + [filepath] * 10
        grep = ['/usr/bin/grep', "-n", '<siteinfo>']

        output, return_ok, failed_commands, failed_with_sigpipe = self.do_pipeline_test(
            uncompress, grep)

        with self.subTest('zcat fails with sigpipe but produces output'):
            # self.assertEqual(output, b'37:  <siteinfo>\n')
            self.assertEqual(output, b'2:  <siteinfo>\n')
            self.assertEqual(return_ok, False)
            self.assertEqual(failed_commands, [[-13, uncompress]])
            self.assertEqual(failed_with_sigpipe, True)

    @staticmethod
    def make_pipeline(uncompress, grep):
        """
        make and return a pipeline ready to be part of a command series
        """
        pipeline = [uncompress]
        pipeline.append(['/usr/bin/head', "-50"])
        pipeline.append(grep)
        pipeline.append(['/usr/bin/head', "-1"])
        return pipeline

    @patch('dumps.wikidump.Wiki.get_known_tables')
    @patch('dumps.runner.FilePartInfo.get_some_stats')
    def test_run_command_without_errorcheck(self, _mock_get_some_stats, _mock_get_known_tables):
        """
        run a command series, get back 1 and the command pipelines with errors
        or 0 and None on success
        this reports sigpipe as an error, the caller must check for that
        """
        runner = Runner(self.wd['wiki'], prefetch=False, prefetchdate=None, spawn=False,
                        job=None, skip_jobs=None,
                        restart=False, notice="", dryrun=False, enabled=None,
                        partnum_todo=None, checkpoint_file=None, page_id_range=None,
                        skipdone=False, cleanup=False, do_prereqs=False, verbose=False)
        # turn off all the progress report crapola
        runner.log = None

        filepath = './test/files/stub-articles-sample.xml.gz'

        uncompress = ['/usr/bin/zcat', filepath]
        grep = ['/usr/bin/grep', "-n", '<page>']
        command_pipeline_1 = [self.make_pipeline(uncompress, grep)]

        grep = ['/usr/bin/grep', "-n", '<nosuchtag>']
        command_pipeline_2 = [self.make_pipeline(uncompress, grep)]

        uncompress = ['/usr/bin/zcat'] + [filepath] * 10
        grep = ['/usr/bin/grep', "-n", '<siteinfo>']
        command_pipeline_3 = [self.make_pipeline(uncompress, grep)]

        command_series = [command_pipeline_1, command_pipeline_2, command_pipeline_3]
        # command series is very wordy, so toss all that away
        with patch('sys.stdout', new=StringIO()):
            with patch('sys.stderr', new=StringIO()):
                error, broken_pipelines = runner.run_command_without_errorcheck(command_series)
                expected_broken = [
                    [[1, ['/usr/bin/grep', '-n', '<nosuchtag>']]],
                    [[-13, ['/usr/bin/zcat'] + ['./test/files/stub-articles-sample.xml.gz'] * 10]]]

                self.assertEqual(error, 1)
                self.assertEqual(
                    [pipeline.get_failed_cmds_with_retcode() for pipeline in broken_pipelines],
                    expected_broken)


if __name__ == '__main__':
    unittest.main()
