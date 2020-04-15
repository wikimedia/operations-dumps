#!/bin/bash
tests="basedumpstest command_management_test filelister_test fileutils_test\
       intervals_test pagerangeinfo_test prefetch_test
       recompressjobs_test report_test\
       tablesjobs_test xml_dump_test_fixtures xml_dump_test"

for testname in $tests; do
    echo "Running test suite: ${testname}"
    python -m unittest test/${testname}.py
done
    
