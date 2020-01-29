#!/bin/env python
#
# Run a test continously, updating an output file with timeing tests
#
import sys
from typing import List
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element
import os
import time

def run_test() -> Element:
    '''
    Runs the test and returns the contents of the pytest output
    '''
    r = 10
    while r != 0:
        r = os.system('pytest -k test_func_adl_query_electrons_and_muons --durations=0 --junitxml stuff.xml')
        if r != 0:
            print ("Error running!")
    return ET.parse('stuff.xml').getroot()

def run_and_log_test(output_csv_log: str) -> None:
    '''
    Run a test, and log its output.
    '''
    test_log_file = run_test()
    testsuites = test_log_file.findall('testsuite')
    assert len(testsuites) == 1
    testsuite = testsuites[0]
    timestamp = testsuite.get('timestamp').replace('T', ' ')
    assert timestamp is not None
    total_time_s = testsuite.get('time')
    assert total_time_s is not None
    total_time = float(total_time_s)

    testcases = testsuite.findall('testcase')
    assert len(testcases) == 1
    testcase = testcases[0]
    test_name = testcase.get('name')
    test_time_s = testcase.get('time')
    assert test_time_s is not None
    test_time = float(test_time_s)

    setup_time = total_time - test_time

    # write the log line out
    if not os.path.exists(output_csv_log):
        with open(output_csv_log, 'a') as f:
            f.write('Time,Test Name,Setup Time,Run Time\n')
    with open(output_csv_log, 'a') as f:
        f.write(f'{timestamp},{test_name},{setup_time},{test_time}\n')

def monitor_test_performance(output_csv_log: str) -> None:
    '''
    Run the test and log the results to an output file
    '''
    while True:
        run_and_log_test(output_csv_log)
        time.sleep(60*60)

if __name__ == '__main__':
    assert len(sys.argv) == 2
    monitor_test_performance(sys.argv[1])