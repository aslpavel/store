# -*- coding: utf-8 -*-

#------------------------------------------------------------------------------#
# Load Test Protocol                                                           #
#------------------------------------------------------------------------------#
def load_tests (loader, tests, pattern):
    from unittest import TestSuite
    from . import serialize, alloc, store, bptree

    suite = TestSuite ()
    for test in (serialize, alloc, store, bptree):
        suite.addTests (loader.loadTestsFromModule (test))

    return suite

# vim: nu ft=python columns=120 :