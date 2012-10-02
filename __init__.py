# -*- coding: utf-8 -*-
from . import store, map

from .store import *
from .map import *

__all__ = store.__all__ + map.__all__
#------------------------------------------------------------------------------#
# Load Test Protocol                                                           #
#------------------------------------------------------------------------------#
def load_tests (loader, tests, pattern):
    """Load test protocol
    """
    from unittest import TestSuite
    from . import tests

    suite = TestSuite ()
    for test in (tests,):
        suite.addTests (loader.loadTestsFromModule (test))

    return suite

# vim: nu ft=python columns=120 :
