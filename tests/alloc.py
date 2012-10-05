# -*- coding: utf-8 -*-
import random
import unittest

from ..store.alloc import StoreAllocator

__all__ = ('StoreAllocatorTest',)
#------------------------------------------------------------------------------#
# Store Allocator Test                                                         #
#------------------------------------------------------------------------------#
class StoreAllocatorTest (unittest.TestCase):
    """Store allocator unit tests
    """

    def testStress (self):
        """Stress test for store allocator
        """
        alloc  = StoreAllocator ()
        blocks = []
        self.assertEqual (len (set (offset for offset, order in blocks)), len (blocks))

        for offset, order in blocks [1 << 15:]:
            alloc.Free (offset, order)
        blocks = blocks [:1 << 15]
        self.assertEqual (len (set (offset for offset, order in blocks)), len (blocks))

        blocks.extend (alloc.AllocByOrder (random.randint (1, 10)) for _ in range (1 << 16))
        self.assertEqual (len (set (offset for offset, order in blocks)), len (blocks))

        for offset, order in blocks:
            alloc.Free (offset, order)
        self.assertEqual (alloc.Size, 0)

# vim: nu ft=python columns=120 :
