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
        size   = 0
        blocks = []

        # fill
        for _ in range (1 << 16):
            order = random.randint (1, 10)
            size += 1 << order
            blocks.append (alloc.AllocByOrder (order))
        self.assertEqual (len (set (offset for offset, order in blocks)), len (blocks))
        self.assertEqual (alloc.Size, size)

        # remove half
        for offset, order in blocks [1 << 15:]:
            size -= 1 << order
            alloc.Free (offset, order)
        blocks = blocks [:1 << 15]
        self.assertEqual (len (set (offset for offset, order in blocks)), len (blocks))
        self.assertEqual (alloc.Size, size)

        # add more
        for _ in range (1 << 15):
            order = random.randint (1, 10)
            size += 1 << order
            blocks.append (alloc.AllocByOrder (order))
        self.assertEqual (len (set (offset for offset, order in blocks)), len (blocks))
        self.assertEqual (alloc.Size, size)

        # remove some
        for offset, order in blocks [1 << 14:]:
            size -= 1 << order
            alloc.Free (offset, order)
        blocks = blocks [:1 << 14]
        self.assertEqual (len (set (offset for offset, order in blocks)), len (blocks))
        self.assertEqual (alloc.Size, size)

        # remove all
        for offset, order in blocks:
            size -= 1 << order
            alloc.Free (offset, order)
        self.assertEqual (size, 0)
        self.assertEqual (alloc.Size, 0)

# vim: nu ft=python columns=120 :
