# -*- coding: utf-8 -*-
import random
import unittest

from ..store.alloc import StoreBlock ,StoreAllocator

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

        def reload ():
            alloc.blocks = [StoreBlock.FromDesc (block.ToDesc ()) for block in alloc.blocks]

        # fill
        for _ in range (1 << 16):
            order = random.randint (1, 10)
            size += 1 << order
            blocks.append (alloc.AllocByOrder (order))
        self.assertEqual (len (set (block.offset for block in blocks)), len (blocks))
        self.assertEqual (alloc.Size, size)
        reload ()

        # remove half
        for block in blocks [1 << 15:]:
            size -= block.size
            alloc.Free (block)
        blocks = blocks [:1 << 15]
        self.assertEqual (len (set (block.offset for block in blocks)), len (blocks))
        self.assertEqual (alloc.Size, size)
        reload ()

        # add more
        for _ in range (1 << 15):
            order = random.randint (1, 10)
            size += 1 << order
            blocks.append (alloc.AllocByOrder (order))
        self.assertEqual (len (set (block.offset for block in blocks)), len (blocks))
        self.assertEqual (alloc.Size, size)
        reload ()

        # remove some
        for block in blocks [1 << 14:]:
            size -= block.size
            alloc.Free (block)
        blocks = blocks [:1 << 14]
        self.assertEqual (len (set (block.offset for block in blocks)), len (blocks))
        self.assertEqual (alloc.Size, size)
        reload ()

        # remove all
        for block in blocks:
            size -= block.size
            alloc.Free (block)
        self.assertEqual (size, 0)
        self.assertEqual (alloc.Size, 0)
        reload ()

# vim: nu ft=python columns=120 :
