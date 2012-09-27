# -*- coding: utf-8 -*-
import operator
import functools
from bisect import bisect_left

from ..list import StructList

__all__ = ('StoreAllocator',)
#------------------------------------------------------------------------------#
# Store Allocator                                                              #
#------------------------------------------------------------------------------#
class StoreAllocator (object):
    """Store Allocator

    Buddy allocator used by Store
    """

    def __init__ (self, mapping = None):
        self.mapping = mapping

    def Alloc (self, size):
        """Allocate block

        Allocate block at least as big as size
        """
        return self.AllocByOrder ((size - 1).bit_length ())

    def AllocByOrder (self, order):
        """Allocate block by order

        Allocate block of 2^order size
        """
        if self.mapping is None:
            # first allocation
            self.mapping = [[] for _ in range (order)]
            self.mapping.append ([1 << order])

            return 0, order

        elif len (self.mapping) <= order:
            # highest order allocation
            self.mapping.extend ([1 << mapping_order] for mapping_order in range (len (self.mapping), order))
            self.mapping.append ([])

            return 1 << order, order

        # find sutable block
        block = None
        for block_order in range (order, len (self.mapping)):
            mapping = self.mapping [block_order]
            if mapping:
                block = mapping.pop (0)
                break

        if block is None:
            self.mapping.append ([])

            block_order += 1
            block = 1 << block_order

        # split until best-fit is found
        for mapping_order in range (order, block_order):
            self.mapping [mapping_order].append (block + (1 << mapping_order))

        return block, order


    def Free (self, offset, order):
        """Free block with given offset and order
        """
        for order in range (order, len (self.mapping)):
            buddy_offset = offset ^ (1 << order)
            mapping = self.mapping [order]

            # check if buddy is free
            buddy_index = bisect_left (mapping, buddy_offset)
            if not mapping or buddy_index >= len (mapping) or buddy_offset != mapping [buddy_index]:
                mapping.insert (buddy_index, offset)
                return

            # merge with buddy
            mapping.pop (buddy_index)
            if offset & (1 << order):
                offset = buddy_offset

        # allocator is free
        self.mapping = None

    @property
    def Used (self):
        """Total size of allocated space
        """
        if self.mapping is None:
            return 0

        return (1 << len (self.mapping)) - functools.reduce (operator.add,
            (len (mapping) * (1 << order) for order, mapping in enumerate (self.mapping)))

    #--------------------------------------------------------------------------#
    # Serialization                                                            #
    #--------------------------------------------------------------------------#
    def ToStream (self, stream):
        """Save allocator to stream
        """
        packed = StructList ('>Q')
        for mapping in self.mapping:
            packed.append (len (mapping))
            packed.extend (mapping)
        packed.ToStream (stream)

        return stream

    @classmethod
    def FromStream (cls, stream):
        """Load allocator from stream
        """
        mapping     = []
        packed      = StructList.FromStream ('>Q', stream)
        packed_size = len (packed)

        index = 0
        while index < packed_size:
            size   = packed [index]
            index += 1
            if size:
                mapping.append (list (packed [index:index + size]))
                index += size
            else:
                mapping.append ([])

        return cls (mapping)

# vim: nu ft=python columns=120 :
