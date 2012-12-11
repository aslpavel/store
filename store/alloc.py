# -*- coding: utf-8 -*-
import operator
import functools
import struct
from bisect import bisect_left, insort

from ..serialize import Serializer

__all__ = ('StoreBlock', 'StoreAllocator',)
#------------------------------------------------------------------------------#
# Store Block                                                                  #
#------------------------------------------------------------------------------#
class StoreBlock (object):
    """Store block object

    Descriptor format:

        0       6      7 + order         64
        +-------+------+-----------------+
        | order | used | offset >> order |
        +-------+------+-----------------+

        Because data offset is always aligned by its order, first order bits
        of the offset can be used to store used size.
    """
    __slots__ = ('order', 'offset', 'used',)

    def __init__ (self, order, offset, used = 0):
        self.order = order
        self.offset = offset
        self.used = used

    #--------------------------------------------------------------------------#
    # Compare                                                                  #
    #--------------------------------------------------------------------------#
    def __lt__ (self, other):
        """Less then
        """
        return (self.order, self.offset) < (other.order, other.offset)

    def __eq__ (self, other):
        """Equal to
        """
        return (self.order, self.offset) == (other.order, other.offset)

    def __hash__ (self):
        """Get hash
        """
        return hash ((self.order, self.offset))

    #--------------------------------------------------------------------------#
    # Properties                                                               #
    #--------------------------------------------------------------------------#
    @property
    def size (self):
        """Size of the block
        """
        return 1 << self.order

    #--------------------------------------------------------------------------#
    # Serialization                                                            #
    #--------------------------------------------------------------------------#
    def ToDesc (self):
        """Convert block to integer descriptor
        """
        return self.order | (self.used << 6) | (self.offset << 7)

    @classmethod
    def FromDesc (cls, desc):
        """Restore block from integer descriptor
        """
        order = desc & 0x3f
        value = desc >> 6
        value_mask = (1 << (order + 1)) - 1

        return cls (order, (value & ~value_mask) >> 1, value & value_mask)

    #--------------------------------------------------------------------------#
    # Representation                                                           #
    #--------------------------------------------------------------------------#
    def __str__ (self):
        """String representation
        """
        return '<StoreBLock [order:{} offset:{} used:{}]>'.format (
            self.order, self.offset, self.used)

    def __repr__ (self):
        """String representation
        """
        return str (self)

#------------------------------------------------------------------------------#
# Store Allocator                                                              #
#------------------------------------------------------------------------------#
class StoreAllocator (object):
    """Store buddy allocator

    ``blocks`` is a sorted list of free blocks.
    """
    max_order = 57 # 64 - 6 (used by order) - 1 (used by size)

    def __init__ (self, blocks = None):
        self.blocks = blocks or [StoreBlock (self.max_order, 0)]

    def Alloc (self, size):
        """Allocate block by size

        Block it is an order-offset pair.
        """
        return self.AllocByOrder ((size - 1).bit_length ())

    def AllocByOrder (self, order):
        """Allocate block by order

        Block it is an order-offset pair.
        """
        index = bisect_left (self.blocks, StoreBlock (order, 0))
        if index >= len (self.blocks):
            raise ValueError ('Out of space')

        block = self.blocks.pop (index)
        for block_order in range (order, block.order):
            insort (self.blocks, StoreBlock (block_order, block.offset + (1 << block_order)))

        return StoreBlock (order, block.offset)

    def Free (self, block):
        """Free previously allocated block
        """
        for order in range (block.order, self.max_order):
            buddy = StoreBlock (order, block.offset ^ (1 << order))

            index = bisect_left (self.blocks, buddy)
            if index < len (self.blocks) and buddy == self.blocks [index]:
                self.blocks.pop (index)
                block = StoreBlock (order + 1,
                    buddy.offset if block.offset & (1 << order) else block.offset)

            else:
                self.blocks.insert (index, block)
                return

        assert not self.blocks
        self.blocks.append (block)

    @property
    def Size (self):
        """Size of allocated space
        """
        return (1 << self.max_order) - functools.reduce (operator.add,
                (block.size for block in self.blocks), 0)

    #--------------------------------------------------------------------------#
    # Serialization                                                            #
    #--------------------------------------------------------------------------#
    desc_struct = struct.Struct ('>Q')

    def ToStream (self, stream):
        """Save allocator to stream
        """
        Serializer (stream).StructListWrite (
            [block.ToDesc () for block in self.blocks], self.desc_struct)
        return stream

    @classmethod
    def FromStream (cls, stream):
        """Load allocator from stream
        """
        return cls ([StoreBlock.FromDesc (desc)
            for desc in Serializer (stream).StructListRead (cls.desc_struct)])

# vim: nu ft=python columns=120 :
