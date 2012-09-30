# -*- coding: utf-8 -*-
from .provider import BPTreeProvider
from ..bptree import BPTreeNode, BPTreeLeaf

__all__ = ('MemoryBPTreeProvider',)
#------------------------------------------------------------------------------#
# Memory B+Tree Provider                                                       #
#------------------------------------------------------------------------------#
class MemoryBPTreeProvider (BPTreeProvider):
    """In memory B+Tree Provider

    Primary use is to test BPTree itself
    """
    def __init__ (self, order):
        self.root = self.NodeCreate ([], [], True)
        self.size = 0
        self.depth = 1
        self.order = order

    #--------------------------------------------------------------------------#
    # Properties                                                               #
    #--------------------------------------------------------------------------#

    def Size (self, value = None):
        if value is not None:
            self.size = value
        return self.size

    def Depth (self, value = None):
        if value is not None:
            self.depth = value
        return self.depth

    def Order (self):
        return self.order

    def Root (self, value = None):
        if value is not None:
            self.root = value
        return self.root

    #--------------------------------------------------------------------------#
    # Nodes                                                                    #
    #--------------------------------------------------------------------------#

    def NodeToDesc (self, node):
        return node

    def DescToNode (self, desc):
        return desc

    def NodeCreate (self, keys, children, is_leaf):
        return BPTreeLeaf (keys, children) if is_leaf else \
               BPTreeNode (keys, children)

    def Dirty (self, node):
        pass

    def Release (self, node):
        pass

# vim: nu ft=python columns=120 :