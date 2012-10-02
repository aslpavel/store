# -*- coding: utf-8 -*-
from .bptree import BPTree
from .provider.store import StoreBPTreeProvider

__all__ = ('StoreMap',)
#------------------------------------------------------------------------------#
# Store Map                                                                    #
#------------------------------------------------------------------------------#
class StoreMap (BPTree):
    """B+Tree with store as back end
    """
    def __init__ (self, store, name, order = None, key_type = None, value_type = None, compress = None):
        BPTree.__init__ (self, StoreBPTreeProvider (store, name, order, key_type, value_type, compress))

    #--------------------------------------------------------------------------#
    # Properties                                                               #
    #--------------------------------------------------------------------------#
    @property
    def Name (self):
        """Name property
        """
        return self.provider.Name

    @property
    def Store (self):
        """Store property
        """
        return self.provider.Store

    #--------------------------------------------------------------------------#
    # Flush                                                                    #
    #--------------------------------------------------------------------------#
    def Flush (self, prune = None):
        """Flush dirty nodes to store
        """
        self.provider.Flush (prune)

    #--------------------------------------------------------------------------#
    # Drop                                                                     #
    #--------------------------------------------------------------------------#
    def Drop (self):
        """Completely delete mapping from the store
        """
        self.provider.Drop ()

    #--------------------------------------------------------------------------#
    # Disposable                                                               #
    #--------------------------------------------------------------------------#
    def Dispose (self):
        """Flush dirty nodes to store
        """
        self.Flush ()

    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        self.Dispose ()
        return False

# vim: nu ft=python columns=120 :
