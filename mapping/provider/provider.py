# -*- coding: utf-8 -*-

__all__ = ('BPTreeProvider',)
#------------------------------------------------------------------------------#
# B+Tree Provider                                                              #
#------------------------------------------------------------------------------#
class BPTreeProvider (object):
    """BPTree Provider Interface
    """
    #--------------------------------------------------------------------------#
    # Properties                                                               #
    #--------------------------------------------------------------------------#
    def Size (self, value = None):
        """Count of key-value pairs inside B+Tree
        """
        raise NotImplementedError ()

    def Depth (self, value = None):
        """Depth of B+Tree
        """
        raise NotImplementedError ()

    def Root (self, value = None):
        """Root node of B+Tree
        """
        raise NotImplementedError ()

    def Order (self):
        """Order of B+Tree Node
        """
        raise NotImplementedError ()

    #--------------------------------------------------------------------------#
    # Nodes                                                                    #
    #--------------------------------------------------------------------------#
    def NodeToDesc (self, node):
        """Get nodes descriptor
        """
        raise NotImplementedError ()

    def DescToNode (self, desc):
        """Get node by its descriptor
        """
        raise NotImplementedError ()

    def NodeCreate (self, keys, children, is_leaf):
        """Create new node
        """
        raise NotImplementedError ()

    def Dirty (self, node):
        """Mark node as dirty
        """
        raise NotImplementedError ()

    def Release (self, node):
        """Release node
        """
        raise NotImplementedError ()

    #--------------------------------------------------------------------------#
    # Dispose                                                                  #
    #--------------------------------------------------------------------------#
    def Flush (self):
        """Flush provider
        """
        pass

    def Dispose (self):
        """Dispose provider
        """
        self.Flush ()

    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        self.Dispose ()
        return False

    #--------------------------------------------------------------------------#
    # Iterator                                                                 #
    #--------------------------------------------------------------------------#
    def __iter__ (self):
        """Iterate over all nodes
        """
        stack = [self.Root ()]
        while stack:
            node = stack.pop ()

            yield node

            if not node.is_leaf:
                for desc in node.children:
                    stack.append (self.DescToNode (desc))

# vim: nu ft=python columns=120 :