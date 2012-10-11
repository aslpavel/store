# -*- coding: utf-8 -*-
import io
import sys

from bisect import bisect, bisect_left
from operator import itemgetter
from collections import MutableMapping

if sys.version_info [0] < 3:
    from itertools import imap as map

__all__ = ('BPTree', 'BPTreeNode', 'BPTreeLeaf')
#------------------------------------------------------------------------------#
# BPTree                                                                       #
#------------------------------------------------------------------------------#
class BPTree (MutableMapping):
    """B+Tree
    """

    class value_nothing (object):
        def __repr__ (self):
            return 'Nothing'
    value_nothing = value_nothing ()

    def __init__ (self, provider):
        self.provider = provider

    def ItemGet (self, key, value = value_nothing):
        """Get value associated with key

        Returns value associated with key if any, otherwise value argument if
        it is set, else raises KeyValue exception.
        """

        desc_node = self.provider.DescToNode

        # find leaf
        node = self.provider.Root ()
        for _ in range (self.provider.Depth () - 1):
            node = desc_node (node.children [bisect (node.keys, key)])

        # find key
        index = bisect_left (node.keys, key)
        if index >= len (node.keys) or key != node.keys [index]:
            if value is self.value_nothing:
                raise KeyError (key)
            return value

        return node.children [index]

    def ItemRange (self, low_key = None, high_key = None):
        """Get range of key-value pairs

        Returns iterator of key-value pairs for keys in [low_key .. high_key]
        """
        # validate range
        if low_key is not None and high_key is not None and low_key >= high_key:
            return

        # find first leaf
        desc_node = self.provider.DescToNode
        node = self.provider.Root ()
        if low_key is not None:
            for depth in range (self.provider.Depth () - 1):
                node = desc_node (node.children [bisect (node.keys, low_key)])
            index = bisect_left (node.keys, low_key)
            if index >= len (node.keys):
                next = desc_node (node.next)
                if next is None:
                    return
                node, index = next, 0
        else:
            for depth in range (self.provider.Depth () - 1):
                node = desc_node (node.children [0])
            index = 0

        # iterate over whole leafs
        while not high_key or node.keys [-1] < high_key:
            for index in range (index, len (node.keys)):
                yield node.keys [index], node.children [index]
            node = desc_node (node.next)
            if node is None:
                return
            index = 0

        # iterate over last leaf
        for index in range (index, len (node.keys)):
            key, value = node.keys [index], node.children [index]
            if key > high_key:
                return
            yield key, value


    def ItemSet (self, key, value):
        """Associate key with value

        If key has already been, replace it with new value.
        """

        order = self.provider.Order ()
        dirty = self.provider.Dirty
        desc_node = self.provider.DescToNode
        node_desc = self.provider.NodeToDesc

        # find path
        node, path = self.provider.Root (), []
        for depth in range (self.provider.Depth () - 1):
            index = bisect (node.keys, key)
            path.append ((index, index + 1, node))
            node = desc_node (node.children [index])

        # check if value is updated
        index = bisect_left (node.keys, key)
        if index < len (node.keys) and key == node.keys [index]:
            node.children [index] = value
            dirty (node)
            return
        path.append ((index, index, node))

        # size += 1
        self.provider.Size (self.provider.Size () + 1)

        # update tree
        sibling = None
        while path:
            key_index, child_index, node = path.pop ()

            # add new key
            node.keys.insert (key_index, key)
            node.children.insert (child_index, value)
            dirty (node)

            if len (node.keys) < order:
                return

            # node is full so we need to split it
            center = len (node.children) >> 1
            keys, node.keys = node.keys [center:], node.keys [:center]
            children, node.children = node.children [center:], node.children [:center]

            if node.is_leaf:
                # create right sibling
                sibling = self.provider.NodeCreate (keys, children, True)

                # keep leafs linked
                sibling_desc, node_next_desc = node_desc (sibling), node.next
                node.next, sibling.prev = sibling_desc, node_desc (node)
                node_next = desc_node (node_next_desc)
                if node_next:
                    node_next.prev, sibling.next = sibling_desc, node_next_desc
                    dirty (node_next)

                # update key
                key, value = sibling.keys [0], sibling_desc

            else:
                # create right sibling
                sibling = self.provider.NodeCreate (keys, children, False)

                # update key
                key, value = node.keys.pop (), node_desc (sibling)

            dirty (sibling)

        # create new root
        self.provider.Depth (self.provider.Depth () + 1) # depth += 1
        self.provider.Root (self.provider.NodeCreate ([key],
            [node_desc (self.provider.Root ()), node_desc (sibling)], False))


    def ItemPop (self, key, value = value_nothing):
        """Pop value associated with key

        Remove value associated with key if any. Returns this value or value
        argument if it is set, else raises KeyValue exception.
        """

        half_order = self.provider.Order () >> 1
        dirty = self.provider.Dirty
        desc_node = self.provider.DescToNode

        # find path
        node, path = self.provider.Root (), []
        for depth in range (self.provider.Depth () - 1):
            index = bisect (node.keys, key)
            parent, node = node, desc_node (node.children [index])
            path.append ((node, index, parent))

        # check if key exists
        index = bisect_left (node.keys, key)
        if index >= len (node.keys) or key != node.keys [index]:
            if value is self.value_nothing:
                raise KeyError (key)
            return value
        value = node.children [index]
        key_index, child_index = index, index

        # size -= 1
        self.provider.Size (self.provider.Size () - 1)

        # update tree
        while path:
            node, node_index, parent = path.pop ()

            # remove scheduled (key | child)
            del node.keys [key_index]
            del node.children [child_index]

            if len (node.keys) >= half_order:
                dirty (node)
                return value

            #------------------------------------------------------------------#
            # Redistribute                                                     #
            #------------------------------------------------------------------#
            left, right = None, None
            if node_index > 0:
                # has left sibling
                left = desc_node (parent.children [node_index - 1])
                if len (left.keys) > half_order: # borrow from left sibling
                    # copy correct key to node
                    node.keys.insert (0, left.keys [-1] if node.is_leaf
                        else parent.keys [node_index - 1])
                    # move left key to parent
                    parent.keys [node_index - 1] = left.keys.pop ()
                    # move left child to node
                    node.children.insert (0, left.children.pop ())

                    dirty (node), dirty (left), dirty (parent)
                    return value

            if node_index < len (parent.keys):
                # has right sibling
                right = desc_node (parent.children [node_index + 1])
                if len (right.keys) > half_order: # borrow from right sibling
                    if node.is_leaf:
                        # move right key to node
                        node.keys.append (right.keys.pop (0))
                        # copy next right key to parent
                        parent.keys [node_index] = right.keys [0]
                    else:
                        # copy correct key to node
                        node.keys.append (parent.keys [node_index])
                        # move right key to parent
                        parent.keys [node_index] = right.keys.pop (0)
                    # move right child to node
                    node.children.append (right.children.pop (0))

                    dirty (node), dirty (right), dirty (parent)
                    return value

            #------------------------------------------------------------------#
            # Merge                                                            #
            #------------------------------------------------------------------#
            src, dst, child_index = ((node, left, node_index) if left
                else (right, node, node_index + 1))

            if node.is_leaf:
                # keep leafs linked
                dst.next = src.next
                src_next = desc_node (src.next)
                if src_next is not None:
                    src_next.prev = src.prev
                    dirty (src_next)
            else:
                # copy parents key
                dst.keys.append (parent.keys [child_index - 1])

            # copy node's (keys | children)
            dst.keys.extend (src.keys)
            dst.children.extend (src.children)

            # mark nodes
            self.provider.Release (src)
            dirty (dst)

            # update key index
            key_index = child_index - 1

        #----------------------------------------------------------------------#
        # Update Root                                                          #
        #----------------------------------------------------------------------#
        root = self.provider.Root ()
        del root.keys [key_index]
        del root.children [child_index]

        if not root.keys:
            depth = self.provider.Depth ()
            if depth > 1:
                # root is not leaf because depth > 1
                self.provider.Root (desc_node (*root.children))
                self.provider.Release (root)
                self.provider.Depth (depth - 1) # depth -= 1
        else:
            dirty (root)

        return value

    #--------------------------------------------------------------------------#
    # Mutable Mapping Interface                                                #
    #--------------------------------------------------------------------------#

    def __len__ (self):
        return self.provider.Size ()

    def __getitem__ (self, key):
        if isinstance (key, slice):
            return self.ItemRange (low_key = key.start, high_key = key.stop)
        return self.ItemGet (key)

    def __setitem__ (self, key, value):
        return self.ItemSet (key, value)

    def __delitem__ (self, key):
        return self.ItemPop (key)

    def __iter__ (self):
        return map (itemgetter (0), self.ItemRange ())

    def __contains__ (self, key):
        return self.ItemGet (key, None) is not None

    def get (self, key, default = None):
        return self.ItemGet (key, default)

    def pop (self, key, default = None):
        return self.ItemPop (key, default)

    def items (self):
        return self.ItemRange ()

    def values (self):
        return map (itemgetter (1), self.ItemRange ())

#------------------------------------------------------------------------------#
# B+Tree Node                                                                  #
#------------------------------------------------------------------------------#
class BPTreeNode (object):
    """B+Tree Node
    """
    __slots__ = ('keys', 'children', 'is_leaf')

    def __init__ (self, keys, children):
        self.keys = keys
        self.children = children
        self.is_leaf = False

class BPTreeLeaf (BPTreeNode):
    """B+Tree Node
    """
    __slots__ = ('keys', 'children', 'prev', 'next', 'is_leaf')

    def __init__ (self, keys, children):
        self.keys = keys
        self.children = children
        self.prev = None
        self.next = None
        self.is_leaf = True

# vim: nu ft=python columns=120 :