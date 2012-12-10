# -*- coding: utf-8 -*-
import io
import sys
import json
import zlib
import struct
import codecs
import binascii
import operator
import functools
from bisect import bisect

if sys.version_info [0] > 2:
    import pickle
else:
    import cPickle as pickle

from .provider import BPTreeProvider
from ..bptree import BPTreeNode, BPTreeLeaf
from ...serialize import Serializer
from ...store.alloc import StoreBlock


__all__ = ('StoreBPTreeProvider',)
#------------------------------------------------------------------------------#
# Store B+Tree Provider                                                        #
#------------------------------------------------------------------------------#
class StoreBPTreeProvider (BPTreeProvider):
    """Store based B+Tree provider

    Keeps serialized (possible compressed) nodes inside store. Keys and values
    are serialized according to specified type. Possible values for type are
    'bytes', 'pickle:protocol', 'struct:struct_type', 'json'.
    """

    order_default    = 128
    type_default     = 'pickle:{}'.format (pickle.HIGHEST_PROTOCOL)
    compress_default = 9
    desc_struct      = struct.Struct ('>Q')
    crc32_struct     = struct.Struct ('>I')
    leaf_struct      = struct.Struct ('>QQ')

    def __init__ (self, store, name, order = None, key_type = None, value_type = None, compress = None):
        """Create provider

        Creates new provider or loads existing one identified by name. Compress
        argument specifies compression level default is 9 (maximum compression).
        If key_type (value_type) is not specified they are set to 'pickle'
        """
        self.store = store
        self.name = name

        self.d2n = {}
        self.desc_next = -1
        self.dirty = set ()

        # unpack state
        state_data = self.store.LoadByName (name)
        if state_data:
            state_json = state_data [:-self.crc32_struct.size]
            crc32 = self.crc32_struct.unpack (state_data [-self.crc32_struct.size:]) [0]

            if crc32 != binascii.crc32 (state_json) & 0xffffffff:
                raise ValueError ('Header checksum failed')

            state = json.loads (state_json.decode ())

            # properties
            self.size  = state ['size']
            self.depth = state ['depth']
            self.order = state ['order']

            # parse type
            self.key_type, self.keys_to_stream, self.keys_from_stream = \
                self.type_parse (state ['key_type'])

            self.value_type, self.values_to_stream, self.values_from_stream = \
                self.type_parse (state ['value_type'])

            # options
            self.compress = state.get ('compress', 0)

            # root
            self.root = self.node_load (state ['root'])

        else:
            # properties
            self.size = 0
            self.depth = 1
            self.order = order or self.order_default

            # parse type
            self.key_type, self.keys_to_stream, self.keys_from_stream = \
                self.type_parse (key_type or self.type_default)

            self.value_type, self.values_to_stream, self.values_from_stream = \
                self.type_parse (value_type or self.type_default)

            # options
            self.compress = compress if compress is not None else self.compress_default

            # root
            self.root = self.NodeCreate ([], [], True)

    #--------------------------------------------------------------------------#
    # Properties                                                               #
    #--------------------------------------------------------------------------#
    @property
    def Name (self):
        """Name property
        """
        return self.name

    @property
    def Store (self):
        """Store property
        """
        return self.store

    #--------------------------------------------------------------------------#
    # Flush                                                                    #
    #--------------------------------------------------------------------------#
    def Flush (self, prune = None):
        """Flush provider and store
        """
        # relocated nodes
        d2n_reloc = {}

        #----------------------------------------------------------------------#
        # Flush Leafs                                                          #
        #----------------------------------------------------------------------#
        leaf_queue = {}
        def leaf_enqueue (leaf):
            leaf_stream = io.BytesIO ()

            # save leaf
            leaf_stream.seek (self.leaf_struct.size)
            if self.compress:
                with CompressorStream (leaf_stream, self.compress) as stream:
                    self.keys_to_stream (stream, leaf.keys)
                    self.values_to_stream (stream, leaf.children)
            else:
                self.keys_to_stream (leaf_stream, leaf.keys)
                self.values_to_stream (leaf_stream, leaf.children)

            # enqueue leaf
            leaf_queue [leaf] = leaf_stream

            # allocate space
            desc = self.store.Reserve (leaf_stream.tell () + 1, None if leaf.desc < 0 else leaf.desc)
            if leaf.desc != desc:
                # queue parent for update
                if leaf is not self.root:
                    parent, key = self.root, leaf.keys [0]
                    while True:
                        parent_desc = parent.children [bisect (parent.keys, key)]
                        if parent_desc == leaf.desc:
                            break
                        parent = self.DescToNode (parent_desc)
                    if parent not in self.dirty:
                        node_queue.add (parent)

                # queue next and previous for update
                for sibling_desc in (leaf.prev, leaf.next):
                    # descriptor is negative, node is dirty
                    if (sibling_desc > 0 and                # negative node is dirty for sure
                        sibling_desc not in d2n_reloc):     # relocated node is also dirty

                        sibling = self.d2n.get (sibling_desc)
                        if sibling:
                            # node has already been loaded
                            if (sibling not in self.dirty and
                                sibling not in leaf_queue):
                                    # queue it for update
                                    leaf_enqueue (sibling)
                        else:
                            # node hasn't been loaded
                            leaf_enqueue (self.node_load (sibling_desc))

                # update descriptor maps
                self.d2n.pop (leaf.desc)
                d2n_reloc [leaf.desc], leaf.desc = leaf, desc
                self.d2n [desc] = leaf

        # enqueue leafs and create dirty nodes queue
        node_queue = set ()
        for node in self.dirty:
            if node.is_leaf:
                leaf_enqueue (node)
            else:
                node_queue.add (node)

        # all leafs has been allocated now
        for leaf, leaf_stream in leaf_queue.items ():
            # update previous
            prev = d2n_reloc.get (leaf.prev)
            if prev is not None:
                leaf.prev = prev.desc
            # update next
            next = d2n_reloc.get (leaf.next)
            if next is not None:
                leaf.next = next.desc

            # leaf header (perv, next)
            leaf_stream.seek (0)
            leaf_stream.write (self.leaf_struct.pack (leaf.prev, leaf.next))

            # leaf tag
            leaf_stream.seek (0, io.SEEK_END)
            leaf_stream.write (b'\x01')

            # put leaf in store
            desc = self.store.Save (leaf_stream.getvalue (), leaf.desc)
            assert leaf.desc == desc

        #----------------------------------------------------------------------#
        # Flush Nodes                                                          #
        #----------------------------------------------------------------------#
        def node_flush (node):
            # flush children
            for index in range (len (node.children)):
                child_desc = node.children [index]
                child = d2n_reloc.get (child_desc)
                if child is not None:
                    # child has already been flushed
                    node.children [index] = child.desc
                else:
                    child = self.d2n.get (child_desc)
                    if child in node_queue:
                        # flush child and update index
                        node.children [index] = node_flush (child)

            # node
            node_stream = io.BytesIO ()
            if self.compress:
                with CompressorStream (node_stream, self.compress) as stream:
                    self.keys_to_stream (stream, node.keys)
                    Serializer (stream).StructTupleWrite (node.children, self.desc_struct)
            else:
                self.keys_to_stream (node_stream, node.keys)
                Serializer (node_stream).StructTupleWrite (node.children, self.desc_struct)

            # node tag
            node_stream.write (b'\x00')

            # put node in store
            desc = self.store.Save (node_stream.getvalue (), None if node.desc < 0 else node.desc)

            # check if node has been relocated
            if node.desc != desc:
                # queue parent for update
                if node is not self.root:
                    parent, key = self.root, node.keys [0]
                    while True:
                        parent_desc = parent.children [bisect (parent.keys, key)]
                        if parent_desc == node.desc:
                            break
                        parent = self.d2n [parent_desc]
                    if parent not in self.dirty:
                        node_queue.add (parent)

                # update descriptor maps
                self.d2n.pop (node.desc)
                d2n_reloc [node.desc], node.desc = node, desc
                self.d2n [desc] = node

            # remove node from dirty set
            node_queue.discard (node)

            return desc

        while node_queue:
            node_flush (node_queue.pop ())

        # clear dirty
        self.dirty.clear ()
        if prune:
            # release all nodes except root
            self.d2n.clear ()
            self.d2n [self.root.desc] = self.root

        #----------------------------------------------------------------------#
        # Flush State                                                          #
        #----------------------------------------------------------------------#
        state = {
            'size'       : self.size,
            'depth'      : self.depth,
            'order'      : self.order,
            'key_type'   : self.key_type,
            'value_type' : self.value_type,
            'compress'   : self.compress,
            'root'       : self.root.desc
        }
        state_json = json.dumps (state, sort_keys = True).encode ()
        crc32 = binascii.crc32 (state_json) & 0xffffffff

        state_data = state_json + self.crc32_struct.pack (crc32)
        if self.store.LoadByName (self.name) != state_data:
            self.store.SaveByName (self.name, state_data)

    #--------------------------------------------------------------------------#
    # Properties                                                               #
    #--------------------------------------------------------------------------#

    def Size (self, value = None):
        self.size = self.size if value is None else value
        return self.size

    def Depth (self, value = None):
        self.depth = self.depth if value is None else value
        return self.depth

    def Order (self):
        return self.order

    def Root (self, value = None):
        self.root = self.root if value is None else value
        return self.root

    def SizeOnStore (self):
        """Size occupied on store
        """
        return functools.reduce (operator.add, (StoreBlock.FromDesc (node.desc).size
            for node in self if node.desc > 0), 0)

    #--------------------------------------------------------------------------#
    # Nodes                                                                    #
    #--------------------------------------------------------------------------#

    def NodeToDesc (self, node):
        return node.desc

    def DescToNode (self, desc):
        if desc:
            return self.d2n.get (desc) or self.node_load (desc)

    def NodeCreate (self, keys, children, is_leaf):
        desc, self.desc_next = self.desc_next, self.desc_next - 1
        node = StoreBPTreeLeaf (desc, keys, children) if is_leaf else \
               StoreBPTreeNode (desc, keys, children)

        self.d2n [desc] = node
        self.dirty.add (node)
        return node

    def Dirty (self, node):
        self.dirty.add (node)

    def Release (self, node):
        self.d2n.pop (node.desc)
        self.dirty.discard (node)
        if node.desc >= 0:
            self.store.Delete (node.desc)

    #--------------------------------------------------------------------------#
    # Drop                                                                     #
    #--------------------------------------------------------------------------#
    def Drop (self):
        """Completely delete provider from the store
        """
        self.Flush ()

        for node in tuple (self):
            self.store.Delete (node.desc)
        self.store.DeleteByName (self.name)

        self.size  = 0
        self.depth = 1
        self.root  = self.NodeCreate ([], [], True)

    #--------------------------------------------------------------------------#
    # Private                                                                  #
    #--------------------------------------------------------------------------#
    def node_load (self, desc):
        """Load node by its descriptor
        """
        node_data   = self.store.Load (desc)
        node_tag    = node_data [-1:]

        if node_tag != b'\x01':
            # load node
            node_stream = io.BytesIO (node_data [:-1]) if not self.compress else \
                          io.BytesIO (zlib.decompress (node_data [:-1]))

            node =  StoreBPTreeNode (desc,
                self.keys_from_stream (node_stream),
                list (Serializer (node_stream).StructTupleRead (self.desc_struct)))
        else:
            # load leaf
            prev, next = self.leaf_struct.unpack (node_data [:self.leaf_struct.size])

            node_stream = io.BytesIO (node_data [self.leaf_struct.size:-1]) if not self.compress else \
                          io.BytesIO (zlib.decompress (node_data [self.leaf_struct.size:-1]))

            node = StoreBPTreeLeaf (desc,
                self.keys_from_stream (node_stream),
                self.values_from_stream (node_stream))
            node.prev = prev
            node.next = next

        self.d2n [desc] = node
        return node

    def type_parse (self, type):
        """Parse type

        Returns (type, to_stream, from_steram) for specified type.
        """
        if type == 'bytes':
            return ('bytes',
                lambda stream, items: Serializer (stream).BytesTupleWrite (items),
                lambda stream: Serializer (stream).BytesTupleRead ())

        elif type.startswith ('pickle'):
            protocol = int (type.partition (':') [-1] or str (pickle.HIGHEST_PROTOCOL))
            return ('pickle:{}'.format (protocol),
                lambda stream, items: pickle.dump (items, stream, protocol),
                lambda stream: pickle.load (stream))

        elif type.startswith ('struct:'):
            format = type.partition (':') [-1].encode ()
            item_struct = struct.Struct (format)

            # determine if structure is complex (more then one value)
            item_complex = len (format.translate (None, b'<>=!@')) > 1

            return ('struct:{}'.format (format.decode ()),
                lambda stream, items: Serializer (stream).StructTupleWrite (items, item_struct, item_complex),
                lambda stream: Serializer (stream).StructTupleRead (item_struct, item_complex))

        elif type == 'json':
            encode = codecs.getencoder ('utf-8')
            decode = codecs.getdecoder ('utf-8')

            header = struct.Struct ('>Q')
            header_size = header.size

            def json_save (stream, items):
                data = encode (json.dumps (items)) [0]
                stream.write (header.pack (len (data)))
                stream.write (data)

            def json_load (stream):
                data_size = header.unpack (stream.read (header_size)) [0]
                return json.loads (decode (stream.read (data_size)) [0])

            return ('json', json_save, json_load)

        raise ValueError ('Unknown serializer type: {}'.format (type))

#------------------------------------------------------------------------------#
# Store B+Tree Node                                                            #
#------------------------------------------------------------------------------#
class StoreBPTreeNode (BPTreeNode):
    """Store B+Tree Node
    """
    __slots__ = BPTreeNode.__slots__ + ('desc',)

    def __init__ (self, desc, keys, children):
        self.desc = desc
        self.keys = keys
        self.children = children
        self.is_leaf = False

class StoreBPTreeLeaf (BPTreeLeaf):
    """Store B+Tree Leaf
    """
    __slots__ = BPTreeLeaf.__slots__ + ('desc',)

    def __init__ (self, desc, keys, children):
        self.desc = desc
        self.keys = keys
        self.children = children
        self.prev = 0
        self.next = 0
        self.is_leaf = True

#------------------------------------------------------------------------------#
# Compressor Stream                                                            #
#------------------------------------------------------------------------------#
class CompressorStream (object):
    """Compression stream adapter
    """
    __slots__ = ('stream', 'compressor',)

    def __init__ (self, stream, level):
        self.stream = stream
        self.compressor = zlib.compressobj (level)

    def write (self, data):
        return self.stream.write (self.compressor.compress (data))

    def __enter__ (self):
        return self

    def __exit__ (self, et, oe, tb):
        self.stream.write (self.compressor.flush ())
        return False

# vim: nu ft=python columns=120 :
