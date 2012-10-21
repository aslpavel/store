# -*- coding: utf-8 -*-
import io
import struct

from .alloc import StoreAllocator
from ..serialize import BytesSerializer, StructSerializer

__all__ = ('Store',)
#------------------------------------------------------------------------------#
# Store                                                                        #
#------------------------------------------------------------------------------#
class Store (object):
    """Store data inside some flat store (file, memory)

    Data can be named and unnamed. Named data addressed by its name.
    Unnamed data addressed by its descriptor which can change if data changed.

    Descriptor format:

        0       8             order           64
        +-------+---------------+-------------+
        | order | data size - 1 | data offset |
        +-------+---------------+-------------+

        Because data offset is always aligned by its order, first order bits
        of the offset can be used to store actual data size.
    """

    header      = struct.Struct ('>QQ')
    header_size = header.size
    desc_format = '>Q'

    def __init__ (self, offset = None):
        self.offset = offset or 0
        self.disposables = []

        header = self.LoadByOffset (self.offset, self.header_size)
        self.alloc_desc, self.names_desc = self.header.unpack (header) if header else (0, 0)

        # allocator
        if self.alloc_desc:
            self.alloc = StoreAllocator.FromStream (io.BytesIO (self.Load (self.alloc_desc)))
        else:
            self.alloc = StoreAllocator ()

        # names
        if self.names_desc:
            stream = io.BytesIO (self.Load (self.names_desc))
            self.names = dict (zip (
                BytesSerializer.FromStream (stream),                      # names
                StructSerializer.FromStream (stream, self.desc_format)))  # descs
        else:
            self.names = {}

    #--------------------------------------------------------------------------#
    # Load                                                                     #
    #--------------------------------------------------------------------------#
    def Load (self, desc):
        """Load data by descriptor
        """
        if not desc:
            return b''

        offset, size, order = self.desc_unpack (desc)
        return self.LoadByOffset (self.offset + self.header_size + offset, size)

    def LoadByName (self, name):
        """Load data by name
        """
        return self.Load (self.names.get (name, 0))

    def __getitem__ (self, name):
        """Load data by name
        """
        return self.LoadByName (name)

    def LoadByOffset (self, offset, size):
        """Load data by offset and size
        """
        raise NotImplementedError ()

    #--------------------------------------------------------------------------#
    # Save                                                                     #
    #--------------------------------------------------------------------------#
    def Save (self, data, desc = None):
        """Save data by descriptor

        Try to save data inside space pointed by descriptor and
        if its not enough allocate new space. Returns descriptor of saved data
        """
        if not data:
            return 0

        desc = self.Reserve (len (data), desc)
        self.SaveByOffset (self.offset + self.header_size + self.desc_unpack (desc) [0], data)
        return desc

    def SaveByName (self, name, data):
        """Save data by name
        """
        if not data:
            self.DeleteByName (name)
        else:
            self.names [name] = self.Save (data, self.names.get (name))

    def __setitem__ (self, name, data):
        """Save data by name
        """
        self.SaveByName (name, data)

    def SaveByOffset (self, offset, data):
        """Save data by offset
        """
        raise NotImplementedError ()

    #--------------------------------------------------------------------------#
    # Delete                                                                   #
    #--------------------------------------------------------------------------#
    def Delete (self, desc):
        """Delete data by descriptor

        Free space occupied by data pointed by descriptor
        """
        if not desc:
            return

        offset, size, order = self.desc_unpack (desc)
        self.alloc.Free (offset, order)

    def DeleteByName (self, name):
        """Delete data by name
        """
        desc = self.names.pop (name, None)
        if desc:
            self.Delete (desc)

    def __delitem__ (self, name):
        """Delete data by name
        """
        self.DeleteByName (name)

    #--------------------------------------------------------------------------#
    # Reserve                                                                  #
    #--------------------------------------------------------------------------#
    def Reserve (self, size, desc = None):
        """Reserve space without actually writing anything in it
        """
        if desc:
            desc_offset, desc_size, desc_order = self.desc_unpack (desc)
            if size <= 1 << desc_order:
                return self.desc_pack (desc_offset, size, desc_order)
            self.alloc.Free (desc_offset, desc_order)

        offset, order = self.alloc.Alloc (size)
        return self.desc_pack (offset, size, order)

    #--------------------------------------------------------------------------#
    # Flush                                                                    #
    #--------------------------------------------------------------------------#
    def Flush (self):
        """Flush current state
        """
        # names
        if self.names:
            names = tuple (self.names.items ())
            stream = io.BytesIO ()
            BytesSerializer.ToStream (stream, (name for name, desc in names))
            StructSerializer.ToStream (stream, self.desc_format, (desc for name, desc in names))
            self.names_desc = self.Save (stream.getvalue (), self.names_desc)

        else:
            self.Delete (self.names_desc)
            self.names_desc = 0

        # allocator
        if (self.alloc_desc or self.alloc.Size and
            self.alloc.Size != 1 << self.desc_unpack (self.alloc_desc) [2]):
            while True:
                alloc_state = self.alloc.ToStream (io.BytesIO ()).getvalue ()
                self.alloc_desc, alloc_desc = self.Save (alloc_state, self.alloc_desc), self.alloc_desc
                if self.alloc_desc == alloc_desc:
                    break
        else:
            alloc_desc, self.alloc_desc = self.alloc_desc, 0
            self.Delete (alloc_desc)
            assert not self.alloc.Size, 'Allocator is broken'

        # header
        self.SaveByOffset (self.offset, self.header.pack (self.alloc_desc, self.names_desc))

    #--------------------------------------------------------------------------#
    # Mapping                                                                  #
    #--------------------------------------------------------------------------#
    def Mapping (self, name, order = None, key_type = None, value_type = None, compress = None):
        """Create name mapping (B+Tree)
        """
        from ..mapping import StoreMapping

        mapping = StoreMapping (self, name, order, key_type, value_type, compress)
        self.disposables.append (mapping)
        return mapping

    #--------------------------------------------------------------------------#
    # Size                                                                     #
    #--------------------------------------------------------------------------#
    @property
    def Size (self):
        """Total space used excluding internal storage data
        """
        size = 0

        # allocator
        if self.alloc_desc:
            size += 1 << self.desc_unpack (self.alloc_desc) [2]

        # names
        if self.names_desc:
            size += 1 << self.desc_unpack (self.names_desc) [2]

        for desc in self.names.values ():
            size += 1 << self.desc_unpack (desc) [2]

        return self.alloc.Size - size

    #--------------------------------------------------------------------------#
    # Private                                                                  #
    #--------------------------------------------------------------------------#
    def desc_pack (self, offset, size, order):
        """Pack descriptor
        """
        assert not (offset & size - 1), 'Offset is not aligned'
        return order | (offset | size - 1) << 8

    def desc_unpack (self, desc):
        """Unpack descriptor
        """
        order      = desc &  0xff
        value      = desc >> 8
        value_mask = (1 << order) - 1
        return value & ~value_mask, (value & value_mask) + 1, order

    #--------------------------------------------------------------------------#
    # Disposable                                                               #
    #--------------------------------------------------------------------------#
    def Dispose (self):
        """Flush and Close
        """

        disposables, self.disposables = self.disposables, []
        for disposable in reversed (disposables):
            disposable.Dispose ()

        self.Flush ()

    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        self.Dispose ()
        return False

# vim: nu ft=python columns=120 :
