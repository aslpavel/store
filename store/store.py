# -*- coding: utf-8 -*-
import io
import struct

from .alloc import StoreBlock, StoreAllocator
from ..serialize import Serializer

__all__ = ('Store',)
#------------------------------------------------------------------------------#
# Store                                                                        #
#------------------------------------------------------------------------------#
class Store (object):
    """Store data inside some flat store (file, memory)

    Data can be named and unnamed. Named data addressed by its name.
    Unnamed data addressed by its descriptor which can change if data changed.
    Descriptor is an unsigned 64-bit integer.
    """

    header_struct = struct.Struct ('>QQ')
    desc_struct = struct.Struct ('>Q')

    def __init__ (self, offset = None):
        offset = offset or 0

        self.offset = offset + self.header_struct.size
        self.disposables = []

        header = self.LoadByOffset (offset, self.header_struct.size)
        self.alloc_desc, self.names_desc = self.header_struct.unpack (header) if header else (0, 0)

        # allocator
        if self.alloc_desc:
            self.alloc = StoreAllocator.FromStream (io.BytesIO (self.Load (self.alloc_desc)))
        else:
            self.alloc = StoreAllocator ()

        # names
        if self.names_desc:
            serialzer = Serializer (io.BytesIO (self.Load (self.names_desc)))
            self.names = dict (zip (
                serialzer.BytesListRead (),                   # names
                serialzer.StructListRead (self.desc_struct))) # descriptors
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

        block = StoreBlock.FromDesc (desc)
        return self.LoadByOffset (self.offset + block.offset, block.used)

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

        block = self.ReserveBlock (len (data), desc)
        self.SaveByOffset (self.offset + block.offset, data)
        return block.ToDesc ()

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

        self.alloc.Free (StoreBlock.FromDesc (desc))

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

        Returns store's block descriptor.
        """
        return self.ReserveBlock (size, desc).ToDesc ()

    def ReserveBlock (self, size, desc = None):
        """Reserve space without actually writing anything in it

        Return store block.
        """
        if desc:
            block = StoreBlock.FromDesc (desc)
            if block.size >= size:
                block.used = size
                return block
            self.alloc.Free (block)

        block = self.alloc.Alloc (size)
        block.used = size
        return block

    #--------------------------------------------------------------------------#
    # Flush                                                                    #
    #--------------------------------------------------------------------------#
    def Flush (self):
        """Flush current state
        """
        # names
        if self.names:
            serializer = Serializer (io.BytesIO ())
            serializer.BytesListWrite (tuple (self.names.keys ()))
            serializer.StructListWrite (tuple (self.names.values ()), self.desc_struct)
            self.names_desc = self.Save (serializer.Stream.getvalue (), self.names_desc)

        else:
            self.Delete (self.names_desc)
            self.names_desc = 0

        # Check if nothing is allocated of the only thing allocated is
        # allocator itself.
        if self.alloc.Size - (StoreBlock.FromDesc (self.alloc_desc).size if self.alloc_desc else 0):
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
        self.SaveByOffset (self.offset - self.header_struct.size,
            self.header_struct.pack (self.alloc_desc, self.names_desc))

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
            size += StoreBlock.FromDesc (self.alloc_desc).size

        # names
        if self.names_desc:
            size += StoreBlock.FromDesc (self.names_desc).size

        for desc in self.names.values ():
            size += StoreBlock.FromDesc (desc).size

        return self.alloc.Size - size

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
