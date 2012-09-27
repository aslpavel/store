# -*- coding: utf-8 -*-
import struct

__all__ = ('StructList', 'BytesList',)
#------------------------------------------------------------------------------#
# Struct List                                                                  #
#------------------------------------------------------------------------------#
class StructList (list):
    """List of structs
    """
    __slots__     = ('fmt',)
    header_struct = struct.Struct ('>Q')

    def __init__ (self, fmt, items = None):
        list.__init__ (self, items or [])

        self.fmt = fmt

    def __getitem__ (self, item):
        return StructList (self.fmt, list.__getitem__ (self, item)) if isinstance (item, slice) \
            else list.__getitem__ (self, item)

    def __repr__ (self): return self.__str__ ()
    def __str__  (self):
        return '<StructList[{}]: [{}]>'.format (self.fmt, ', '.join (str (item) for item in self))

    #--------------------------------------------------------------------------#
    # Serialization                                                            #
    #--------------------------------------------------------------------------#
    def ToStream (self, stream):
        """Save to stream
        """
        write       = stream.write
        item_struct = struct.Struct (self.fmt)

        write (self.header_struct.pack (len (self)))
        if len (self):
            if isinstance (self [0], tuple):
                for item in self:
                    write (item_struct.pack (*item))
            else:
                for item in self:
                    write (item_struct.pack (item))

        return stream

    @classmethod
    def FromStream (cls, fmt, stream):
        """Load from stream
        """
        read = stream.read
        length = cls.header_struct.unpack (read (cls.header_struct.size)) [0]

        instance = cls (fmt)
        item_struct = struct.Struct (fmt)
        item_size = item_struct.size

        if length:
            item = item_struct.unpack (read (item_size))
            if len (item) > 1:
                instance.append (item)
                for _ in range (length - 1):
                    instance.append (item_struct.unpack (read (item_size)))
            else:
                instance.append (item [0])
                for _ in range (length - 1):
                    instance.append (item_struct.unpack (read (item_size)) [0])

        return instance

#------------------------------------------------------------------------------#
# Bytes List                                                                   #
#------------------------------------------------------------------------------#
class BytesList (list):
    """List of bytes (bytearrays)
    """
    __slots__   = tuple ()
    size_format = '>L'

    def __getitem__ (self, item):
        return BytesList (list.__getitem__ (self, item)) if isinstance (item, slice) \
            else list.__getitem__ (self, item)

    def __repr__ (self): return self.__str__ ()
    def __str__  (self):
        return '<BytesList: [{}]>'.format (', '.join (str (item) for item in self))

    #--------------------------------------------------------------------------#
    # Serialization                                                            #
    #--------------------------------------------------------------------------#
    def ToStream (self, stream):
        """Save to stream
        """
        write = stream.write

        StructList (self.size_format, (len (item) for item in self)).ToStream (stream)
        for item in self:
            write (item)

        return stream

    @classmethod
    def FromStream (cls, stream):
        """Load from stream
        """
        read = stream.read
        return cls (read (size) for size in StructList.FromStream (cls.size_format, stream))

# vim: nu ft=python columns=120 :
