# -*- coding: utf-8 -*-
import struct

__all__ = ('StructSeriaizer', 'BytesSerializer',)
#------------------------------------------------------------------------------#
# Structures Serializer                                                        #
#------------------------------------------------------------------------------#
class StructSerializer (object):
    """Structures serializer

    Save or loads list of structures to or from stream
    """
    header_struct = struct.Struct ('>Q')

    def __init__ (self):
        raise TypeError ('Serializer is a static type')

    @classmethod
    def ToStream (cls, stream, fmt, items):
        """Save to stream
        """
        write = stream.write
        items = items if hasattr (items, '__len__') else list (items)
        item_struct = struct.Struct (fmt)

        write (cls.header_struct.pack (len (items)))
        if len (items):
            if isinstance (items [0], tuple):
                for item in items:
                    write (item_struct.pack (*item))
            else:
                for item in items:
                    write (item_struct.pack (item))

        return stream

    @classmethod
    def FromStream (cls, stream, fmt):
        """Load from stream
        """
        read  = stream.read
        count = cls.header_struct.unpack (read (cls.header_struct.size)) [0]

        items = []
        item_struct = struct.Struct (fmt)
        item_size   = item_struct.size

        if count:
            item = item_struct.unpack (read (item_size))
            if len (item) > 1:
                items.append (item)
                for _ in range (count - 1):
                    items.append (item_struct.unpack (read (item_size)))
            else:
                items.append (item [0])
                for _ in range (count - 1):
                    items.append (item_struct.unpack (read (item_size)) [0])

        return items

#------------------------------------------------------------------------------#
# Bytes Serializer                                                             #
#------------------------------------------------------------------------------#
class BytesSerializer (object):
    """Bytes serializer

    Save or loads list of bytes (bytes is a type) to or from stream
    """
    size_format = '>L'

    def __init__ (self):
        raise TypeError ('Serializer is a static type')

    @classmethod
    def ToStream (cls, stream, items):
        """Save to stream
        """
        write = stream.write
        items = items if hasattr (items, '__len__') else list (items)

        StructSerializer.ToStream (stream, cls.size_format, (len (item) for item in items))
        for item in items:
            write (item)

        return stream

    @classmethod
    def FromStream (cls, stream):
        """Load from stream
        """
        read = stream.read
        return [read (size) for size in
            StructSerializer.FromStream (stream, cls.size_format)]

# vim: nu ft=python columns=120 :
