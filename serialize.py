# -*- coding: utf-8 -*-
import struct

__slots__ = ('Serializer',)
#------------------------------------------------------------------------------#
# Serializer                                                                   #
#------------------------------------------------------------------------------#
class Serializer (object):
    """Stream serializer

    It is synchronous version for BufferedStream's serializer.
    """
    size_struct = struct.Struct ('>I')

    def __init__ (self, stream):
        self.stream = stream

    #--------------------------------------------------------------------------#
    # Properties                                                               #
    #--------------------------------------------------------------------------#
    @property
    def Stream (self):
        """Backing store stream
        """
        return self.stream

    #--------------------------------------------------------------------------#
    # Bytes                                                                    #
    #--------------------------------------------------------------------------#
    def BytesRead (self):
        """Read bytes object
        """
        return self.stream.read (self.size_struct.unpack (
               self.stream.read (self.size_struct.size)) [0])

    def BytesWrite (self, bytes):
        """Write bytes object to buffer
        """
        self.stream.write (self.size_struct.pack (len (bytes)))
        self.stream.write (bytes)

    #--------------------------------------------------------------------------#
    # Tuple of structures                                                      #
    #--------------------------------------------------------------------------#
    def StructTupleRead (self, struct, complex = None):
        """Read tuple of structures
        """
        struct_data = self.BytesRead ()
        if complex:
            return tuple (struct.unpack (struct_data [offset:offset + struct.size])
                for offset in range (0, len (struct_data), struct.size))
        else:
            return tuple (struct.unpack (struct_data [offset:offset + struct.size]) [0]
                for offset in range (0, len (struct_data), struct.size))

    def StructTupleWrite (self, struct_tuple, struct, complex = None):
        """Write tuple of structures to buffer
        """
        self.stream.write (self.size_struct.pack (len (struct_tuple) * struct.size))
        if complex:
            for struct_target in struct_tuple:
                self.stream.write (struct.pack (*struct_target))
        else:
            for struct_target in struct_tuple:
                self.stream.write (struct.pack (struct_target))

    #--------------------------------------------------------------------------#
    # Tuple of bytes                                                           #
    #--------------------------------------------------------------------------#
    def BytesTupleRead (self):
        """Read array of bytes
        """
        return tuple (self.stream.read (size)
            for size in self.StructTupleRead (self.size_struct))

    def BytesTupleWrite (self, bytes_tuple):
        """Write bytes array object to buffer
        """
        self.StructTupleWrite (tuple (len (bytes) for bytes in bytes_tuple), self.size_struct)

        for bytes in bytes_tuple:
            self.stream.write (bytes)

# vim: nu ft=python columns=120 :
