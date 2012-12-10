# -*- coding: utf-8 -*-
import io
import struct
import unittest

from ..serialize import Serializer

#------------------------------------------------------------------------------#
# Serializer Test                                                              #
#------------------------------------------------------------------------------#
class SerializerTest (unittest.TestCase):
    """Serializer unit test
    """

    def testStruct (self):
        """Structure serializer
        """
        struct_save = []
        format = struct.Struct ('>L')

        # empty
        stream = io.BytesIO ()
        serial = Serializer (stream)
        serial.StructTupleWrite (struct_save, format)

        stream.seek (0)
        struct_load = serial.StructTupleRead (format)
        self.assertEqual (tuple (struct_save), struct_load)

        # normal
        stream = io.BytesIO ()
        serial = Serializer (stream)
        struct_save.extend (range (10))
        serial.StructTupleWrite (struct_save, format)

        stream.seek (0)
        struct_load = serial.StructTupleRead (format)
        self.assertEqual (tuple (struct_save), struct_load)

        # tuple
        stream = io.BytesIO ()
        serial = Serializer (stream)
        struct_save = tuple ((i, i) for i in range (10))
        format = struct.Struct ('BB')
        serial.StructTupleWrite (struct_save, format, True)

        stream.seek (0)
        struct_load = serial.StructTupleRead (format, True)
        self.assertEqual (tuple (struct_save), struct_load)

    def testBytes (self):
        """Bytes serializer
        """
        bytes_save = []

        # empty
        stream = io.BytesIO ()
        serial = Serializer (stream)
        serial.BytesTupleWrite (bytes_save)

        stream.seek (0)
        bytes_load = serial.BytesTupleRead ()
        self.assertEqual (tuple (bytes_save), bytes_load)

        # normal
        stream = io.BytesIO ()
        serial = Serializer (stream)
        bytes_save.extend (str (i).encode () for i in range (10))
        serial.BytesTupleWrite (bytes_save)

        stream.seek (0)
        bytes_load = serial.BytesTupleRead ()
        self.assertEqual (tuple (bytes_save), bytes_load)

# vim: nu ft=python columns=120 :
