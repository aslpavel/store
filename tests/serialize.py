# -*- coding: utf-8 -*-
import io
import unittest

from ..serialize import StructSerializer, BytesSerializer

#------------------------------------------------------------------------------#
# Struct List Test                                                             #
#------------------------------------------------------------------------------#
class StructListTest (unittest.TestCase):
    def testPersist (self):
        struct_save = []

        # emtpy
        stream = io.BytesIO ()
        self.assertEqual (StructSerializer.ToStream (stream, '>L', struct_save), stream)

        stream.seek (0)
        struct_load = StructSerializer.FromStream (stream, '>L')
        self.assertEqual (struct_save, struct_load)

        # normal
        stream = io.BytesIO ()
        struct_save.extend (range (10))
        StructSerializer.ToStream (stream, '>L', struct_save)

        stream.seek (0)
        struct_load = StructSerializer.FromStream (stream, '>L')
        self.assertEqual (struct_save, struct_load)

        # tuple
        stream = io.BytesIO ()
        struct_save = list ((i, i) for i in range (10))
        StructSerializer.ToStream (stream, 'BB', struct_save)

        stream.seek (0)
        struct_load = StructSerializer.FromStream (stream, 'BB')
        self.assertEqual (struct_save, struct_load)

#------------------------------------------------------------------------------#
# Bytes List Test                                                              #
#------------------------------------------------------------------------------#
class BytesListTest (unittest.TestCase):
    def testPersist (self):
        bytes_save = []

        # empty
        stream = io.BytesIO ()
        self.assertEqual (BytesSerializer.ToStream (stream, bytes_save), stream)

        stream.seek (0)
        bytes_load = BytesSerializer.FromStream (stream)
        self.assertEqual (bytes_save, bytes_load)

        # normal
        stream = io.BytesIO ()
        bytes_save.extend (str (i).encode () for i in range (10))
        BytesSerializer.ToStream (stream, bytes_save)

        stream.seek (0)
        bytes_load = BytesSerializer.FromStream (stream)
        self.assertEqual (bytes_save, bytes_load)

# vim: nu ft=python columns=120 :
