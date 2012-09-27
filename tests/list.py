# -*- coding: utf-8 -*-
import io
import unittest

from ..list import StructList, BytesList

#------------------------------------------------------------------------------#
# Struct List Test                                                             #
#------------------------------------------------------------------------------#
class StructListTest (unittest.TestCase):
    def testPersist (self):
        list_save = StructList ('>L')

        # emtpy
        stream = io.BytesIO ()
        self.assertEqual (list_save.ToStream (stream), stream)

        stream.seek (0)
        list_load = StructList.FromStream ('>L', stream)
        self.assertEqual (list_save, list_load)

        # normal
        stream = io.BytesIO ()
        list_save.extend (range (10))
        self.assertEqual (len (list_save), 10)
        list_save.ToStream (stream)

        stream.seek (0)
        list_load = StructList.FromStream ('>L', stream)
        self.assertEqual (list_save, list_load)

        # tuple
        stream = io.BytesIO ()
        list_save = StructList ('BB', ((i, i) for i in range (10)))
        list_save.ToStream (stream)

        stream.seek (0)
        list_load = StructList.FromStream ('BB', stream)
        self.assertEqual (list_save, list_load)

    def testSlice (self):
        self.assertEqual (type (StructList (range (10)) [1:]), StructList)

#------------------------------------------------------------------------------#
# Bytes List Test                                                              #
#------------------------------------------------------------------------------#
class BytesListTest (unittest.TestCase):
    def testPersist (self):
        list_save = BytesList ()

        # empty
        stream = io.BytesIO ()
        self.assertEqual (list_save.ToStream (stream), stream)

        stream.seek (0)
        list_load = BytesList.FromStream (stream)
        self.assertEqual (list_save, list_load)

        # normal
        stream = io.BytesIO ()
        list_save.extend (str (i).encode () for i in range (10))
        self.assertEqual (len (list_save), 10)
        list_save.ToStream (stream)

        stream.seek (0)
        list_load = BytesList.FromStream (stream)
        self.assertEqual (list_save, list_load)

    def testSlice (self):
        self.assertEqual (type (BytesList (str (i).encode () for i in range (10)) [1:]), BytesList)

# vim: nu ft=python columns=120 :
