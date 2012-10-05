# -*- coding: utf-8 -*-
import io
import random
import unittest

from ..store import StreamStore

#------------------------------------------------------------------------------#
# Store Test                                                                   #
#------------------------------------------------------------------------------#
class StoreTest (unittest.TestCase):
    """Store unit tests
    """

    def testSimple (self):
        """Simple tests
        """
        stream = io.BytesIO ()
        with StreamStore (stream, 1) as store:
            # empty
            self.assertEqual (store.Save (b''), 0)
            self.assertEqual (store.Load (0), b'')

            # unamed
            desc_data = b'some test data'
            desc = store.Save (desc_data)
            self.assertEqual (store.Load (desc), desc_data)

            # named
            name, name_data  = b'name', b'some test value'
            store [name] = name_data
            self.assertEqual (store [name], name_data)

        with StreamStore (stream, 1) as store:
            self.assertEqual (store.Load (desc), desc_data)
            self.assertEqual (store [name], name_data)


    def testStress (self):
        """Stress tests
        """
        count = 1 << 14
        datas = [str (random.randint (0, count)).encode () * random.randint (1, 16) for _ in range (count)]

        stream = io.BytesIO ()
        descs  = []

        with StreamStore (stream) as store:
            for data in datas:
                descs.append (store.Save (data))

        with StreamStore (stream) as store:
            for data, desc in zip (datas, descs):
                self.assertEqual (data, store.Load (desc))

            # delete half
            for desc in descs [int (count / 2):]:
                store.Delete (desc)

            datas = datas [:int (count / 2)]
            descs = descs [:int (count / 2)]

        with StreamStore (stream) as store:
            for data, desc in zip (datas, descs):
                self.assertEqual (data, store.Load (desc))

            # create
            for _ in range (count):
                data = str (random.randint (0, count)).encode () * random.randint (1, 16)
                datas.append (data)
                descs.append (store.Save (data))

        with StreamStore (stream) as store:
            for data, desc in zip (datas, descs):
                self.assertEqual (data, store.Load (desc))


# vim: nu ft=python columns=120 :