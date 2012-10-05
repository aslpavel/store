# -*- coding: utf-8 -*-
import io
import os

from .store import Store

__all__ = ('StreamStore', 'FileStore',)
#------------------------------------------------------------------------------#
# Stream Store                                                                 #
#------------------------------------------------------------------------------#
class StreamStore (Store):
    """Stream based store
    """

    def __init__ (self, stream, offset = None):
        self.stream = stream

        Store.__init__ (self, offset)

    def SaveByOffset (self, offset, data):
        self.stream.seek (offset)
        return self.stream.write (data)

    def LoadByOffset (self, offset, size):
        self.stream.seek (offset)
        return self.stream.read (size)

    def Flush (self):
        Store.Flush (self)
        self.stream.flush ()

#------------------------------------------------------------------------------#
# File Store                                                                   #
#------------------------------------------------------------------------------#
class FileStore (StreamStore):
    """File based store
    """

    def __init__ (self, path, mode = None, offset = None):
        mode = mode or 'r'
        self.mode = mode

        if mode == 'r':
            filemode = 'rb'
        elif mode == 'w':
            filemode = 'r+b'
        elif mode == 'c':
            if not os.path.lexists (path):
                filemode = 'w+b'
            else:
                filemode = 'r+b'
        elif mode == 'n':
            filemode = 'w+b'
        else:
            raise ValueError ('Unknown mode: {}'.format (mode))

        StreamStore.__init__ (self, io.open (path, filemode, buffering = 0), offset)

    def Flush (self):
        if self.mode != 'r':
            StreamStore.Flush (self)

    def Dispose (self):
        StreamStore.Dispose (self)
        self.stream.close ()

# vim: nu ft=python columns=120 :
