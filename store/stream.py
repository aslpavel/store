# -*- coding: utf-8 -*-
import io

from .store import Store

__all__ = ('StreamStore', 'FileStore',)
#------------------------------------------------------------------------------#
# Stream Store                                                                 #
#------------------------------------------------------------------------------#
class StreamStore (Store):
    """Stream based store
    """

    def __init__ (self, stream):
        self.stream = stream

        Store.__init__ (self)

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

    def __init__ (self, path, mode = None):
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

        StreamStore.__init__ (self, io.open (path, filemode, buffering = 0))

    def Dispose (self):
        StreamStore.Dispose (self)
        self.stream.close ()

# vim: nu ft=python columns=120 :
