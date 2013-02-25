# -*- coding: utf-8 -*-
import zlib

__all__ = ('StoreStream',)
#------------------------------------------------------------------------------#
# Store Stream                                                                 #
#------------------------------------------------------------------------------#
class StoreStream (object):
    """Stream object with Store backend.
    """
    default_compress  = 9
    default_chunk_size = 1 << 16

    def __init__ (self, store, name, buffer_size = None, compress = None):
        self.name = name
        self.store = store
        self.states = store.Mapping (b'.streams', key_type = 'bytes', value_type = 'json')

        state = self.states.get (name)
        if state is None:
            self.chunk_size = buffer_size or self.defult_chunk_size
            self.chunks = []
            self.size = 0
            self.compress = self.default_compress if compress is None else compress
        else:
            self.chunk_size = state ['chunk_size']
            self.chunks = state ['chunks']
            self.size = state ['size']
            self.compress = state ['compress']
        self.chunk_zero = b'\x00' * self.chunk_size

        self.seeked = False
        self.chunk_index = None
        self.chunk_dirty = False
        self.chunk_switch (0)

    def chunk_switch (self, index = None):
        """Switch current chunk
        """
        if self.chunk_index == index:
            return

        if self.chunk_dirty:
            self.chunk_dirty = False
            self.chunks [self.chunk_index] = self.store.Save (self.chunk.bytes ()
                if not self.compress else zlib.compress (self.chunk.bytes (), self.compress))

        self.chunk_index = self.chunk_index + 1 if index is None else index
        if self.chunk_index < len (self.chunks):
            self.chunk_desc = self.chunks [self.chunk_index]
            self.chunk = Chunk (self.chunk_size,
                    self.chunk_zero if self.chunk_desc is None else
                    self.store.Load (self.chunk_desc) if not self.compress else
                    zlib.decompress (self.store.Load (self.chunk_desc)))
        else:
            self.chunks.extend ((None,) * (self.chunk_index - len (self.chunks) + 1))
            self.chunk_desc = None
            self.chunk = Chunk (self.chunk_size)

    def seek_real (self, pos):
        """Actual seek implementation
        """
        index, offset = divmod (pos, self.chunk_size)
        self.chunk_switch (index)
        self.chunk.seek (offset)
        self.seeked = None

    #--------------------------------------------------------------------------#
    # Write                                                                    #
    #--------------------------------------------------------------------------#
    def Write (self, data):
        """Write data to stream
        """
        if self.seeked is not None:
            self.seek_real (self.seeked)

        data_size = len (data)
        data_offset = 0
        while True:
            data_offset += self.chunk.write (data [data_offset:])
            self.chunk_dirty = True
            if data_offset == data_size:
                break
            self.chunk_switch ()

        self.size = max (self.size, self.chunk_index * self.chunk_size + self.chunk.tell ())
        return len (data)

    def write (self, data): return self.Write (data)

    #--------------------------------------------------------------------------#
    # Read                                                                     #
    #--------------------------------------------------------------------------#
    def Read (self, size = None):
        """Read data from stream
        """

        if self.seeked is not None:
            if self.seeked < self.size:
                self.seek_real (self.seeked)
            else:
                return b''

        size = self.size if size is None else size
        data = []
        data_size = 0

        while True:
            chunk = self.chunk.read (size - data_size)
            data.append (chunk)
            data_size += len (chunk)
            if data_size == size:
                break
            if len (self.chunks) <= self.chunk_index + 1:
                break
            else:
                self.chunk_switch ()

        return b''.join (data)

    def read (self, size = None): return self.Read (size)

    #--------------------------------------------------------------------------#
    # Seek                                                                     #
    #--------------------------------------------------------------------------#
    def Seek (self, pos, whence = 0):
        """Seek stream
        """
        if whence == 0:   # SEEK_SET
            self.seeked = pos

        elif whence == 1: # SEEK_CUR
            self.seeked = self.chunk_index * self.chunk_size + self.chunk.tell () + pos

        elif whence == 2: # SEEK_END
            self.seeked = self.size + pos

        else:
            raise ValueError ('Invalid whence argument: {}'.format (whence))

        return self.seeked

    def seek (self, pos, whence = 0): return self.Seek (pos, whence)

    #--------------------------------------------------------------------------#
    # Tell                                                                     #
    #--------------------------------------------------------------------------#
    def Tell (self):
        """Tell current position inside stream
        """
        if self.seeked is None:
            return self.chunk_index * self.chunk_size + self.chunk.tell ()
        else:
            return self.seeked

    def tell (self): return self.Tell ()

    #--------------------------------------------------------------------------#
    # Flush                                                                    #
    #--------------------------------------------------------------------------#
    def Flush (self):
        """Flush stream
        """
        if self.chunk_dirty:
            self.chunk_dirty = False
            self.chunks [self.chunk_index] = self.store.Save (self.chunk.bytes ()
                if not self.compress else zlib.compress (self.chunk.bytes (), self.compress))

        self.states [self.name] = {
            'chunk_size': self.chunk_size,
            'chunks': self.chunks,
            'size': self.size,
            'compress': self.compress,
        }
        self.states.Flush ()

    def flush (self): return self.Flush ()
    def close (self): return self.Flush ()

    #--------------------------------------------------------------------------#
    # Dispose                                                                  #
    #--------------------------------------------------------------------------#
    def Dispose (self):
        """Dispose stream
        """
        self.Flush ()

    def __enter__ (self):
        return self

    def __exit__ (self, et, eo, tb):
        self.Dispose ()
        return False

#------------------------------------------------------------------------------#
# Chunk                                                                        #
#------------------------------------------------------------------------------#
class Chunk (object):
    """Chunk

    Stream like object but with fixed capcity.
    """
    __slots__ = ('buf', 'cap', 'pos', 'size')

    def __init__ (self, cap, data = None):
        self.buf  = bytearray (cap)
        self.cap  = cap
        self.pos  = 0
        if data:
            self.size = len (data)
            self.buf [:self.size] = data
        else:
            self.size = 0

    def write (self, data):
        data_size = min (self.cap - self.pos, len (data))
        self.buf [self.pos:self.pos + data_size] = data [:data_size]
        self.pos += data_size
        self.size = max (self.size, self.pos)
        return data_size

    def read (self, size = None):
        data = (self.buf [self.pos:self.size] if size is None else
            self.buf [self.pos:min (self.pos + size, self.size)])
        self.pos += len (data)
        return data

    def seek (self, pos, whence = 0):
        if whence == 0:   # SEEK_SET
            self.pos = pos
        elif whence == 1: # SEEK_CUR
            self.pos += pos
        elif whence == 2: # SEEK_END
            self.pos = self.size + pos
        return self.pos

    def tell (self):
        return self.pos

    def truncate (self, pos):
        self.size = pos
        return pos

    def bytes (self):
        return self.buf [:self.size]

    def __str__ (self):
        return 'Chunk [data:{} pos:{} cap:{}]'.format (
            self.bytes (), self.pos, self.cap)

    def __repr__ (self):
        return str (self)

# vim: nu ft=python columns=120 :
