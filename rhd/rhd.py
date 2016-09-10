from __future__ import print_function
import crc16


class Rhd(object):

    def __init__(self, connections):
        # list of redis connections
        self.connections = connections
        self.num_conns = len(self.connections)

    def set(self, c, _bytes):
        self.get_connection(c).set(c, _bytes)

    def setbit(self, c, i, bit):
        self.get_connection(c).setbit(c, i, bit)

    def set_mask(self, c, _bytes):
        c = self._index_to_mask_index(c)
        self.set(c, _bytes)

    def setbit_mask(self, c, i, bit):
        c = self._index_to_mask_index(c)
        self.setbit(c, i, bit)

    def get(self, c):
        return self.get_connection(c).get(c)

    def getbit(self, c, i):
        return self.get_connection(c).getbit(c, i)

    def get_mask(self, c):
        c = self._index_to_mask_index(c)
        return self.get(c)

    def getbit_mask(self, c, i):
        c = self._index_to_mask_index(c)
        return self.getbit(c, i)

    def _index_to_mask_index(self, c):
        return "".join([str(c), "_mask"])

    def get_connection(self, c):
        if isinstance(c, str):
            c = str.encode(c)
        elif isinstance(c, int):
            c = str.encode(str(c))
        return self._get_connection_from_crc16(crc16.crc16xmodem(c))

    def _get_connection_from_crc16(self, crc16):
        index = crc16 % self.num_conns
        return self.connections[index]
