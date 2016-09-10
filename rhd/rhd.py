from __future__ import print_function
import crc16


class Rhd(object):

    def __init__(self, connections):
        # list of redis connections
        self.connections = connections
        self.num_conns = len(self.connections)

    def set(self, c, bytes):
        pass

    def setbit(self, c, i, bit):
        pass

    def set_mask(self, c, bytes):
        pass

    def set_bit_mask(self, c):
        pass

    def get(self, c):
        pass

    def getbit(self, c):
        pass

    def get_mask(self, c):
        pass

    def getbit_mask(self, c):
        pass

    def get_connection(self, c):
        if isinstance(c, str):
            c = str.encode(c)
        elif isinstance(c, int):
            c = str.encode(str(c))
        return self._get_connection_from_crc16(crc16.crc16xmodem(c))

    def _get_connection_from_crc16(self, crc16):
        index = crc16 % self.num_conns
        return self.connections[index]
