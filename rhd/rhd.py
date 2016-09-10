from __future__ import print_function
from rhd.decorators import pipeiflist
import crc16


class RedisCluster(object):

    def __init__(self, connections):
        # list of redis connections
        self.connections = connections
        self.num_conns = len(self.connections)

    @pipeiflist
    def set(self, c, _bytes, conn=None):
        conn.set(c, _bytes)

    @pipeiflist
    def setbit(self, c, i, bit, conn=None):
        conn.setbit(c, i, bit)

    @pipeiflist
    def get(self, c, conn=None):
        return conn.get(c)

    @pipeiflist
    def getbit(self, c, i, conn=None):
        return conn.getbit(c, i)

    def get_connection(self, c):
        if isinstance(c, str):
            c = str.encode(c)
        elif isinstance(c, int):
            c = str.encode(str(c))
        return self._get_connection_from_crc16(crc16.crc16xmodem(c))

    def get_connection_index(self, c):
        if isinstance(c, str):
            c = str.encode(c)
        elif isinstance(c, int):
            c = str.encode(str(c))
        return self._get_connection_index_from_crc16(crc16.crc16xmodem(c))

    def _get_connection_index_from_crc16(self, crc16):
        return crc16 % self.num_conns

    def _get_connection_from_crc16(self, crc16):
        index = self._get_connection_index_from_crc16(crc16)
        return self.connections[index]

    def _create_pipelines(self):
        return [c.pipeline() for c in self.connections]


class Rhd(RedisCluster):

    def __init(self, connections):
        super().__init__(connections)

    def _index_to_mask_index(self, c):
        return "".join([str(c), "_mask"])

    def set_mask(self, c, _bytes):
        c = self._index_to_mask_index(c)
        self.set(c, _bytes)

    def setbit_mask(self, c, i, bit):
        c = self._index_to_mask_index(c)
        self.setbit(c, i, bit)

    def get_mask(self, c):
        c = self._index_to_mask_index(c)
        return self.get(c)

    def getbit_mask(self, c, i):
        c = self._index_to_mask_index(c)
        return self.getbit(c, i)
