from __future__ import print_function
from redispartition import RedisCluster


class Rhd(RedisCluster):

    def __init(self, connections):
        super().__init__(connections)

    def _index_to_mask_index(self, k):
        if isinstance(k, list):
            return ["".join([str(i), "_mask"]) for i in k]
        else:
            return "".join([str(k), "_mask"])

    def set_mask(self, k, _bytes):
        k = self._index_to_mask_index(k)
        self.set(k, _bytes)

    def setbit_mask(self, k, i, bit):
        k = self._index_to_mask_index(k)
        self.setbit(k, i, bit)

    def get_mask(self, k):
        k = self._index_to_mask_index(k)
        return self.get(k)

    def getbit_mask(self, k, i):
        k = self._index_to_mask_index(k)
        return self.getbit(k, i)
