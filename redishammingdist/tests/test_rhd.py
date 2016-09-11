from redishammingdist import Rhd
import redis
from bitstring import Bits
from hypothesis import given
from hypothesis.strategies import text
from hypothesis.strategies import integers
from hypothesis.strategies import binary
import hypothesis.strategies as st

conn1 = redis.StrictRedis(
    host='localhost', port=6379, db=0)
conn2 = redis.StrictRedis(
    host='localhost', port=6379, db=1)
connections = [conn1, conn2]

st_key = text(min_size=1)


@given(k=text(), _bytes=binary(min_size=1))
def test_set_get_mask(k, _bytes):
    rhd = Rhd(connections)
    rhd.set_mask(k, _bytes)
    assert rhd.get_mask(k) == _bytes
    assert rhd.get(k+"_mask") == _bytes


@given(k=text(min_size=1), i=integers(min_value=0, max_value=100000), bit=integers(min_value=0, max_value=1))
def test_setbit_getbit_mask(k, i, bit):
    rhd = Rhd(connections)
    rhd.setbit_mask(k, i, bit)
    assert rhd.getbit_mask(k, i) == bit
    assert rhd.getbit(k+"_mask", i) == bit


@given(k=st.lists(text(min_size=1), unique=True, max_size=5, min_size=5), _bytes=st.lists(binary(min_size=1), unique=True, max_size=5, min_size=5))
def test_set_get_mask(k, _bytes):
    rhd = Rhd(connections)
    rhd.set_mask(k, _bytes)
    assert rhd.get_mask(k) == _bytes
    assert rhd.get([x+"_mask" for x in k]) == _bytes

st_bitstring = text(min_size=10, max_size=10, alphabet=['1', '0'])


def setter(rhd, k, bitstring):
    rhd.setbit([k]*10, [i for i in range(10)],
               [int(i) for i in list(bitstring)])


@given(k1=st_key, k2=st_key, bitstring1=st_bitstring, bitstring2=st_bitstring)
def test_hamming_dist(k1, k2, bitstring1, bitstring2):
    rhd = Rhd(connections)
    rhd.flushall()

    setter(rhd, k1, bitstring1)
    setter(rhd, k2, bitstring2)
    assert all(rhd.getbit_mask([k1]*10, [i for i in range(10)]))
    bit1 = Bits(bin=bitstring1)
    bit2 = Bits(bin=bitstring2)
    if k1 != k2:
        assert rhd.hamming_dist(k1, k2) == (bit1 ^ bit2).count(True)
    else:
        assert rhd.hamming_dist(k1, k2) == 0


@given(k1=st_key, k2=st_key, bitstring1=st_bitstring, bitstring2=st_bitstring, mask1=st_bitstring, mask2=st_bitstring)
def test_hamming_dist_with_masking(k1, k2, bitstring1, bitstring2, mask1, mask2):
    rhd = Rhd(connections)
    rhd.flushall()
    setter(rhd, k1, bitstring1)
    setter(rhd, k2, bitstring2)

    rhd.setbit_mask([k1]*10, [i for i in range(10)],
                    [int(i) for i in list(mask1)])
    rhd.setbit_mask([k2]*10, [i for i in range(10)],
                    [int(i) for i in list(mask2)])
    bit1 = Bits(bin=bitstring1)
    bit2 = Bits(bin=bitstring2)
    bit1_mask = Bits(bin=mask1)
    bit2_mask = Bits(bin=mask2)
    if k1 != k2:

        assert rhd.hamming_dist(k1, k2) == (
            (bit1 ^ bit2) & bit2_mask & bit1_mask).count(True)
    else:
        assert rhd.hamming_dist(k1, k2) == 0


@given(k1=st_key, k2=st_key, k3=st_key, bitstring1=st_bitstring, bitstring2=st_bitstring, bitstring3=st_bitstring)
def test_dist_matrix(k1, k2, k3, bitstring1, bitstring2, bitstring3):
    rhd = Rhd(connections)
    rhd.flushall()
    setter(rhd, k1, bitstring1)
    setter(rhd, k2, bitstring2)
    setter(rhd, k3, bitstring3)
    dist_matrix = rhd.distance_matrix(k1, k2, k3)
    assert dist_matrix[0][0] == 0
    assert dist_matrix[1][1] == 0
    assert dist_matrix[2][2] == 0
    assert dist_matrix[0][1] == rhd.hamming_dist(k1, k2)
    assert dist_matrix[1][0] == rhd.hamming_dist(k1, k2)
    assert dist_matrix[0][2] == rhd.hamming_dist(k1, k3)
    assert dist_matrix[1][2] == rhd.hamming_dist(k2, k3)
