from rhd.rhd import Rhd
import redis
from hypothesis import given
from hypothesis.strategies import text
from hypothesis.strategies import integers
from hypothesis.strategies import binary
conn1 = redis.StrictRedis(
    host='localhost', port=6379, db=0)
conn2 = redis.StrictRedis(
    host='localhost', port=6379, db=1)
connections = [conn1, conn2]


@given(integers())
def test_choose_connection(i):
    rhd = Rhd(connections)
    assert rhd._get_connection_from_crc16(i) in connections


@given(text(min_size=1))
def test_choose_connection_str(i):
    rhd = Rhd(connections)
    assert rhd.get_connection(i) in connections


@given(integers())
def test_choose_connection_int(i):
    rhd = Rhd(connections)
    assert rhd.get_connection(i) == rhd.get_connection(str(i))


@given(text())
def test_choose_connection_bytes(i):
    rhd = Rhd(connections)
    assert rhd.get_connection(i) == rhd.get_connection(str.encode(i))


@given(c=text(min_size=1), _bytes=binary(min_size=1))
def test_set_get_text(c, _bytes):
    rhd = Rhd(connections)
    rhd.set(c, _bytes)
    assert rhd.get(c) == _bytes


@given(c=integers(), _bytes=binary(min_size=1))
def test_set_get_int(c, _bytes):
    rhd = Rhd(connections)
    rhd.set(c, _bytes)
    assert rhd.get(c) == _bytes


@given(c=text(min_size=1), i=integers(min_value=0, max_value=100000), bit=integers(min_value=0, max_value=1))
def test_setbit_getbit_int(c, i, bit):
    rhd = Rhd(connections)
    rhd.setbit(c, i, bit)
    assert rhd.getbit(c, i) == bit


@given(c=text(), _bytes=binary(min_size=1))
def test_set_get_mask(c, _bytes):
    rhd = Rhd(connections)
    rhd.set_mask(c, _bytes)
    assert rhd.get_mask(c) == _bytes
    assert rhd.get(c+"_mask") == _bytes


@given(c=text(min_size=1), i=integers(min_value=0, max_value=100000), bit=integers(min_value=0, max_value=1))
def test_setbit_getbit_mask(c, i, bit):
    rhd = Rhd(connections)
    rhd.setbit_mask(c, i, bit)
    assert rhd.getbit_mask(c, i) == bit
    assert rhd.getbit(c+"_mask", i) == bit
