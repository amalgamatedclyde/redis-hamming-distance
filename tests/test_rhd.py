from rhd.rhd import Rhd
import redis
from hypothesis import given
from hypothesis.strategies import text
from hypothesis.strategies import integers
from hypothesis.strategies import binary
conn1 = redis.StrictRedis(
    host='localhost', port=6397, db=0)
conn2 = redis.StrictRedis(
    host='localhost', port=6397, db=1)
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
