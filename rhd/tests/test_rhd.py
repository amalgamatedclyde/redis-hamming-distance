from rhd.rhd import Rhd
import redis
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


@given(c=st.lists(text(min_size=1), unique=True, max_size=5, min_size=5), _bytes=st.lists(binary(min_size=1), unique=True, max_size=5, min_size=5))
def test_set_get_mask(c, _bytes):
    rhd = Rhd(connections)
    rhd.set_mask(c, _bytes)
    assert rhd.get_mask(c) == _bytes
    assert rhd.get([x+"_mask" for x in c]) == _bytes
