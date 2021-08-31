import pytest
import igsn_lib.time
import isb_lib.sesar_adapter

@pytest.mark.xfail
def test_SESARIdentifiers_01():
    ids = isb_lib.sesar_adapter.SESARIdentifiers()
    ids.__next__()
    assert len(ids) > 0


@pytest.mark.xfail
def test_SESARIdentifiers_02():
    # Should result in about 20 items
    tmax = igsn_lib.time.datetimeFromSomething("2013-06-19T17:28:28Z")
    ids = isb_lib.sesar_adapter.SESARIdentifiers(date_end=tmax)
    print("tmax = %s", str(tmax))
    for i in ids:
        print(str(i[1]))
        assert i[1] <= tmax


@pytest.mark.xfail
def test_SESARIdentifiers_03():
    # May be no records in response
    tmin = igsn_lib.time.datetimeFromSomething("2021-04-24 04:28:12+00:00")
    ids = isb_lib.sesar_adapter.SESARIdentifiers(date_start=tmin)
    print("tmin = %s", str(tmin))
    for i in ids:
        print(str(i[1]))
        assert i[1] > tmin
