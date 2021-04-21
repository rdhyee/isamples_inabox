import pytest
import isb_lib.core

iterator_testcases = [
    (1, 1),
    (55, 55),
    (999, 999),
    (1000,1000),
    (1001, 1000),
    (2020, 1000),
]

@pytest.mark.parametrize("max_entries,expected_outcome", iterator_testcases)
def test_IdentifierIterator(max_entries, expected_outcome):
    itr = isb_lib.core.IdentifierIterator(max_entries=max_entries)
    cnt = 0
    for e in itr:
        cnt += 1
    assert cnt == expected_outcome
