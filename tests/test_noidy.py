import random

from isb_lib.identifiers.noidy.n2tminter import N2TMinter


def test_noidy():
    naan = random.randrange(100000)
    prefix = "fk4"
    num_identifiers = 10
    shoulder = f"{naan}/{prefix}"
    minter = N2TMinter(shoulder)
    _mint_identifiers_and_assert(minter, num_identifiers)


def _mint_identifiers_and_assert(minter, num_identifiers):
    identifiers = minter.mint(num_identifiers)
    identifier_list = []
    for identifier in identifiers:
        assert len(identifier) > 0
        identifier_list.append(identifier)
    assert len(identifier_list) == num_identifiers
    idset = set(identifier_list)
    assert len(idset) == len(identifier_list)
