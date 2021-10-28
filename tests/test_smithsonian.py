import isb_lib
import isb_lib.core


def test_normalized_identifier():
    raw_id = "https://n2t.net/foobarbaz"
    normalized_id = isb_lib.normalized_id(raw_id)
    assert "foobarbaz" == normalized_id

    raw_id = "http://n2t.net/joeyjohnny"
    normalized_id = isb_lib.normalized_id(raw_id)
    assert "joeyjohnny" == normalized_id
