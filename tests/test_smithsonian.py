from isb_lib import smithsonian_adapter

def test_normalized_identifier():
    raw_id = "https://n2t.net/foobarbaz"
    normalized_id = smithsonian_adapter._normalized_id(raw_id)
    assert "foobarbaz" == normalized_id

    raw_id = "http://n2t.net/joeyjohnny"
    normalized_id = smithsonian_adapter._normalized_id(raw_id)
    assert "joeyjohnny" == normalized_id