from starlette.datastructures import URL

from isb_lib.utilities import url_utilities


def test_last_path_component():
    last_path = url_utilities.last_path_component(URL("http://foo.bar/index.html"))
    assert "index.html" == last_path


def test_joined_url():
    joined_url = url_utilities.joined_url("https://hyde.cyverse.org", "/isamples_central/ui/#/dois")
    assert "https://hyde.cyverse.org/isamples_central/ui/#/dois" == joined_url
    joined_url = url_utilities.joined_url("https://hyde.cyverse.org/", "/isamples_central/ui/#/dois")
    assert "https://hyde.cyverse.org/isamples_central/ui/#/dois" == joined_url
