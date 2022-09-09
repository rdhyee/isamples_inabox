import pytest
import requests
import os
import lxml.etree
import random
import dateparser
from lxml.etree import _Element


@pytest.fixture
def rsession():
    return requests.session()


@pytest.fixture
def sitemap_index_url():
    sitemap_index_url = os.getenv("INPUT_SITEMAP_INDEX_URL")
    if sitemap_index_url is None:
        sitemap_index_url = "https://hyde.cyverse.org/sitemaps/sitemap-index.xml"
    return sitemap_index_url


def _assert_date_tag_text(date_tag: _Element):
    last_mod_date = dateparser.parse(date_tag.text)
    assert last_mod_date is not None


def test_sitemap(rsession: requests.Session, sitemap_index_url: str):
    """
    General approach is as follows:
    (1) Hit the sitemap-index.xml, and choose one of the random sitemaps inside
    (2) Download the sitemap and unzip it
    (3) Choose 100 of the random urls in the sitemap and resolve the URL
    (4) Also, for all dates, verify that they are parseable by dateparser
    """
    res = rsession.get(sitemap_index_url)
    xmlp = lxml.etree.XMLParser(
        recover=True,
        remove_comments=True,
        resolve_entities=False,
    )
    root = lxml.etree.fromstring(res.content, parser=xmlp)
    sitemap_list = root.getchildren()
    """These sitemap children look like this:
          <sitemap>
            <loc>http://mars.cyverse.org/sitemaps/sitemap-5.xml</loc>
            <lastmod>2006-08-10T12:00:00Z</lastmod>
          </sitemap>
    """
    random_sitemap_index = random.randrange(0, len(sitemap_list) - 1)
    random_sitemap_element = sitemap_list[random_sitemap_index]
    random_sitemap_children = random_sitemap_element.getchildren()
    for random_sitemap_child in random_sitemap_children:
        if "lastmod" in random_sitemap_child.tag:
            _assert_date_tag_text(random_sitemap_child)
        elif "loc" in random_sitemap_child.tag:
            sitemap_file_loc = random_sitemap_child.text
            res = rsession.get(sitemap_file_loc)
            sitemap_file_root = lxml.etree.fromstring(res.content, parser=xmlp)
            urlset_children = sitemap_file_root.getchildren()
            """The urlset children look like this:
                  <url>
                    <loc>http://mars.cyverse.org/thing/IGSN:NEON01IJE?full=false&amp;format=core</loc>
                    <lastmod>2020-03-14T10:07:56Z</lastmod>
                  </url>
            """

            # Get 100 randomly selected urlset children and try and resolve the url in the loc
            for n in range(0, 100):
                random_url_index = random.randrange(0, len(urlset_children))
                random_url_tag = urlset_children[random_url_index]
                for url_child in random_url_tag.getchildren():
                    if "lastmod" in url_child.tag:
                        _assert_date_tag_text(url_child)
                    elif "loc" in url_child.tag:
                        loc = url_child.text
                        # Open a request to this URL and verify we get some valid JSON back
                        url_child_res = rsession.get(loc)
                        url_child_json = url_child_res.json()
                        assert url_child_json is not None
                        # also verify it has something reasonable, like our sampleidentifier
                        assert url_child_json.get("sampleidentifier") is not None
