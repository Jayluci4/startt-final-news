import pytest
from bs4 import BeautifulSoup

# Example: contract test for Inc42 source HTML
@pytest.mark.parametrize("html_sample, selector", [
    ("<html><body><h2 class='entry-title'><a href='/a'>Title</a></h2></body></html>", "h2.entry-title a"),
])
def test_source_contract(html_sample, selector):
    soup = BeautifulSoup(html_sample, 'html.parser')
    elem = soup.select_one(selector)
    assert elem is not None
    assert elem.get_text() == "Title" 