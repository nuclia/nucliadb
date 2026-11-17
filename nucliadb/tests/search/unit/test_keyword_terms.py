import pytest

from nucliadb.search.search.keyword_terms import STOP_WORDS, keyword_terms


@pytest.mark.parametrize(
    "query, expected",
    [
        ("The bank can increase the credit limit", ["bank", "increase", "credit", "limit"]),
        (
            "El Banco podr\u00e1 ampliar el l\u00edmite de cr\u00e9dito por el importe necesario en caso de que usted",
            [
                "Banco",
                "podr\u00e1",
                "ampliar",
                "l\u00edmite",
                "cr\u00e9dito",
                "importe",
                "necesario",
                "caso",
                "usted",
            ],
        ),
        ('"the bank" and "el banco"', ['"the bank"', '"el banco"']),
        ("and", ["and"]),
        ("THE bank bank", ["bank", "bank"]),
        ("el, banco y cr\u00e9dito", ["el,", "banco", "cr\u00e9dito"]),
        ("music is classical", ["music", "classical"]),
        ('music "is" classical', ["music", '"is"', "classical"]),
        ("music -is classical", ["music", "-is", "classical"]),
        ("music IS classical", ["music", "classical"]),
        ("classical music is", ["classical", "music", "is"]),
        ("we shouldn't be here", ["here"]),
        ("", []),
    ],
)
def test_keyword_terms(query, expected):
    assert keyword_terms(query) == expected


@pytest.mark.parametrize("word", ["is", "le", "el", "gli", "els", "und", "het", "uma"])
def test_stop_words_from_all_loaded_languages(word):
    assert word in STOP_WORDS


def test_non_stop_word():
    assert "nuclia" not in STOP_WORDS
