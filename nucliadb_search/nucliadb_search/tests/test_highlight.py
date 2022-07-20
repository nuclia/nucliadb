from nucliadb_search.search.fetch import highlight, split_text


def test_highlight():

    res = highlight(
        "Query whatever you want my to make it work my query with this",
        "this is my query",
        True,
    )
    assert (
        res[0]
        == "Query whatever you want <b>my</b> to make it work <b>my</b> <b>query</b> with <b>this</b>"
    )

    res = highlight(
        "Query whatever you want to make it work my query with this",
        'this is "my query"',
        True,
    )

    assert (
        res[0]
        == "Query whatever you want to make it work <b>my query</b> with <b>this</b>"
    )

    res = highlight(
        "Query whatever you redis want to make it work my query with this",
        'this is "my query"',
        True,
    )

    assert (
        res[0]
        == "Query whatever you red<b>is</b> want to make it work <b>my query</b> with <b>this</b>"
    )


TEXT1 = """Explanation: the methods based on + (including the implied use in sum) are, of necessity, O(T**2) when there are T sublists -- as the intermediate result list keeps getting longer, at each step a new intermediate result list object gets allocated, and all the items in the previous intermediate result must be copied over (as well as a few new ones added at the end). So, for simplicity and without actual loss of generality, say you have T sublists of k items each: the first k items are copied back and forth T-1 times, the second k items T-2 times, and so on; total number of copies is k times the sum of x for x from 1 to T excluded, i.e., k * (T**2)/2.

The list comprehension just generates one list, once, and copies each item over (from its original place of residence to the result list) also exactly once"""


def test_split_text():

    res = split_text(
        TEXT1,
        "including copies",
        True,
    )

    import pdb

    pdb.set_trace()
    assert (
        res[0]
        == "Query whatever you want <b>my</b> to make it work <b>my</b> <b>query</b> with <b>this</b>"
    )

    res = split_text(
        TEXT1,
        'this is "each item over"',
        True,
    )

    import pdb

    pdb.set_trace()
    assert (
        res[0]
        == "Query whatever you want to make it work <b>my query</b> with <b>this</b>"
    )
