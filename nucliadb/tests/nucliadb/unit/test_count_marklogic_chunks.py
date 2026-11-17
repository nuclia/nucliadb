import sys
from importlib import import_module
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

count_script = import_module("count_marklogic_chunks")


def test_summarize_distribution_reports_totals_percentiles_and_empty_kbs():
    summary = count_script.summarize_distribution([30, 0, 10, 20])

    assert summary == {
        "total": 60,
        "mean": 15.0,
        "median": 15.0,
        "p25": 0,
        "p75": 20,
        "min": 0,
        "max": 30,
        "zero_kbs": 1,
    }


def test_summarize_distribution_handles_no_kbs():
    assert count_script.summarize_distribution([]) == {
        "total": 0,
        "mean": None,
        "median": None,
        "p25": None,
        "p75": None,
        "min": None,
        "max": None,
        "zero_kbs": 0,
    }
