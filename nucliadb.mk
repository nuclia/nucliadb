# Common Makefile variables and patterns. Include this inside other makefiles with:
#
# include path/to/nucliadb.mk
#

pytest_flags := -s -rfE -v --tb=native
pytest_extra_flags :=
pytest_cov_report_flags := --cov-report xml --cov-report term-missing:skip-covered

PYTEST := pytest $(pytest_flags) $(pytest_extra_flags)

