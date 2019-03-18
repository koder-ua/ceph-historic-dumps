.PHONY: mypy pylint pylint_e

ALL_FILES := $(wildcard collect_historic_ops/*.py)
STUBS="stubs:../venvs/wally/lib/python3.5/site-packages/"
MYPYOPTS="--ignore-missing-imports --disallow-untyped-defs --disallow-incomplete-defs --strict-optional --follow-imports=skip"
PYLINT_FMT=--msg-template={path}:{line}: [{msg_id}({symbol}), {obj}] {msg}

mypy:
		MYPYPATH=${STUBS} python -m mypy ${MYPYOPTS} ${ALL_FILES}

pylint:
		python -m pylint '${PYLINT_FMT}' --rcfile=pylint.rc ${ALL_FILES}

pylint_e:
		python3 -m pylint -E '${PYLINT_FMT}' --rcfile=pylint.rc ${ALL_FILES}