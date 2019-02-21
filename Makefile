.PHONY: mypy pylint pylint_e docker

ALL_FILES=ceph_ho_dumper.py
STUBS="stubs:../venvs/wally/lib/python3.5/site-packages/"

mypy:
		MYPYPATH=${STUBS} python -m mypy --ignore-missing-imports --follow-imports=skip ${ALL_FILES}