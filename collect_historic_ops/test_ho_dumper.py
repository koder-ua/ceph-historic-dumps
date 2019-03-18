import inspect
import subprocess
from pathlib import Path
from typing import List, Union, Dict, Iterable
from unittest.mock import patch

import ceph_ho_dumper


test_data_dir = Path(inspect.getfile(inspect.currentframe())).absolute().parent / 'test_data'


class TimeModule:
    def __init__(self, curr_time: float, stop_time: float) -> None:
        self.curr_time = curr_time
        self.stop_time = stop_time

    def time(self) -> float:
        return self.curr_time

    def sleep(self, time: float):
        self.curr_time += time
        if self.curr_time >= self.stop_time:
            raise ceph_ho_dumper.UTExit()


class SubprocessModule:
    def __init__(self, cmdresults: Dict[str, Union[bytes, List[bytes]]], timeout: int = None) -> None:
        self.cmdresults = cmdresults
        self.timeout = timeout
        self.SubprocessError = subprocess.SubprocessError
        self.CalledProcessError = subprocess.CalledProcessError

    def check_output(self, cmd: Union[str, Iterable[str]], shell: bool = False, timeout: int = None, **other) -> bytes:
        assert shell == isinstance(cmd, str)
        cmd_s = " ".join(cmd) if isinstance(cmd, list) else cmd
        if self.timeout:
            assert abs(timeout - self.timeout) < 1E-1

        res = self.cmdresults[cmd_s]
        if isinstance(res, bytes):
            return res
        else:
            return res.pop()


def test_simple():
    with patch("ceph_ho_dumper.subprocess", SubprocessModule()):
        with patch("ceph_ho_dumper.time", TimeModule()):
            ceph_ho_dumper.main()
