import csv
from dataclasses import dataclass
from typing import List


@dataclass
class PslxLine:
    match: int      # 0
    mismatch: int  # 1
    rep_match: int  # 2
    ns: int  # 3
    q_gap_count: int  # 4
    q_gap_bases: int    # 5
    t_gap_count: int    # 5
    t_gap_bases: int    # 5
    strand: str    # 5
    q_name: str    # 9
    q_size: int
    q_start: int
    q_end: int
    t_name: str     # 13
    t_size: int
    t_start: int
    t_end: int
    block_count: int
    block_sizes: List[int]
    q_starts: List[int]
    t_starts: List[int]
    q_seqs: List[str]
    t_seqs: List[str]

    types = [int, int, int, int, int, int, int, int, str, str, int, int, int, str, int, int, int, int]


def as_list(t: type, s: str):
    return [t(s) for s in s.split(',') if s]

def cast(t, v):
    return t(v)


class PslxReader:
    def __init__(self, stream):
        self.stream = stream
        self.csv_reader = csv.reader(stream, delimiter='\t')

    def __iter__(self):
        return self

    def __next__(self):
        line = next(self.csv_reader)
        if not line:
            raise StopIteration()
        return PslxLine(*([cast(PslxLine.types[i], x) for i, x in enumerate(line[:-5])]),
                        as_list(int, line[-5]),
                        as_list(int, line[-4]),
                        as_list(int, line[-3]),
                        as_list(str, line[-2]),
                        as_list(str, line[-1]))
