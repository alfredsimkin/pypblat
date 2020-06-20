from Bio import SeqIO
from pathlib import Path
from itertools import islice, groupby
from collections import defaultdict


def windowed(seq, n=2):
    """Returns a sliding window (of width n) over data from the iterable"""
    "   s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...                   "
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result


class ReferenceStatistics:
    def __init__(self):
        self.unique_windows = {}
        self.non_unique_windows = {}
        # Dict[TE Name, int]
        self.theoretical_counts = {}


def make_reference_statistics(read_len: int, path: Path, seq_format: str = "fasta") -> ReferenceStatistics:

    theoretical_counts = defaultdict(int)
    stats = ReferenceStatistics()
    all_windows = ((''.join(window), record.id)
                   for record in SeqIO.parse(str(path), seq_format)
                   for window in windowed(record.seq, read_len))
    sorted_windows = sorted(all_windows, key=lambda x: x[0])

    for seq, seq_ids in groupby(sorted_windows, key=lambda x: x[0]):

        seq_ids = list(x[1] for x in seq_ids)
        for seq_id in seq_ids:
            theoretical_counts[seq_id] += 1

        # Either sequence maps to only one window or maps to multiple windows but all from the same sequence
        if all(x == seq_ids[0] for x in seq_ids):
            stats.unique_windows[seq] = seq_ids
            continue

        stats.non_unique_windows[seq] = seq_ids

    stats.theoretical_counts = dict(theoretical_counts)
    return stats
