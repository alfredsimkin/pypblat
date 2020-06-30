from tempfile import gettempdir
from ctypes import *
import sys
import os
from pathlib import Path
from multiprocessing import Process, Queue
from typing import List, Dict
import random
from functools import reduce
import csv
from . import pslx_reader
from .te_stats import ReferenceStatistics, make_reference_statistics

UTF8 = 'utf8'

_pblat = cdll.LoadLibrary('pypblat/libpblat.so')

_pblat.blatWithArgs.argtypes = [
    c_char_p,  # referenceFile
    c_char_p,  # readFile
    c_char_p,  # pipePattern
    c_char_p,  # t
    c_char_p,  # q
    c_bool,  # prot
    c_char_p,  # ooc
    c_int,  # threads
    c_int,  # tileSize
    c_int,  # stepSize
    c_int,  # oneOff,
    c_int,  # minMatch
    c_int,  # minScore
    c_float,  # minIdentity
    c_int,  # maxGap
    c_bool,  # noHead
    c_char_p,  # makeOoc
    c_int,  # repMatch
    c_char_p,  # mask
    c_char_p,  # qMask
    c_char_p,  # repeats
    c_float,  # minRepDivergence
    c_int,  # dots
    c_bool,  # trimT
    c_bool,  # noTrimA
    c_bool,  # trimHardA
    c_bool,  # fastMap
    c_char_p,  # out
    c_bool,  # fine
    c_int,  # maxIntron
    c_bool  # extendThroughN
]


def _call_with_default_args():
    _pblat.blatWithArgs(
        None,  # c_char_p                  referenceFile
        None,  # c_char_p                  readFile
        None,  # c_char_p                   pipePattern
        None,  # c_char_p,                 t
        None,  # c_char_p,                 q
        False,  # c_bool,                  prot
        None,  # c_char_p,                 ooc
        -1,  # c_int,                      threads
        -1,  # c_int,                      tileSize
        -1,  # c_int,                      stepSize
        -1,  # c_int,                      oneOff,
        -1,  # c_int,                      minMatch
        -1,  # c_int,                      minScore
        -1.0,  # c_float,                  minIdentity
        -1,  # c_int,                      maxGap
        False,  # c_bool,                  noHead
        None,  # c_char_p,                 makeOoc
        -1,  # c_int,                      repMatch
        None,  # c_char_p,                 mask
        None,  # c_char_p,                 qMask
        None,  # c_char_p,                 repeats
        -1.0,  # c_float,                  minRepDivergence
        -1,  # c_int,                      dots
        False,  # c_bool,                  trimT
        False,  # c_bool,                  noTrimA
        False,  # c_bool,                  trimHardA
        False,  # c_bool,                  fastMap
        None,  # c_char_p                  out
        False,  # c_bool,                  fine
        -1,  # c_int,                      maxIntron
        False  # c_bool                   extendThroughN
    )


class TranscriptionExpressResults:
    def __init__(self):
        self.non_unique_reads = 0
        self.unique_reads = 0
        self.randomly_mapped_reads = 0
        self.split_reads = 0

    def total_size(self):
        return self.unique_reads + self.non_unique_reads

    def unique_over_total(self):
        return 0 if self.total_size() == 0 else self.unique_reads / self.total_size()

class TranscriptionExpressionValues:
    def __init__(self, q_name):
        self.q_name = q_name
        self.best_score = 0
        self.targets = []

    def update(self, target_name: str, score: int):
        if self.best_score < score:
            self.targets = [target_name]
            self.best_score = score
        elif self.best_score == score:
            self.targets.append(target_name)

    def save(self, d):
        split_val = 1.0 / len(self.targets)
        is_unique = len(self.targets) == 1
        for target in self.targets:
            try:
                record = d[target]
            except KeyError:
                record = TranscriptionExpressResults()
                d[target] = record

            if is_unique:
                record.unique_reads += 1
            else:
                record.non_unique_reads += 1
            record.split_reads += split_val

        d[random.choice(self.targets)].randomly_mapped_reads += 1


def make_fifos(pattern: str, num_threads: int) -> List[Path]:
    """Creates PID-specific named pipes"""
    pid = os.getpid()
    fifos = [Path(f'{pattern}/pblat.fifo.{pid}-{idx}') for idx in range(num_threads)]
    for fifo in fifos:
        os.mkfifo(fifo)
    return fifos


def rm_files(files: List[Path]) -> None:
    """Removes all files in path"""
    for fifo in files:
        fifo.unlink()


def read_named_pipe(path: Path, queue: Queue) -> None:
    """Pipes can only be written to if they have a reader on the other end. If there's no reader, any write to the pipe
    pauses. Hence, these are opened before the writers."""
    transcript_expressions = {}

    with path.open('r') as f:
        reader = pslx_reader.PslxReader(f)

        # Init the first record
        record = next(reader)
        score, q_name, t_name = record.match, record.q_name, record.t_name
        working_record = TranscriptionExpressionValues(q_name)
        working_record.update(t_name, score)

        # Handle all the other records
        for record in reader:
            score, q_name, t_name = record.match, record.q_name, record.t_name

            if q_name != working_record.q_name:
                working_record.save(transcript_expressions)
                working_record = TranscriptionExpressionValues(q_name)

            working_record.update(t_name, score)

        queue.put(transcript_expressions)


def write_output(results: Dict[str, TranscriptionExpressResults], stats: ReferenceStatistics, out) -> None:

    writer = csv.writer(out)
    fieldnames = ['TE', 'non-unique_reads', 'unique_reads', 'randomly_mapped_reads', 'split_reads', 'fraction_unique',
                  'exp_distinct_reads', 'exp_unique_reads', 'RPU']
    writer.writerow(fieldnames)

    for k, v in results.items():
        writer.writerow((k, v.non_unique_reads, v.unique_reads, v.randomly_mapped_reads, v.split_reads,
                         v.unique_over_total(), 0, 0, 0))


def merge_results(results: List[Dict[str, TranscriptionExpressResults]]) -> Dict[str, TranscriptionExpressResults]:
    all_keys = reduce(set.union, [set(result.keys()) for result in results])
    d = {k: TranscriptionExpressResults() for k in all_keys}

    for result in results:
        for key in result.keys():
            d[key].non_unique_reads += result[key].non_unique_reads
            d[key].unique_reads += result[key].unique_reads
            d[key].randomly_mapped_reads += result[key].randomly_mapped_reads
            d[key].split_reads += result[key].split_reads

    return d


def run_pblat(referenceFile: str, readFile: str, pipePattern: str = None,
        readLength = 10, outfile=None, t=None, q=None, prot=False, ooc=None,
        threads=-1, tileSize=-1, stepSize=-1, oneOff=-1, minMatch=-1,
        minScore=-1, minIdentity=-1.0, maxGap=-1, noHead=False, makeOoc=None,
        repMatch=-1, mask=None, qMask=None, repeats=None,
        minRepDivergence=-1.0, dots=-1, trimT=False, noTrimA=False,
        trimHardA=False, fastMap=False, out_fmt=None, maxIntron=-1,
        extendThroughN=False):

    if referenceFile is None:
        print("You must specify a reference file", file=sys.stderr)
        return

    if readFile is None:
        print("You must specify a query file", file=sys.stderr)
        return

    if outfile == None:
        outfile = sys.stdout

    if pipePattern is None:
        pipePattern = gettempdir()

    if threads == -1:
        threads = len(os.sched_getaffinity(0))

    if out_fmt is None:
        out_fmt = "pslx"

    if tileSize == -1:
        tileSize = 10

    named_pipes = make_fifos(pipePattern, threads)
    stats = make_reference_statistics(readLength, Path(referenceFile))

    # First setup the receiving end of the pipes and start them.
    queue = Queue()
    reading_subprocesses = [Process(target=read_named_pipe, args=(path, queue)) for path in named_pipes]
    for reader in reading_subprocesses:
        reader.start()

    _pblat.blatWithArgs(bytes(referenceFile, UTF8), bytes(readFile, UTF8),
                        bytes(pipePattern, UTF8), t, q, prot, ooc, threads, tileSize,
                        stepSize, oneOff, minMatch, minScore, minIdentity, maxGap, noHead,
                        makeOoc, repMatch, mask, qMask, repeats, minRepDivergence, dots,
                        trimT, noTrimA, trimHardA, fastMap, bytes(out_fmt, UTF8), False,
                        maxIntron, extendThroughN)

    results = [queue.get() for _ in range(threads)]
    merged_result = merge_results(results)

    write_output(merged_result, stats, outfile)

    # Wait for all the readers to terminate
    for subprocess in reading_subprocesses:
        subprocess.join()

    # cleanup fifos
    rm_files(named_pipes)
