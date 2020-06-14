from tempfile import gettempdir
from ctypes import *
import sys
import os
from pathlib import Path
from multiprocessing import Process
from typing import List

_pblat = cdll.LoadLibrary('python/libpblat.so')
# int blatWithArgs(char *t, char* q, boolean prot, char *ooc, int threads, int tileSize, int stepSize, int oneOff, int minMatch, int minSocre, float minIdentity, int maxGap, 
#         boolean noHead, char *makeOoc, int repMatch, char *mask, char *qMask, char *repeats, float minRepDivergence, int dots, boolean trimT, boolean noTrimA, boolean trimHardA,
#         boolean fastMap, char *out, boolean fine, int maxIntron, boolean extendThroughN)
_pblat.blatWithArgs.argtypes = [
        c_char_p,                   # referenceFile
        c_char_p,                   # readFile
        c_char_p,                   # pipePattern
        c_char_p,                   # t
        c_char_p,                   # q
        c_bool,                     # prot
        c_char_p,                   # ooc
        c_int,                      # threads
        c_int,                      # tileSize
        c_int,                      # stepSize
        c_int,                      # oneOff,
        c_int,                      # minMatch
        c_int,                      # minScore
        c_float,                    # minIdentity
        c_int,                      # maxGap
        c_bool,                     # noHead
        c_char_p,                   # makeOoc
        c_int,                      # repMatch
        c_char_p,                   # mask
        c_char_p,                   # qMask
        c_char_p,                   # repeats
        c_float,                    # minRepDivergence
        c_int,                      # dots
        c_bool,                     # trimT
        c_bool,                     # noTrimA
        c_bool,                     # trimHardA
        c_bool,                     # fastMap
        c_char_p,                   # out
        c_bool,                     # fine
        c_int,                      # maxIntron
        c_bool                      # extendThroughN
    ]

def _call_with_default_args():
    _pblat.blatWithArgs(
            None, # c_char_p                  referenceFile
            None, # c_char_p                  readFile
            None,# c_char_p                   pipePattern
            None, # c_char_p,                 t
            None, # c_char_p,                 q
            False, # c_bool,                  prot
            None, # c_char_p,                 ooc
            -1, # c_int,                      threads
            -1, # c_int,                      tileSize
            -1, # c_int,                      stepSize
            -1, # c_int,                      oneOff,
            -1, # c_int,                      minMatch
            -1, # c_int,                      minScore
            -1.0, # c_float,                  minIdentity
            -1, # c_int,                      maxGap
            False, # c_bool,                  noHead
            None, # c_char_p,                 makeOoc
            -1, # c_int,                      repMatch
            None, # c_char_p,                 mask
            None, # c_char_p,                 qMask
            None, # c_char_p,                 repeats
            -1.0, # c_float,                  minRepDivergence
            -1, # c_int,                      dots
            False, # c_bool,                  trimT
            False, # c_bool,                  noTrimA
            False, # c_bool,                  trimHardA
            False, # c_bool,                  fastMap
            None, # c_char_p                  out
            False, # c_bool,                  fine
            -1, # c_int,                      maxIntron
            False # c_bool                   extendThroughN
    )

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

def read_named_pipe(path: Path) -> None:
    """Pipes can only be written to if they have a reader on the other end. If there's no reader, any write to the pipe
    pauses. Hence, these are opened before the writers."""
    lines = 0
    with path.open('r') as f:
        for line in f:
            lines += 1
        print(f"PID {os.getpid()} finished! Read {lines} lines.")


def run_pblat(referenceFile: str, readFile: str, pipePattern: str=None, t=None, q=None,
        prot=False, ooc=None, threads=-1, tileSize=-1, stepSize=-1, oneOff=-1,
        minMatch=-1, minScore=-1, minIdentity=-1.0, maxGap=-1, noHead=False,
        makeOoc=None, repMatch=-1, mask=None, qMask=None, repeats=None,
        minRepDivergence=-1.0, dots=-1, trimT=False, noTrimA=False,
        trimHardA=False, fastMap=False, out=None, maxIntron=-1,
        extendThroughN=False):

    if referenceFile == None:
        print("You must specify a reference file", file=sys.stderr)
        return

    if readFile == None:
        print("You must specify a query file", file=sys.stderr)
        return

    if pipePattern == None:
        pipePattern = gettempdir()

    if threads == -1:
        threads = len(os.sched_getaffinity(0))

    if out == None:
        out = "pslx"

    if tileSize == -1:
        tileSize = 10

    named_pipes = make_fifos(pipePattern, threads)

    # First setup the receiving end of the pipes and start them.
    reading_subprocesses = [Process(target=read_named_pipe, args=(path,)) for path in named_pipes]
    for reader in reading_subprocesses:
        reader.start()

    _pblat.blatWithArgs(bytes(referenceFile, 'utf8'), bytes(readFile, 'utf8'),
            bytes(pipePattern, 'utf8'), t, q, prot, ooc, threads, tileSize,
            stepSize, oneOff, minMatch, minScore, minIdentity, maxGap, noHead,
            makeOoc, repMatch, mask, qMask, repeats, minRepDivergence, dots,
            trimT, noTrimA, trimHardA, fastMap, bytes(out, 'utf8'), False,
            maxIntron, extendThroughN)

    # Wait for all the readers to terminate
    for subprocess in reading_subprocesses:
        subprocess.join()

    # cleanup fifos
    rm_files(named_pipes)

# ./pblat \
#   /home/alfred/big_data/Dropbox/Travis_Alfred_shared/reference_files/TEomes/COPIA_DM_copies_plus_shCopia.fa  \ reference file
#   /home/owynblatt/big_data/Run/simulated_reads/sample_01.fasta  \ read file
#   -threads=30 \
#   -out=pslx \
#   -tileSize=10 \
#   testout

