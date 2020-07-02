import sys
import click
from pathlib import Path
from . import pypblat


@click.group()
def cli():
    pass


@click.command()
@click.argument('reference-file', type=click.Path(exists=True))
@click.argument('read-file', type=click.Path(exists=True))
@click.option('--identity-cutoff', default=0.95, help="The sequence identity (according to pblat) above which a sequence is considered to be a hit and below which the sequence is ignored")
@click.option('--read-length', default=43, help="The length of the sequence reads")
@click.option('--threads', default=-1, help="Number of threads to use for task parallelization")
@click.option('--output', type=click.File('w'), default='-', help="The output file for pypblat. Defaults to stdout.")
def copy_count(reference_file: Path, read_file: Path, identity_cutoff: float, read_length: int, threads: int, output: click.File):
    """Counts the number of transcript expressions from READ-FILE in the genome of REFERENCE-FILE.

    REFERENCE-FILE is the genome or list of genes in FASTA format that the reads should be matched against.
    READ-FILE is the reads from the sequencer.  Specify the length of the reads in the --read-length option.
    """
    pypblat.run_pblat(str(reference_file), str(read_file), noHead=True, minIdentity=identity_cutoff,
                      readLength=read_length, threads=threads, outfile=output)


cli.add_command(copy_count)


if __name__ == "__main__":
    cli()
