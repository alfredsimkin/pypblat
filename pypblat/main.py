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
@click.option('--identity-cutoff', default=0.95)
@click.option('--read-length', default=43)
def copy_count(reference_file: Path, read_file: Path, identity_cutoff: float, read_length: int):
    pypblat.run_pblat(str(reference_file), str(read_file), noHead=True, minIdentity=identity_cutoff,
                      readLength=read_length)


cli.add_command(copy_count)


if __name__ == "__main__":
    cli()
