import sys
import click
from pathlib import Path
from . import pypblat


@click.group()
def cli():
    # align blat 2

    # RPKM calculator4.3
    pass


@click.command()
@click.argument('reference-file', type=click.Path(exists=True))
@click.argument('read-file', type=click.Path(exists=True))
@click.option('--identity-cutoff', default=0.95)
def copy_count(reference_file: Path, read_file: Path, identity_cutoff: float):
    print(f'reference file: {reference_file}\nread file: {read_file}\nidentity cutoff: {identity_cutoff}')

    pypblat.run_pblat(str(reference_file), str(read_file), noHead=True, minIdentity=identity_cutoff)


cli.add_command(copy_count)


if __name__ == "__main__":
    cli(len(sys.argv), sys.argv)
