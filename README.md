## pypblat
#### A python-driven wrapper for pblat

This program uses Python 3.6 with pipenv. If you don't have pipenv, you can install it following the instructions [here](https://pipenv.pypa.io/en/latest/).  
To install the program:

1. Clone this repository
2. run `make sharedlib` in the repository's root directory
3. run `pipenv install --dev` from the repository's root directory.
4. run `pipenv shell` to activate the python environment

To run the program:
1. There is a pypblat program that is now on your path.  Run `pypblat --help` to see the available commands. For now there's only one, `copy-count`. You can run `pypblat copy-count --help` to see the available flags for that program. 
2. A sample run could look like:

```
pypblat copy-count /mnt/nas/Shared/Nate/COPIA_DM_copies_plus_shCopia.fa /mnt/nas/Shared/Nate/clipped.fasta
```

