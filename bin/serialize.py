import pickle

from pathlib import Path


def load_data(datadir, name):
	fname = Path(datadir) / f'{name}.pickle'
	with open(fname, 'rb') as f:
		return pickle.load(f)


def save_data(datadir, data, replace=False):
	datadir = Path(datadir)
	datadir.mkdir(parents=True, exist_ok=True)
	for key, df in data.items():
		mode = 'wb' if replace else 'xb'
		with open(datadir / f'{key}.pickle', mode) as f:
			pickle.dump(df, f)
