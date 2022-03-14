import pickle

from pandas  import DataFrame
from pathlib import Path
from typing  import Any, Mapping, Optional


def load_data(datadir: str |  Path, name: str) -> Any:
	fname = Path(datadir) / f'{name}.pickle'
	with open(fname, 'rb') as f:
		return pickle.load(f)


def try_load_data(datadir: str | Path, name: str) -> Optional[Any]:
	try:
		return load_data(datadir, name)
	except IOError:
		return None


def save_data(datadir: str | Path, data: Mapping[str, Any], replace: bool = False) -> None:
	datadir = Path(datadir)
	datadir.mkdir(parents=True, exist_ok=True)
	for key, df in data.items():
		mode = 'wb' if replace else 'xb'
		with open(datadir / f'{key}.pickle', mode) as f:
			pickle.dump(df, f)
