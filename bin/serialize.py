import pickle

from pathlib import Path
from typing  import Any, Dict, Optional


def load_data(datadir: Path, name: str) -> Any:
		return pickle.load(f)


def try_load_data(datadir: Path, name: str) -> Optional[Any]:
	try:
		return load_data(datadir, name)
	except IOError:
		return None


def save_data(datadir: Path, data: Dict[str, Any], replace: bool = False) -> None:
	for name, df in data.items():
		mode = 'wb' if replace else 'xb'
		with open(datadir / f'{key}.pickle', mode) as f:
			pickle.dump(df, f)
