import pickle

from pathlib import Path
from typing  import Any, Dict, Optional


def load_data(datadir: Path, name: str) -> Any:
	path = Path(datadir).joinpath(f'{name}.pickle')
	with open(path, 'rb') as f:
		return pickle.load(f)


def try_load_data(datadir: Path, name: str) -> Optional[Any]:
	try:
		return load_data(datadir, name)
	except IOError:
		return None


def save_data(datadir: Path, data: Dict[str, Any], replace: bool = False) -> None:
	for name, df in data.items():
		mode = 'wb' if replace else 'xb'
		path = Path(datadir).joinpath(f'{name}.pickle')
		path.parent.mkdir(parents=True, exist_ok=True)
		with open(path, mode) as f:
			pickle.dump(df, f)
