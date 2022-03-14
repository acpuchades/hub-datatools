from argparse import Namespace
from pathlib  import Path
from typing   import Any, Dict, List, Protocol

from pandas import DataFrame


class DataSource(Protocol):
	__dict__: Dict[str, Any]

	@staticmethod
	def load_data(args: Namespace) -> Dict[str, DataFrame]:
		pass


def data_source_names() -> List[str]:
	srcdir = Path(__file__).parent
	return [ d.stem for d in srcdir.iterdir()
	                if not d.name.startswith('_' ) ]