from argparse import ArgumentParser, Namespace
from pathlib  import Path
from typing   import Dict, List, Protocol

from pandas import DataFrame


class DataSource(Protocol):

	@staticmethod
	def add_data_source_arguments(parser: ArgumentParser) -> None:
		pass

	@staticmethod
	def load_data(args: Namespace) -> Dict[str, DataFrame]:
		pass


def data_source_names() -> List[str]:
	srcdir = Path(__file__).parent
	return [ d.stem for d in srcdir.iterdir()
	                if not d.name.startswith('_' ) ]