from pathlib import Path
from typing  import Dict, List, Protocol

from pandas import DataFrame


class Project(Protocol):

	@staticmethod
	def describe(datadir: str) -> None:
		pass

	@staticmethod
	def export_data(datadir: str) -> DataFrame | Dict[str, DataFrame]:
		pass


def project_names() -> List[str]:
	projdir = Path(__file__).parent
	return [ d.stem for d in projdir.iterdir()
	                if not d.name.startswith('_' ) ]