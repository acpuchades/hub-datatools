from argparse  import ArgumentParser, Namespace
from abc       import ABC, abstractmethod, abstractstaticmethod
from importlib import import_module
from pathlib   import Path
from typing    import Dict, List, Optional

from pandas    import DataFrame


class DataSource(ABC):

	@abstractstaticmethod
	def add_arguments(parser: ArgumentParser) -> None:
		pass

	@abstractstaticmethod
	def has_arguments(args: Namespace) -> bool:
		pass

	@abstractmethod
	def load_data(self, args: Namespace) -> Dict[str, DataFrame]:
		pass


_registered_datasources: Dict[str, type[DataSource]] = dict()


def datasource(name: str):
	def datasource_decorator_helper(cls):
		_registered_datasources.setdefault(name, cls)
		return cls

	return datasource_decorator_helper


def get_datasource_names() -> List[str]:
	return _registered_datasources.keys()


def get_datasource_class(name: str) -> Optional[type[DataSource]]:
	return _registered_datasources.get(name)


def load_datasource_modules() -> None:
	sourcesdir = Path(__file__).parent
	for modname in sourcesdir.iterdir():
		if not modname.name.startswith('_'):
			import_module(f'datasources.{modname.stem}')
