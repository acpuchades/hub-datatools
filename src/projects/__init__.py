from abc       import ABC, abstractmethod
from importlib import import_module
from pathlib   import Path
from typing    import Dict, List, Optional

from pandas    import DataFrame


class Project(ABC):

	@abstractmethod
	def export_data(self) -> DataFrame | Dict[str, DataFrame]:
		pass


_registered_projects: Dict[str, type[Project]] = dict()


def project(name: str):
	def project_decorator_helper(cls):
		_registered_projects.setdefault(name, cls)
		return cls

	return project_decorator_helper


def get_project_names() -> List[str]:
	return _registered_projects.keys()


def get_project_class(name: str) -> Optional[type[Project]]:
	return _registered_projects.get(name)


def load_project_modules() -> None:
	projectdir = Path(__file__).parent
	for modname in projectdir.iterdir():
		if not modname.name.startswith('_'):
			import_module(f'projects.{modname.stem}')
