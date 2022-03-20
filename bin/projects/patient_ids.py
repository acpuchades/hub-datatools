from pathlib import Path

from pandas import DataFrame

from projects  import Project, project
from serialize import load_data


EXPORT_COLUMNS = [
	'nhc',
	'dni',
	'cip',
]

@project('patient_ids')
class PatientIDs(Project):

	def __init__(self, datadir: Path):
		self._patients = load_data(datadir, 'ufmn/patients')

	def export_data(self) -> DataFrame:
		patients = self._patients[EXPORT_COLUMNS]
		return patients.reset_index(drop=True)
