from pathlib import Path

from pandas import DataFrame

from serialize import load_data


def export_data(datadir: Path) -> DataFrame:
	patients = load_data(datadir, 'ufmn/patients')
	patients = patients[['nhc', 'dni', 'cip']]
	return patients.reset_index(drop=True)
