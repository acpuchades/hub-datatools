import logging
from pathlib import Path
from typing import Dict

from pandas import DataFrame

from hub_datatools.serialize import load_data
from hub_datatools.projects import Project, project
from hub_datatools.projects._followup import load_followup_data


@project('als-geo')
class ALSGeo(Project):

    def __init__(self, datadir: Path):
        patients = load_data(datadir, 'ufmn/patients').sort_index()
        patients = patients[patients.fecha_exitus.isna()]
        patients = patients[patients.fecha_dx.notna()]
        followups = load_followup_data(datadir)
        followups = followups.merge(patients, on='id_paciente')
        self._patients = followups.drop_duplicates(subset=['id_paciente'])

    def _count_patients_by(self, by: str, count: str = 'Nº pacientes') -> DataFrame:
        df = self._patients.groupby(by).id_paciente.count()
        return df.rename(index=count).sort_values(ascending=False)

    def export_data(self) -> DataFrame | Dict[str, DataFrame]:
        logging.info('ALS-GEO: Exporting patients location data')

        return {
            'Municipio': self._count_patients_by('municipio_residencia').rename_axis('Municipio'),
            'Provincia': self._count_patients_by('provincia_residencia').rename_axis('Provincia'),
            'Código postal': self._count_patients_by('codigo_postal').rename_axis('Código postal').sort_index(),
        }
