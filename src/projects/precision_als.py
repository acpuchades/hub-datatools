from pathlib import Path

import pandas as pd

from projects import Project, project
from projects.followup import load_followup_data
from serialize import load_data


GENE_FIELDS = {
	 'C9orf72': 'estado_c9',
	    'SOD1': 'estado_sod1',
	   'ATXN2': 'estado_atxn2',
}

GENE_STATUS_CATEGORIES = {
	  'Normal': 'Normal',
	'Alterado': 'Altered',
}

SEX_CATEGORIES = {
	'Hombre': 'Male',
	 'Mujer': 'Female'
}

ALSFRS_COLUMNS = [
	'alsfrs_total_c',
	'alsfrs_bulbar_c',
	'alsfrs_motorf_c',
	'alsfrs_motorg_c',
	'alsfrs_resp_c',
]


def _resample_patient_followups(df: pd.DataFrame, start: pd.Series, freq: str):

	def _resample_helper(group: pd.DataFrame):
		assert(group.id_paciente.nunique() == 1)
		pid = group.iloc[0].id_paciente
		end = group.index.max()
		begin = None
		if pid in start.index:
			begin = start[pid]
		if pd.isna(begin):
			begin = end
		index = pd.date_range(name='fecha', start=begin, end=end, freq='D')
		return group.reindex(index).ffill().asfreq(freq)

	df = df.set_index('fecha_visita').groupby('id_paciente').apply(_resample_helper)
	df = df.drop('id_paciente', axis=1).reset_index()
	return df


@project('precision_als')
class PrecisionALS(Project):

	def __init__(self, datadir: Path):
		patients = load_data(datadir, 'ufmn/patients')
		self._patients = patients[patients.fecha_dx.notna()]
		self._followups = load_followup_data(datadir)
		self._urg_episodes = load_data(datadir, 'hub_urg/episodes')
		self._urg_diagnoses = load_data(datadir, 'hub_urg/diagnoses')
		self._hosp_episodes = load_data(datadir, 'hub_hosp/episodes')
		self._hosp_diagnoses = load_data(datadir, 'hub_hosp/diagnoses')

	def describe(self) -> None:
		print()
		print(' .:: PRECISION ALS ::.')
		print()

		print(' Biogen Extant Task 1')
		print(' --------------------')
		print(f' > Total number of cases: {len(self._cases)}')
		print(f' > Current number of living cases: {len(self._cases[~self._cases.exitus])}')

		gene_info = self._cases[self._cases[GENE_FIELDS.values()].notna().any(axis=1)]
		print(f' > Number of cases with some genetic data available: {len(gene_info)}')
		for name, field in GENE_FIELDS.items():
			print(f'\t * {name} -> {sum(gene_info[field].notna())} (altered: {sum(gene_info[field] == "Alterado")})')
		print()

		print(' Biogen Extant Task 2')
		print(' --------------------')
		print(' > Number of cases with follow-up data available:')
		print(f'\t * 1+ follow-up -> {len(self._followups.value_counts("id_paciente"))}')
		print(f'\t * 2+ follow-up -> {sum(self._followups.value_counts("id_paciente") >= 2)}')
		print()

		print(' Biogen Extant Task 3')
		print(' --------------------')
		print(f' > Time to ambulation support: {len(self._followups[self._followups.caminar <= 2].value_counts("id_paciente"))}')
		print(f' > Time to CPAP: {len(self._followups[self._followups.inicio_cpap.notna()].value_counts("id_paciente"))}')
		print(f' > Time to VMNI: {len(self._followups[self._followups.inicio_vmni.notna()].value_counts("id_paciente"))}')
		print(f' > Time to PEG: {len(self._followups[self._followups.fecha_indicacion_peg.notna()].value_counts("id_paciente"))}')
		print(f' > Time to MiToS: {len(self._followups[self._followups.mitos_c.notna()].value_counts("id_paciente"))}')
		for n in range(5):
			print(f'\t * Stage {n} -> {len(self._followups[self._followups.mitos_c == n].value_counts("id_paciente"))}')
		print(f' > Time to King\'s: {len(self._followups[self._followups.kings_c.notna()].value_counts("id_paciente"))}')
		for n in range(5):
			print(f'\t * Stage {n} -> {len(self._followups[self._followups.kings_c == n].value_counts("id_paciente"))}')
		print(f' > Time to death: {len(self._cases[self._cases.fecha_exitus.notna()])}')
		print()

		print(' Biogen Extant Task 4')
		print(' --------------------')
		print(f' > Patients on Riluzole: {sum(self._cases.riluzol.fillna(False))}')
		print(f' > Time to Riluzole data available: {sum(self._cases.inicio_riluzol.notna())}')
		print(f' > Patients with symptomatic treatment data available: (pending)')
		print()

		print(' Biogen Extant Task 5')
		print(' --------------------')
		print(f' > Patients with hospitalization data available: {len(self._cases[self._cases.nhc.notna()])}')
		print(f' > Patients with ER consultation data available: {len(self._cases[self._cases.nhc.notna()])}')
		print()

		print(' Biogen Extant Task 6')
		print(' --------------------')
		print(f' > Patients with working status data available: (pending)')
		print(f' > Patients with level of assistance data available: (pending)')
		print()

	def export_data(self) -> pd.DataFrame:
		from_dx = _resample_patient_followups(self._followups, start=self._patients.fecha_dx, freq='M')
		from_onset = _resample_patient_followups(self._followups, start=self._patients.inicio_clinica, freq='M')

		return pd.DataFrame({
			'site': 'Bellvitge Barcelona',
			'patient_id': self._patients.cip,

			'birthdate': self._patients.fecha_nacimiento,
			'sex': self._patients.sexo.map(SEX_CATEGORIES),

			'c9_status': self._patients.estado_c9.map(GENE_STATUS_CATEGORIES),
			'sod1_status': self._patients.estado_sod1.map(GENE_STATUS_CATEGORIES),
			'atxn2_status': self._patients.estado_atxn2.map(GENE_STATUS_CATEGORIES),

			'clinical_onset': self._patients.inicio_clinica,
			'als_dx': self._patients.fecha_dx,

			'kings_0': self._followups[self._followups.kings_c == 0].groupby('id_paciente').fecha_visita.min(),
			'kings_1': self._followups[self._followups.kings_c == 1].groupby('id_paciente').fecha_visita.min(),
			'kings_2': self._followups[self._followups.kings_c == 2].groupby('id_paciente').fecha_visita.min(),
			'kings_3': self._followups[self._followups.kings_c == 3].groupby('id_paciente').fecha_visita.min(),
			'kings_4': self._followups[self._followups.kings_c == 4].groupby('id_paciente').fecha_visita.min(),

			'mitos_0': self._followups[self._followups.mitos_c == 0].groupby('id_paciente').fecha_visita.min(),
			'mitos_1': self._followups[self._followups.mitos_c == 1].groupby('id_paciente').fecha_visita.min(),
			'mitos_2': self._followups[self._followups.mitos_c == 2].groupby('id_paciente').fecha_visita.min(),
			'mitos_3': self._followups[self._followups.mitos_c == 3].groupby('id_paciente').fecha_visita.min(),
			'mitos_4': self._followups[self._followups.mitos_c == 4].groupby('id_paciente').fecha_visita.min(),

			'death': self._patients.fecha_exitus,

			'alsfrs_dx': from_dx.groupby('id_paciente').nth(0).alsfrs_total_c,
			'alsfrs_dx_m3': from_dx.groupby('id_paciente').nth(3).alsfrs_total_c,
			'alsfrs_dx_y1': from_dx.groupby('id_paciente').nth(1 * 12).alsfrs_total_c,
			'alsfrs_dx_y2': from_dx.groupby('id_paciente').nth(2 * 12).alsfrs_total_c,
			'alsfrs_dx_y3': from_dx.groupby('id_paciente').nth(3 * 12).alsfrs_total_c,
			'alsfrs_dx_y4': from_dx.groupby('id_paciente').nth(4 * 12).alsfrs_total_c,
			'alsfrs_dx_y5': from_dx.groupby('id_paciente').nth(5 * 12).alsfrs_total_c,

			'alsfrs_onset_m3': from_onset.groupby('id_paciente').nth(3).alsfrs_total_c,
			'alsfrs_onset_y1': from_onset.groupby('id_paciente').nth(1 * 12).alsfrs_total_c,
			'alsfrs_onset_y2': from_onset.groupby('id_paciente').nth(2 * 12).alsfrs_total_c,
			'alsfrs_onset_y3': from_onset.groupby('id_paciente').nth(3 * 12).alsfrs_total_c,
			'alsfrs_onset_y4': from_onset.groupby('id_paciente').nth(4 * 12).alsfrs_total_c,
			'alsfrs_onset_y5': from_onset.groupby('id_paciente').nth(5 * 12).alsfrs_total_c,

			'kings_dx': from_dx.groupby('id_paciente').nth(0).kings_c,
			'kings_dx_m3': from_dx.groupby('id_paciente').nth(3).kings_c,
			'kings_dx_y1': from_dx.groupby('id_paciente').nth(1 * 12).kings_c,
			'kings_dx_y2': from_dx.groupby('id_paciente').nth(2 * 12).kings_c,
			'kings_dx_y3': from_dx.groupby('id_paciente').nth(3 * 12).kings_c,
			'kings_dx_y4': from_dx.groupby('id_paciente').nth(4 * 12).kings_c,
			'kings_dx_y5': from_dx.groupby('id_paciente').nth(5 * 12).kings_c,

			'kings_onset_m3': from_onset.groupby('id_paciente').nth(3).kings_c,
			'kings_onset_y1': from_onset.groupby('id_paciente').nth(1 * 12).kings_c,
			'kings_onset_y2': from_onset.groupby('id_paciente').nth(2 * 12).kings_c,
			'kings_onset_y3': from_onset.groupby('id_paciente').nth(3 * 12).kings_c,
			'kings_onset_y4': from_onset.groupby('id_paciente').nth(4 * 12).kings_c,
			'kings_onset_y5': from_onset.groupby('id_paciente').nth(5 * 12).kings_c,

			'mitos_dx': from_dx.groupby('id_paciente').nth(0).mitos_c,
			'mitos_dx_m3': from_dx.groupby('id_paciente').nth(3).mitos_c,
			'mitos_dx_y1': from_dx.groupby('id_paciente').nth(1 * 12).mitos_c,
			'mitos_dx_y2': from_dx.groupby('id_paciente').nth(2 * 12).mitos_c,
			'mitos_dx_y3': from_dx.groupby('id_paciente').nth(3 * 12).mitos_c,
			'mitos_dx_y4': from_dx.groupby('id_paciente').nth(4 * 12).mitos_c,
			'mitos_dx_y5': from_dx.groupby('id_paciente').nth(5 * 12).mitos_c,

			'mitos_onset_m3': from_onset.groupby('id_paciente').nth(3).mitos_c,
			'mitos_onset_y1': from_onset.groupby('id_paciente').nth(1 * 12).mitos_c,
			'mitos_onset_y2': from_onset.groupby('id_paciente').nth(2 * 12).mitos_c,
			'mitos_onset_y3': from_onset.groupby('id_paciente').nth(3 * 12).mitos_c,
			'mitos_onset_y4': from_onset.groupby('id_paciente').nth(4 * 12).mitos_c,
			'mitos_onset_y5': from_onset.groupby('id_paciente').nth(5 * 12).mitos_c,

			'vmni_initiation': self._followups.groupby('id_paciente').inicio_vmni.min(),
			'peg_initiation': self._followups.groupby('id_paciente').fecha_colocacion_peg.min(),
			'oral_supl_initiation': self._followups.groupby('id_paciente').inicio_supl_oral.min(),
			'enteric_supl_initiation': self._followups.groupby('id_paciente').inicio_supl_enteral.min(),

			'riluzole_received': self._patients.riluzol,
			'riluzole_initiation': self._patients.inicio_riluzol,
		}).reset_index(drop=True)
