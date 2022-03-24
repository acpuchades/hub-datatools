from pathlib import Path

from pandas import DataFrame

from projects import Project, project
from projects._followup import load_followup_data, resample_followup_data
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


@project('precision_als')
class PrecisionALS(Project):

	def __init__(self, datadir: Path):
		patients = load_data(datadir, 'ufmn/patients')
		patients = self._patients = patients[patients.fecha_dx.notna()]

		self._urg_episodes = load_data(datadir, 'hub_urg/episodes')
		self._urg_diagnoses = load_data(datadir, 'hub_urg/diagnoses')
		self._hosp_episodes = load_data(datadir, 'hub_hosp/episodes')
		self._hosp_diagnoses = load_data(datadir, 'hub_hosp/diagnoses')

		followups = load_followup_data(datadir)
		self._followups = self._patients.merge(followups, on='id_paciente')

	def describe(self) -> None:
		print()
		print(' .:: PRECISION ALS ::.')
		print()

		print(' Biogen Extant Task 1')
		print(' --------------------')
		print(f' > Total number of cases: {len(self._patients)}')
		print(f' > Current number of living cases: {len(self._patients[~self._patients.exitus])}')

		gene_info = self._patients[self._patients[GENE_FIELDS.values()].notna().any(axis=1)]
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
		print(f' > Time to death: {len(self._patients[self._patients.fecha_exitus.notna()])}')
		print()

		print(' Biogen Extant Task 4')
		print(' --------------------')
		print(f' > Patients on Riluzole: {sum(self._patients.riluzol.fillna(False))}')
		print(f' > Time to Riluzole data available: {sum(self._patients.inicio_riluzol.notna())}')
		print(f' > Patients with symptomatic treatment data available: (pending)')
		print()

		print(' Biogen Extant Task 5')
		print(' --------------------')
		print(f' > Patients with hospitalization data available: {len(self._patients[self._patients.nhc.notna()])}')
		print(f' > Patients with ER consultation data available: {len(self._patients[self._patients.nhc.notna()])}')
		print()

		print(' Biogen Extant Task 6')
		print(' --------------------')
		print(f' > Patients with working status data available: (pending)')
		print(f' > Patients with level of assistance data available: (pending)')
		print()

	def export_data(self) -> DataFrame:
		return DataFrame({
			'site': 'Bellvitge Barcelona',
			'patient_id': self._patients.index,

			'birthdate': self._patients.fecha_nacimiento,
			'sex': self._patients.sexo.map(SEX_CATEGORIES),

			'c9_status': self._patients.estado_c9.map(GENE_STATUS_CATEGORIES),
			'sod1_status': self._patients.estado_sod1.map(GENE_STATUS_CATEGORIES),
			'atxn2_status': self._patients.estado_atxn2.map(GENE_STATUS_CATEGORIES),

			'clinical_onset': self._patients.inicio_clinica,
			'als_dx': self._patients.fecha_dx,
			'last_followup': self._followups.groupby('id_paciente').fecha_visita.max(),

			'riluzole_received': self._patients.riluzol,
			'riluzole_initiation': self._patients.inicio_riluzol,

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

			'walking_aids': self._followups[self._followups.caminar <= 2].groupby('id_paciente').fecha_visita.min(),
			'cpap_initiation': self._followups.groupby('id_paciente').inicio_cpap.min(),
			'nivs_initiation': self._followups.groupby('id_paciente').inicio_vmni.min(),
			'ivs_initiation': self._followups[self._followups.insuf_resp == 0].groupby('id_paciente').fecha_visita.min(),
			'peg_initiation': self._followups.groupby('id_paciente').fecha_colocacion_peg.min(),
			'food_thickener_initiation': self._followups.groupby('id_paciente').inicio_espesante.min(),
			'oral_supl_initiation': self._followups.groupby('id_paciente').inicio_supl_oral.min(),
			'enteric_supl_initiation': self._followups.groupby('id_paciente').inicio_supl_enteral.min(),

			'death': self._patients.fecha_exitus,
		}).set_index(['site', 'patient_id'])