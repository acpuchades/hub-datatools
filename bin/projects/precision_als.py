from pathlib   import Path

from pandas    import DataFrame

from projects  import Project, project
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


def load_ufmn_patient_data(datadir: Path, name: str, patients: DataFrame) -> DataFrame:
	return load_data(datadir, name).merge(patients, left_on='id_paciente', right_index=True)


def load_hub_episode_diagnoses(datadir: Path, name: str, episodes: DataFrame) -> DataFrame:
	return load_data(datadir, name).merge(episodes, left_on='id_episodio', right_index=True)


@project(name='precision_als')
class PrecisionALS(Project):

	def __init__(self, datadir: Path):
		patients = load_data(datadir, 'ufmn/patients')
		patients = patients[patients.fecha_dx.notna()]
		self._patients = patients

		self._als_data  = load_ufmn_patient_data(datadir,  'ufmn/als_data', patients)
		self._resp_data = load_ufmn_patient_data(datadir, 'ufmn/resp_data', patients)
		self._nutr_data = load_ufmn_patient_data(datadir, 'ufmn/nutr_data', patients)

		urg_episodes = load_data(datadir, 'hub_urg/episodes')
		urg_episodes = urg_episodes.merge(patients, on='nhc').set_index('nhc')
		self._urg_episodes = urg_episodes

		hosp_episodes = load_data(datadir, 'hub_hosp/episodes')
		hosp_episodes = hosp_episodes.merge(patients, on='nhc').set_index('nhc')
		self._hosp_episodes = hosp_episodes

		self._urg_diagnoses = load_hub_episode_diagnoses(datadir, 'hub_urg/diagnoses', urg_episodes)
		self._hosp_diagnoses = load_hub_episode_diagnoses(datadir, 'hub_hosp/diagnoses', hosp_episodes)


	def describe(self) -> None:
		gene_info = self._patients[['estado_c9', 'estado_sod1', 'estado_atxn2']]
		gene_cases = self._patients[gene_info.notna().any(axis='columns')]

		print()
		print(' .:: PRECISION ALS ::.')
		print()

		print(' Biogen Extant Task 1')
		print(' --------------------')
		print(f' > Total number of cases: {len(self._patients)}')
		print(f' > Current number of living cases: {len(self._patients[~self._patients.exitus])}')

		print(f' > Number of cases with genetic testing available: {len(gene_cases)}')
		for name, field in GENE_FIELDS.items():
			gene_status = self._patients[field]
			print(f'\t * {name} -> {len(self._patients[gene_status.notna()])} (altered: {len(self._patients[gene_status == "Alterado"])})')

		print()

		print(' Biogen Extant Task 2')
		print(' --------------------')
		print(' > Number of cases with follow-up data available:')
		print(f'\t * 1+ follow-up -> {len(self._als_data.value_counts("id_paciente"))}')
		print(f'\t * 2+ follow-up -> {sum(self._als_data.value_counts("id_paciente") >= 2)}')
		print()

		print(' Biogen Extant Task 3')
		print(' --------------------')
		print(f' > Time to ambulation support: {len(self._als_data[self._als_data.caminar <= 2].value_counts("id_paciente"))}')
		print(f' > Time to CPAP: {len(self._resp_data[self._resp_data.cpap.fillna(False)].value_counts("id_paciente"))}')
		print(f' > Time to VMNI: {len(self._resp_data[self._resp_data.portador_vmni.fillna(False)].value_counts("id_paciente"))}')
		print(f' > Time to PEG: {len(self._nutr_data[self._nutr_data.indicacion_peg.fillna(False)].value_counts("id_paciente"))}')

		print(f' > Time to MiToS: {len(self._als_data[self._als_data.mitos.notna()].value_counts("id_paciente"))}')
		for n in range(1, 5):
			print(f'\t * Stage {n} -> {len(self._als_data[self._als_data.mitos == n].value_counts("id_paciente"))}')

		print(f' > Time to King\'s: {len(self._als_data[self._als_data.kings.notna()].value_counts("id_paciente"))}')
		for n in range(1, 5):
			print(f'\t * Stage {n} -> {len(self._als_data[self._als_data.kings == n].value_counts("id_paciente"))}')

		print(f' > Time to death: {len(self._patients[~self._patients.fecha_exitus.isna()])}')
		print()

		print(' Biogen Extant Task 4')
		print(' --------------------')
		print(f' > Patients on Riluzole: {len(self._patients[self._patients.riluzol.fillna(False)])}')
		print(f' > Time to Riluzole data available: {len(self._patients[~self._patients.inicio_riluzol.isna()])}')
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
			'patient_id': self._patients.cip,

			'birthdate': self._patients.fecha_nacimiento,
			'sex': self._patients.sexo.replace({ 'Hombre': 'Male', 'Mujer': 'Female' }),

			'c9_status': self._patients.estado_c9.replace(GENE_STATUS_CATEGORIES),
			'sod1_status': self._patients.estado_sod1.replace(GENE_STATUS_CATEGORIES),
			'fus_status': None,
			'tardbp_status': None,

			'clinical_onset': self._patients.inicio_clinica,
			'als_dx': self._patients.fecha_dx,

			'kings_1': self._als_data[self._als_data.kings == 1].groupby('id_paciente').fecha_visita.min(),
			'kings_2': self._als_data[self._als_data.kings == 2].groupby('id_paciente').fecha_visita.min(),
			'kings_3': self._als_data[self._als_data.kings == 3].groupby('id_paciente').fecha_visita.min(),
			'kings_4': self._als_data[self._als_data.kings == 4].groupby('id_paciente').fecha_visita.min(),

			'mitos_1': self._als_data[self._als_data.mitos == 1].groupby('id_paciente').fecha_visita.min(),
			'mitos_2': self._als_data[self._als_data.mitos == 2].groupby('id_paciente').fecha_visita.min(),
			'mitos_3': self._als_data[self._als_data.mitos == 3].groupby('id_paciente').fecha_visita.min(),
			'mitos_4': self._als_data[self._als_data.mitos == 4].groupby('id_paciente').fecha_visita.min(),

			'death': self._patients.fecha_exitus,

			'alsfrs_baseline': None,
			'alsfrs_dx_y1': None,
			'alsfrs_dx_y2': None,
			'alsfrs_dx_y3': None,
			'alsfrs_dx_y4': None,
			'alsfrs_dx_y5': None,

			'alsfrs_resp_baseline': None,
			'alsfrs_bulbar_dx_y1': None,
			'alsfrs_bulbar_dx_y2': None,
			'alsfrs_bulbar_dx_y3': None,
			'alsfrs_bulbar_dx_y4': None,
			'alsfrs_bulbar_dx_y5': None,

			'vmni_initiation': None,
			'peg_initiation': None,
			'oral_supl_initiation': None,
			'enteric_supl_initiation': None,

			'riluzole_received': self._patients.riluzol,
			'riluzole_initiation': self._patients.inicio_riluzol,

			'fvc_sitting_baseline': None,
			'fvc_sitting_dx_y1': None,
			'fvc_sitting_dx_y2': None,
			'fvc_sitting_dx_y3': None,
			'fvc_sitting_dx_y4': None,
			'fvc_sitting_dx_y5': None,

			'fvc_lying_baseline': None,
			'fvc_lying_dx_y1': None,
			'fvc_lying_dx_y2': None,
			'fvc_lying_dx_y3': None,
			'fvc_lying_dx_y4': None,
			'fvc_lying_dx_y5': None,

		}).reset_index(drop=True)
