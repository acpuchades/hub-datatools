from pathlib import Path

from pandas import DataFrame, Series, Timedelta

from projects import Project, project
from projects._followup import load_followup_data, resample_followup_data
from serialize import load_data

ALSFRS_MAX_VALUE = 4 * 12

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

def _calculate_alsfrs_decline_rate(alsfrs_data: DataFrame) -> Series:
	first_followup = alsfrs_data.groupby('id_paciente').nth(0)
	last_followup = alsfrs_data.groupby('id_paciente').nth(-1)

	dx_delta = first_followup.fecha_visita - first_followup.inicio_clinica
	duration = last_followup.fecha_visita - first_followup.fecha_visita

	alsfrs_first = first_followup.alsfrs_total.where(duration > Timedelta(0), ALSFRS_MAX_VALUE)
	alsfrs_last = last_followup.alsfrs_total
	progression = alsfrs_last - alsfrs_first

	duration.mask(duration == Timedelta(0), dx_delta, inplace=True)
	return -progression / (duration.dt.days / 30)

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
		self._followups = followups = self._patients.merge(followups, on='id_paciente')

		alsfrs_data = followups[followups.alsfrs_total.notna()]
		followup_start = alsfrs_data.groupby('id_paciente').fecha_visita.min()
		self._from_dx = resample_followup_data(alsfrs_data, patients.fecha_dx, freq='3M')
		self._from_followup_start = resample_followup_data(alsfrs_data, followup_start, freq='3M')


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

			'n_followups': self._followups.value_counts('id_paciente').astype('Int64'),
			'first_followup': self._followups.groupby('id_paciente').fecha_visita.min(),
			'last_followup': self._followups.groupby('id_paciente').fecha_visita.max(),

			'riluzole_received': self._patients.riluzol,
			'riluzole_initiation': self._patients.inicio_riluzol,

			'alsfrs_dx': self._from_dx.groupby('id_paciente').nth(0).alsfrs_total,
			'alsfrs_dx_m3': self._from_dx.groupby('id_paciente').nth(1).alsfrs_total,
			'alsfrs_dx_y1': self._from_dx.groupby('id_paciente').nth(1 * 4).alsfrs_total,
			'alsfrs_dx_y2': self._from_dx.groupby('id_paciente').nth(2 * 4).alsfrs_total,
			'alsfrs_dx_y3': self._from_dx.groupby('id_paciente').nth(3 * 4).alsfrs_total,
			'alsfrs_dx_y4': self._from_dx.groupby('id_paciente').nth(4 * 4).alsfrs_total,
			'alsfrs_dx_y5': self._from_dx.groupby('id_paciente').nth(5 * 4).alsfrs_total,

			'alsfrs_followup_start': self._from_followup_start.groupby('id_paciente').nth(0).alsfrs_total,
			'alsfrs_followup_m3': self._from_followup_start.groupby('id_paciente').nth(1).alsfrs_total,
			'alsfrs_followup_m6': self._from_followup_start.groupby('id_paciente').nth(2).alsfrs_total,
			'alsfrs_followup_m9': self._from_followup_start.groupby('id_paciente').nth(3).alsfrs_total,
			'alsfrs_followup_m12': self._from_followup_start.groupby('id_paciente').nth(4).alsfrs_total,
			'alsfrs_followup_m15': self._from_followup_start.groupby('id_paciente').nth(5).alsfrs_total,
			'alsfrs_followup_m18': self._from_followup_start.groupby('id_paciente').nth(6).alsfrs_total,
			'alsfrs_followup_m21': self._from_followup_start.groupby('id_paciente').nth(7).alsfrs_total,
			'alsfrs_followup_m24': self._from_followup_start.groupby('id_paciente').nth(8).alsfrs_total,
			'alsfrs_followup_m27': self._from_followup_start.groupby('id_paciente').nth(9).alsfrs_total,
			'alsfrs_followup_m30': self._from_followup_start.groupby('id_paciente').nth(10).alsfrs_total,
			'alsfrs_followup_m33': self._from_followup_start.groupby('id_paciente').nth(11).alsfrs_total,
			'alsfrs_followup_m36': self._from_followup_start.groupby('id_paciente').nth(12).alsfrs_total,
			'alsfrs_followup_m39': self._from_followup_start.groupby('id_paciente').nth(13).alsfrs_total,
			'alsfrs_followup_m42': self._from_followup_start.groupby('id_paciente').nth(14).alsfrs_total,
			'alsfrs_followup_m45': self._from_followup_start.groupby('id_paciente').nth(15).alsfrs_total,
			'alsfrs_followup_m48': self._from_followup_start.groupby('id_paciente').nth(16).alsfrs_total,
			'alsfrs_followup_m51': self._from_followup_start.groupby('id_paciente').nth(17).alsfrs_total,
			'alsfrs_followup_m54': self._from_followup_start.groupby('id_paciente').nth(18).alsfrs_total,
			'alsfrs_followup_m57': self._from_followup_start.groupby('id_paciente').nth(19).alsfrs_total,
			'alsfrs_followup_m60': self._from_followup_start.groupby('id_paciente').nth(20).alsfrs_total,

			'kings_1': self._followups[self._followups.kings_c == 1].groupby('id_paciente').fecha_visita.min(),
			'kings_2': self._followups[self._followups.kings_c == 2].groupby('id_paciente').fecha_visita.min(),
			'kings_3': self._followups[self._followups.kings_c == 3].groupby('id_paciente').fecha_visita.min(),
			'kings_4': self._followups[self._followups.kings_c == 4].groupby('id_paciente').fecha_visita.min(),

			'mitos_1': self._followups[self._followups.mitos_c == 1].groupby('id_paciente').fecha_visita.min(),
			'mitos_2': self._followups[self._followups.mitos_c == 2].groupby('id_paciente').fecha_visita.min(),
			'mitos_3': self._followups[self._followups.mitos_c == 3].groupby('id_paciente').fecha_visita.min(),
			'mitos_4': self._followups[self._followups.mitos_c == 4].groupby('id_paciente').fecha_visita.min(),

			'walking_aids': self._followups[self._followups.caminar <= 2].groupby('id_paciente').fecha_visita.min(),
			'cpap_initiation': self._followups.groupby('id_paciente').inicio_cpap.min(),
			'niv_support': self._followups.groupby('id_paciente').inicio_vmni.min(),
			'imv_support': self._followups[self._followups.insuf_resp == 0].groupby('id_paciente').fecha_visita.min(),
			'peg_colocation': self._followups.groupby('id_paciente').fecha_colocacion_peg.min(),
			'food_thickener_start': self._followups.groupby('id_paciente').inicio_espesante.min(),
			'oral_supl_start': self._followups.groupby('id_paciente').inicio_supl_oral.min(),
			'enteric_supl_start': self._followups.groupby('id_paciente').inicio_supl_enteral.min(),

			'death': self._patients.fecha_exitus,
		})