from pathlib import Path
from typing import Dict

from pandas import DataFrame, Series, Timedelta

from projects import Project, project
from projects._followup import load_followup_data
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

SMOKING_CATEGORIES = {
	  'Fumador': 'Active',
	'Exfumador': 'Ceased',
	    'Nunca': 'Never',
}

HOSP_DISCHARGE_TYPE_CATEGORIES = {
	'A DOMICILI': 'Planned',
	'RESID.SOCIAL': 'Planned',
	'ICO': 'Transfer',
	'ALTA CONT.CENTR': 'Transfer',
	'AGUTS/PSIQUIATRIC': 'Transfer',
	'H. DOMICILARIA': 'Home Hospitalization',
	'SOCI SANITARI': 'Rehabilitation',
	'EXITUS': 'Exitus',
}

URG_DISCHARGE_TYPE_CATEGORIES = {
	'ALTA VOLUNTARIA': 'DAMA',
	'FUGIDA/ABANDONAMENT': 'Abscond',
	'ALTA A DOMICILI': 'Planned',
	'REMISIO A ATENCIO PRIMARIA': 'Planned',
	'DERIVACIO A UN ALTRE CENTRE': 'Transfer',
	'INGRES A L\'HOSPITAL': 'Admitted',
	'EXITUS': 'Exitus',
}


@project('precision_als')
class PrecisionALS(Project):

	def __init__(self, datadir: Path):
		patients = load_data(datadir, 'ufmn/patients')
		self._patients = patients = patients[patients.fecha_dx.notna()]
		self._alsfrs_data = alsfrs_data = load_data(datadir, 'ufmn/alsfrs')
		self._nutr_data = nutr_data = load_data(datadir, 'ufmn/nutr')
		self._resp_data = resp_data = load_data(datadir, 'ufmn/resp')
		self._followups = load_followup_data(datadir, alsfrs_data=alsfrs_data,
		                               nutr_data=nutr_data, resp_data=resp_data)

		self._urg_episodes = load_data(datadir, 'hub_urg/episodes')
		self._urg_diagnoses = load_data(datadir, 'hub_urg/diagnoses')
		self._hosp_episodes = load_data(datadir, 'hub_hosp/episodes')
		self._hosp_diagnoses = load_data(datadir, 'hub_hosp/diagnoses')

	def _export_patient_data(self) -> DataFrame:
		return DataFrame({
			'birthdate': self._patients.fecha_nacimiento,
			'sex': self._patients.sexo.map(SEX_CATEGORIES),
			'smoking': self._patients.fumador.map(SMOKING_CATEGORIES),
			'clinical_onset': self._patients.inicio_clinica,
			'dx_date': self._patients.fecha_dx,
			'riluzole_received': self._patients.riluzol,
			'riluzole_start': self._patients.inicio_riluzol,
			'walking_aids': self._followups[self._followups.caminar <= 2].groupby('id_paciente').fecha_visita.min(),
			'cpap_initiation': self._followups.groupby('id_paciente').inicio_cpap.min(),
			'niv_support': self._followups.groupby('id_paciente').inicio_vmni.min(),
			'imv_support': self._followups[self._followups.insuf_resp == 0].groupby('id_paciente').fecha_visita.min(),
			'peg_colocation': self._followups.groupby('id_paciente').fecha_colocacion_peg.min(),
			'food_thickener_start': self._followups.groupby('id_paciente').inicio_espesante.min(),
			'oral_supl_start': self._followups.groupby('id_paciente').inicio_supl_oral.min(),
			'enteric_supl_start': self._followups.groupby('id_paciente').inicio_supl_enteral.min(),
			'last_followup': self._followups.groupby('id_paciente').fecha_visita.max(),
			'death': self._patients.fecha_exitus,
		})

	def _export_genetic_data(self) -> DataFrame:
		return DataFrame({
			'c9_status': self._patients.estado_c9.map(GENE_STATUS_CATEGORIES),
			'sod1_status': self._patients.estado_sod1.map(GENE_STATUS_CATEGORIES),
			'atxn2_status': self._patients.estado_atxn2.map(GENE_STATUS_CATEGORIES),
		})

	def _export_alsfrs_data(self) -> DataFrame:
		alsfrs_data = self._alsfrs_data.merge(self._followups, how='left',
		                                      on=['id_paciente', 'fecha_visita'],
		                                      suffixes=[None, '_x'])

		return DataFrame({
			'patient_id': alsfrs_data.id_paciente,
			'assessment_date': alsfrs_data.fecha_visita,
			'speech': alsfrs_data.lenguaje,
			'salivation': alsfrs_data.salivacion,
			'swallowing': alsfrs_data.deglucion,
			'handwriting': alsfrs_data.escritura,
			'cutting': alsfrs_data.cortar,
			'cutting_w_peg': alsfrs_data.cortar_con_peg,
			'cutting_wo_peg': alsfrs_data.cortar_sin_peg,
			'dressing': alsfrs_data.vestido,
			'bed': alsfrs_data.cama,
			'walking': alsfrs_data.caminar,
			'stairs': alsfrs_data.subir_escaleras,
			'dyspnea': alsfrs_data.disnea,
			'orthopnea': alsfrs_data.ortopnea,
			'resp_insuf': alsfrs_data.insuf_resp,
			'alsfrs_bulbar': alsfrs_data.alsfrs_bulbar_c,
			'alsfrs_fine_motor': alsfrs_data.alsfrs_fine_motor_c,
			'alsfrs_gross_motor': alsfrs_data.alsfrs_gross_motor_c,
			'alsfrs_resp': alsfrs_data.alsfrs_resp_c,
			'alsfrs_total': alsfrs_data.alsfrs_total_c,
			'kings': alsfrs_data.kings_c,
			'mitos': alsfrs_data.mitos_c,
		})

	def _export_respiratory_data(self) -> DataFrame:
		non_psng = self._resp_data.polisomnografia == False
		self._resp_data.loc[non_psng, 'sas_apneas_obstructivas'] = None
		self._resp_data.loc[non_psng, 'sas_apneas_no_claramente_obstructivas'] = None
		self._resp_data.loc[non_psng, 'sas_apneas_centrales'] = None
		self._resp_data.loc[non_psng, 'sas_apneas_mixtas'] = None

		return DataFrame({
			'patient_id': self._resp_data.id_paciente,
			'assesment_date': self._resp_data.fecha_visita,
			'abg_ph': self._resp_data.ph_sangre_arterial,
			'abg_po2': self._resp_data.pao2,
			'abg_pco2': self._resp_data.paco2,
			'abg_hco3': self._resp_data.hco3,
			'snip': self._resp_data.pns,
			'pcf': self._resp_data.pcf,
			'pcf_below_threshold': self._resp_data.pcf_below_threshold,
			'mip': self._resp_data.pim,
			'mip_below_threshold': self._resp_data.pim_below_threshold,
			'mep': self._resp_data.pem,
			'fvc_sitting': self._resp_data.fvc_sentado,
			'fvc_sitting_abs': self._resp_data.fvc_sentado_absoluto,
			'fvc_lying': self._resp_data.fvc_estirado,
			'fvc_lying_abs': self._resp_data.fvc_estirado_absoluto,
			'psng': self._resp_data.polisomnografia,
			'psng_date': self._resp_data.fecha_realizacion_polisomnografia,
			'psng_ct90': self._resp_data.ct90,
			'psng_odi3': self._resp_data.odi3,
			'psng_iah': self._resp_data.iah,
			'psng_mean_spo2': self._resp_data.sao2_media,
			'psng_obstr_apneas': self._resp_data.sas_apneas_obstructivas,
			'psng_non_obstr_apneas': self._resp_data.sas_apneas_no_claramente_obstructivas,
			'psng_central_apneas': self._resp_data.sas_apneas_centrales,
			'psng_mixed_apneas': self._resp_data.sas_apneas_mixtas,
		})

	def _export_emergencies_data(self) -> DataFrame:
		urg_data = (self._urg_episodes.reset_index()
		                .merge(self._patients.reset_index(), on='nhc')
		                .rename(columns={'id_episodio': 'episode_id'})
		                .set_index('episode_id'))

		return DataFrame({
			'id_paciente': urg_data.id_paciente,
			'admission_date': urg_data.inicio_episodio,
			'discharge_date': urg_data.fin_episodio,
			'discharge_type': urg_data.destino_alta.map(URG_DISCHARGE_TYPE_CATEGORIES),
		})

	def _export_emergencies_dx_data(self) -> DataFrame:
		urg_dx = self._urg_diagnoses.reset_index()

		return DataFrame({
			'episode_id': urg_dx.id_episodio,
			'dx_code': urg_dx.codigo_dx,
			'dx_description': urg_dx.descripcion_dx,
		})

	def _export_hospitalization_data(self) -> DataFrame:
		hosp_data = (self._hosp_episodes.reset_index()
		                                .merge(self._patients.reset_index(), on='nhc')
		                                .rename(columns={'id_episodio': 'episode_id'})
		                                .set_index('episode_id'))

		return DataFrame({
			'id_paciente': hosp_data.id_paciente,
			'admission_date': hosp_data.inicio_episodio,
			'discharge_date': hosp_data.fin_episodio,
			'discharge_type': hosp_data.destino_alta.map(HOSP_DISCHARGE_TYPE_CATEGORIES),
			'discharge_department': hosp_data.servicio_alta,
		})

	def _export_hospitalization_dx_data(self) -> DataFrame:
		hosp_dx = self._hosp_diagnoses.reset_index()

		return DataFrame({
			'episode_id': hosp_dx.id_episodio,
			'dx_code': hosp_dx.codigo_dx,
			'dx_description': hosp_dx.descripcion_dx,
		})

	def export_data(self) -> Dict[str, DataFrame]:
		return {
			'patients': self._export_patient_data(),
			'genetics': self._export_genetic_data(),
			'alsfrs': self._export_alsfrs_data(),
			'respiratory': self._export_respiratory_data(),
			'emergencies': self._export_emergencies_data(),
			'emergencies_dx': self._export_emergencies_dx_data(),
			'hospitalization': self._export_hospitalization_data(),
			'hospitalization_dx': self._export_hospitalization_dx_data(),
		}