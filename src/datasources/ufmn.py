import sqlite3
from pandas      import DataFrame
from sqlite3     import Connection

from argparse    import ArgumentParser, Namespace
from typing      import Dict

from datasources import DataSource, datasource
from transform   import *


PATIENT_DATA_TABLE  = 'pacientes'
CLINICAL_DATA_TABLE = 'datos_clinicos'
ALS_DATA_TABLE      = 'esc_val_ela'
RESP_DATA_TABLE     = 'fun_res'
NUTR_DATA_TABLE     = 'datos_antro'


WORKING_STATUS_CATEGORIES = {
	'Incapacitado (o con invalidez permanente)': 'Discapacidad',
	'Jubilado': 'Jubilado',
	'Labores de la casa': 'Hogar',
	'Parado': 'Desempleado',
	'Parado con subsidio | Prestación': 'Desempleado',
	'Trabaja': 'Trabajando',
	'Otra': 'Otro',
}

SMOKE_CATEGORIES = {
	'Fumador': 'Activo',
	'Exfumador': 'Exfumador',
	'No fumador': 'Nunca',
}

ACTIVE_WORKING_STATUS_VALUES = (
	'Trabaja', 'Hogar'
)

ALS_PHENOTYPE_CATEGORIES = {
	'Atrofia Muscular Progresiva (AMP)': 'AMP',
	'ELA Bulbar': 'ELA Bulbar',
	'ELA Espinal': 'ELA Espinal',
	'ELA Respiratoria': 'ELA Respiratory',
	'Esclerosis Lateral Primaria (ELP)': 'ELP',
	'Flail arm': 'Flail-Arm',
	'Flail leg': 'Flail-Leg',
	'Hemipléjica (Mills)': 'Hemiplejica',
	'Monomiélica': 'Monomielica',
	'Otro': 'Otro',
	'Parálisis bulbar progresiva': 'PBP',
	'Pseudopolineurítica': 'Pseudopolineuritica',
}

COGNITIVE_DX_CATEGORIES = {
	'Demencia frontotemporal': 'DFT',
	'Demencia tipo alzheimer': 'DTA',
	'Deterioro Cognitivo Leve cognitivo (DCL cognitivo)': 'DCL-Cognitiva',
	'Deterioro Cognitivo Leve conductual (DCL conductual)': 'DCL-Conductual',
	'Deterioro Cognitivo Leve mixto (DCL mixto)': 'DCL-Mixta',
	'Normal': 'Normal',
	'Otros': 'Otro',
}

GENE_STATUS_NORMAL_VALUE = 'Normal'
GENE_STATUS_ALTERED_VALUE = 'Alterado'

PATIENT_RENAME_COLUMNS = {
	'fecha_diagnostico_ELA': 'fecha_dx',
	'fecha_inicio_clinica': 'inicio_clinica',
	'fecha_inicio_riluzol': 'inicio_riluzol',
	'fecha_visita_datos_clinicos': 'fecha_primera_visita',
	'fenotipo_al_diagnostico': 'fenotipo_dx',
	'fenotipo_al_exitus': 'fenotipo_exitus',
	'resultado_estudio_c9': 'estado_c9',
	'resultado_estudio_sod1': 'estado_sod1',
}

ALS_DATA_RENAME_COLUMNS = {
	'fecha_visita_esc_val_ela': 'fecha_visita',
	'insuficiencia_respiratoria': 'insuf_resp',
	'pid': 'id_paciente',
	'total': 'alsfrs_total',
	'total_bulbar': 'alsfrs_bulbar',
}

NUTR_DATA_RENAME_COLUMNS = {
	'fecha_inicio_espesante': 'inicio_espesante',
	'fecha_inicio_suplementacion_nutricional_entera': 'inicio_supl_enteral',
	'fecha_suplementacion_nutricional': 'inicio_supl_oral',
	'fecha_visita_datos_antro': 'fecha_visita',
	'imc_actual': 'imc',
	'pid': 'id_paciente',
	'restrenimiento': 'estreñimiento',
	'retirada': 'retirada_peg',
	'suplementacion_nutricional_entera': 'supl_enteral',
	'suplementacion_nutricional_oral': 'supl_oral',
}

RESP_DATA_RENAME_COLUMNS = {
	'fecha_cpap': 'inicio_cpap',
	'fecha_colocacion_vmni': 'inicio_vmni',
	'fecha_visita_fun_res': 'fecha_visita',
	'sas_apneas_no_claramanete_obstructivas': 'sas_no_claramente_obstructivas',
	'sintomas_sintomas_de_hipoventilacion_nocturna': 'sintomas_hipoventilacion_nocturna',
	'motivo_retirada_vmi_intolerancia': 'motivo_retirada_vmni_intolerancia',
	'motivo_retirada_vmi_no_cumplimiento': 'motivo_retirada_vmni_incumplimiento',
	'motivo_retirada_vmi_rechazo_del_paciente': 'motivo_retirada_vmni_rechazo',
	'motivo_retirada_vmi_otros': 'motivo_retirada_vmni_otros',
	'pid': 'id_paciente',
}

def _load_patients_sql(con: Connection) -> DataFrame:
	patients = pd.read_sql_query(f'SELECT * FROM {PATIENT_DATA_TABLE}', con)
	patients.rename(columns={'pid': 'id_paciente'}, inplace=True)
	patients.set_index('id_paciente', inplace=True)
	patients.drop(columns=['id', 'created_datetime', 'updated_datetime'], inplace=True)
	_clean_patient_data(patients)

	clinical_data = pd.read_sql_query(f'SELECT * FROM {CLINICAL_DATA_TABLE}', con)
	clinical_data.rename(columns={'pid': 'id_paciente'}, inplace=True)
	clinical_data.set_index('id_paciente', inplace=True)
	clinical_data.drop(columns=['id', 'created_datetime', 'updated_datetime'], inplace=True)
	_clean_clinical_data(clinical_data)

	df = patients.merge(clinical_data, left_index=True, right_index=True)
	df.rename(columns=PATIENT_RENAME_COLUMNS, inplace=True)
	df.sort_index(inplace=True)

	return df


def _load_als_data_sql(con: Connection) -> DataFrame:
	als_data = pd.read_sql_query(f'SELECT * FROM {ALS_DATA_TABLE}', con)
	als_data.drop(columns=['created_datetime', 'updated_datetime'], inplace=True)
	als_data.rename(columns={'id': 'id_visita'}, inplace=True)
	als_data.set_index('id_visita', inplace=True)
	_clean_als_data(als_data)
	als_data.rename(columns=ALS_DATA_RENAME_COLUMNS, inplace=True)
	return als_data


def _load_nutr_data_sql(con: Connection) -> DataFrame:
	nutr_data = pd.read_sql_query(f'SELECT * FROM {NUTR_DATA_TABLE}', con)
	nutr_data.drop(columns=['created_datetime', 'updated_datetime'], inplace=True)
	nutr_data.rename(columns={'id': 'id_visita'}, inplace=True)
	nutr_data.set_index('id_visita', inplace=True)
	_clean_nutr_data(nutr_data)
	nutr_data.rename(columns=NUTR_DATA_RENAME_COLUMNS, inplace=True)
	return nutr_data


def _load_resp_data_sql(con: Connection) -> DataFrame:
	resp_data = pd.read_sql_query(f'SELECT * FROM {RESP_DATA_TABLE}', con)
	resp_data.drop(columns=['created_datetime', 'updated_datetime'], inplace=True)
	resp_data.rename(columns={'id': 'id_visita'}, inplace=True)
	resp_data.set_index('id_visita', inplace=True)
	_clean_resp_data(resp_data)
	resp_data.rename(columns=RESP_DATA_RENAME_COLUMNS, inplace=True)
	return resp_data


def _clean_patient_data(df: DataFrame) -> None:
	apply_transform_pipeline(df, 'sexo', OPT_ENUM_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'exitus', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_exitus', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_nacimiento', OPT_DATE_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'situacion_laboral_actual', OPT_ENUM_PIPELINE, values=WORKING_STATUS_CATEGORIES, inplace='situacion_laboral_inicial')
	df['situacion_activa_inicial'] = df.situacion_laboral_inicial.isin(ACTIVE_WORKING_STATUS_VALUES)


def _add_patient_genetic_data(df: DataFrame) -> None:
	OTHER_GENES_COLUMN = 'estudio_genetico_otro'

	apply_transform_pipeline(df, 'resultado_estudio_c9', OPT_ENUM_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'resultado_estudio_sod1', OPT_ENUM_PIPELINE, inplace=True)

	df['estado_atxn2'] = None
	df.loc[df[OTHER_GENES_COLUMN].str.contains('ATXN2[^@]+NORMAL', case=False), 'estado_atxn2'] = GENE_STATUS_NORMAL_VALUE
	df.loc[df[OTHER_GENES_COLUMN].str.contains('ATXN2[^@]+INTERMEDIO', case=False), 'estado_atxn2'] = GENE_STATUS_ALTERED_VALUE
	df['estado_atxn2'] = df['estado_atxn2'].astype('category')

	df['estado_ar'] = None
	df.loc[df[OTHER_GENES_COLUMN].str.contains('KENNEDY[^@]+NORMAL', case=False), 'estado_ar'] = GENE_STATUS_NORMAL_VALUE
	df.loc[df[OTHER_GENES_COLUMN].str.contains('KENNEDY[^@]+POSITIVO', case=False), 'estado_ar'] = GENE_STATUS_ALTERED_VALUE
	df['estado_ar'] = df['estado_ar'].astype('category')


def _clean_clinical_data(df: DataFrame) -> None:
	apply_transform_pipeline(df, 'fecha_visita_datos_clinicos', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_inicio_clinica', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_diagnostico_ELA', OPT_DATE_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'fenotipo_al_diagnostico', OPT_ENUM_PIPELINE, values=ALS_PHENOTYPE_CATEGORIES, inplace=True)
	apply_transform_pipeline(df, 'fenotipo_al_exitus', OPT_ENUM_PIPELINE, values=ALS_PHENOTYPE_CATEGORIES, inplace=True)
	apply_transform_pipeline(df, 'deterioro_cognitivo', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'estudio_cognitivo', OPT_ENUM_PIPELINE, values=COGNITIVE_DX_CATEGORIES, inplace=True)

	_add_patient_genetic_data(df)

	apply_transform_pipeline(df, 'historia_familiar', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'historia_familiar_motoneurona', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'historia_familiar_alzheimer', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'historia_familiar_parkinson', OPT_BOOL_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'fumador', OPT_ENUM_PIPELINE, values=SMOKE_CATEGORIES, inplace=True)

	apply_transform_pipeline(df, 'riluzol', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_inicio_riluzol', OPT_DATE_PIPELINE, inplace=True)


def _clean_als_data(df: DataFrame) -> None:
	apply_transform_pipeline(df, 'fecha_visita_esc_val_ela', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'lenguaje', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'salivacion', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'deglucion', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'escritura', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'cortar_sin_peg', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'cortar_con_peg', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'vestido', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'cama', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'caminar', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'subir_escaleras', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'disnea', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'ortopnea', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'insuficiencia_respiratoria', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'total', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'total_bulbar', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'mitos', OPT_INT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'kings', OPT_INT_PIPELINE, inplace=True)


def _clean_nutr_data(df: DataFrame) -> None:
	df.loc['40c68842-eeb1-4cd2-a0d8-c5cbc839730c', 'fecha_visita_datos_antro'] = None # was '99-99-9999'
	df.loc['67e615f4-5f01-11eb-a21b-8316bff80df0', 'fecha_visita_datos_antro'] = '03-12-2021' # was '03-12-20219'
	df.loc['f9054526-1dcc-11eb-bb4a-9745fc970131', 'fecha_indicacion_peg'] = '23-10-2020' # was '23-10-20020'
	df.loc['8c5b0f46-df7a-11e9-9c30-274ab37b3217', 'fecha_indicacion_peg'] = '20-07-2018' # was '20-07-3018'
	df.loc['eb700688-3dfe-11eb-9383-d3a3b2195eff', 'fecha_complicacion_peg'] = '22-11-2020' # was '22-11-202'
	df.replace('29-02-2015', '28-02-2015', regex=False, inplace=True) # 2015 was not a leap year

	apply_transform_pipeline(df, 'fecha_visita_datos_antro', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'peso', OPT_FLOAT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_peso', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'estatura', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'imc_actual', OPT_FLOAT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'peso_premorbido', OPT_FLOAT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_peso_premorbido', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'indicacion_peg', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_indicacion_peg', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_indicacion_peg_disfagia', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_indicacion_peg_perdida_de_peso', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_indicacion_peg_insuficiencia_respiratoria', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_indicacion_peg_otro', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'portador_peg', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_colocacion_peg', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'uso_peg', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'complicacion_peg', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_complicacion_peg', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'retirada', OPT_BOOL_PIPELINE, True)
	apply_transform_pipeline(df, 'fecha_retirada_peg', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'disfagia', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'espesante', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_inicio_espesante', OPT_DATE_PIPELINE, True)
	apply_transform_pipeline(df, 'suplementacion_nutricional_oral', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_suplementacion_nutricional', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'restrenimiento', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'laxante', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'peso_colocacion_peg', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'suplementacion_nutricional_entera', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_inicio_suplementacion_nutricional_entera', OPT_DATE_PIPELINE, inplace=True)


def _clean_resp_data(df: DataFrame) -> None:
	df.loc['c2049bdf-4a91-43e0-b6c4-f770881b7499', 'fecha_visita_fun_res'] = None # was '99-99-9999'
	df.loc['31f94d2a-fb08-11e9-b780-81f732616a71', 'odi3'] = None # was '17/7'
	df.loc['a3608f72-82eb-11e9-aed7-57f320d0dba4', 'fecha_realizacion_polisomnografia'] = None # was '14'
	df.loc['f508e4b8-db93-11e9-b372-090a91bd3693', 'fecha_realizacion_polisomnografia'] = None # was '14'

	apply_transform_pipeline(df, 'fecha_visita_fun_res', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'patologia_respiratoria_previa', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'tipo_patologia_respiratoria_epoc', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'tipo_patologia_respiratoria_asma', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'tipo_patologia_respiratoria_bronquiectasias', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'tipo_patologia_respiratoria_patologia_instersticial', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'tipo_patologia_respiratoria_saos', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'tipo_patologia_respiratoria_otra', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'tipo_patologia_respiratoria_nsnc', OPT_BOOL_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'pns', OPT_FLOAT_PIPELINE, errors='coerce', inplace=True)

	df['pcf_below_threshold'] = df.pcf == '<60'
	apply_transform_pipeline(df, 'pcf', OPT_FLOAT_PIPELINE, errors='coerce', inplace=True)
	apply_transform_pipeline(df, 'fvc_sentado', OPT_FLOAT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fvc_estirado', OPT_FLOAT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'pem', OPT_FLOAT_PIPELINE, inplace=True)
	df['pim_below_threshold'] = df.pim == '<60'
	apply_transform_pipeline(df, 'pim', OPT_FLOAT_PIPELINE, errors='coerce', inplace=True)
	apply_transform_pipeline(df, 'ph_sangre_arterial', OPT_FLOAT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'pao2', OPT_FLOAT_PIPELINE, errors='coerce', inplace=True)
	apply_transform_pipeline(df, 'paco2', OPT_FLOAT_PIPELINE, errors='coerce', inplace=True)
	apply_transform_pipeline(df, 'hco3', OPT_FLOAT_PIPELINE, errors='coerce', inplace=True)
	df['sao2_media_below_threshold'] = df.sao2_media == '<90'
	apply_transform_pipeline(df, 'sao2_media', OPT_FLOAT_PIPELINE, errors='coerce', inplace=True)
	apply_transform_pipeline(df, 'ct90', OPT_FLOAT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'odi3', OPT_FLOAT_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'polisomnografia', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_realizacion_polisomnografia', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'ct90_polisomnografia', OPT_FLOAT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'iah', OPT_FLOAT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'sas_no', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'sas_apneas_obstructivas', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'sas_apneas_no_claramanete_obstructivas', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'sas_apneas_centrales', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'sas_apneas_mixtas', OPT_BOOL_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'sintomas_intolerancia_al_decubito', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'sintomas_disnea_de_esfuerzo', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'sintomas_sintomas_de_hipoventilacion_nocturna', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'sintomas_tos_ineficaz', OPT_BOOL_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'cpap', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_cpap', OPT_DATE_PIPELINE, exact=False, inplace=True)
	apply_transform_pipeline(df, 'cumplimiento_cpap', OPT_BOOL_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'vmni_indicacion', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_indicacion_vmni_sintomas', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_indicacion_vmni_fvc', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_indicacion_vmni_desaturacion_nocturna', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_indicacion_vmni_hipercapnia_nocturna', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_indicacion_vmni_hipercapnia_diurna', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_indicacion_vmni_otros', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'portador_vmni', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_colocacion_vmni', OPT_DATE_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'complicacion_vmni', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_complicacion_vmni', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_complicacion_vmni_ulcera_nasal_por_presion', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_complicacion_vmni_aerofagia', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_complicacion_vmni_sequedad_orofaringea', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_complicacion_vmni_otros', OPT_BOOL_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'retirada_vmni', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_retirada_vmni', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_retirada_vmi_intolerancia', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_retirada_vmi_no_cumplimiento', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_retirada_vmi_rechazo_del_paciente', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_retirada_vmi_otros', OPT_BOOL_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'fvc_sentado_absoluto', OPT_FLOAT_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fvc_estirado_absoluto', OPT_FLOAT_PIPELINE, inplace=True)


@datasource('ufmn')
class UFMN(DataSource):

	@staticmethod
	def add_arguments(parser: ArgumentParser) -> None:
		parser.add_argument('-u', '--ufmn', metavar='DATABASE_FILE',
		                    help='SQLite file to load data from')

	@staticmethod
	def has_arguments(args: Namespace) -> bool:
		return args.ufmn is not None

	def load_data(self, args: Namespace) -> Dict[str, DataFrame]:
		with sqlite3.connect(f'file:{args.ufmn}?mode=ro', uri=True) as con:
			return {
				 'ufmn/patients': _load_patients_sql(con),
				 'ufmn/als_data': _load_als_data_sql(con),
				'ufmn/resp_data': _load_resp_data_sql(con),
				'ufmn/nutr_data': _load_nutr_data_sql(con),
			}