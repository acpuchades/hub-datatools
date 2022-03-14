import sqlite3
from pandas  import DataFrame
from sqlite3 import Connection

from argparse import Namespace
from typing   import Dict

from transform import *


CMDLINE_SHORT  = '-u'
CMDLINE_KWARGS = {
	'help': 'SQLite file to load data from',
	'metavar': 'DATABASE_FILE',
}


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
	'ELA Bulbar': 'Bulbar-ALS',
	'ELA Espinal': 'Spinal-ALS',
	'ELA Respiratoria': 'Respiratory-ALS',
	'Esclerosis Lateral Primaria (ELP)': 'ELP',
	'Flail arm': 'Flail-Arm',
	'Flail leg': 'Flail-Leg',
	'Hemipléjica (Mills)': 'Hemiplegic',
	'Monomiélica': 'Monomielic',
	'Otro': 'Other',
	'Parálisis bulbar progresiva': 'PBP',
	'Pseudopolineurítica': 'Pseudopolineuritic',
}

COGNITIVE_DX_CATEGORIES = {
	'Demencia frontotemporal': 'DFT',
	'Demencia tipo alzheimer': 'DTA',
	'Deterioro Cognitivo Leve cognitivo (DCL cognitivo)': 'DCL-Cognitive',
	'Deterioro Cognitivo Leve conductual (DCL conductual)': 'DCL-Behavioral',
	'Deterioro Cognitivo Leve mixto (DCL mixto)': 'DCL-Mixed',
	'Normal': 'Normal',
	'Otros': 'Other',
}

GENE_STATUS_NORMAL_VALUE = 'Normal'
GENE_STATUS_ALTERED_VALUE = 'Alterado'


def load_patients_sql(con: Connection) -> DataFrame:
	df = pd.read_sql_query(f'SELECT * FROM {PATIENT_DATA_TABLE}', con)
	clean_patient_data(df)

	clinical_data = pd.read_sql_query(f'SELECT * FROM {CLINICAL_DATA_TABLE}', con)
	clean_clinical_data(clinical_data)

	df = df.merge(clinical_data, on='pid')
	return df


def load_als_data_sql(df: DataFrame, con: Connection) -> DataFrame:
	als_data = pd.read_sql_query(f'SELECT * FROM {ALS_DATA_TABLE}', con)
	df = df.merge(als_data, on='pid')
	clean_als_data(df)
	return df


def load_resp_data_sql(df: DataFrame, con: Connection) -> DataFrame:
	resp_data = pd.read_sql_query(f'SELECT * FROM {RESP_DATA_TABLE}', con)
	df = df.merge(resp_data, on='pid')
	clean_resp_data(df)
	return df


def load_nutr_data_sql(df: DataFrame, con: Connection) -> DataFrame:
	nutr_data = pd.read_sql_query(f'SELECT * FROM {NUTR_DATA_TABLE}', con)
	df = df.merge(nutr_data, on='pid')
	clean_nutr_data(df)
	return df


def clean_patient_data(df: DataFrame) -> None:
	apply_transform_pipeline(df, 'sexo', OPT_ENUM_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'exitus', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_exitus', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_nacimiento', OPT_DATE_PIPELINE, inplace=True)

	df['situacion_laboral_actual'] = df.situacion_laboral_actual.replace(WORKING_STATUS_CATEGORIES).astype('category')
	df['situacion_activa'] = df.situacion_laboral_actual.isin(ACTIVE_WORKING_STATUS_VALUES)

	df.rename(columns={
		'situacion_laboral_actual': 'situacion_laboral',
	}, inplace=True)


def add_patient_genetic_data(df: DataFrame) -> None:
	OTHER_GENES_COLUMN = 'estudio_genetico_otro'

	apply_transform_pipeline(df, 'resultado_estudio_c9', OPT_ENUM_PIPELINE, inplace='c9_status')
	apply_transform_pipeline(df, 'resultado_estudio_sod1', OPT_ENUM_PIPELINE, inplace='sod1_status')

	df['atxn2_status'] = None
	df.loc[df[OTHER_GENES_COLUMN].str.contains('ATXN2[^@]+NORMAL', case=False), 'atxn2_status'] = GENE_STATUS_NORMAL_VALUE
	df.loc[df[OTHER_GENES_COLUMN].str.contains('ATXN2[^@]+INTERMEDIO', case=False), 'atxn2_status'] = GENE_STATUS_ALTERED_VALUE
	df['atxn2_status'] = df['atxn2_status'].astype('category')

	df['ar_status'] = None
	df.loc[df[OTHER_GENES_COLUMN].str.contains('KENNEDY[^@]+NORMAL', case=False), 'ar_status'] = GENE_STATUS_NORMAL_VALUE
	df.loc[df[OTHER_GENES_COLUMN].str.contains('KENNEDY[^@]+POSITIVO', case=False), 'ar_status'] = GENE_STATUS_ALTERED_VALUE
	df['ar_status'] = df['ar_status'].astype('category')


def clean_clinical_data(df: DataFrame) -> None:
	apply_transform_pipeline(df, 'fecha_visita_datos_clinicos', OPT_DATE_PIPELINE, inplace='fecha_primera_visita')
	apply_transform_pipeline(df, 'fecha_inicio_clinica', OPT_DATE_PIPELINE, inplace='inicio_clinica')
	apply_transform_pipeline(df, 'fecha_diagnostico_ELA', OPT_DATE_PIPELINE, inplace='fecha_fx')

	apply_transform_pipeline(df, 'fenotipo_al_diagnostico', OPT_ENUM_PIPELINE, values=ALS_PHENOTYPE_CATEGORIES, inplace='fenotipo_dx')
	apply_transform_pipeline(df, 'fenotipo_al_exitus', OPT_ENUM_PIPELINE, values=ALS_PHENOTYPE_CATEGORIES, inplace='fenotipo_exitus')
	apply_transform_pipeline(df, 'deterioro_cognitivo', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'estudio_cognitivo', OPT_ENUM_PIPELINE, values=COGNITIVE_DX_CATEGORIES, inplace=True)

	add_patient_genetic_data(df)

	apply_transform_pipeline(df, 'historia_familiar', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'historia_familiar_motoneurona', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'historia_familiar_alzheimer', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'historia_familiar_parkinson', OPT_BOOL_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'fumador', OPT_ENUM_PIPELINE, values=SMOKE_CATEGORIES, inplace=True)

	apply_transform_pipeline(df, 'riluzol', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_inicio_riluzol', OPT_DATE_PIPELINE, inplace='inicio_riluzol')


def clean_als_data(df: DataFrame) -> None:
	apply_transform_pipeline(df, 'fecha_visita_esc_val_ela', OPT_DATE_PIPELINE, inplace='fecha_visita')
	apply_transform_pipeline(df, 'lenguaje', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'salivacion', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'deglucion', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'escritura', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'cortar_sin_peg', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'cortar_con_peg', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'vestido', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'cama', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'caminar', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'subir_escaleras', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'disnea', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'ortopnea', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'insuficiencia_respiratoria', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'total', OPT_NUMBER_PIPELINE, inplace='alsfrs')
	apply_transform_pipeline(df, 'total_bulbar', OPT_NUMBER_PIPELINE, inplace='alsfrs_resp')
	apply_transform_pipeline(df, 'mitos', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'kings', OPT_NUMBER_PIPELINE, inplace=True)


def clean_resp_data(df: DataFrame) -> None:
	apply_transform_pipeline(df, 'fecha_visita_fun_res', OPT_DATE_PIPELINE, inplace='fecha_visita')
	apply_transform_pipeline(df, 'patologia_respiratoria_previa', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'pns', OPT_NUMBER_PIPELINE, errors='coerce', inplace=True)

	df['pcf_below_threshold'] = df.pcf == '<60'
	apply_transform_pipeline(df, 'pcf', OPT_NUMBER_PIPELINE, errors='coerce', inplace=True)

	apply_transform_pipeline(df, 'fvc_sentado', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fvc_estirado', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'pem', OPT_NUMBER_PIPELINE, inplace=True)

	df['pim_below_threshold'] = df.pim == '<60'
	apply_transform_pipeline(df, 'pim', OPT_NUMBER_PIPELINE, errors='coerce', inplace=True)

	apply_transform_pipeline(df, 'ph_sangre_arterial', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'pao2', OPT_NUMBER_PIPELINE, errors='coerce', inplace=True)
	apply_transform_pipeline(df, 'paco2', OPT_NUMBER_PIPELINE, errors='coerce', inplace=True)
	apply_transform_pipeline(df, 'hco3', OPT_NUMBER_PIPELINE, errors='coerce', inplace=True)

	df['sao2_media_below_threshold'] = df.sao2_media == '<90'
	apply_transform_pipeline(df, 'sao2_media', OPT_NUMBER_PIPELINE, errors='coerce', inplace=True)

	apply_transform_pipeline(df, 'ct90', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'odi3', OPT_NUMBER_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'polisomnografia', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_realizacion_polisomnografia', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'ct90_polisomnografia', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'iah', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'sas_no', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'sas_apneas_obstructivas', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'sas_apneas_no_claramanete_obstructivas', OPT_BOOL_PIPELINE, inplace='sas_no_claramente_obstructivas')
	apply_transform_pipeline(df, 'sas_apneas_centrales', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'sas_apneas_mixtas', OPT_BOOL_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'sintomas_intolerancia_al_decubito', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'sintomas_disnea_de_esfuerzo', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'sintomas_sintomas_de_hipoventilacion_nocturna', OPT_BOOL_PIPELINE, inplace='sintomas_hipoventilacion_nocturna')
	apply_transform_pipeline(df, 'sintomas_tos_ineficaz', OPT_BOOL_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'cpap', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_cpap', OPT_DATE_PIPELINE, exact=False, inplace='inicio_cpap')
	apply_transform_pipeline(df, 'portador_vmni', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_colocacion_vmni', OPT_DATE_PIPELINE, inplace='inicio_vmni')

	apply_transform_pipeline(df, 'complicacion_vmni', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_complicacion_vmni', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_complicacion_vmni_ulcera_nasal_por_presion', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_complicacion_vmni_aerofagia', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_complicacion_vmni_sequedad_orofaringea', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_complicacion_vmni_otros', OPT_BOOL_PIPELINE, inplace=True)

	apply_transform_pipeline(df, 'retirada_vmni', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_retirada_vmni', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'motivo_retirada_vmi_intolerancia', OPT_BOOL_PIPELINE, inplace='motivo_retirada_vmni_intolerancia')
	apply_transform_pipeline(df, 'motivo_retirada_vmi_no_cumplimiento', OPT_BOOL_PIPELINE, inplace='motivo_retirada_vmni_incumplimiento')
	apply_transform_pipeline(df, 'motivo_retirada_vmi_rechazo_del_paciente', OPT_BOOL_PIPELINE, inplace='motivo_retirada_vmni_rechazo')
	apply_transform_pipeline(df, 'motivo_retirada_vmi_otros', OPT_BOOL_PIPELINE, inplace='motivo_retirada_vmni_otros')

	apply_transform_pipeline(df, 'fvc_sentado_absoluto', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fvc_estirado_absoluto', OPT_NUMBER_PIPELINE, inplace=True)


def clean_nutr_data(df: DataFrame) -> None:
	df.loc[df.id == '67e615f4-5f01-11eb-a21b-8316bff80df0', 'fecha_visita_datos_antro'] = '03-12-2021' # was '03-12-20219'
	df.loc[df.id == 'f9054526-1dcc-11eb-bb4a-9745fc970131', 'fecha_indicacion_peg'] = '23-10-2020' # was '23-10-20020'
	df.loc[df.id == '8c5b0f46-df7a-11e9-9c30-274ab37b3217', 'fecha_indicacion_peg'] = '20-07-2018' # was '20-07-3018'
	df.loc[df.id == 'eb700688-3dfe-11eb-9383-d3a3b2195eff', 'fecha_complicacion_peg'] = '22-11-2020' # was '22-11-202'
	df.replace('29-02-2015', '28-02-2015', regex=False, inplace=True) # 2015 was not a leap year

	apply_transform_pipeline(df, 'fecha_visita_datos_antro', OPT_DATE_PIPELINE, inplace='fecha_visita')
	apply_transform_pipeline(df, 'peso', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_peso', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'estatura', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'imc_actual', OPT_NUMBER_PIPELINE, inplace='imc')
	apply_transform_pipeline(df, 'peso_premorbido', OPT_NUMBER_PIPELINE, inplace=True)
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
	apply_transform_pipeline(df, 'retirada', OPT_BOOL_PIPELINE, inplace='retirada_peg')
	apply_transform_pipeline(df, 'fecha_retirada_peg', OPT_DATE_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'disfagia', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'espesante', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'fecha_inicio_espesante', OPT_DATE_PIPELINE, inplace='inicio_espesante')
	apply_transform_pipeline(df, 'suplementacion_nutricional_oral', OPT_BOOL_PIPELINE, inplace='supl_nutr_oral')
	apply_transform_pipeline(df, 'fecha_suplementacion_nutricional', OPT_DATE_PIPELINE, inplace='inicio_supl_nutr_oral')
	apply_transform_pipeline(df, 'restrenimiento', OPT_BOOL_PIPELINE, inplace='estreñimiento')
	apply_transform_pipeline(df, 'laxante', OPT_BOOL_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'peso_colocacion_peg', OPT_NUMBER_PIPELINE, inplace=True)
	apply_transform_pipeline(df, 'suplementacion_nutricional_entera', OPT_BOOL_PIPELINE, inplace='supl_nutr_ent')
	apply_transform_pipeline(df, 'fecha_inicio_suplementacion_nutricional_entera', OPT_DATE_PIPELINE, inplace='inicio_supl_nutr_ent')


def load_data(args: Namespace) -> Dict[str, DataFrame]:
	with sqlite3.connect(f'file:{args.ufmn}?mode=ro', uri=True) as con:
		df = load_patients_sql(con)
		return {
			'ufmn/patients': df,
			'ufmn/als_data': load_als_data_sql(df, con),
			'ufmn/resp_data': load_resp_data_sql(df, con),
			'ufmn/nutr_data': load_nutr_data_sql(df, con),
		}