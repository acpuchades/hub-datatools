#!/usr/bin/env python3

import sys
from argparse import ArgumentParser
from pathlib  import Path

import sqlite3
import pandas as pd

from core import *


PATIENT_DATA_TABLE  = 'pacientes'
CLINICAL_DATA_TABLE = 'datos_clinicos'
ALS_DATA_TABLE      = 'esc_val_ela'
RESP_DATA_TABLE     = 'fun_res'
NUTR_DATA_TABLE     = 'datos_antro'

NA_VALUES    = ('', '-', 'NS/NC', 'NA')
TRUE_VALUES  = ('Sí', 'TRUE')
FALSE_VALUES = ('No', 'FALSE')

ALS_PHENOTYPES = {
	'-': None,
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

COGNITIVE_DX = {
	'Demencia frontotemporal': 'DFT',
	'Demencia tipo alzheimer': 'DTA',
	'Deterioro Cognitivo Leve cognitivo (DCL cognitivo)': 'DCL-Cognitive',
	'Deterioro Cognitivo Leve conductual (DCL conductual)': 'DCL-Behavioral',
	'Deterioro Cognitivo Leve mixto (DCL mixto)': 'DCL-Mixed',
	'Normal': 'Normal',
	'Otros': 'Other',
}


def load_patients_sql(connection):
	df = pd.read_sql_query(f'SELECT * FROM {PATIENT_DATA_TABLE}', con)
	clinical_data = pd.read_sql_query(f'SELECT * FROM {CLINICAL_DATA_TABLE}', con)
	df = df.merge(clinical_data, on='pid')
	clean_patient_data(df)
	clean_clinical_data(df)
	return df


def load_als_data_sql(df, con):
	als_data = pd.read_sql_query(f'SELECT * FROM {ALS_DATA_TABLE}', con)
	df = df.merge(als_data, on='pid')
	clean_als_data(df)
	return df


def load_resp_data_sql(df, con):
	resp_data = pd.read_sql_query(f'SELECT * FROM {RESP_DATA_TABLE}', con)
	df = df.merge(resp_data, on='pid')
	clean_resp_data(df)
	return df


def load_nutr_data_sql(df, con):
	nutr_data = pd.read_sql_query(f'SELECT * FROM {NUTR_DATA_TABLE}', con)
	df = df.merge(nutr_data, on='pid')
	clean_nutr_data(df)
	return df


def transform_opt(data, na_values=NA_VALUES, inplace=False, **kwargs):
	if not inplace:
		data = data.copy()
	
	data.replace(na_values, None, inplace=True)
	return data


def transform_bool(data, true_values=TRUE_VALUES, false_values=FALSE_VALUES, inplace=False, **kwargs):
	if not inplace:
		data = data.copy()
	
	data.replace(true_values, True, inplace=True)
	data.replace(false_values, False, inplace=True)
	return data


def transform_strip(data, **kwargs):
	return data.str.strip()


def transform_fix_typos(data, **kwargs):
	data = data.str.replace(r'^º', '', regex=True)
	data = data.str.replace(r'º$', '', regex=True)
	return data


def transform_fix_date_typos(data, **kwargs):
	data = data.str.replace(r'^\?\?', '01', regex=True)
	data = data.str.replace(r'-+', '/', regex=True)
	data = data.str.replace(r'^(\d{1,2})/(\d{1,2})(\d{2,4})$', r'\1/\2/\3', regex=True)
	return data


def transform_date(data, yearfirst=False, dayfirst=True, format=None, exact=True, **kwargs):
	return pd.to_datetime(data, yearfirst=yearfirst, dayfirst=dayfirst, format=format, exact=exact)


def transform_number(data, errors='raise', **kwargs):
	data = data.str.replace(',', '.', regex=False)
	data = data.str.replace('..', '.', regex=False)
	return pd.to_numeric(data, errors=errors)


def apply_pipeline(df, field, pipeline, inplace=False, **kwargs):
	data = df[field]
	for fn in pipeline:
		data = fn(data, **kwargs, inplace=inplace)
	
	if inplace:
		df[field] = data
	return data


OPT_BOOL_PIPELINE   = (transform_strip, transform_fix_typos, transform_opt, transform_bool)
OPT_DATE_PIPELINE   = (transform_strip, transform_fix_typos, transform_opt, transform_fix_date_typos, transform_date)
OPT_NUMBER_PIPELINE = (transform_strip, transform_fix_typos, transform_opt, transform_number)


def clean_patient_data(df):
	df['sexo'] = df['sexo'].astype('category')
	df['exitus'] = transform_bool(df.exitus, inplace=True)
	
	apply_pipeline(df, 'fecha_exitus', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_nacimiento', OPT_DATE_PIPELINE, inplace=True)


def add_patient_genetic_data(df):
	OTHER_GENES_COLUMN = 'estudio_genetico_otro'
	
	df['c9_status'] = df['resultado_estudio_c9'].replace({
		'Normal': 'Normal',
		'Alterado': 'Mutado',
		'NS/NC': None,
	}).astype('category')

	df['sod1_status'] = df['resultado_estudio_sod1'].replace({
		'Normal': 'Normal',
		'Alterado': 'Mutado',
		'NS/NC': None,
	}).astype('category')
	
	df['atxn2_status'] = None
	df.loc[df[OTHER_GENES_COLUMN].str.contains('ATXN2[^@]+NORMAL', case=False), 'atxn2_status'] = 'Normal'
	df.loc[df[OTHER_GENES_COLUMN].str.contains('ATXN2[^@]+INTERMEDIO', case=False), 'atxn2_status'] = 'Mutado'
	df['atxn2_status'] = df['atxn2_status'].astype('category')
	
	df['ar_status'] = None
	df.loc[df[OTHER_GENES_COLUMN].str.contains('KENNEDY[^@]+NORMAL', case=False), 'ar_status'] = 'Normal'
	df.loc[df[OTHER_GENES_COLUMN].str.contains('KENNEDY[^@]+POSITIVO', case=False), 'ar_status'] = 'Mutado'
	df['ar_status'] = df['ar_status'].astype('category')


def clean_clinical_data(df):
	apply_pipeline(df, 'fecha_visita_datos_clinicos', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_inicio_clinica', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_inicio_riluzol', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'fumador', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'riluzol', OPT_BOOL_PIPELINE, inplace=True)
	
	apply_pipeline(df, 'historia_familiar', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'historia_familiar_motoneurona', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'historia_familiar_alzheimer', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'historia_familiar_parkinson', OPT_BOOL_PIPELINE, inplace=True)
	
	add_patient_genetic_data(df)
	
	df['fenotipo_al_diagnostico'] = df['fenotipo_al_diagnostico'].replace(ALS_PHENOTYPES).astype('category')
	df['fenotipo_al_exitus'] = df['fenotipo_al_exitus'].replace(ALS_PHENOTYPES).astype('category')
	df['estudio_cognitivo'] = df['estudio_cognitivo'].replace(COGNITIVE_DX).astype('category')
	
	df.rename({
		'fecha_visita_datos_clinicos': 'fecha_primera_visita',
		'fenotipo_al_diagnostico': 'fenotipo_dx',
		'fenotipo_al_exitus': 'fenotipo_exitus',
	}, inplace=True)


def clean_als_data(df):
	apply_pipeline(df, 'fecha_visita_esc_val_ela', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'lenguaje', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'salivacion', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'deglucion', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'escritura', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'cortar_sin_peg', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'cortar_con_peg', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'vestido', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'cama', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'caminar', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'subir_escaleras', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'disnea', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'ortopnea', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'insuficiencia_respiratoria', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'total', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'total_bulbar', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'mitos', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'kings', OPT_NUMBER_PIPELINE, inplace=True)
	
	df.rename({
		'total': 'alsfrs',
		'total_bulbar': 'alsfrs_resp',
		'fecha_visita_esc_val_ela': 'fecha_visita',
	}, inplace=True)
	
	return df

def clean_resp_data(df):
	apply_pipeline(df, 'fecha_visita_fun_res', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'patologia_respiratoria_previa', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'pns', OPT_NUMBER_PIPELINE, errors='coerce', inplace=True)
	
	df['pcf_below_threshold'] = df.pcf == '<60'
	apply_pipeline(df, 'pcf', OPT_NUMBER_PIPELINE, errors='coerce', inplace=True)
	
	apply_pipeline(df, 'fvc_sentado', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'fvc_estirado', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'pem', OPT_NUMBER_PIPELINE, inplace=True)
	
	df['pim_below_threshold'] = df.pim == '<60'
	apply_pipeline(df, 'pim', OPT_NUMBER_PIPELINE, errors='coerce', inplace=True)
	
	apply_pipeline(df, 'ph_sangre_arterial', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'pao2', OPT_NUMBER_PIPELINE, errors='coerce', inplace=True)
	apply_pipeline(df, 'paco2', OPT_NUMBER_PIPELINE, errors='coerce', inplace=True)
	apply_pipeline(df, 'hco3', OPT_NUMBER_PIPELINE, errors='coerce', inplace=True)
	
	df['sao2_media_below_threshold'] = df.sao2_media == '<90'
	apply_pipeline(df, 'sao2_media', OPT_NUMBER_PIPELINE, errors='coerce', inplace=True)
	
	apply_pipeline(df, 'ct90', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'odi3', OPT_NUMBER_PIPELINE, inplace=True)
	
	apply_pipeline(df, 'polisomnografia', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_realizacion_polisomnografia', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'ct90_polisomnografia', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'iah', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'sas_no', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'sas_apneas_obstructivas', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'sas_apneas_no_claramanete_obstructivas', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'sas_apneas_centrales', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'sas_apneas_mixtas', OPT_BOOL_PIPELINE, inplace=True)
	
	apply_pipeline(df, 'sintomas_intolerancia_al_decubito', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'sintomas_disnea_de_esfuerzo', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'sintomas_sintomas_de_hipoventilacion_nocturna', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'sintomas_tos_ineficaz', OPT_BOOL_PIPELINE, inplace=True)
	
	apply_pipeline(df, 'cpap', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_cpap', OPT_DATE_PIPELINE, exact=False, inplace=True)
	apply_pipeline(df, 'portador_vmni', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_colocacion_vmni', OPT_DATE_PIPELINE, inplace=True)
	
	apply_pipeline(df, 'complicacion_vmni', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_complicacion_vmni', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'motivo_complicacion_vmni_ulcera_nasal_por_presion', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'motivo_complicacion_vmni_aerofagia', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'motivo_complicacion_vmni_sequedad_orofaringea', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'motivo_complicacion_vmni_otros', OPT_BOOL_PIPELINE, inplace=True)
	
	apply_pipeline(df, 'retirada_vmni', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_retirada_vmni', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'motivo_retirada_vmi_intolerancia', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'motivo_retirada_vmi_no_cumplimiento', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'motivo_retirada_vmi_rechazo_del_paciente', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'motivo_retirada_vmi_otros', OPT_BOOL_PIPELINE, inplace=True)
	
	apply_pipeline(df, 'fvc_sentado_absoluto', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'fvc_estirado_absoluto', OPT_NUMBER_PIPELINE, inplace=True)
	
	df.rename({
		'fecha_visita_fun_res': 'fecha_visita',
		'sas_no_claramanete_obstructivas': 'sas_no_claramente_obstructivas',
		'sintomas_sintomas_de_hipoventilacion_nocturna': 'sintomas_hipoventilacion_nocturna',
		'motivo_retirada_vmi_intolerancia': 'motivo_retirada_vmni_intolerancia',
		'motivo_retirada_vmi_no_cumplimiento': 'motivo_retirada_vmni_no_cumplimiento',
		'motivo_retirada_vmi_rechazo_del_paciente': 'motivo_retirada_vmni_rechazo',
		'motivo_retirada_vmi_otros': 'motivo_retirada_vmni_otros',
	}, inplace=True)


def clean_nutr_data(df):
	df.loc[df.id == '67e615f4-5f01-11eb-a21b-8316bff80df0', 'fecha_visita_datos_antro'] = '03-12-2021' # was '03-12-20219'
	df.loc[df.id == 'f9054526-1dcc-11eb-bb4a-9745fc970131', 'fecha_indicacion_peg'] = '23-10-2020' # was '23-10-20020'
	df.loc[df.id == '8c5b0f46-df7a-11e9-9c30-274ab37b3217', 'fecha_indicacion_peg'] = '20-07-2018' # was '20-07-3018'
	df.loc[df.id == 'eb700688-3dfe-11eb-9383-d3a3b2195eff', 'fecha_complicacion_peg'] = '22-11-2020' # was '22-11-202'
	df.replace('29-02-2015', '28-02-2015', regex=False, inplace=True) # 2015 was not a leap year
	
	apply_pipeline(df, 'fecha_visita_datos_antro', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'peso', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_peso', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'estatura', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'imc_actual', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'peso_premorbido', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_peso_premorbido', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'indicacion_peg', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_indicacion_peg', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'motivo_indicacion_peg_disfagia', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'motivo_indicacion_peg_perdida_de_peso', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'motivo_indicacion_peg_insuficiencia_respiratoria', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'motivo_indicacion_peg_otro', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'portador_peg', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_colocacion_peg', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'uso_peg', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'complicacion_peg', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_complicacion_peg', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'retirada', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_retirada_peg', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'disfagia', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'espesante', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_inicio_espesante', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'suplementacion_nutricional_oral', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_suplementacion_nutricional', OPT_DATE_PIPELINE, inplace=True)
	apply_pipeline(df, 'restrenimiento', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'laxante', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'peso_colocacion_peg', OPT_NUMBER_PIPELINE, inplace=True)
	apply_pipeline(df, 'suplementacion_nutricional_entera', OPT_BOOL_PIPELINE, inplace=True)
	apply_pipeline(df, 'fecha_inicio_suplementacion_nutricional_entera', OPT_DATE_PIPELINE, inplace=True)

	df.rename({
		'fecha_visita_datos_antro': 'fecha_visita',
		'retirada': 'retirada_peg',
		'restrenimiento': 'estreñimiento',
	}, inplace=True)


def make_argument_parser(name=sys.argv[0]):
	parser = ArgumentParser(prog=name)
	parser.add_argument('db', help='Path to SQLite snapshot file')
	parser.add_argument('-d', '--datadir', required=True, help='Directory to store snapshot data')
	parser.add_argument('-r', '--replace', action='store_true', help='Replace snapshot data if already exists')
	return parser


def save_snapshot(path, mapping, replace=False):
	datadir = Path(path)
	datadir.mkdir(parents=True, exist_ok=replace)
	for key, df in mapping.items():
		with open(datadir / f'{key}.pickle', 'wb') as f:
			pickle.dump(df, f)


if __name__ == '__main__':
	parser = make_argument_parser()
	args = parser.parse_args()

	try:
		with sqlite3.connect(f'file:{args.db}?mode=ro', uri=True) as con:
			df = load_patients_sql(con)
			als_data = load_als_data_sql(df, con)
			resp_data = load_resp_data_sql(df, con)
			nutr_data = load_nutr_data_sql(df, con)
			
			save_snapshot(args.datadir, {
				 'patients': df,
				 'als_data': als_data,
				'resp_data': resp_data,
				'nutr_data': nutr_data,
			}, replace=args.replace)

	except Exception as e:
		if DEBUG:
			raise
		else:
			print_message(e)