from argparse import ArgumentParser, Namespace

import pandas as pd

PATIENT_ID_COLUMN = 'Pacient (NHC)'
EPISODE_ID_COLUMN = 'Episodi'
EPISODE_BEGIN_COLUMN = 'Data ingres'
EPISODE_END_COLUMN = 'Data hora alta'
DIAGNOSIS_CODE_COLUMN = 'Diagnòstic codi'
DIAGNOSIS_DESCRIPTION_COLUMN = 'Diagnòstic desc'
DIAGNOSIS_MAIN_COLUMN = 'Marca diagnòstic principal'
DISCHARGE_TYPE_COLUMN = 'Classe alta desc'
DISCHARGE_DESTINATION_COLUMN = 'Centre destí alta desc'
DISCHARGE_DEPARTMENT_COLUMN = 'Servei alta desc'

EPISODE_COLUMNS = [
	PATIENT_ID_COLUMN,
	EPISODE_ID_COLUMN,
	EPISODE_BEGIN_COLUMN,
	EPISODE_END_COLUMN,
	DISCHARGE_TYPE_COLUMN,
	DISCHARGE_DESTINATION_COLUMN,
	DISCHARGE_DEPARTMENT_COLUMN,
]

DIAGNOSES_COLUMNS = [
	EPISODE_ID_COLUMN,
	DIAGNOSIS_CODE_COLUMN,
	DIAGNOSIS_DESCRIPTION_COLUMN,
	DIAGNOSIS_MAIN_COLUMN,

]

FFILL_COLUMNS = [
	PATIENT_ID_COLUMN,
	EPISODE_ID_COLUMN,
	EPISODE_BEGIN_COLUMN,
	EPISODE_END_COLUMN,
]


def add_data_source_arguments(parser: ArgumentParser) -> None:
	parser.add_argument('--hub-hosp', metavar='EXCEL_FILE',
	                    help='Excel file containing HUB hospitalization data')
	parser.add_argument('--hub-hosp-excel-tab', default=0,
	                    metavar='NAME', help='Excel tab containing HUB hospitalization data')
	parser.add_argument('--hub-hosp-column-row', type=int, default=1,
	                    metavar='ROW', help='Excel row number containing column names')


def load_episodes_from_df(df: pd.DataFrame) -> pd.DataFrame:
	df = df[EPISODE_COLUMNS].drop_duplicates().set_index(EPISODE_ID_COLUMN)
	df[EPISODE_BEGIN_COLUMN] = pd.to_datetime(df[EPISODE_BEGIN_COLUMN])
	df[EPISODE_END_COLUMN] = pd.to_datetime(df[EPISODE_END_COLUMN])
	df.dropna(axis='index', inplace=True)
	return df


def load_diagnoses_from_df(df: pd.DataFrame) -> pd.DataFrame:
	df = df[DIAGNOSES_COLUMNS].set_index([EPISODE_ID_COLUMN, DIAGNOSIS_CODE_COLUMN])
	df.dropna(axis='index', inplace=True)
	return df


def load_data(args: Namespace) -> pd.DataFrame:
	df = pd.read_excel(args.hub_hosp, sheet_name=args.hub_hosp_excel_tab,
	                   header=args.hub_hosp_column_row - 1)
	df[FFILL_COLUMNS] = df[FFILL_COLUMNS].ffill()

	return {
		 'hub_hosp/episodes': load_episodes_from_df(df),
		'hub_hosp/diagnoses': load_diagnoses_from_df(df),
	}
