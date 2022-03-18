from argparse    import ArgumentParser, Namespace

from datasources import DataSource, datasource

import pandas as pd

PATIENT_ID_COLUMN = 'Pacient (NHC)'
EPISODE_ID_COLUMN = 'Episodi'
EPISODE_BEGIN_COLUMN = 'Data hora entrada'
EPISODE_END_COLUMN = 'Data hora sortida'
DISCHARGE_TYPE_COLUMN = 'Classe fi episodi desc'
TRIAGE_CATEGORY_COLUMN = 'Triatge desc (Darrer)'
DISCHARGE_DEPARTMENT_COLUMN = 'Servei alta desc'
DISCHARGE_DESTINATION_COLUMN = 'Centre destí desc'
DISCHARGE_MODULE_COLUMN = 'Darrera UT desc'
DIAGNOSIS_CODE_COLUMN = 'Diagnòstic codi'
DIAGNOSIS_DESCRIPTION_COLUMN = 'Diagnòstic descripció'

EPISODE_COLUMNS = {
	PATIENT_ID_COLUMN: 'nhc',
	EPISODE_ID_COLUMN: 'episode_id',
	EPISODE_BEGIN_COLUMN: 'episode_begin',
	EPISODE_END_COLUMN: 'episode_end',
	TRIAGE_CATEGORY_COLUMN: 'triage_category',
	DISCHARGE_TYPE_COLUMN: 'discharge_type',
	DISCHARGE_DESTINATION_COLUMN: 'discharge_dest',
	DISCHARGE_DEPARTMENT_COLUMN: 'discharge_dept',
	DISCHARGE_MODULE_COLUMN: 'discharge_mod',
}

DIAGNOSES_COLUMNS = {
	EPISODE_ID_COLUMN: 'episode_id',
	DIAGNOSIS_CODE_COLUMN: 'dx_code',
	DIAGNOSIS_DESCRIPTION_COLUMN: 'dx_desc',
}

FFILL_COLUMNS = [
	PATIENT_ID_COLUMN,
	EPISODE_ID_COLUMN,
	EPISODE_BEGIN_COLUMN,
	EPISODE_END_COLUMN,
]


def load_episodes_from_df(df: pd.DataFrame) -> pd.DataFrame:
	df = df.copy()
	df.drop_duplicates(subset=EPISODE_COLUMNS.keys(), inplace=True)
	df[EPISODE_BEGIN_COLUMN] = pd.to_datetime(df[EPISODE_BEGIN_COLUMN])
	df[EPISODE_END_COLUMN] = pd.to_datetime(df[EPISODE_END_COLUMN])
	df.rename(columns=EPISODE_COLUMNS, inplace=True)
	df.set_index('episode_id', inplace=True)
	df.dropna(axis='index', inplace=True)
	return df


def load_diagnoses_from_df(df: pd.DataFrame) -> pd.DataFrame:
	df = df.copy()[DIAGNOSES_COLUMNS.keys()]
	df.rename(columns=DIAGNOSES_COLUMNS, inplace=True)
	df.set_index(['episode_id', 'dx_code'], inplace=True)
	df.dropna(axis='index', inplace=True)
	return df


@datasource(name='hub_urg')
class HUBUrg(DataSource):

	@staticmethod
	def add_arguments(parser: ArgumentParser) -> None:
		parser.add_argument('--hub-urg', metavar='EXCEL_FILE',
		                    help='Excel file containing HUB ER data')
		parser.add_argument('--hub-urg-excel-tab', default=0, metavar='NAME',
		                    help='Excel tab containing HUB ER data')
		parser.add_argument('--hub-urg-column-row', type=int, default=1, metavar='ROW',
		                    help='Excel row number containing column names')

	@staticmethod
	def has_arguments(args: Namespace) -> bool:
		return args.hub_urg is not None

	def load_data(self, args: Namespace) -> pd.DataFrame:
		df = pd.read_excel(args.hub_urg, sheet_name=args.hub_urg_excel_tab,
		                   header=args.hub_urg_column_row - 1)
		df[FFILL_COLUMNS] = df[FFILL_COLUMNS].ffill()

		return {
		     'hub_urg/episodes': load_episodes_from_df(df),
		    'hub_urg/diagnoses': load_diagnoses_from_df(df),
		}