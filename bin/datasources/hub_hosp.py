from argparse    import ArgumentParser, Namespace

import pandas as pd

from datasources import DataSource, datasource


PATIENT_ID_COLUMN = 'Pacient (NHC)'
EPISODE_ID_COLUMN = 'Episodi'
EPISODE_BEGIN_COLUMN = 'Data ingres'
EPISODE_END_COLUMN = 'Data hora alta'
DIAGNOSIS_CODE_COLUMN = 'Diagnòstic codi'
DIAGNOSIS_DESCRIPTION_COLUMN = 'Diagnòstic desc'
DIAGNOSIS_CLASS_COLUMN = 'Marca diagnòstic principal'
DISCHARGE_TYPE_COLUMN = 'Classe alta desc'
DISCHARGE_DESTINATION_COLUMN = 'Centre destí alta desc'
DISCHARGE_DEPARTMENT_COLUMN = 'Servei alta desc'

EPISODE_COLUMNS = {
	PATIENT_ID_COLUMN: 'nhc',
	EPISODE_ID_COLUMN: 'id_episodio',
	EPISODE_BEGIN_COLUMN: 'inicio_episodio',
	EPISODE_END_COLUMN: 'fin_episodio',
	DISCHARGE_TYPE_COLUMN: 'destino_alta',
	DISCHARGE_DESTINATION_COLUMN: 'centro_destino_alta',
	DISCHARGE_DEPARTMENT_COLUMN: 'servicio_alta',
}

DIAGNOSES_COLUMNS = {
	EPISODE_ID_COLUMN: 'id_episodio',
	DIAGNOSIS_CODE_COLUMN: 'codigo_dx',
	DIAGNOSIS_DESCRIPTION_COLUMN: 'descripcion_dx',
	DIAGNOSIS_CLASS_COLUMN:'clase_dx',
}

FFILL_COLUMNS = [
	PATIENT_ID_COLUMN,
	EPISODE_ID_COLUMN,
	EPISODE_BEGIN_COLUMN,
	EPISODE_END_COLUMN,
]


def _load_episodes_from_df(df: pd.DataFrame) -> pd.DataFrame:
	df = df.copy()
	df.drop_duplicates(subset=EPISODE_COLUMNS.keys(), inplace=True)
	df[EPISODE_BEGIN_COLUMN] = pd.to_datetime(df[EPISODE_BEGIN_COLUMN])
	df[EPISODE_END_COLUMN] = pd.to_datetime(df[EPISODE_END_COLUMN])
	df.rename(columns=EPISODE_COLUMNS, inplace=True)
	df.inicio_episodio = pd.to_datetime(df.inicio_episodio)
	df.fin_episodio = pd.to_datetime
	df.set_index('id_episodio', inplace=True)
	df.dropna(how='all', inplace=True)
	return df


def _load_diagnoses_from_df(df: pd.DataFrame) -> pd.DataFrame:
	df = df.copy()[DIAGNOSES_COLUMNS.keys()]
	df.rename(columns=DIAGNOSES_COLUMNS, inplace=True)
	df.set_index(['id_episodio', 'codigo_dx'], inplace=True)
	df.dropna(how='all', inplace=True)
	return df


@datasource('hub_hosp')
class HUBHosp(DataSource):

	@staticmethod
	def add_arguments(parser: ArgumentParser) -> None:
		parser.add_argument('--hub-hosp', metavar='EXCEL_FILE',
		                    help='Excel file containing HUB hospitalization data')
		parser.add_argument('--hub-hosp-excel-tab', default=0,
		                    metavar='NAME', help='Excel tab containing HUB hospitalization data')
		parser.add_argument('--hub-hosp-column-row', type=int, default=1,
		                    metavar='ROW', help='Excel row number containing column names')

	@staticmethod
	def has_arguments(args: Namespace) -> bool:
		return args.hub_hosp is not None

	def load_data(self, args: Namespace) -> pd.DataFrame:
		df = pd.read_excel(args.hub_hosp, sheet_name=args.hub_hosp_excel_tab,
		                   header=args.hub_hosp_column_row - 1)
		df[FFILL_COLUMNS] = df[FFILL_COLUMNS].ffill()

		return {
		     'hub_hosp/episodes': _load_episodes_from_df(df),
		    'hub_hosp/diagnoses': _load_diagnoses_from_df(df),
		}
