import re
import logging
import pandas as pd

from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Dict, Sequence

from hub_datatools.datasources import DataSource, datasource

EDMUS_DATES_V5_7 = [
    'onset_date',
    'outcome_date',
    'date',
    'birth_date',
    'relapse_date',
    'date_of_previous_assessment',
    'test_date',
    'csf_date',
    'time_clinical_poser_date',
    'time_clinical_mcdonald_date',
    'time_mri_date',
    'ms_onset',
    'progression_onset',
    'compared_date',
    'clinical_assessment_date',
    'date_of_birth',
    'first_exam',
    'last_clinical_follow_up',
    'last_clinical_assessment',
    'last_info',
    'date_consent_form',
    'ofsep_date_submission',
    'ofset_date_signature',
    'vital_status_date',
    'deceased_date_of_information',
    'created',
    'last_modified',
    'validation_date',
    'last_menstruation',
    'consent_date_biology',
    'consent_date_genetics',
    'ongoing_date',
    'date_inclusion',
    'date_consent',
    'date_withdrawal',
    'before',
    'latest_date',
    'end_date',
]

EDMUS_INDEXES_V5_7 = {
    'AE': 'ae_id',
    'AEDetail': 'ae_detail_id',
    'AutoAb': 'antinmo_id',
    'CardMonito': 'monitoring_id',
    'Child': 'children_id',
    'Clinical': 'assessment_id',
    'ClinicalD': 'assessment_id',
    'Comment': 'comment_id',
    'CSF': 'csf_id',
    'DataChckup': 'patient_id',
    'Diagnosis': 'patient_id',
    'EP': 'ep_id',
    'Episode': 'episode_id',
    'Exam': 'exam_id',
    'FamDisease': 'family_disease_id',
    'Keyword': 'keyword_id',
    'MRI': 'mri_id',
    'PatDisease': 'disease_id',
    'Personal': 'patient_id',
    'Posology': 'posology_id',
    'Pregnancy': 'pregnancy_id',
    'Protocol': 'protocol_id',
    'Rehab': 'record_id',
    'Sample': 'record_id',
    'Sibship': 'sibship_id',
    'SocioEco': 'socioeco_id',
    'Study': 'study_id',
    'Trt_DM': 'treatment_id',
    'Trt_Other': 'treatment_id',
    'Vaccine': 'vaccine_id',
}

EDMUS_INDEXES = {
    '5.7': EDMUS_INDEXES_V5_7,
}

EDMUS_DATES = {
    '5.7': EDMUS_DATES_V5_7,
}


def _normalize_string(s: str) -> str:
    s = re.sub(r'\W+', '_', s)
    return s.lower()


def _transform_parse_dates(df: pd.DataFrame, datecols: Sequence[str]) -> pd.DataFrame:
    cols = [col for col in df.columns if col in datecols]
    df.loc[:, cols] = df.loc[:, cols].apply(lambda x: pd.to_datetime(x, format='%d/%m/%Y'))


def _try_load_edmus_data_file(path: Path, indexes: Dict[str, str], datecols: Sequence[str] = None) -> pd.DataFrame:
    pattern = r'(?P<site>\w+)-(?P<section>\w+)-(?:\d+)-(?:\d{6})_(?:\d{6})-(?P<export_mode>\w+)\.txt'
    result = re.match(pattern, path.name)
    if not result:
        return None

    section = result.group('section')
    logging.info(f'EDMUS: Loading "{section}" data file')
    df = pd.read_csv(path, sep='\t', encoding='utf-16')
    df.rename(columns=_normalize_string, inplace=True)
    _transform_parse_dates(df, datecols)

    index = indexes.get(section)
    if index is not None:
        df.set_index(index, inplace=True)

    return _normalize_string(section), df


@datasource('edmus')
class EDMUS(DataSource):

    @ staticmethod
    def add_arguments(parser: ArgumentParser) -> None:
        parser.add_argument('--edmus', metavar='EXPORT_FILE',
                            help='EDMUS exported data directory')
        parser.add_argument('--edmus-version', metavar='VERSION',
                            choices=EDMUS_INDEXES.keys(),
                            help='EDMUS version')

    @ staticmethod
    def is_active(args: Namespace) -> bool:
        return args.edmus is not None

    def load_data(self, args: Namespace) -> Dict[str, pd.DataFrame]:
        if args.edmus_version is None:
            raise ValueError('missing --edmus-version argument')

        indexes = EDMUS_INDEXES.get(args.edmus_version)
        coldates = EDMUS_DATES.get(args.edmus_version)
        if not indexes or not coldates:
            raise NotImplemented('EDMUS: Unsupported version given')

        try:
            results = {}
            for path in Path(args.edmus).iterdir():
                section, data = _try_load_edmus_data_file(path, indexes, coldates)
                results[f'edmus/{section}'] = data
            return results

        except FileNotFoundError as e:
            raise FileNotFoundError('EDMUS: Data directory does not exist')
