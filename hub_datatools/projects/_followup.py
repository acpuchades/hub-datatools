from pathlib import Path
from typing import Optional

import pandas as pd

from hub_datatools.serialize import load_data


ALSFRS_TOTAL_COLUMNS = [
    'lenguaje',
    'salivacion',
    'deglucion',
    'escritura',
    'cortar',
    'vestido',
    'cama',
    'caminar',
    'subir_escaleras',
    'disnea',
    'ortopnea',
    'insuf_resp',
]

ALSFRS_BULBAR_COLUMNS = [
    'lenguaje',
    'salivacion',
    'deglucion',
]

ALSFRS_FINE_MOTOR_COLUMNS = [
    'escritura',
    'cortar',
    'vestido',
]

ALSFRS_GROSS_MOTOR_COLUMNS = [
    'cama',
    'caminar',
    'subir_escaleras',
]

ALSFRS_RESPIRATORY_COLUMNS = [
    'disnea',
    'ortopnea',
    'insuf_resp',
]


def _calculate_kings_from_alsfrs(df: pd.DataFrame) -> pd.Series:
    result = pd.DataFrame([], index=df.index)
    result['bulbar'] = (df[['lenguaje', 'salivacion', 'deglucion']] < 4).any(axis=1).astype('Int64')
    result['upper'] = (df[['escritura', 'cortar_sin_peg']] < 4).any(axis=1).astype('Int64')
    result['lower'] = (df.caminar < 4).astype('Int64')
    result['regions'] = result.bulbar + result.upper + result.lower
    result['nutr_failure'] = df.indicacion_peg
    result['resp_failure'] = (df.disnea == 0) | (df.insuf_resp < 4)

    result['stage'] = None
    result.loc[result.regions.notna(), 'stage'] = result.regions.apply(str)
    result.loc[result.nutr_failure == True, 'stage'] = '4A'
    result.loc[result.resp_failure == True, 'stage'] = '4B'
    return result.stage


def _calculate_mitos_from_alsfrs(df: pd.DataFrame) -> pd.Series:
    walking_selfcare = (df.caminar <= 1) | (df.vestido <= 1)
    swallowing = df.deglucion <= 1
    communicating = (df.lenguaje <= 1) | (df.escritura <= 1)
    breathing = (df.disnea <= 1) | (df.insuf_resp <= 2)
    domains = walking_selfcare.astype('Int64')
    domains += swallowing.astype('Int64')
    domains += communicating.astype('Int64')
    domains += breathing.astype('Int64')
    return domains


def _add_calculated_fields(df: pd.DataFrame, inplace: bool = False) -> Optional[pd.DataFrame]:
    if not inplace:
        df = df.copy()

    # known peg carriers
    cutting_peg = df[df.portador_peg == True].cortar_con_peg
    df.loc[cutting_peg.index, 'cortar'] = cutting_peg

    # known peg non-carriers
    cutting_nopeg = df[df.portador_peg == False].cortar_sin_peg
    df.loc[cutting_nopeg.index, 'cortar'] = cutting_nopeg

    # we only score peg version of cutting item for peg carriers
    cutting_peg_nonzero = df[df.portador_peg.isna() & (df.cortar_con_peg != 0)].cortar_con_peg
    df.loc[cutting_peg_nonzero.index, 'cortar'] = cutting_peg_nonzero

    # we only score non-peg version of cutting item for peg non-carriers
    cutting_nopeg_nonzero = df[df.portador_peg.isna() & (df.cortar_sin_peg != 0)].cortar_sin_peg
    df.loc[cutting_nopeg_nonzero.index, 'cortar'] = cutting_nopeg_nonzero

    # we don't know peg status, but it doesn't matter
    cutting_all_equal = df[df.cortar_con_peg == df.cortar_sin_peg].cortar_con_peg
    df.loc[cutting_all_equal.index, 'cortar'] = cutting_all_equal

    df['alsfrs_bulbar_c'] = df[ALSFRS_BULBAR_COLUMNS].sum(axis=1, skipna=False).astype('Int64')
    df['alsfrs_fine_motor_c'] = df[ALSFRS_FINE_MOTOR_COLUMNS].sum(axis=1, skipna=False).astype('Int64')
    df['alsfrs_gross_motor_c'] = df[ALSFRS_GROSS_MOTOR_COLUMNS].sum(axis=1, skipna=False).astype('Int64')
    df['alsfrs_respiratory_c'] = df[ALSFRS_RESPIRATORY_COLUMNS].sum(axis=1, skipna=False).astype('Int64')
    df['alsfrs_total_c'] = df[ALSFRS_TOTAL_COLUMNS].sum(axis=1, skipna=False).astype('Int64')

    df['kings_c'] = _calculate_kings_from_alsfrs(df)
    df['mitos_c'] = _calculate_mitos_from_alsfrs(df)

    if not inplace:
        return df


def load_followup_data(datadir: Path = None, alsfrs_data: pd.DataFrame = None,
                       nutr_data: pd.DataFrame = None, resp_data: pd.DataFrame = None) -> pd.DataFrame:
    alsfrs_data = alsfrs_data if alsfrs_data is not None else load_data(datadir, 'ufmn/alsfrs')
    nutr_data = nutr_data if nutr_data is not None else load_data(datadir, 'ufmn/nutr')
    resp_data = resp_data if resp_data is not None else load_data(datadir, 'ufmn/resp')

    followups = alsfrs_data.merge(nutr_data, how='outer', on=['id_paciente', 'fecha_visita'])
    followups = followups.merge(resp_data, how='outer', on=['id_paciente', 'fecha_visita'])
    followups.dropna(subset=['id_paciente', 'fecha_visita'], inplace=True)

    followups = (followups.set_index(['id_paciente', 'fecha_visita'])
                 .groupby(level=[0, 1]).bfill().reset_index()
                 .drop_duplicates(['id_paciente', 'fecha_visita']))

    _add_calculated_fields(followups, inplace=True)
    return followups
