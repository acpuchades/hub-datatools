import logging
from pathlib import Path
from typing import Dict

from pandas import DataFrame

from hub_datatools.serialize import load_data
from hub_datatools.projects import Project, project
from hub_datatools.projects._followup import load_followup_data


ALSFRS_FIELDS = [
    'lenguaje',
    'salivacion',
    'deglucion',
    'escritura',
    'vestido',
    'cama',
    'caminar',
    'subir_escaleras',
    'disnea',
    'ortopnea',
    'insuf_resp',
]

GENE_FIELDS = {
    'C9orf72': 'estado_c9',
    'SOD1': 'estado_sod1',
    'ATXN2': 'estado_atxn2',
}

GENE_STATUS_CATEGORIES = {
    'Normal': 'Normal',
    'Alterado': 'Altered',
}

DYSPHAGIA_CATEGORIES = {
    'No': 'None',
    'Sólidos': 'Solids',
    'Líquidos': 'Liquids',
    'Sólidos+Líquidos': 'Solids+Liquids',
}

SEX_CATEGORIES = {
    'Hombre': 'Male',
    'Mujer': 'Female'
}

SMOKING_CATEGORIES = {
    'Activo': 'Active',
    'Exfumador': 'Ceased',
    'Nunca': 'Never',
}

COGNITIVE_DX_CATEGORIES = {
    'DTA': 'AD',
    'DFT': 'DFT',
    'DCL-Cognitivo': 'MCI',
    'DCL-Conductual': 'MBI',
    'DCL-Mixto': 'MCI+MBI',
    'Normal': 'Normal',
    'Otros': 'Other',
}

PHENOTYPE_CATEGORIES = {
    'AMP': 'PMA',
    'ELA Bulbar': 'Bulbar',
    'ELA Espinal': 'Spinal',
    'ELA Respiratoria': 'Respiratory',
    'ELP': 'PLS',
    'Flail-Arm': 'Flail-Arm',
    'Flail-Leg': 'Flail-Leg',
    'Hemiplejica': 'Hemiplegic',
    'Monomielica': 'Monomielic',
    'Otro': 'Other',
    'PBP': 'PBP',
    'Pseudopolineuritica': 'Pseudopolyneuritic',
}

MN_INVOLVEMENT_CATEGORIES = {
    'MNS': 'UMN',
    'MNI': 'LMN',
    'MNS+MNI': 'UMN+LMN',
    'MNS>MNI': 'UMN>LMN',
    'MNI>MNS': 'LMN>UMN',
}

WEAKNESS_PATTERN_CATEGORIES = {
    'Bulbar': 'Bulbar',
    'MMSS': 'Upper Limbs',
    'MMII': 'Lower Limbs',
    'MMSS+MMII': 'Both Limbs',
}

HOSP_DISCHARGE_TYPE_CATEGORIES = {
    'A DOMICILI': 'Planned',
    'RESID.SOCIAL': 'Planned',
    'ICO': 'Transfer',
    'ALTA CONT.CENTR': 'Transfer',
    'AGUTS/PSIQUIATRIC': 'Transfer',
    'H. DOMICILARIA': 'Home Hospitalization',
    'SOCI SANITARI': 'Rehabilitation',
    'EXITUS': 'Death',
}

URG_DISCHARGE_TYPE_CATEGORIES = {
    'ALTA VOLUNTARIA': 'DAMA',
    'FUGIDA/ABANDONAMENT': 'Abscond',
    'ALTA A DOMICILI': 'Planned',
    'REMISIO A ATENCIO PRIMARIA': 'Planned',
    'DERIVACIO A UN ALTRE CENTRE': 'Transfer',
    'INGRES A L\'HOSPITAL': 'Admitted',
    'EXITUS': 'Death',
}


@project('precision_als')
class PrecisionALS(Project):

    def __init__(self, datadir: Path):
        patients = load_data(datadir, 'ufmn/patients').sort_index()
        self._patients = patients = patients[patients.fecha_dx.notna()]

        alsfrs_data = load_data(datadir, 'ufmn/alsfrs')
        alsfrs_data.dropna(how='all', subset=ALSFRS_FIELDS, inplace=True)

        nutr_data = load_data(datadir, 'ufmn/nutr')
        resp_data = load_data(datadir, 'ufmn/resp')

        followups = load_followup_data(datadir, alsfrs_data, nutr_data, resp_data)
        followups = followups.merge(patients, on='id_paciente')
        followups = followups.set_index('id_paciente').sort_index()
        self._followups = followups = followups[followups.fecha_dx.notna()]

        urg_episodes = load_data(datadir, 'hub_urg/episodes')
        hosp_episodes = load_data(datadir, 'hub_hosp/episodes')
        self._urg_diagnoses = load_data(datadir, 'hub_urg/diagnoses')
        self._hosp_diagnoses = load_data(datadir, 'hub_hosp/diagnoses')

        self._alsfrs_data = (alsfrs_data.reset_index()
                             .merge(followups, on=['id_paciente', 'fecha_visita'], suffixes=[None, '_x'])
                             .set_index(['id_paciente', 'fecha_visita']).sort_index())

        self._nutr_data = (nutr_data.reset_index()
                           .set_index(['id_paciente', 'fecha_visita']).sort_index())

        self._resp_data = (resp_data.reset_index()
                           .set_index(['id_paciente', 'fecha_visita']).sort_index())

        self._urg_episodes = (urg_episodes.reset_index()
                              .merge(self._patients.reset_index(), on='nhc')
                              .sort_values(['id_paciente', 'inicio_episodio'])
                              .set_index(['id_paciente', 'id_episodio']))

        self._hosp_episodes = (hosp_episodes.reset_index()
                               .merge(self._patients.reset_index(), on='nhc')
                               .sort_values(['id_paciente', 'inicio_episodio'])
                               .set_index(['id_paciente', 'id_episodio']))

    def _export_patient_data(self) -> DataFrame:
        logging.info('Precision ALS: Exporting patient data')

        return DataFrame({
            'birthdate': self._patients.fecha_nacimiento,
            'sex': self._patients.sexo.map(SEX_CATEGORIES),
            'smoking': self._patients.fumador.map(SMOKING_CATEGORIES),
            'fh_als': self._patients.historia_familiar_motoneurona,
            'fh_alzheimer': self._patients.historia_familiar_alzheimer,
            'fh_parkinson': self._patients.historia_familiar_parkinson,
            'cognitive_imp': self._patients.deterioro_cognitivo,
            'cognitive_dx': self._patients.estudio_cognitivo.map(COGNITIVE_DX_CATEGORIES),
            'clinical_onset': self._patients.inicio_clinica,
            'phenotype_dx': self._patients.fenotipo_dx.map(PHENOTYPE_CATEGORIES),
            'phenotype_death': self._patients.fenotipo_exitus.map(PHENOTYPE_CATEGORIES),
            'mn_involvement': self._patients.afectacion_mn.map(MN_INVOLVEMENT_CATEGORIES),
            'weakness_pattern': self._patients.patron_debilidad.map(WEAKNESS_PATTERN_CATEGORIES),
            'dx_date': self._patients.fecha_dx,
            'riluzole_received': self._patients.riluzol,
            'riluzole_start': self._patients.inicio_riluzol,
            'last_followup': self._followups.groupby('id_paciente').fecha_visita.max(),
            'niv_support': self._alsfrs_data[self._alsfrs_data.insuf_resp == 1].reset_index().groupby('id_paciente').fecha_visita.min(),
            'imv_support': self._alsfrs_data[self._alsfrs_data.insuf_resp == 0].reset_index().groupby('id_paciente').fecha_visita.min(),
            'death': self._patients.fecha_exitus,
        }).rename_axis('patient_id')

    def _export_genetic_data(self) -> DataFrame:
        logging.info('Precision ALS: Exporting genetic data')

        return DataFrame({
            'c9_status': self._patients.estado_c9.map(GENE_STATUS_CATEGORIES),
            'sod1_status': self._patients.estado_sod1.map(GENE_STATUS_CATEGORIES),
            'atxn2_status': self._patients.estado_atxn2.map(GENE_STATUS_CATEGORIES),
        }).rename_axis('patient_id')

    def _export_alsfrs_data(self) -> DataFrame:
        logging.info('Precision ALS: Exporting ALSFRS-R assesments data')

        return DataFrame({
            'speech': self._alsfrs_data.lenguaje,
            'salivation': self._alsfrs_data.salivacion,
            'swallowing': self._alsfrs_data.deglucion,
            'handwriting': self._alsfrs_data.escritura,
            'cutting': self._alsfrs_data.cortar,
            'cutting_peg': self._alsfrs_data.cortar_con_peg,
            'cutting_no_peg': self._alsfrs_data.cortar_sin_peg,
            'dressing': self._alsfrs_data.vestido,
            'bed': self._alsfrs_data.cama,
            'walking': self._alsfrs_data.caminar,
            'stairs': self._alsfrs_data.subir_escaleras,
            'dyspnea': self._alsfrs_data.disnea,
            'orthopnea': self._alsfrs_data.ortopnea,
            'resp_insuf': self._alsfrs_data.insuf_resp,
            'alsfrs_bulbar': self._alsfrs_data.alsfrs_bulbar_c,
            'alsfrs_fine_motor': self._alsfrs_data.alsfrs_fine_motor_c,
            'alsfrs_gross_motor': self._alsfrs_data.alsfrs_gross_motor_c,
            'alsfrs_respiratory': self._alsfrs_data.alsfrs_respiratory_c,
            'alsfrs_total': self._alsfrs_data.alsfrs_total_c.where(
                self._alsfrs_data.alsfrs_total_c.notna(),
                self._alsfrs_data.alsfrs_total
            ),
            'peg_carrier': self._alsfrs_data.portador_peg,
            'peg_indicated': self._alsfrs_data.indicacion_peg,
            'kings_r': self._alsfrs_data.kings,
            'kings_c': self._alsfrs_data.kings_c,
            'mitos': self._alsfrs_data.mitos_c,
        }).rename_axis(['patient_id', 'assessment_date'])

    def _export_respiratory_data(self) -> DataFrame:
        logging.info('Precision ALS: Exporting respiratory assesments data')

        return DataFrame({
            'abg_ph': self._resp_data.ph_sangre_arterial,
            'abg_po2': self._resp_data.pao2,
            'abg_pco2': self._resp_data.paco2,
            'abg_hco3': self._resp_data.hco3,
            'pns': self._resp_data.pns,
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
        }).rename_axis(['patient_id', 'assessment_date'])

    def _export_nutritional_data(self) -> DataFrame:
        logging.info('Precision ALS: Exporting nutritional assesments data')

        return DataFrame({
            'weight': self._nutr_data.peso,
            'height': self._nutr_data.estatura,
            'bmi': self._nutr_data.imc,
            'peg_indication': self._nutr_data.indicacion_peg,
            'peg_indication_date': self._nutr_data.fecha_indicacion_peg,
            'peg_carrier': self._nutr_data.portador_peg,
            'peg_colocation_date': self._nutr_data.fecha_colocacion_peg,
            'peg_removal': self._nutr_data.retirada_peg,
            'peg_removal_date': self._nutr_data.fecha_retirada_peg,
            'dysphagia': self._nutr_data.disfagia.map(DYSPHAGIA_CATEGORIES),
            'food_thickener_usage': self._nutr_data.espesante,
            'food_thickener_start': self._nutr_data.inicio_espesante,
            'oral_supplementation': self._nutr_data.supl_oral,
            'oral_supplementation_start': self._nutr_data.inicio_supl_oral,
            'enteric_supplementation': self._nutr_data.supl_enteral,
            'enteric_supplementation_start': self._nutr_data.inicio_supl_enteral,
            'constipation': self._nutr_data.estreñimiento,
            'laxative_usage': self._nutr_data.laxante,
        }).rename_axis(['patient_id', 'assessment_date'])

    def _export_ER_episodes_data(self) -> DataFrame:
        logging.info('Precision ALS: Exporting ER episodes data')

        return DataFrame({
            'admission_date': self._urg_episodes.inicio_episodio,
            'discharge_date': self._urg_episodes.fin_episodio,
            'discharge_type': self._urg_episodes.destino_alta.map(URG_DISCHARGE_TYPE_CATEGORIES),
        }).rename_axis(['patient_id', 'episode_id'])

    def _export_ER_diagnoses_data(self) -> DataFrame:
        logging.info('Precision ALS: Exporting ER diagnoses data')

        return DataFrame({
            'dx_description': self._urg_diagnoses.descripcion_dx,
        }).rename_axis(['episode_id', 'dx_code'])

    def _export_hospital_episodes_data(self) -> DataFrame:
        logging.info('Precision ALS: Exporting hospitalization episodes data')

        return DataFrame({
            'admission_date': self._hosp_episodes.inicio_episodio,
            'discharge_date': self._hosp_episodes.fin_episodio,
            'discharge_type': self._hosp_episodes.destino_alta.map(HOSP_DISCHARGE_TYPE_CATEGORIES),
            'discharge_department': self._hosp_episodes.servicio_alta,
        }).rename_axis(['patient_id', 'episode_id'])

    def _export_hospital_diagnoses_data(self) -> DataFrame:
        logging.info('Precision ALS: Exporting hospitalization diagnoses data')

        return DataFrame({
            'dx_description': self._hosp_diagnoses.descripcion_dx,
        }).rename_axis(['episode_id', 'dx_code'])

    def export_data(self) -> Dict[str, DataFrame]:
        return {
            'Patients': self._export_patient_data(),
            'Genetics': self._export_genetic_data(),
            'ALSFRS-R': self._export_alsfrs_data(),
            'Respiratory': self._export_respiratory_data(),
            'Nutritional': self._export_nutritional_data(),
            'ER Episodes': self._export_ER_episodes_data(),
            'ER Diagnoses': self._export_ER_diagnoses_data(),
            'Hospital Episodes': self._export_hospital_episodes_data(),
            'Hospital Diagnoses': self._export_hospital_diagnoses_data(),
        }
