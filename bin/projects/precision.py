from pathlib   import Path

from serialize import load_data


def describe(datadir: Path) -> None:
	patients  = load_data(datadir, 'ufmn/patients')
	als_data  = load_data(datadir, 'ufmn/als_data')
	resp_data = load_data(datadir, 'ufmn/resp_data')
	nutr_data = load_data(datadir, 'ufmn/nutr_data')

	cases = patients[patients.fecha_dx != False]
	gene_cases = cases[cases.c9_status.notna() | cases.sod1_status.notna() | cases.atxn2_status.notna()]

	print('Biogen Extant Task 1')
	print('--------------------')
	print(f'> Total number of cases: {len(cases)}')
	print(f'> Current number of living cases: {len(cases[~cases.exitus])}')
	print(f'> Number of cases with genetic testing available: {len(gene_cases)}')
	print(f'\t* C9orf72 -> {len(cases[cases.c9_status.notna()])} (positive: {len(cases[cases.c9_status == "Mutado"])})')
	print(f'\t* SOD1 -> {len(cases[cases.sod1_status.notna()])} (positive: {len(cases[cases.sod1_status == "Mutado"])})')
	print(f'\t* ATXN2 -> {len(cases[cases.atxn2_status.notna()])} (positive: {len(cases[cases.atxn2_status == "Mutado"])})')
	print()

	print('Biogen Extant Task 2')
	print('--------------------')
	print('> Number of cases with follow-up data available:')
	print(f'\t* 1+ follow-up -> {len(als_data.value_counts("pid"))}')
	print(f'\t* 2+ follow-up -> {sum(als_data.value_counts("pid") >= 2)}')
	print()

	print('Biogen Extant Task 3')
	print('--------------------')
	print(f'> Time to ambulation support: {len(als_data[als_data.caminar <= 2].value_counts("pid"))}')
	print(f'> Time to CPAP: {len(resp_data[resp_data.cpap.fillna(False)].value_counts("pid"))}')
	print(f'> Time to VMNI: {len(resp_data[resp_data.portador_vmni.fillna(False)].value_counts("pid"))}')
	print(f'> Time to PEG: {len(nutr_data[nutr_data.indicacion_peg.fillna(False)].value_counts("pid"))}')
	print(f'> Time to death: {len(cases[~cases.fecha_exitus.isna()])}')
	print()

	print('Biogen Extant Task 4')
	print('--------------------')
	print(f'> Patients on Riluzole: {len(cases[cases.riluzol.fillna(False)])}')
	print(f'> Time to Riluzole: {len(cases[~cases.inicio_riluzol.isna()])}')
	print()

	print('Biogen Extant Task 6')
	print('--------------------')
	print(f'> Patients currently working: {len(cases[cases.situacion_activa])}')
	print()