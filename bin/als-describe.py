#!/usr/bin/env python3

import sys
from argparse import ArgumentParser

from core import *


def make_argument_parser(name=sys.argv[0]):
	parser = ArgumentParser(prog=name)
	parser.add_argument('datadir', help='Directory containing snapshot data')
	return parser

if __name__ == '__main__':
	try:
		parser = make_argument_parser()
		args = parser.parse_args()

		patients  = load_snapshot_data(args.datadir, 'patients')
		als_data  = load_snapshot_data(args.datadir, 'als_data')
		resp_data = load_snapshot_data(args.datadir, 'resp_data')
		nutr_data = load_snapshot_data(args.datadir, 'nutr_data')

		cases = patients[patients.fecha_diagnostico_ELA != False]
		gene_cases = cases[cases.c9_status.notna() | cases.sod1_status.notna() | cases.atxn2_status.notna()]

		print('Biogen Extant Task 1')
		print('--------------------')
		print(f'> Total number of cases: {len(cases)}')
		print(f'> Current number of living cases: {len(cases[~cases.exitus])}')
		print(f'> Number of cases with genetic testing available:')
		print(f'\t* C9orf72 -> {len(cases[cases.c9_status.notna()])} (positive: {len(cases[cases.c9_status == "Mutado"])})')
		print(f'\t* SOD1 -> {len(cases[cases.sod1_status.notna()])} (positive: {len(cases[cases.sod1_status == "Mutado"])})')
		print(f'\t* ATXN2 -> {len(cases[cases.atxn2_status.notna()])} (positive: {len(cases[cases.atxn2_status == "Mutado"])})')
		print()

		print('Biogen Extant Task 2')
		print('--------------------')
		print('> Number of cases with follow-up data available:')
		print(f'\t* 1+ follow-up -> {len(als_data["pid"].value_counts())}')
		print(f'\t* 2+ follow-up -> {sum(als_data["pid"].value_counts() >= 2)}')
		print()

	except Exception as e:
		if DEBUG:
			raise
		else:
			print_message(e)
