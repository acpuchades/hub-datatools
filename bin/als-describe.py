#!/usr/bin/env python3

import sys
from argparse  import ArgumentParser
from importlib import import_module
from pathlib   import Path

from core import *


def project_choices():
	projectdir = Path(__file__).parent / 'projects'
	return [d.stem for d in projectdir.iterdir()]


def make_argument_parser(name=sys.argv[0]):
	parser = ArgumentParser(prog=name)
	parser.add_argument('datadir', help='Directory containing snapshot data')
	parser.add_argument('-p', '--project', choices=project_choices(), help='Prepare data for selected project')
	return parser


if __name__ == '__main__':
	try:
		parser = make_argument_parser()
		args = parser.parse_args()
		
		projmod = import_module(f'projects.{args.project}')
		projmod.describe(args.datadir)

	except Exception as e:
		if DEBUG:
			raise
		else:
			print_message(e)
