#!/usr/bin/env python3

import sys
from argparse  import ArgumentParser
from importlib import import_module
from pathlib   import Path


def project_names():
	projectsdir = Path(__file__).parent / 'projects'
	return [ d.stem for d in projectsdir.iterdir()
	                if not d.name.startswith('_' ) ]


def make_argument_parser(name=sys.argv[0]):
	parser = ArgumentParser(prog=name)
	parser.add_argument('-d', '--datadir', required=True, help='Directory containing snapshot data')
	parser.add_argument('-p', '--project', choices=project_names(), help='Prepare data for selected project')
	return parser


if __name__ == '__main__':
	try:
		parser = make_argument_parser()
		args = parser.parse_args()
		
		project = import_module(f'projects.{args.project}')
		project.describe(args.datadir)

	except Exception as e:
		parser.error(e)
