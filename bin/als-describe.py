#!/usr/bin/env python3

import sys
import warnings
from argparse  import ArgumentParser
from importlib import import_module
from pathlib   import Path

from projects  import project_names


def make_argument_parser(name: str = sys.argv[0]) -> ArgumentParser:
	parser = ArgumentParser(prog=name)
	parser.add_argument('-d', '--datadir', required=True, help='directory containing snapshot data')
	parser.add_argument('-p', '--project', choices=project_names(), help='prepare data for selected project')
	parser.add_argument('-q', '--quiet', action='store_true', help='supress warnings and debug messages')
	return parser


if __name__ == '__main__':
	try:
		parser = make_argument_parser()
		args = parser.parse_args()
		
		if args.quiet:
			warnings.filterwarnings('ignore')
		
		project = import_module(f'projects.{args.project}')
		project.describe(datadir=args.datadir)

	except Exception as e:
		parser.error(e)
