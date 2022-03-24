#!/usr/bin/env python3

import sys
import warnings
from argparse import ArgumentParser

from errors import *
from projects import get_project_class, get_project_names, load_project_modules


def make_argument_parser(name: str = sys.argv[0]) -> ArgumentParser:
	parser = ArgumentParser(prog=name)
	parser.add_argument('-d', '--datadir', required=True, help='directory containing snapshot data')
	parser.add_argument('-p', '--project', required=True, choices=get_project_names(), help='prepare data for selected project')
	parser.add_argument('-q', '--quiet', action='store_true', help='supress warnings and debug messages')
	return parser


if __name__ == '__main__':
	try:
		load_project_modules()

		parser = make_argument_parser()
		args = parser.parse_args()

		if args.quiet:
			warnings.filterwarnings('ignore')

		projectclass = get_project_class(args.project)
		project = projectclass(args.datadir)
		project.describe()

	except NotImplementedError as e:
		print(f'{sys.argv[0]}: {e}', file=sys.stderr)
		sys.error(ExitCode.NotImplemented.value)