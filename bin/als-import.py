#!/usr/bin/env python3

import sys
import warnings

from argparse    import ArgumentParser
from importlib   import import_module
from pathlib     import Path
from serialize   import load_data

from serialize   import save_data
from datasources import DataSource, get_datasource_class, get_datasource_names, load_datasource_modules


def make_argument_parser(name: str = sys.argv[0]) -> ArgumentParser:
	parser = ArgumentParser(prog=name)
	parser.add_argument('-d', '--datadir', required=True, help='directory to store snapshot data')
	parser.add_argument('-r', '--replace', action='store_true', help='replace snapshot data if already exists')
	parser.add_argument('-q', '--quiet', action='store_true', help='supress warnings and debug messages')

	for name in get_datasource_names():
		group = parser.add_argument_group(name)
		datasource_class = get_datasource_class(name)
		datasource_class.add_arguments(group)

	return parser


if __name__ == '__main__':
	try:
		load_datasource_modules()

		parser = make_argument_parser()
		args = parser.parse_args()

		if args.quiet:
			warnings.filterwarnings('ignore')

		nsources = 0
		for name in get_datasource_names():
			datasource_class = get_datasource_class(name)
			if not datasource_class.has_arguments(args):
				continue

			datasource = datasource_class()
			data = datasource.load_data(args)
			save_data(args.datadir, data, replace=args.replace)
			nsources += 1

		if nsources == 0:
			parser.error('no data sources given')

	except FileExistsError:
		print(f'{sys.argv[0]}: data file already exists', file=sys.stderr)