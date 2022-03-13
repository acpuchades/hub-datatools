#!/usr/bin/env python3

import sys
import warnings
from argparse  import ArgumentParser
from importlib import import_module
from pathlib   import Path

from serialize import save_data


def data_source_names():
	sourcesdir = Path(__file__).parent / 'sources'
	return [ d.stem for d in sourcesdir.iterdir()
	                if not d.name.startswith('_') ]


def add_data_source_arguments(parser, name, dsource):
	args = [f'--{name}']
	if 'CMDLINE_SHORT' in dsource.__dict__:
		args.insert(0, dsource.CMDLINE_SHORT)
	kwargs = dsource.__dict__.get('CMDLINE_KWARGS', {})
	parser.add_argument(*args, **kwargs)


def make_argument_parser(name=sys.argv[0]):
	parser = ArgumentParser(prog=name)
	parser.add_argument('-d', '--datadir', required=True, help='directory to store snapshot data')
	parser.add_argument('-r', '--replace', action='store_true', help='replace snapshot data if already exists')
	parser.add_argument('-q', '--quiet', action='store_true', help='supress warnings and debug messages')
	
	datagroup = parser.add_argument_group(title='Data sources')
	for name in data_source_names():
		dsource = import_module(f'sources.{name}')
		add_data_source_arguments(parser, name, dsource)
	
	return parser


if __name__ == '__main__':
	parser = make_argument_parser()
	args = parser.parse_args()
	vargs = vars(args)
	
	if args.quiet:
		warnings.filterwarnings('ignore')

	try:
		nsources = 0
		for name in data_source_names():
			if vargs.get(name) is None:
				continue
			
			dsource = import_module(f'sources.{name}')
			data = dsource.load_data(args)
			save_data(args.datadir, data, replace=args.replace)
			nsources += 1
		
		if nsources == 0:
			parser.error('no data sources given')

	except Exception as e:
		parser.error(e)