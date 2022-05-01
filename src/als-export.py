#!/usr/bin/env python3

import sys
import warnings
from argparse import ArgumentParser
from pathlib import Path
from typing import Any, Dict, Optional

from pandas import DataFrame, ExcelWriter

from errors import *
from serialize import load_data
from projects import get_project_class, get_project_names, load_project_modules


FORMAT_SUFFIXES = {
	'csv': '.csv',
	'excel': '.xlsx',
}

SUFFIX_FORMATS = {
	'.csv': 'csv',
	'.xls': 'excel',
	'.xlsx': 'excel',
}


def _export_data_csv(data: DataFrame | Dict[str, DataFrame], path: Path, replace: bool = False, **kwargs: Dict[str, Any]) -> None:
	if isinstance(data, dict):
		for key, child in data.items():
			childpath = path.joinpath(key)
			_export_data_csv(child, childpath, replace)
	else:
		path = path.with_suffix('.csv')
		if path.exists() and not replace:
			raise FileExistsError('output file already exists')

		path.parent.mkdir(exist_ok=True)
		data.to_csv(path, **kwargs)


def _export_data_excel(data: DataFrame | Dict[str, DataFrame], path: Path, replace: bool = False, **kwargs: Dict[str, Any]) -> None:
	path.parent.mkdir(exist_ok=True)
	if isinstance(data, dict):
		try:
			with ExcelWriter(path) as writer:
				for key, child in data.items():
					child.to_excel(writer, sheet_name=key)
		except ValueError:
			raise FileExistsError('output excel tab already exists')
	else:
		data.to_excel(path, **kwargs)


EXPORT_FORMATS = {
	'csv': _export_data_csv,
	'excel': _export_data_excel,
}

DEFAULT_FORMAT = 'csv'


def export_data(data: DataFrame | Dict[str, DataFrame], path: Path, format: str,
                replace: bool = False, **kwargs: Dict[str, Any]) -> None:

	if path.suffix is None:
		path = path.with_suffix(FORMAT_SUFFIXES.get(format))

	if path.exists() and not replace:
		raise FileExistsError('output file already exists')

	exportfn = EXPORT_FORMATS.get(format)
	if exportfn is None:
		raise NotImplementedError('unsupported output file format')
	exportfn(data, path, replace, **kwargs)


def make_argument_parser(name: str = sys.argv[0]) -> ArgumentParser:
	parser = ArgumentParser(prog=name)
	parser.add_argument('-d', '--datadir', required=True, help='directory containing snapshot data')

	group = parser.add_mutually_exclusive_group()
	group.add_argument('-s', '--source', help='output data from selected data source')
	group.add_argument('-p', '--project', choices=get_project_names(), help='output data for selected project')

	parser.add_argument('-f', '--format', choices=EXPORT_FORMATS.keys(), help='file output format to use')
	parser.add_argument('-c', '--columns', help='output only selected data columns')
	parser.add_argument('-q', '--query', help='select rows based on given query')

	parser.add_argument('-o', '--output', type=Path, help='file or directory to output project results')
	parser.add_argument('-r', '--replace', action='store_true', help='replace existing file if already exists')
	parser.add_argument('--quiet', action='store_true', help='supress warnings and debug messages')
	return parser


if __name__ == '__main__':
	try:
		load_project_modules()

		parser = make_argument_parser()
		args = parser.parse_args()

		if args.quiet:
			warnings.filterwarnings('ignore')

		if args.project is not None:
			projectclass = get_project_class(args.project)
			project = projectclass(datadir=args.datadir)
			data = project.export_data()
		elif args.source is not None:
			data = load_data(args.datadir, args.source)
		else:
			parser.error('no data sources to be exported were given')

		if args.query is not None:
			if isinstance(data, DataFrame):
				data = data.query(args.query)
			else:
				raise NotImplementedError('filtering of compound results not implemented')

		if args.columns is not None:
			if isinstance(data, DataFrame):
				data = data[args.columns.split(',')]
			else:
				raise NotImplementedError('subsetting of compound results not implemented')

		format = args.format

		if format is None:
			suffix = args.output.suffix
			format = SUFFIX_FORMATS.get(suffix)

		if format is None:
			format = DEFAULT_FORMAT

		export_data(data, args.output, format=format, replace=args.replace)

	except FileNotFoundError as e:
		print(f'{sys.argv[0]}: {e}', file=sys.stderr)
		sys.exit(ExitCode.NotFound.value)

	except NotImplementedError as e:
		print(f'{sys.argv[0]}: {e}', file=sys.stderr)
		sys.exit(ExitCode.NotImplemented.value)

	except FileExistsError as e:
		print(f'{sys.argv[0]}: {e}', file=sys.stderr)
		sys.exit(ExitCode.AlreadyExists.value)
