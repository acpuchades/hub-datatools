#!/usr/bin/env python3

import sys
import warnings
from argparse import ArgumentParser
from pathlib import Path
from typing import Any, Dict, Optional

from pandas import DataFrame

from serialize import load_data
from projects import get_project_class, get_project_names, load_project_modules


EXPORT_FORMATS = {
	  'csv': lambda df, path: df.to_csv(path),
	'excel': lambda df, path: df.to_excel(path),
}

FORMAT_SUFFIXES = {
	  'csv': '.csv',
	'excel': '.xlsx',
}

SUFFIX_FORMATS = {
	 '.csv': 'csv',
	 '.xls': 'excel',
	'.xlsx': 'excel',
}


def output_file_format(path: Path, format: Optional[str]) -> Optional[str]:
	if path.suffix != '':
		return SUFFIX_FORMATS.get(path.suffix)

	if format is not None:
		return format

	return None


def output_file_path(path: Path, format: str) -> Path:
	if path.suffix != '':
		return path

	suffix = FORMAT_SUFFIXES.get(format)
	if suffix is not None:
		return path.with_suffix(suffix)

	return path


def export_data(data: DataFrame, path: Path, format: Optional[str], replace: bool = False) -> None:
	format = output_file_format(path, format)
	path = output_file_path(path, format)

	if path.exists() and not replace:
		raise FileExistsError(path)

	export_fn = EXPORT_FORMATS.get(format)
	if export_fn:
		export_fn(data, path)
	else:
		raise ValueError('unsupported output format')


def export_output_data(data: DataFrame | Dict[str, DataFrame], path: Path, format: Optional[str], **kwargs: Dict[str, Any]) -> None:
	if isinstance(data, dict):
		for key, df in data.items():
			filepath = path.joinpath(key)
			filepath.parent.mkdir(exist_ok=True)
			export_data(df, filepath, format=format, **kwargs)
	else:
		export_data(data, path, format, **kwargs)


def make_argument_parser(name: str = sys.argv[0]) -> ArgumentParser:
	parser = ArgumentParser(prog=name)
	parser.add_argument('-d', '--datadir', required=True, help='directory containing snapshot data')

	group = parser.add_mutually_exclusive_group()
	group.add_argument('-s', '--source', help='output data from selected data source')
	group.add_argument('-p', '--project', choices=get_project_names(), help='output data for selected project')

	parser.add_argument('-f', '--format', default='csv', choices=EXPORT_FORMATS.keys(), help='file output format to use')
	parser.add_argument('-c', '--columns', help='output only selected data columns')
	parser.add_argument('--query', help='select rows based on given query')

	parser.add_argument('-o', '--output', type=Path, help='file or directory to output project results')
	parser.add_argument('-r', '--replace', action='store_true', help='replace existing file if already exists')
	parser.add_argument('-q', '--quiet', action='store_true', help='supress warnings and debug messages')
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
				print(f'{sys.argv[0]}: filtering of compound results not implemented', file=sys.stderr)

		if args.columns is not None:
			if isinstance(data, DataFrame):
				data = data[args.columns.split(',')]
			else:
				print(f'{sys.argv[0]}: subsetting of compound results not implemented', file=sys.stderr)

		export_output_data(data, path=args.output,
		                   format=args.format, replace=args.replace)


	except FileExistsError:
		print(f'{sys.argv[0]}: output file already exists', file=sys.stderr)

	except FileNotFoundError:
		print(f'{sys.argv[0]}: source file not found', file=sys.stderr)