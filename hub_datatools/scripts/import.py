#!/usr/bin/env python3

import sys
import logging
from argparse import ArgumentParser

from hub_datatools import console
from hub_datatools.datasources import *
from hub_datatools.serialize import save_data


def _make_argument_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument('-d', '--datadir', required=True, help='directory to store snapshot data')
    parser.add_argument('-r', '--replace', action='store_true',
                        help='replace snapshot data if already exists')

    for name in get_datasource_names():
        group = parser.add_argument_group(name)
        datasource_class = get_datasource_class(name)
        datasource_class.add_arguments(group)

    return parser


def main() -> None:
    try:
        console.initialize()
        load_datasource_modules()

        parser = _make_argument_parser()
        args = parser.parse_args()

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        nsources = 0
        for name in get_datasource_names():
            datasource_class = get_datasource_class(name)
            if not datasource_class.is_active(args):
                continue

            datasource = datasource_class()
            data = datasource.load_data(args)
            save_data(args.datadir, data, replace=args.replace)
            nsources += 1

        if nsources == 0:
            parser.error('no data sources given')

        logging.info('Done')

    except Exception as e:
        logging.error(e)
        sys.exit(-1)


if __name__ == '__main__':
    main()
