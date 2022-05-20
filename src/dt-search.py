#!/usr/bin/env python3

from abc import abstractproperty
import logging
import sys

from abc import abstractproperty
from pathlib import Path
from argparse import ArgumentParser
from typing import Any, Dict, Optional, Sequence

import pandas as pd
from pandas.core.computation.ops import UndefinedVariableError

import console
from serialize import load_data


class Namespace:

    @abstractproperty
    def prompt(self) -> str:
        pass

    def install(self, console: 'Search'):
        pass

    def exec(self, console: 'Search', command: str, args: Sequence[str]) -> int:
        return None

    def exec_global(self, console: 'Search', command: str, args: Sequence[str]) -> int:
        return None

    def uninstall(self, console: 'Search') -> None:
        pass


class ExitEventLoop(Exception):
    pass


def try_eval_query(df, query) -> Optional[pd.DataFrame]:
    try:
        return df.query(query)
    except Exception as e:
        logging.error(f'Invalid query: {e}')
        return None


class DefGroupNS(Namespace):

    def __init__(self, groupname: str):
        self._groupname = groupname
        self._records = None
        self._selected = pd.Index([])

    @property
    def prompt(self) -> str:
        return f'defgroup({self._groupname})> '

    def _info(self, console, args) -> int:
        if self._records is None:
            logging.info('No records loaded yet')
        else:
            logging.info(
                f'{len(self._records)} records loaded, {len(self._selected)} selected')
        return 0

    def _load(self, console, args) -> int:
        if self._records is not None:
            logging.error('There are records loaded already')
            return -1

        try:
            datadir = console.get('DATADIR')
            self._records = load_data(datadir, args[0])
            logging.info(f'Loaded {len(self._records)} records')
            return 0

        except FileNotFoundError:
            logging.error('Data file does not exist')
            return -1

    def _loadgroup(self, console, args) -> int:
        records = console.get(f'groups:{args[0]}')
        if records is None:
            logging.error(f'Group "{args[0]}" not found')
            return -1
        self._records = records
        logging.info(f'Loaded {len(records)} records')
        return 0

    def _join(self, console, args) -> int:
        if len(args) < 2:
            logging.error('No key to join on was given')
            return -1

        if self._records is None:
            logging.error('There are no records loaded yet')
            return -1

        if len(self._selected) > 0:
            logging.error('Joins with records already selected not supported')
            return -1

        try:
            datadir = console.get('DATADIR')
            df = load_data(datadir, args[0])
            if len(args) == 2:
                self._records = self._records.merge(df, on=args[1])
                logging.info(f'Joined fields from {args[0]} on {args[1]}')
            else:
                self._records = self._records.merge(df, left_on=args[1], right_on=args[2])
                logging.info(f'Joined fields from {args[0]} on {args[1]}={args[2]}')
            return 0

        except FileNotFoundError:
            logging.error('Data file does not exist')
            return -1

    def _select(self, console, args) -> int:
        if args[0] == 'all':
            self._selected = self._records.index
            logging.info(f'{len(self._records)} records added')
            return 0
        else:
            query = ' '.join(args)
            matched = try_eval_query(self._records, query)
            if matched is None:
                return -1

            prevcount = len(self._selected)
            self._selected = self._selected.union(matched.index)
            addcount = len(self._selected) - prevcount
            logging.info(f'{len(matched)} records matched, {addcount} added')
            return 0

    def _remove(self, console, args) -> int:
        if args[0] == 'all':
            prevcount = len(self._selected)
            self._selected = pd.Index([])
            logging.info(f'{prevcount} records removed')
            return 0
        else:
            query = ' '.join(args)
            matched = try_eval_query(self._selected, query)
            if matched is None:
                return -1

            prevcount = len(self._selected)
            self._selected = self._selected.difference(matched.index)
            removecount = prevcount - len(self._selected)
            logging.info(f'{removecount} records removed, {len(self._selected)} left')
            return 0

    def _save(self, console, args) -> int:
        logging.info(f'{len(self._selected)} records saved as group "{self._groupname}"')
        console.set(f'groups:{self._groupname}', self._records.loc[self._selected])
        console.pop_namespace()
        return 0

    def exec(self, console: 'Search', command: str, args: Sequence[str]) -> Optional[int]:
        match command:
            case 'info': return self._info(console, args)
            case 'load': return self._load(console, args)
            case 'loadgroup': return self._loadgroup(console, args)
            case 'join': return self._join(console, args)
            case 'select': return self._select(console, args)
            case 'remove': return self._remove(console, args)
            case 'save': return self._save(console, args)
        return None


class GlobalNS(Namespace):

    def __init__(self, prompt: str):
        self._prompt = prompt

    @property
    def prompt(self) -> str:
        return self._prompt

    def _defgroup(self, console, args) -> int:
        if len(args) < 1:
            logging.error('No group name given')
            return -1

        if console.get(f'groups:{args[0]}') is None:
            console.push_namespace(DefGroupNS(args[0]))
            return 0
        else:
            logging.error(f'Group "{args[0]}" already exists')
            return -1

    def _output(self, console, args) -> int:
        if len(args) < 1:
            logging.error('No group names were given')

        groupname = args[0]
        path = Path(args[1] if len(args) >= 2 else groupname)
        records = console.get(f'groups:{groupname}')
        records.to_csv(path)
        logging.info(f'{len(records)} records saved to {path.name}')
        return 0

    def _back(self, console, args) -> int:
        console.pop_namespace()
        return 0

    def _exit(self, console, args) -> int:
        raise ExitEventLoop()

    def exec(self, console: 'Search', command: str, args: Sequence[str]) -> Optional[int]:
        match command:
            case 'defgroup': return self._defgroup(console, args)
            case 'output': return self._output(console, args)

    def exec_global(self, console: 'Search', command: str, args: Sequence[str]) -> Optional[int]:
        match command:
            case 'back': return self._back(console, args)
            case 'exit': return self._exit(console, args)


class Search:

    def __init__(self, prompt='> '):
        self._names = {}
        self._vars = {}
        self._ns = [GlobalNS(prompt=prompt)]

    @property
    def prompt(self) -> str:
        for ns in reversed(self._ns):
            if ns.prompt:
                return ns.prompt

    def get(self, name: str) -> Optional[Any]:
        return self._vars.get(name)

    def set(self, name: str, value: Any) -> None:
        self._vars[name] = value

    def unset(self, name: str) -> None:
        del self._vars[name]

    def eval(self, cmdline: str) -> int:
        command, *args = cmdline.strip().split(' ')

        for ns in reversed(self._ns):
            result = ns.exec(self, command, args)
            if result is not None:
                return result

        for ns in reversed(self._ns):
            result = ns.exec_global(self, command, args)
            if result is not None:
                return result

        return None

    def push_namespace(self, ns: Namespace) -> None:
        self._ns.append(ns)
        ns.install(self)

    def pop_namespace(self) -> bool:
        if len(self._ns) <= 1:
            return False

        ns = self._ns.pop()
        ns.uninstall(self)
        return True

    def event_loop(self) -> None:
        exited = False
        while not exited:
            try:
                print(self.prompt, file=sys.stderr, end='', flush=True)
                cmdline = sys.stdin.readline().strip()
                if cmdline == '':
                    continue

                result = self.eval(cmdline)
                if result is None:
                    logging.error('Unrecognized command')
            except ExitEventLoop:
                exited = True


def make_argument_parser(name: str = sys.argv[0]) -> ArgumentParser:
    parser = ArgumentParser(prog=name)
    parser.add_argument('-d', '--datadir', type=Path, required=True,
                        help='directory containing snapshot data')
    parser.add_argument('-f', '--file', type=Path, help='file with commands to execute')
    return parser


if __name__ == '__main__':
    console.initialize()
    parser = make_argument_parser()
    args = parser.parse_args()

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    search = Search()
    search.set('DATADIR', args.datadir)

    if args.file is not None:
        with open(args.file, 'r') as f:
            for line in f.readlines():
                search.eval(line.strip())
    else:
        search.event_loop()
