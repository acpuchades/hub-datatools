#!/usr/bin/env python3

import logging
import re
import readline
import sys


from abc import abstractproperty
from pathlib import Path
from argparse import ArgumentParser
from typing import Any, Callable, Dict, Optional, Sequence

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

    def exec(self, console: 'Search', command: str, args: Sequence[str]) -> Optional[int]:
        return None

    def exec_global(self, console: 'Search', command: str, args: Sequence[str]) -> Optional[int]:
        return None

    def uninstall(self, console: 'Search') -> None:
        pass


class UnknownCommand(RuntimeError):

    def __init__(self, command, **kwargs):
        super().__init__(**kwargs)
        self.command = command


class ExitEventLoop(Exception):
    pass


def try_eval_query(df: pd.DataFrame, query: str) -> Optional[pd.DataFrame]:
    try:
        return df.query(query)
    except Exception as e:
        logging.error(f'Invalid query: {e}')
        return None


def try_eval_expr(df: pd.DataFrame, expr: str) -> Optional[pd.DataFrame]:
    try:
        return df.eval(expr)
    except Exception as e:
        logging.error(f'Invalid expression: {e}')
        return None


def load_cached(console: 'Search', key: str, fn: Callable[[], Any]) -> Any:
    value = console.get(f'cache:{key}')
    if value is None:
        value = fn(key)
        console.set(f'cache:{key}', value)
    return value


PADDING = 40


class GroupNS(Namespace):

    def __init__(self, groupname: str):
        self._groupname = groupname
        self._records = None
        self._selected = pd.Index([])

    @ property
    def prompt(self) -> str:
        if self._records is None:
            return f'{self._groupname} [*/*]> '
        else:
            return f'{self._groupname} [{len(self._selected)}/{len(self._records)}]> '

    def _load(self, console: 'Search', args: Sequence[str]) -> int:
        if self._records is not None:
            logging.error('There are records loaded already')
            return -1

        try:
            datafile, *_ = args
            datadir = console.get('DATADIR')
            def load_datafile(key): return load_data(datadir, key)
            self._records = load_cached(console, datafile, load_datafile)
            logging.info(f'Loaded {len(self._records)} records')
            return 0

        except FileNotFoundError:
            logging.error('Data file does not exist')
            return -1

    def _loadgroup(self, console: 'Search', args: Sequence[str]) -> int:
        try:
            groupname, *_ = args
            records = console.get(f'groups:{groupname}')
            if records is None:
                logging.error(f'Group "{groupname}" not found')
                return -1
            self._records = records
            logging.info(f'Loaded {len(records)} records')
            return 0

        except ValueError:
            logging.error('Group name not specified')
            return -1

    def _join(self, console: 'Search', args: Sequence[str]) -> int:
        if self._records is None:
            logging.error('There are no records loaded yet')
            return -1

        if len(self._selected) > 0:
            logging.error('Joins with records already selected not supported')
            return -1

        try:
            datafile, key, rkey, *_ = args + [None]
            datadir = console.get('DATADIR')
            def load_datafile(k): return load_data(datadir, k)
            df = load_cached(console, datafile, load_datafile)
            if rkey is not None and key != rkey:
                self._records = self._records.merge(df, left_on=key, right_on=rkey)
                logging.info(f'Joined fields from "{datafile}" on {key}={rkey}')
            else:
                self._records = self._records.merge(df, on=key)
                logging.info(f'Joined fields from "{datafile}" on {key}')
            return 0

        except ValueError:
            logging.error('Joining key not specified')
            return -1

        except FileNotFoundError:
            logging.error('Data file does not exist')
            return -1

    def _set(self, console: 'Search', args: Sequence[str]) -> int:
        if self._records is None:
            logging.error('There are no records loaded yet')
            return -1

        try:
            name, *expr = args
            expr = ' '.join(expr)
            self._records[name] = try_eval_expr(self._records, expr)
            logging.info(f'Added computed field {name}')
            return 0

        except ValueError:
            logging.error('Field name and/or value expression not specified')
            return -1

    def _unset(self, console: 'Search', args: Sequence[str]) -> int:
        if self._records is None:
            logging.error('There are no records loaded yet')
            return -1

        try:
            name, *_ = args
            self._records.drop(columns=[name], inplace=True)
            logging.info(f'Removed field {name}')
            return 0

        except ValueError:
            logging.error('Field name to remove not specified')
            return -1

    def _trim(self, console: 'Search', args: Sequence[str]) -> int:
        if self._records is None:
            logging.error('There are no records loaded yet')
            return -1

        if len(args) < 1:
            logging.error('Data file not specified')
            return -1

        for datafile in args:
            datadir = console.get('DATADIR')
            def load_datafile(k): return load_data(datadir, k)
            df = load_cached(console, datafile, load_datafile)
            self._records = self._records.loc[:, df.columns]
        return 0

    def _showcols(self, console: 'Search', args: Sequence[str]) -> int:
        if self._records is None:
            logging.error('There are no records loaded yet')
            return -1

        logging.info('Available columns:')
        for name in self._records.columns:
            logging.info(f'- {name}')
        return 0

    def _include(self, console: 'Search', args: Sequence[str]) -> int:
        if len(args) == 0:
            logging.error('Include condition not specified')
            return -1

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

    def _exclude(self, console: 'Search', args: Sequence[str]) -> int:
        if len(args) == 0:
            logging.error('Exclude condition not specified')
            return -1

        if args[0] == 'all':
            prevcount = len(self._selected)
            self._selected = pd.Index([])
            logging.info(f'{prevcount} records removed')
            return 0
        else:
            query = ' '.join(args)
            records = self._records.loc[self._selected]
            matched = try_eval_query(records, query)
            if matched is None:
                return -1

            prevcount = len(self._selected)
            self._selected = self._selected.difference(matched.index)
            removecount = prevcount - len(self._selected)
            logging.info(f'{removecount} records removed, {len(self._selected)} left')
            return 0

    def _save(self, console: 'Search', args: Sequence[str]) -> int:
        logging.info(f'{len(self._selected)} records saved as group {self._groupname}')
        console.set(f'groups:{self._groupname}', self._records.loc[self._selected])
        console.pop_namespace()
        return 0

    def _help(self, console: 'Search', args: Sequence[str]) -> int:
        logging.info('Available commands:')
        logging.info('- load <name>'.ljust(PADDING, ' ') + 'Load records from data file')
        logging.info('- loadgroup <group>'.ljust(PADDING, ' ') + 'Load records from group')
        logging.info('- join <name> [key] [rkey]'.ljust(PADDING, ' ') + 'Join records from data file')
        logging.info('- trim <name>'.ljust(PADDING, ' ') + 'Trim columns not in data file')
        logging.info('- set <name> <expr>'.ljust(PADDING, ' ') + 'Add computed field with value')
        logging.info('- unset <name>'.ljust(PADDING, ' ') + 'Remove field from records')
        logging.info('- showcols'.ljust(PADDING, ' ') + 'Show columns from records')
        logging.info('- include all | include <expr>'.ljust(PADDING, ' ') + 'Add matching records')
        logging.info('- exclude all | exclude <expr>'.ljust(PADDING, ' ') + 'Remove matching records')
        logging.info('- save'.ljust(PADDING, ' ') + 'Save selected records and exit group context')

    def exec(self, console: 'Search', command: str, args: Sequence[str]) -> Optional[int]:
        match command:
            case 'load': return self._load(console, args)
            case 'loadgroup': return self._loadgroup(console, args)
            case 'join': return self._join(console, args)
            case 'trim': return self._trim(console, args)
            case 'set': return self._set(console, args)
            case 'unset': return self._unset(console, args)
            case 'showcols': return self._showcols(console, args)
            case 'include': return self._include(console, args)
            case 'exclude': return self._exclude(console, args)
            case 'save': return self._save(console, args)
            case 'help': return self._help(console, args)
        return None


class GlobalNS(Namespace):

    def __init__(self, prompt: str):
        self._prompt = prompt

    @ property
    def prompt(self) -> str:
        return self._prompt

    def _group(self, console: 'Search', args: Sequence[str]) -> int:
        try:
            groupname, *_ = args
            if console.get(f'groups:{groupname}') is None:
                console.push_namespace(GroupNS(groupname))
                return 0
            else:
                logging.error(f'Group "{groupname}" already exists')
                return -1

        except ValueError:
            logging.error('Group name not specified')
            return -1

    def _output(self, console: 'Search', args: Sequence[str]) -> int:
        try:
            groupname, path, *_ = args + [None]
            path = Path(path if path is not None else groupname)

            records = console.get(f'groups:{groupname}')
            if records is None:
                logging.error(f'Group "{groupname}" does not exist')
                return -1

            match console.get('OUTPUTFORMAT'):
                case 'csv':
                    path = path.with_suffix('.csv')
                    records.to_csv(path)
                case 'excel':
                    path = path.with_suffix('.xlsx')
                    records.to_excel(path)

            logging.info(f'{len(records)} records saved to {path}')
            return 0

        except ValueError:
            logging.error('No group name was given')
            return -1

    def _back(self, console: 'Search', args: Sequence[str]) -> int:
        console.pop_namespace()
        return 0

    def _exit(self, console: 'Search', args: Sequence[str]) -> int:
        raise ExitEventLoop()

    def _help(self, console: 'Search', args: Sequence[str]) -> int:
        logging.info('Available commands:')
        logging.info('- group <name>'.ljust(PADDING, ' ') + 'Enter group context')
        logging.info('- output <group> [file]'.ljust(PADDING, ' ') + 'Save group records to file')

    def _help_global(self, console: 'Search', args: Sequence[str]) -> int:
        logging.info('- back'.ljust(PADDING, ' ') + 'Return to previous context')
        logging.info('- help'.ljust(PADDING, ' ') + 'Prints this help')
        logging.info('- exit'.ljust(PADDING, ' ') + 'Exit console')
        return 0

    def exec(self, console: 'Search', command: str, args: Sequence[str]) -> Optional[int]:
        match command:
            case 'group': return self._group(console, args)
            case 'output': return self._output(console, args)
            case 'help': return self._help(console, args)

    def exec_global(self, console: 'Search', command: str, args: Sequence[str]) -> Optional[int]:
        match command:
            case 'help': return self._help_global(console, args)
            case 'back': return self._back(console, args)
            case 'exit': return self._exit(console, args)


class Search:

    def __init__(self, prompt='> '):
        self._names = {}
        self._vars = {}
        self._ns = [GlobalNS(prompt=prompt)]

    @ property
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

    def eval(self, cmdline: str) -> Optional[int]:
        cmdline = re.sub('#.*$', '', cmdline)
        command, *args = cmdline.strip().split(' ')
        if command == '':
            return None

        result = self._ns[-1].exec(self, command, args)
        if result is not None:
            return result

        for ns in reversed(self._ns):
            result = ns.exec_global(self, command, args)
            if result is not None:
                return result

        raise UnknownCommand(command)

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
        while True:
            try:
                result = self.eval(input(self.prompt))

            except UnknownCommand as e:
                logging.error(f'Unrecognized command {e.command}')

            except (EOFError, KeyboardInterrupt):
                print('', file=sys.stderr)
                break

            except ExitEventLoop:
                break


def make_argument_parser(name: str = sys.argv[0]) -> ArgumentParser:
    parser = ArgumentParser(prog=name)
    parser.add_argument('-d', '--datadir', type=Path, required=True,
                        help='directory containing snapshot data')
    parser.add_argument('-i', '--import', dest='import_', nargs='+', type=Path,
                        default=[], help='command modules to import')
    parser.add_argument('-f', '--format', default='csv', choices=['csv', 'excel'],
                        help='output file format')
    return parser


if __name__ == '__main__':
    console.initialize()

    parser = make_argument_parser()
    args = parser.parse_args()

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    search = Search()
    search.set('DATADIR', args.datadir)
    search.set('OUTPUTFORMAT', args.format)

    for file in args.import_:
        try:
            with open(file, 'r') as f:
                for i, line in enumerate(f.readlines()):
                    search.eval(line.strip())

        except UnknownCommand as e:
            logging.error(f'{file.name}:{i}: unrecognized command {e.command}')

        except ExitEventLoop:
            break

    else:
        search.event_loop()
