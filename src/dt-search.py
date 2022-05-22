#!/usr/bin/env python3

import logging
from os import terminal_size
import re
import readline
import sys


from abc import abstractproperty
from pathlib import Path
from argparse import ArgumentParser
from typing import Any, Callable, Dict, Optional, Sequence

import pandas as pd

import console
from serialize import load_data


class Context:

    @abstractproperty
    def prompt(self) -> str:
        pass

    def install(self, console: 'Search') -> None:
        pass

    def exec(self, console: 'Search', command: str, args: Sequence[str]) -> Optional[int]:
        return None

    def exec_global(self, console: 'Search', command: str, args: Sequence[str]) -> Optional[int]:
        return None

    def uninstall(self, console: 'Search') -> None:
        pass


MAXWIDTH = 80
PADDING = MAXWIDTH // 2


class UnknownCommand(RuntimeError):

    def __init__(self, command, **kwargs):
        super().__init__(**kwargs)
        self.command = command


class ExitEventLoop(Exception):
    pass


def _try_eval_query(df: pd.DataFrame, query: str) -> Optional[pd.DataFrame]:
    try:
        return df.query(query)
    except Exception as e:
        logging.error(f'Invalid query: {e.args[0]}')
        return None


def _show_dataframe(df: pd.DataFrame) -> None:
    with pd.option_context('display.width', 120):
        for line in str(df).split('\n'):
            logging.info(line)


def _show_dataframe_columns(df: pd.DataFrame) -> None:
    for name in df.index.names:
        logging.info(f'* {name}')

    for name in df.columns:
        logging.info(f'- {name}')


def _try_eval_expr(df: pd.DataFrame, expr: str) -> Optional[pd.DataFrame]:
    try:
        return df.eval(expr)
    except Exception as e:
        logging.error(f'Invalid expression: {e.args[0]}')
        return None


def _load_cached(console: 'Search', key: str, fn: Callable[[], Any]) -> Any:
    value = console.get(f'cache:{key}')
    if value is None:
        value = fn(key)
        console.set(f'cache:{key}', value)
    return value


class GroupByContext(Context):

    def __init__(self, key, records):
        self._key = key
        self._values = {}
        self._records = records
        self._grouped = records.groupby(key)

    @property
    def prompt(self) -> str:
        return f'*{self._key}* ({len(self._grouped)})> '

    def _summarize(self, console: 'Search', args: Sequence[str]) -> int:
        try:
            name, col, agg, *_ = args
            self._values[name] = pd.NamedAgg(col, aggfunc=agg)
            logging.info(f'Added aggregating field {name} as {agg}({col})')
            return 0

        except ValueError:
            logging.error('Field names and/or summary function not specified')
            return -1

    def _ungroup(self, console: 'Search', args: Sequence[int]) -> int:
        try:
            cols = self._values.keys()
            if len(cols) > 0:
                summary = self._records.groupby(self._key).agg(**self._values)
                data = self._records.join(summary, on=self._key, rsuffix='_')
                self._records.loc[:, cols] = data.loc[:, cols]
                logging.info(f'Added {len(cols)} fields')
            console.pop_namespace()
            return 0

        except Exception as e:
            logging.error(f'Ungrouping error: {e.args[0]}')
            console.pop_namespace()
            return -1

    def exec(self, console: 'Search', command: str, args: Sequence[str]) -> int:
        match command:
            case 'summarize': return self._summarize(console, args)
            case 'ungroup': return self._ungroup(console, args)


def _load_from_origin(console: 'Search', origin: str) -> Optional[pd.DataFrame]:
    if origin.startswith('@'):
        groupname = origin[1:]
        records = console.get(f'groups:{groupname}')
        if records is None:
            logging.error('Group {groupname} not found')
            return None

    else:
        datadir = console.get('DATADIR')
        def load_from_datafile(key): return load_data(datadir, key)
        records = _load_cached(console, origin, load_from_datafile)
        if records is None:
            return None

    return records


class GroupContext(Context):

    def __init__(self, groupname: str):
        self._groupname = groupname
        self._records = None
        self._included = pd.Index([])

    @ property
    def prompt(self) -> str:
        if self._records is None:
            return f'{self._groupname} [-/-]> '
        else:
            return f'{self._groupname} [{len(self._included)}/{len(self._records)}]> '

    def _load(self, console: 'Search', args: Sequence[str]) -> int:
        if self._records is not None:
            logging.error('There are records loaded already')
            return -1

        try:
            origin, *_ = args
            self._records = _load_from_origin(console, origin)
            if self._records is None:
                return -1

            logging.info(f'Loaded {len(self._records)} records from {origin}')
            return 0

        except ValueError:
            logging.error('Data source not specified')
            return -1

        except FileNotFoundError:
            logging.error('Data file does not exist')
            return -1

    def _join(self, console: 'Search', args: Sequence[str]) -> int:
        if self._records is None:
            logging.error('There are no records loaded yet')
            return -1

        try:
            origin, key, rkey, *_ = args + [None]
            df = _load_from_origin(console, origin)
            if rkey is not None and key != rkey:
                if len(self._included) > 0:
                    selected = self._records.loc[self._included]
                    self._included = selected.merge(df, left_on=key, right_on=rkey).index
                self._records = self._records.merge(df, left_on=key, right_on=rkey)
                logging.info(f'Joined fields from {origin} on {key}={rkey}')
            else:
                if len(self._included) > 0:
                    selected = self._records.loc[self._included]
                    self._included = selected.merge(df, on=key).index
                self._records = self._records.merge(df, on=key)
                logging.info(f'Joined fields from {origin} on {key}')
            return 0

        except ValueError:
            logging.error('Joining key not specified')
            return -1

        except FileNotFoundError:
            logging.error('Data file does not exist')
            return -1

    def _select(self, console: 'Search', args: Sequence[str]) -> int:
        if self._records is None:
            logging.error('There are no records loaded yet')
            return -1

        if len(args) < 1:
            logging.error('Selected fields not specified')
            return -1

        try:
            self._records = self._records.loc[:, args]
            logging.info(f'Selected fields: {", ".join(args)}')
            return 0

        except KeyError as e:
            logging.error(f'Invalid fields: {e.args[0]}')
            return -1

    def _sort(self, console: 'Search', args: Sequence[str]) -> int:
        if self._records is None:
            logging.error('There are no records loaded yet')
            return -1

        if len(args) < 1:
            logging.error('Sorting fields not specified')
            return -1

        try:
            fields = list(map(lambda x: re.sub(r'^[+-]', '', x), args))
            ascending = list(map(lambda x: not x.startswith('-'), args))
            self._records.sort_values(fields, ascending=ascending, inplace=True)
            logging.info('Records sorted')
            return 0

        except KeyError as e:
            logging.error(f'Invalid fields: {e.args[0]}')
            return -1

    def _groupby(self, console: 'Search', args: Sequence[str]) -> int:
        if self._records is None:
            logging.error('There are no records loaded yet')
            return -1

        if len(args) < 1:
            logging.error('Grouping key not specified')
            return -1

        console.push_namespace(GroupByContext(args, self._records))
        return 0

    def _set(self, console: 'Search', args: Sequence[str]) -> int:
        if self._records is None:
            logging.error('There are no records loaded yet')
            return -1

        try:
            name, *expr = args
            expr = ' '.join(expr)
            self._records[name] = _try_eval_expr(self._records, expr)
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

    def _showcols(self, console: 'Search', args: Sequence[str]) -> int:
        if self._records is None:
            logging.error('There are no records loaded yet')
            return -1

        included = self._records.loc[self._included]
        logging.info('Available columns:')
        _show_dataframe_columns(included)
        return 0

    def _include(self, console: 'Search', args: Sequence[str]) -> int:
        if len(args) == 0:
            logging.error('Include condition not specified')
            return -1

        if args[0] == 'all':
            self._included = self._records.index
            logging.info(f'{len(self._records)} records added')
            return 0
        else:
            query = ' '.join(args)
            matched = _try_eval_query(self._records, query)
            if matched is None:
                return -1

            prevcount = len(self._included)
            self._included = self._included.union(matched.index)
            self._included.names = self._records.index.names
            addcount = len(self._included) - prevcount
            logging.info(f'{len(matched)} records matched, {addcount} added')
            return 0

    def _exclude(self, console: 'Search', args: Sequence[str]) -> int:
        if len(args) == 0:
            logging.error('Exclude condition not specified')
            return -1

        if args[0] == 'all':
            prevcount = len(self._included)
            self._included = pd.Index([])
            logging.info(f'{prevcount} records removed')
            return 0
        else:
            query = ' '.join(args)
            records = self._records.loc[self._included]
            matched = _try_eval_query(records, query)
            if matched is None:
                return -1

            prevcount = len(self._included)
            self._included = self._included.difference(matched.index)
            self._included.names = self._records.index.names
            removecount = prevcount - len(self._included)
            logging.info(f'{removecount} records removed, {len(self._included)} left')
            return 0

    def _show(self, console: 'Search', args: Sequence[str]) -> int:
        if self._records is None:
            logging.error('There are no records loaded yet')
            return -1

        if len(self._included) == 0:
            logging.info('No records were included yet')
            return 0

        included = self._records.loc[self._included]
        _show_dataframe(included)
        return 0

    def _save(self, console: 'Search', args: Sequence[str]) -> int:
        logging.info(f'{len(self._included)} records saved as @{self._groupname}')
        console.set(f'groups:{self._groupname}', self._records.loc[self._included])
        console.pop_namespace()
        return 0

    def _help(self, console: 'Search', args: Sequence[str]) -> int:
        logging.info('Available commands:')
        logging.info('- load <datafile | @group>'.ljust(PADDING, ' ') + 'Load records from source')
        logging.info('- join <datafile | @group> <key> [rkey]'.ljust(PADDING, ' ') + 'Join records')
        logging.info('- groupby <keys>...'.ljust(PADDING, ' ') + 'Enter groupby context with group key')
        logging.info('- set <name> <expr>'.ljust(PADDING, ' ') + 'Add computed field with value')
        logging.info('- unset <name>'.ljust(PADDING, ' ') + 'Remove field from records')
        logging.info('- select <fields>...'.ljust(PADDING, ' ') + 'Select fields from records')
        logging.info('- sort <fields>...'.ljust(PADDING, ' ') + 'Sort records by fields')
        logging.info('- show'.ljust(PADDING, ' ') + 'Show records in group')
        logging.info('- showcols'.ljust(PADDING, ' ') + 'Show columns from records')
        logging.info('- include <all | expr>'.ljust(PADDING, ' ') + 'Add matching records')
        logging.info('- exclude <all | expr>'.ljust(PADDING, ' ') + 'Remove matching records')
        logging.info('- save'.ljust(PADDING, ' ') + 'Save selected records and exit group context')

    def exec(self, console: 'Search', command: str, args: Sequence[str]) -> Optional[int]:
        match command:
            case 'load': return self._load(console, args)
            case 'join': return self._join(console, args)
            case 'groupby': return self._groupby(console, args)
            case 'set': return self._set(console, args)
            case 'unset': return self._unset(console, args)
            case 'select': return self._select(console, args)
            case 'sort': return self._sort(console, args)
            case 'show': return self._show(console, args)
            case 'showcols': return self._showcols(console, args)
            case 'include': return self._include(console, args)
            case 'exclude': return self._exclude(console, args)
            case 'save': return self._save(console, args)
            case 'help': return self._help(console, args)
        return None


class GlobalContext(Context):

    def __init__(self, prompt: str):
        self._prompt = prompt

    @ property
    def prompt(self) -> str:
        return self._prompt

    def _group(self, console: 'Search', args: Sequence[str]) -> int:
        try:
            groupname, *_ = args
            if console.get(f'groups:{groupname}') is None:
                console.push_namespace(GroupContext(groupname))
                return 0
            else:
                logging.error(f'Group {groupname} already exists')
                return -1

        except ValueError:
            logging.error('Group name not specified')
            return -1

    def _show(self, console: 'Search', args: Sequence[str]) -> int:
        try:
            groupname, *_ = args
            groupname = re.sub('^@', '', groupname)

            records = console.get(f'groups:{groupname}')
            if records is None:
                logging.error(f'Group {groupname} does not exist')
                return -1

            _show_dataframe(records)
            return 0

        except ValueError:
            logging.error('Group name not specified')
            return -1

    def _showcols(self, console: 'Search', args: Sequence[str]) -> int:
        try:
            groupname, *_ = args
            groupname = re.sub('^@', '', groupname)

            records = console.get(f'groups:{groupname}')
            if records is None:
                logging.error(f'Group {groupname} does not exist')
                return -1

            logging.info('Available columns:')
            _show_dataframe_columns(records)
            return 0

        except ValueError:
            logging.error('Group name not specified')
            return -1

    def _output(self, console: 'Search', args: Sequence[str]) -> int:
        try:
            groupname, path, *_ = args + [None]
            groupname = re.sub('^@', '', groupname)
            path = Path(path if path is not None else groupname)

            records = console.get(f'groups:{groupname}')
            if records is None:
                logging.error(f'Group {groupname} does not exist')
                return -1

            match console.get('OUTPUTFORMAT'):
                case 'csv':
                    path = path.with_suffix('.csv')
                    records.to_csv(path)
                case 'excel':
                    path = path.with_suffix('.xlsx')
                    records.to_excel(path)

            logging.info(f'{len(records)} records exported to {path}')
            return 0

        except ValueError:
            logging.error('Group name not specified')
            return -1

    def _back(self, console: 'Search', args: Sequence[str]) -> int:
        console.pop_namespace()
        return 0

    def _exit(self, console: 'Search', args: Sequence[str]) -> int:
        raise ExitEventLoop()

    def _help(self, console: 'Search', args: Sequence[str]) -> int:
        logging.info('Available commands:')
        logging.info('- group <name>'.ljust(PADDING, ' ') + 'Enter group context')
        logging.info('- show <@group>'.ljust(PADDING, ' ') + 'Show group records')
        logging.info('- showcols <@group>'.ljust(PADDING, ' ') + 'Show columns in group')
        logging.info('- output <@group> [file]'.ljust(PADDING, ' ') + 'Save group records to file')

    def _help_global(self, console: 'Search', args: Sequence[str]) -> int:
        logging.info('- back'.ljust(PADDING, ' ') + 'Return to previous context')
        logging.info('- help'.ljust(PADDING, ' ') + 'Prints this help')
        logging.info('- exit'.ljust(PADDING, ' ') + 'Exit console')
        return 0

    def exec(self, console: 'Search', command: str, args: Sequence[str]) -> Optional[int]:
        match command:
            case 'group': return self._group(console, args)
            case 'show': return self._show(console, args)
            case 'showcols': return self._showcols(console, args)
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
        self._contexts = [GlobalContext(prompt=prompt)]

    @ property
    def prompt(self) -> str:
        for ns in reversed(self._context):
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
        cmdline = re.sub('\s+', ' ', cmdline)
        command, *args = cmdline.strip().split(' ')
        if command == '':
            return None

        result = self._context[-1].exec(self, command, args)
        if result is not None:
            return result

        for ns in reversed(self._context):
            result = ns.exec_global(self, command, args)
            if result is not None:
                return result

        raise UnknownCommand(command)

    def push_namespace(self, ns: Namespace) -> None:
        self._context.append(ns)
        ns.install(self)

    def pop_namespace(self) -> bool:
        if len(self._context) <= 1:
            return False

        ns = self._context.pop()
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
    parser.add_argument('-i', '--import', metavar='IMPORT', dest='import_', nargs='+',
                        type=Path, default=[], help='command modules to import')
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
