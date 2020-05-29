import os
import configparser
import ctypes
import socket
import csv
import datetime
import numpy as np
import pandas as pd
from pathlib import Path
from collections import abc

from devo.sender import Sender

import warnings

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))
warnings.simplefilter('always', UserWarning)


class Writer:

    def __init__(self, profile='default', key=None, crt=None,
                       chain=None, relay=None, port=443,
                       credential_path=None, **kwargs):

        self.profile = profile
        self.key = key
        self.crt = crt
        self.chain = chain
        self.relay = relay
        self.port = port

        if credential_path is None:
                self.credential_path = Path.home() / '.devo_credentials'
        else:
            self.credential_path = Path(credential_path).resolve().expanduser()

        if not all([key, crt, chain, relay]):
            self._read_profile()

        if not all([self.key, self.crt, self.chain, self.relay]):
            raise Exception('Credentials and relay must be specified or in ~/.devo_credentials')
        
        config_dict = kwargs
        config_dict.update(dict(address=self.relay, port=self.port,
                                  key=self.key, cert=self.crt,chain=self.chain))

        self.sender = Sender(config_dict)

    def _read_profile(self):

        config = configparser.ConfigParser()
        config.read(self.credential_path)

        if self.profile in config:
            profile_config = config[self.profile]

            self.key = profile_config.get('key')
            self.crt = profile_config.get('crt')
            self.chain = profile_config.get('chain')
            self.relay = profile_config.get('relay')
            self.port = int(profile_config.get('port', 443))


    def load_file(self, file_path, tag, historical=True, ts_index=None, ts_name=None,
                        delimiter=',', header=False, columns=None, linq_func=print):

        with open(file_path, 'r') as f:
            data = csv.reader(f, delimiter=delimiter)
            first = next(data)

            if historical:
                chunk_size = 50
                num_cols = len(first) - 1
            else:
                chunk_size = 1
                num_cols = len(first)

            if header:
                if columns is None:
                    columns = first
                if ts_name is not None:
                    ts_index = columns.index(ts_name)
            else:
                f.seek(0)

            if linq_func is not None:
                linq = self._build_linq(tag, num_cols, columns)
                linq_output = linq_func(linq)
            else:
                linq_output = None

            self._load(data, tag, historical, ts_index, chunk_size)

        return linq_output

    def load(self, data, tag, historical=True, ts_index=None,
                   ts_name=None, columns=None, linq_func=print):

        data = iter(data)
        first = next(data)

        if historical:
            chunk_size = 50
            num_cols = len(first) - 1
        else:
            chunk_size = 1
            num_cols = len(first)

        if isinstance(first, abc.Sequence):
            data = self._process_seq(data, first)
        elif isinstance(first, (abc.Mapping, np.ndarray, pd.core.series.Series)) and not isinstance(first, str):
            if columns:
                names = columns[:]
            else:
                names = sorted(first)

            if historical and columns:
                names.append(ts_name)
                ts_index = num_cols
            elif historical:
                names.remove(ts_name)
                columns = names[:]
                names.append(ts_name)
                ts_index = num_cols
            else:
                columns = names
            data = self._process_mapping(data, first, names)
        else:
            raise Exception(f'data of type {type(first)} is not supported for loading')

        if linq_func is not None:
            linq = self._build_linq(tag, num_cols, columns)
            linq_output = linq_func(linq)
        else:
            linq_output = None

        self._load(data, tag, historical, ts_index, chunk_size)

        return linq_output

    def load_df(self, df, tag, ts_index=None, ts_name=None, linq_func=print):
        data = df.values.tolist()

        if ts_index is None:
            ts_index = df.columns.get_loc(ts_name)

        self.load(data, tag, historical=True ,ts_index=ts_index ,linq_func=linq_func)

    def _load(self, data, tag, historical, ts_index=None, chunk_size=50):

        message_header_base = self._make_message_header(tag, historical)
        counter = 0
        bulk_msg = ''

        if not historical:
            message_header = message_header_base

        for row in data:

            if historical:
                ts = row.pop(ts_index)
                ts = self._to_ts_string(ts)
                message_header = message_header_base.format(ts)

            bulk_msg += self._make_msg(message_header, row)
            counter += 1

            if counter == chunk_size:
                self.sender.send_raw(bulk_msg.encode())
                counter = 0
                bulk_msg = ''

        if bulk_msg:
            self.sender.send_raw(bulk_msg.encode())

    @staticmethod
    def _make_message_header(tag, historical):
        hostname = socket.gethostname()

        if historical:
            tag = f'(usd){tag}'
            prefix = f'<14>{{0}}'
        else:
            prefix = '<14>Jan  1 00:00:00'

        return f'{prefix} {hostname} {tag}: '

    @staticmethod
    def _make_msg(header, row):
        """
        Takes row (without timestamp)

        Concats column values in row
        calculates string indices of
        where columns start and begin

        :param row: list with column values as strings
        :return: string in form ofL indices<>cols
        """

        lengths = [len(s) for s in row]
        lengths.insert(0, 0)

        indices = np.cumsum(lengths)
        indices = ','.join(str(i) for i in indices)

        row_concated = ''.join(row)

        msg = indices + '<>' + row_concated

        return header + msg + '\n'

    @staticmethod
    def _process_seq(data, first):
        yield [str(c) for c in first]
        for row in data:
            yield [str(c) for c in row]

    @staticmethod
    def _process_mapping(data, first, names):
        yield [str(first[c]) for c in names]
        for row in data:
            yield [str(row[c]) for c in names]

    @staticmethod
    def _build_linq(tag, num_cols=None, columns=None):

        if columns is None:
            columns = ['col_{0}'.format(i) for i in range(num_cols)]

        col_extract = '''
        select substring(payload,
        int(split(indices, ",", {i})),
        int(split(indices, ",", {i}+1)) - int(split(indices, ",", {i}))
        ) as `{col_name}`
        '''

        linq = '''
        from {tag}

        select split(message, "<>", 0) as indices
        select subs(message, re("[0-9,]*<>"), template("")) as payload

        '''.format(tag=tag)

        for i, col_name in enumerate(columns):
            linq += col_extract.format(i=i, col_name=col_name)

        return linq


    def load_multi(self, data, tag_name=None,
                   historical=True,
                   ts_name=None, default_schema=None, schemas=None,
                   linq_func=None):

        data = iter(data)
        first = next(data)

        chunk_size = 50 if historical else 1

        if isinstance(first, (abc.Sequence, np.ndarray, pd.core.series.Series)) and not isinstance(first, str):
            self.processor = ListProcessor(historical, linq_func)
        elif isinstance(first, abc.Mapping):
            self.processor = DictProcessor(schemas, default_schema, historical,
                                           tag_name, ts_name, linq_func)
        else:
            raise Exception(f'data of type {type(first)} is not supported for loading')

        data = self.processor.process_data(data, first)
        self._load_multi(data, historical, chunk_size)

    def _load_multi(self, data, historical, chunk_size=50):

        counter = 0
        bulk_msg = ''

        for header, row in data:
            if historical:
                ts, tag = header
                ts = self._to_ts_string(ts)
                message_header = self._make_message_header(tag, historical).format(ts)
            else:
                tag = header[0]
                message_header = self._make_message_header(tag, historical)

            bulk_msg += self._make_msg(message_header, row)
            counter += 1

            if counter == chunk_size:
                self.sender.send_raw(bulk_msg.encode())
                counter = 0
                bulk_msg = ''

        if bulk_msg:
            self.sender.send_raw(bulk_msg.encode())

    @staticmethod
    def _to_ts_string(ts):
        if isinstance(ts, (int,float)):
            ts = pd.to_datetime(ts, unit='s')
        elif isinstance(ts, str):
            ts = pd.to_datetime(ts)
        elif isinstance(ts, (pd.Timestamp, datetime.datetime)):
            ts = ts.replace(tzinfo=None)

        return str(ts)


class Processor:

    def process_data(self, data, first):
        yield self.process_row(first)
        for row in data:
            yield self.process_row(row)

    def process_linq(self, tag, num_cols=None, schema=None):
        if self.linq_func is not None:
            linq = Writer._build_linq(tag, num_cols=num_cols, columns=schema)
            self.linq_func(linq)


class ListProcessor(Processor):

    def __init__(self, historical, linq_func):
        self.seen_tags = set()
        self.historical = historical
        self.linq_func = linq_func

    def process_row(self, row):
        if self.historical:
            num_cols = len(row) - 2
            tag = row[1]
            header, row = row[:2], row[2:]
        else:
            num_cols = len(row) - 1
            tag = row[0]
            header, row = row[:1], row[1:]

        if tag not in self.seen_tags:
            self.seen_tags.add(tag)
            self.process_linq(tag, num_cols=num_cols)

        return header, [str(c) for c in row]


class DictProcessor(Processor):

    def __init__(self, schemas, default_schema, historical,
                 tag_name, ts_name, linq_func):
        self.schemas = schemas if schemas else {}
        self.default_schema = default_schema
        self.historical = historical
        self.linq_func = linq_func
        self.tag_name = tag_name
        self.ts_name = ts_name

        for tag, schema in self.schemas.items():
            self.process_linq(tag, schema=schema)

    def process_row(self, row):
        tag = row[self.tag_name]
        names = list(row)
        names.remove(self.tag_name)
        if self.historical:
            names.remove(self.ts_name)
            ts = row[self.ts_name]
            header = [str(ts), str(tag)]
        else:
            header = [str(tag)]

        schema = self.schemas.get(tag)
        if (schema is None) and (self.default_schema is not None):
            schema = self.default_schema
            self.schemas[tag] = schema
            self.process_linq(tag, schema=schema)

        elif schema is None:
            schema = sorted(names)
            self.schemas[tag] = schema
            self.process_linq(tag, schema=schema)

        return header, [str(row[c]) for c in schema]
