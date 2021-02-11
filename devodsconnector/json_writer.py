import os
import configparser
import ctypes
import socket
import csv
import datetime
import json
import numpy as np
import pandas as pd
from pathlib import Path
from collections import abc


from devo.sender import Sender

import warnings

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))
warnings.simplefilter('always', UserWarning)



class _Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super().default(obj)





class JSONWriter:

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
                        delimiter=',', header=False, columns=None):

        with open(file_path, 'r') as f:
            data = csv.reader(f, delimiter=delimiter)

            if header:
                if columns is None:
                    columns = next(data)
                else:
                    next(data)

            self.load(data, tag, historical, ts_index=ts_index, ts_name=ts_name, columns=columns)

    def load(self, data, tag, historical=True, ts_index=None,
                   ts_name=None, columns=None):

        data = iter(data)
        first = next(data)

        if historical:
            chunk_size = 50
        else:
            chunk_size = 1

        if isinstance(first, (abc.Sequence, np.ndarray, pd.core.series.Series)) and not isinstance(first, str):
            if (columns is None) and (historical):
                columns = [f'col{i}' for i in range(1, ts_index +1)] + ['ts'] + [f'col{i}' for i in range(ts_index + 1, len(first) + 1)]
                ts_name = 'ts'
            elif historical:
                if ts_name is None:
                    ts_name = columns[ts_index]
            elif columns is None:
                columns = [f'col{i}' for i in range(1, len(first) + 1)]
            data = self._process_seq(data, first, columns)
        elif isinstance(first, abc.Mapping):
            data = self._process_mapping(data,first)
        else:
            raise Exception(f'data of type {type(first)} is not supported for loading')

        self._load(data, tag, historical, ts_name, chunk_size)



    def load_df(self, df, tag, ts_index=None, ts_name=None):
        if (ts_index is None) and (ts_name is None):
            raise Exception("must specify ts_index or ts_name")

        data = df.to_dict(orient="records")

        if ts_name is None:
            ts_name = df.columns[ts_index]

        self.load(data, tag=tag, historical=True, ts_name=ts_name)

    def _load(self, data, tag, historical, ts_name=None, chunk_size=50):

        message_header_base = self._make_message_header(tag, historical)
        counter = 0
        bulk_msg = ''

        if not historical:
            message_header = message_header_base

        for row in data:

            if historical:
                ts = row.pop(ts_name)
                ts = self._to_ts_string(ts)
                message_header = message_header_base.format(ts)

            bulk_msg += message_header + json.dumps(row, cls=_Encoder) + '\n'
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
    def _process_seq(data, first, columns):
        yield dict(zip(columns, first))
        for row in data:
            yield dict(zip(columns, row))

    @staticmethod
    def _process_mapping(data, first):
        yield first.copy()
        for row in data:
            yield row.copy()


    def load_multi(self, data, tag_name=None, historical=True, ts_name=None):
        data = iter(data)
        first = next(data)

        if not isinstance(first, abc.Mapping):
            raise Exception(f'data of type {type(first)} is not supported for multi loading. data must be a dict')


        chunk_size = 50 if historical else 1

        data = self._process_mapping(data, first)

        counter = 0
        bulk_msg = ''

        for row in data:
            tag = row.pop(tag_name)
            if historical:
                ts = row.pop(ts_name)
                ts = self._to_ts_string(ts)
                message_header = self._make_message_header(tag, historical).format(ts)
            else:
                message_header = self._make_message_header(tag, historical)

            bulk_msg += message_header + json.dumps(row) + '\n'
            counter += 1

            if counter == chunk_size:
                self.sender.send_raw(bulk_msg.encode())
                counter = 0
                bulk_msg = ''

        if bulk_msg:
            self.sender.send_raw(bulk_msg.encode())

    def load_df_multi(self, df, tag_name=None, tag_index=None, ts_index=None, ts_name=None):
        if (ts_index is None) and (ts_name is None):
            raise Exception("must specify ts_index or ts_name")
        if (tag_index is None) and (tag_name is None):
            raise Exception("must specify tag_index or tag_name")

        data = df.to_dict(orient="records")

        if ts_name is None:
            ts_name = df.columns[ts_index]
        if tag_name is None:
            tag_name = df.columns[tag_index]

        self.load_multi(data, tag_name=tag_name, historical=True, ts_name=ts_name)

    @staticmethod
    def _to_ts_string(ts):
        if isinstance(ts, (int,float,np.integer)):
            ts = pd.to_datetime(ts, unit='s')
        elif isinstance(ts, str):
            ts = pd.to_datetime(ts)
        elif isinstance(ts, (pd.Timestamp, datetime.datetime)):
            ts = ts.replace(tzinfo=None)

        return str(ts)
