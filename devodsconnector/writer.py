import os
import configparser
import sys
import socket
import csv
import pathlib
import numpy as np
from collections import abc

from devo.sender import Sender

csv.field_size_limit(sys.maxsize)


class Writer:

    def __init__(self, profile='default', key=None, crt=None,
                       chain=None, relay=None, port=443,
                       credential_path='~/.devo_credentials'):

        self.profile = profile
        self.key = key
        self.crt = crt
        self.chain = chain
        self.relay = relay
        self.port = port

        self.credential_path = pathlib.Path(credential_path).expanduser()

        if not all([key, crt, chain, relay]):
            self._read_profile()

        if not all([self.key, self.crt, self.chain, self.relay]):
            raise Exception('Credentials and relay must be specified or in ~/.devo_credentials')

        self.sender = Sender(dict(address=self.relay, port=self.port,
                                  key=self.key, cert=self.crt,chain=self.chain))

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


    def load_file(self, file_path, tag, historical=True, ts_index=None,
                        ts_name=None, header=False, columns=None, linq_func=print):

        with open(file_path, 'r') as f:
            data = csv.reader(f)
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
        elif isinstance(first, abc.Mapping):
            names = list(first.keys())
            if historical:
                ts_index = num_cols
                names.remove(ts_name)
                columns = names[:]
                names += [ts_name]
            else:
                columns = names
            data = self._process_mapping(data, first, names)

        if linq_func is not None:
            linq = self._build_linq(tag, num_cols, columns)
            linq_output = linq_func(linq)
        else:
            linq_output = None

        self._load(data, tag, historical, ts_index, chunk_size)

        return linq_output

    def load_df(self, df, tag, ts_name, linq_func=print):

        data = df.values.tolist()
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
            tag = '(usd)' + tag
            prefix = '<14>{0} '
        else:
            prefix = '<14>Jan  1 00:00:00 '

        return prefix + '{0} {1}: '.format(hostname, tag)

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
    def _build_linq(tag, num_cols, columns=None):

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
