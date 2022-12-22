import os
import struct, socket
import ctypes
import configparser
import datetime
from datetime import timezone
import json
import csv
import warnings
from pathlib import Path
from collections import namedtuple, defaultdict
import numpy as np
import pandas as pd
from scipy.stats import norm

from devo.api import Client

from .error_checking import check_status

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))
warnings.simplefilter('always', UserWarning)


class DSResults:
    def __init__(self,res,results):
        self.res = res
        self.results = results
    def __iter__(self):
        return self.results
    def __next__(self):
        return next(self.results)
    def close(self):
        self.res.close()





class Reader(object):

    def __init__(self, profile='default', api_key=None, api_secret=None,
                       end_point=None, oauth_token=None, jwt=None,
                       credential_path=None, timeout=None, retries=1,
                       verify=True, user=None,app_name=None, **kwargs):

        self.profile = profile
        self.api_key = api_key
        self.api_secret = api_secret
        self.end_point = end_point
        self.oauth_token = oauth_token
        self.jwt = jwt

        if credential_path is None:
                self.credential_path = Path.home() / '.devo_credentials'
        else:
            self.credential_path = Path(credential_path).expanduser().resolve()

        if not (self.end_point and (self.oauth_token or self.jwt or (self.api_key and self.api_secret))):
            self._read_profile()

        if not (self.end_point and (self.oauth_token or self.jwt or (self.api_key and self.api_secret))):
            raise Exception('End point and either API keys or OAuth Token must be specified or in ~/.devo_credentials')

        config = kwargs
        config.update({
            'auth': {'key': self.api_key, 'secret': self.api_secret, 'token': self.oauth_token, 'jwt': self.jwt},
            'address': self.end_point
        })
        self.client = Client(config=config)

        self.client.timeout = timeout
        self.client.retries = retries
        self.client.verify = verify

        if user:
            self.client.config.set_user(user)
        if app_name:
            self.client.config.set_app_name(app_name)

    def _read_profile(self):
        """
        Read Devo API keys from a credentials file located
        at ~/.devo_credentials if credentials are not provided

        Use profile to specify which set of credentials to use
        """

        config = configparser.ConfigParser()
        config.read(self.credential_path)

        if self.profile in config:
            profile_config = config[self.profile]

            self.api_key = profile_config.get('api_key')
            self.api_secret = profile_config.get('api_secret')
            self.end_point = profile_config.get('end_point')
            self.oauth_token = profile_config.get('oauth_token')

            if self.end_point == 'USA':
                self.end_point = 'https://apiv2-us.devo.com/search/query'
            elif self.end_point == 'EU':
                self.end_point = 'https://apiv2-eu.devo.com/search/query'


    def query(self, linq_query, start, stop=None, output='dict', ts_format='datetime', comment=None):

        valid_outputs = ('dict', 'list', 'namedtuple', 'dataframe')
        if output not in valid_outputs:
            raise Exception(f"Output must be one of {valid_outputs}")

        if output=='dataframe' and stop is None:
            raise Exception("DataFrame can't be build from continuous query")

        res = self._query(linq_query, start, stop, mode='json/simple/compact', stream=True, comment=comment)

        ### will this always load in correct order? ordering dict is feature for py 3.7 ?
        tmp = next(res)
        if not isinstance(tmp, str):
            tmp = tmp.decode()
        col_data = json.loads(tmp)['m']
        cols = list(col_data.keys())

        type_map = self._make_type_map(ts_format)
        type_list = [type_map[v['type']] for c,v in col_data.items()]

        results = self._stream(res,type_list)
        results = getattr(self, f'_to_{output}')(results,cols)


        if output == 'dataframe':
            return results
        else:
            return DSResults(res,results)


    def _stream(self, res, type_list):
        try:
            for row in res:
                if row:
                    if not isinstance(row, str):
                        row = row.decode()
                    decoded_row = json.loads(row)['d']
                    yield [t(v) for t, v in zip(type_list, decoded_row)]
        except Exception as e:
            try:
                print(row)
            except Exception:
                pass
            res.close()
            raise(e)


    def _make_type_map(self,ts_format):

        funcs = {
                'timestamp': self.make_ts_func(ts_format),
                'ipv4': lambda i: socket.inet_ntoa(struct.pack('!L', i)),
                'json': json.loads

               }

        decorated_funcs = {t: self._null_decorator(f) for t, f in funcs.items()}


        return defaultdict(lambda: (lambda x : x), decorated_funcs)


    @staticmethod
    def _null_decorator(f):
        def null_f(v):
            if v is None:
                return None
            else:
                return f(v)
        return null_f


    def _query(self, linq_query, start, stop=None, mode='csv', stream=False, limit=None, comment=None):
        if (getattr(start, 'tzinfo', 1) is None) or (getattr(stop, 'tzinfo', 1) is None):
            warnings.warn('Naive date interpreted as UTC')

        start = self._to_unix(start)
        stop = self._to_unix(stop)

        dates = {'from': start, 'to':stop}
        self.client.config.response = mode
        self.client.config.stream = stream

        response = self.client.query(query=linq_query,
                                     dates=dates,
                                     limit=limit,
                                     comment=comment)

        return response


    @staticmethod
    def make_ts_func(ts_format):
        if ts_format not in ('datetime', 'iso', 'timestamp'):
            raise Exception('ts_format must be one of: datetime, iso, or timestamp ')

        def ts_func(t):
            if ts_format == 'timestamp':
                return t

            dt = datetime.datetime.utcfromtimestamp(t / 1000)
            dt = dt.replace(tzinfo=timezone.utc)

            if ts_format == 'datetime':
                return dt
            elif ts_format == 'iso':
                return dt.isoformat()

        return ts_func


    @staticmethod
    def _to_unix(date, milliseconds=False):
        """
        Convert date to a unix timestamp

        date: A unix timestamp in second, a datetime object,
        pandas.Timestamp object, or string to be parsed
        by pandas.to_datetime
        """

        if date is None:
            return None

        elif date == 'now':
            epoch = datetime.datetime.now().timestamp()
        elif type(date) == str:
            epoch = pd.to_datetime(date).timestamp()
        elif isinstance(date, (pd.Timestamp, datetime.datetime)):
            if date.tzinfo is None:
                epoch = date.replace(tzinfo=timezone.utc).timestamp()
            else:
                epoch = date.timestamp()
        elif isinstance(date, (int,float)):
            epoch = date
        else:
            raise Exception('Invalid Date')

        if milliseconds:
            epoch *= 1000

        return int(epoch)



    @staticmethod
    def _to_list(results,cols):
        yield from results

    @staticmethod
    def _to_dict(results, cols):
        for row in results:
            yield {c:v for c,v in zip(cols,row)}

    @staticmethod
    def _to_namedtuple(results, cols):
        Row = namedtuple('Row', cols)
        for row in results:
            yield Row(*row)

    @staticmethod
    def _to_dataframe(results,cols):
        return pd.DataFrame(results, columns=cols).fillna(np.nan)



    def randomSample(self,linq_query,start,stop,sample_size):

        if (sample_size < 1) or (not isinstance(sample_size, int)):
            raise Exception('Sample size must be a positive int')

        size_query = f'{linq_query} group select count() as count'

        r = self.query(size_query,start,stop,output='list')
        table_size = next(r)[0]

        if sample_size >= table_size:
            warning_msg = 'Sample size greater than or equal to total table size. Returning full table'
            warnings.warn(warning_msg)
            return self.query(linq_query,start,stop,output='dataframe')

        p = self._find_optimal_p(n=table_size,k=sample_size,threshold=0.99)

        sample_query = f'{linq_query} where simplify(float8(rand())) < {p}'

        while True:
            df = self.query(sample_query,start,stop,output='dataframe')

            if df.shape[0] >= sample_size:
                return df.sample(sample_size).sort_index().reset_index(drop=True)
            else:
                pass

    @staticmethod
    def _loc_scale(n,p):
        """
        Takes parameters of a binomial
        distribution and finds the mean
        and std for a normal approximation

        :param n: number of trials
        :param p: probability of success
        :return: mean, std
        """
        loc = n*p
        scale = np.sqrt(n*p*(1-p))

        return loc,scale

    def _find_optimal_p(self,n,k,threshold):
        """
        Use a normal approximation to the
        binomial distribution.  Starts with
        p such that mean of B(n,p) = k
        and iterates.

        :param n: number of trials
        :param k: desired number of successes
        :param threshold: desired probability to achieve k successes

        :return: probability that a single trial that will yield
                 at least k success with n trials with probability of threshold

        """
        p = k / n
        while True:
            loc, scale = self._loc_scale(n, p)
            # sf = 1 - cdf, but can be more accurate according to scipy docs
            if norm.sf(x=k - 0.5, loc=loc, scale=scale) > threshold:
                break
            else:
                p = min(1.001*p, 1)

        return p

    def population_sample(self, query, start, stop, column, sample_size):

        population_query = f'{query} group by {column}'

        df = self.randomSample(population_query, start, stop, sample_size)
        population = df[column]
        sample_set = ','.join(f'"{x}"' for x in population)

        sample_query = f'{query} where str({column}) in {{{sample_set}}}'

        return self.query(sample_query, start, stop, output='dataframe')
