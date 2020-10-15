[![LICENSE](https://img.shields.io/dub/l/vibe-d.svg)](https://github.com/devods/devodsconnector/blob/master/LICENSE)
![python](https://img.shields.io/badge/python-3.5|3.6|3.7-blue.svg)
![version](https://img.shields.io/pypi/v/devodsconnector?label=version)


# devodsconnector

The Devo Data Science Connector is a python package for integrating data stored in Devo into a data science workflow. This package is built on top of the [Devo Python-SDK](https://github.com/DevoInc/python-sdk), a python package for querying and uploading data to Devo. Data can be pull from Devo into a variety of formats including pandas DataFrames.  In addition, random or population sampling can be applied to the queried dataset.  

The Connector also offers the ability to upload a variety of data formats back to Devo including directly upload a pandas DataFrame.

## Installing

The Devo DS Connector requires Python 3.5+

```
pip install devodsconnector
```

## Usage

`import devodsconnector as ds`

## Querying Devo

### Creating a Reader object

To query Devo, create a `Reader` object found in [reader.py](devodsconnector/reader.py)

Credentials must be specified when creating a `Reader` object in order to access the data in Devo.  In addition to credentials, an end point must be specified as well.  Credentials and end points can be specified in three ways:

1. API key and secret: `devo_reader = ds.Reader(api_key={your api key}, api_secret={your api secret key}, end_point={your end point})`
2. OAuth Token: `devo_reader = ds.Reader(oauth_token={your oauth token}, end_point={your end point})`
3. Profile: `devo_reader = ds.Reader(profile={your profile})`

The API key and secret as well as the OAuth token can be found and generated from the Devo web UI in the Credentials section under the Administration tab.  These credentials are passed as strings.  A profile can be setup to store credential and end point information in one place.  See the section on credentials file for more information.

The `end_point` for the US is `'https://apiv2-us.devo.com/search/query'` and
for the EU is `'https://apiv2-eu.devo.com/search/query'`

###### Additional Arguments

`user` and `app_name` can be specified optionally. If supplied, these values are added to the query pragma and can be used to trach who or what process is running a query  


The devodsconnector Reader supports all of the additional configuration supported by the `Client` and `ClientConfig` from the [Devo Python SDK](https://github.com/DevoInc/python-sdk/blob/master/docs/api/api.md)

#### Methods

`Reader.query(linq_query, start, stop=None, output='dict', ts_format='datetime')`  

`linq_query`: Linq query to run against Devo as a string.

`start`: The start time to run the Linq query on. `start` may be specified as a string, a datetime object, pandas Timestamp, or as a unix timestamp in seconds.  Timezone aware datetimes and Timestamp objects will use the specified timezone when calculating the start time.  Naive datetimes and Timestamps as well as strings will be treated as UTC times. Examples of valid strings are: `'2018-01-01'`,  `'Feb 10, 2019'`, `'2019-01-01 10:05:00'`, or `'2019-02-05T00:00:00'`. Note that strings will be converted by [pandas.to_datetime](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html).

`stop`: The end time (in UTC) to run the Linq query on. stop may be None or specified in the same way as start.  Set `stop` to `None` for a continuous query.

`output`: Determines how the results of the Linq query will be returned.  Valid options are `'dict', 'list', 'namedtuple', or 'dataframe'`.  If output is `'dataframe'` the results will be returned in a `pandas.DataFrame`.  Note that a dataframe cannot be build from a continuous query.  For any other type of output a generator is returned.  Each element of the generator represents one row data in the results of the Linq query. That row will be stored in the data structure specified by output.  For example, an output of `'dict'` means rows will be represented as dictionaries where the keys are the column names corresponding to the values of that row.

`ts_format`: Determines how any timestamp columns are returned.  Valid options are `'datetime'`, `'iso'`, or `'timestamp'`.  `'datetime'` will return a `datetime.datetime` object, `'iso'` will return a string in ISO 8601 format, and `'timestamp'` will return a unix timestamp in seconds.


```
linq_query = '''
from siem.logtrust.web.activity
select eventdate, userid, url'''  

results = devo_api.query(linq_query, start='2018-12-01', stop='2018-12-02', output='dict')
next(results)
```
will return
```
{
  'eventdate': datetime.datetime(2018, 12, 1, 19, 27, 41, 817000),
  'userid': 'dd10c103-020d-4d7b-b018-106d67819afd',
  'url': 'https://us.devo.com/login'
}
 ```

`Reader.randomSample(linq_query, start, stop, sample_size)`

Run a Linq query and return a random sample of the results as a `pandas.DataFrame`.  

`linq_query`, `start`, and `stop` are all specified in the same way as the `query` method above. Note that `randomSample` only returns dataframes and hence `stop` must be specified as a time and may not be left as `None`.

`sample_size`: The number of rows to be returned specified as an `int`.

`Reader.population_sample(linq_query, start, stop, column, sample_size)`

Run a Linq query and return all the rows for a random subset of a population.

`linq_query`, `start`, and `stop` are all specified in the same way as the `randomSample` method above. Similar to `randomSample`, `population_sample` only returns dataframes and hence `stop` must be specified.

`column`: Column name of the population to sample.

`sample_size`: Size of the subset of the population to sample with.

For example, the code below will return the all of the data for three randomly selected userids.

```
linq_query = '''
from siem.logtrust.web.activity
select eventdate, userid, responseLength, responseTime
'''

sample_df = reader.population_sample(
  linq_query, start='2019-01', stop='2019-02',
  column='userid', sample_size=3)
```
We can verify that there are three distinct userids in the data.

```
sample_df.userid.value_counts()

400d338d-c9a6-4930-90a5-357937f3e735    39346
988409ce-3955-44a8-bcbb-b613bc8d9f8e    26222
7de03c62-85fb-44b9-928c-ea40cf29872d     1068
Name: userid, dtype: int64
```




## Loading Data into Devo

To load data, create a `Writer` object found in [writer.py](devodsconnector/writer.py)

Credentials must also be specified when creating a `Writer` object in order to send data into Devo.  In addition to credentials, a relay must be specified as well.  Credentials and relays can be specified in two ways:

1. Credentials: `devo_writer = ds.Writer(key={path_to_key}, crt={path_to_crt}, chain={path_to_chain}, relay={relay})`
2. Profile: `devo_writer = ds.Writer(profile={your_profile})`

The credentials of the writer are files and the paths to them are passed to the class as strings.  



#### Real Time vs historical

Both real time and historical data can be sent into Devo using the `Writer`.
The `historical` argument to any of the loading method is used to specify if
the data should be loaded in real time or with a historical timestamp.

For real time uploads, each record sent to Devo will be given an eventdate corresponding to the time that it was received by Devo. In the case of real time uploads, no timestamp needs to be provided within the data itself.

For historical uploads, each record must have a timestamp.  The timestamp should be specified using either `ts_index` or `ts_name` (see the description of the methods for more information). In general, timestamps may be in any format accepted by the `start` argument to `Reader.query` (string, datetime, pandas Timestamp, or unix timestamp in seconds). Note, that when loading data from a file the timestamp must be in    
`YYYY-MM-DD hh:mm:ss` format with the seconds having an optional fractional component.

###### Warning: Historical data should be sent into Devo in order.        


#### Methods

`Writer.load(data, tag, historical=True, ts_index=None, ts_name=None, columns=None, linq_func=print)`

`data`: An iterable of lists or dictionaries.  Each element of the iterable should represent a row of the data to be uploaded.  If the iterable is of dictionaries, each dictionary should have the column names as keys and the data as values.

`tag`: Full name of the table to load the data into.

`historical`: Denotes if the data being uploaded has an associated historical timestamp.  If `historical` is `False`, all data is uploaded with the current timestamp.  If `historical` is `True`, either `ts_index` or `ts_name` must be specified.  

`ts_index`: Use when `historical` is `True` and data is an iterable of lists.  `ts_index` is an `int` that specifies the list index that contains the historical timestamp.

`ts_name`: Use when `historical` is `True` and data is an iterable of dictionaries.  `ts_name` specifies key of the dictionary that contains the historical timestamp.

`columns`: If data is an iterable of lists, columns can optionally be specified to include column names in the generated Linq that parses the uploaded data.  See the section on accessing uploaded data.

For iterables of dictionaries, columns can optionally be supplied to define the order for the keys of each dictionary to be loaded. If columns is not supplied, the order will be determined by sorting the keys

For historical loading of either list or dictionaries, the the timestamp column should not be included in the columns argument.    

`linq_func`: A callback function to process the Linq generated when loading data. The function will be called with the generated linq as its only argument.  The return value of `linq_func` will be returned by this method.  Note that `linq_func` will be called before the data is actually uploaded to ensure the linq is processed in the case of continuous uploads.  Common use cases are writing the linq to a file or returning the linq. Set `linq_func` to `None` to not generate and process the linq. See the section on accessing uploaded data for more information.

`Writer.load_file(file_path, tag, historical=True, ts_index=None, ts_name=None, delimiter=',', header=False, columns=None, linq_func=print)`

`file_path`: path to a csv file containing the data to be uploaded as a string

`delimiter`: specifies the character used to to split fields in the the file

`header`: Denotes if the csv file contains a header row

`tag`, `historical`, and `linq_func` are specified the same as in the `load` method

`ts_index` Can be used when `historical` is `True` to specify the column in the csv containing the historical timestamp.

`ts_name` Can be used when both `historical` and `header` are `True`.  `ts_name` specifies the column in the csv containing the historical timestamp by column name.


`Writer.load_df(df, df, tag, ts_index=None, ts_name=None, linq_func=print)`

`df`: `pandas.DataFrame` to be loaded into Devo

`tag`: Full name of the table to load the data into.

`ts_index`: Index of the column containing the historical timestamp.

`ts_name`: The column name containing the historical timestamp.

`linq_func`: specified the same as the `load` method above.

Note that `load_df` can only be used for historical data uploads. This means that exactly one of `ts_index` or `ts_name` should be specified.


`Writer.load_multi(data, tag_name=None, historical=True,
ts_name=None, default_schema=None, schemas=None, linq_func=None)`

This method can be used to load data to multiple different tables.  

`data`, `historical`, and `ts_name` are specified the same is in the `load` method

`tag_name`: Use when data is an iterable of dictionaries.  `tag_name` specifies key of the dictionary that contains the tag to load this row into.

For loading lists, the format must have the tag to load to in the first position and the rest of the data following for real time uploades ie.  `[tag, ... ]`.  For historical loading the first column must be the historical timestamp and the second column must be the tag to load the data to ie. `[ts, tag, ... ]`. Unlike in the `Writer.load` method, the position of the tag and timestamp is fixed.  This restriction is because the length of the lists for each tag may be of different length.

`schemas`: Used when loading dictionaries. A dictionary mapping tags to the order the keys should be loaded in (optional).

`default_scheama`:  Used when loading dictionaries.. A list of columns to be used for sorting the keys if a tag is being loaded without a value in `schemas`.  This option should only be used if all tags without a provided schema are expected to have the same keys.  

If a tag is not in `schemas` and `default_schema` is `None`, the keys will be loaded in sorted order for that tag  

`linq_func`: If supplied, the `linq_func` will be called for each tag included in `schemas` and  each time a new schema is encountered.  Unlike in the other load methods, the output of the `linq_func` will not be returned.


#### Accessing Uploaded Data

In addition to loading the data, the `Writer` generates a Linq query that can be used to retrieve the loaded data. This Linq is processed by the `linq_func` argument of the data loading methods. When loading the data, the `Writer` appends a header a to each row of input data before sending this record to Devo. These records are stored in the message column of the specified table. The `Writer` provides a Linq query that uses the header to parse the message column and extract the data loaded into Devo.  

## Credential File

A credentials files can be used to store credentials for both the `Reader` and the `Writer` as well as end points and relays.
The default path for the credentials is `~/.devo_credentials`, but any location can be specified with `credential_path={credentials locations}` when creating a `Reader` or `Writer`.

#### Basic example

```
[example]
api_key=xxxxxx
api_secret=xxxxxx
end_point=https://apiv2-us.devo.com/search/query

key=/path/to/credentials/example.key
crt=/path/to/credentials/example.crt
chain=/path/to/credentials/chain.crt
relay=usa.elb.relay.logtrust.net
```

With the above stored in a text file located at `~/.devo_credentials` we can create `Reader` and `Writer` objects using the stored credentials

```
import devodsconnector as ds

devo_reader = ds.Reader(profile='example')
devo_writer = ds.Writer(profile='example')
```

If the credentials file was located at `'/alternate/credentials/file'` we could create the create `Reader` and `Writer` objects with

```
devo_reader = ds.Reader(profile='example', credential_path='/alternate/credentials/file')
devo_writer = ds.Writer(profile='example', credential_path='/alternate/credentials/file')
```

The `credential_path` can be sepcified as either a string or a `pathlib.Path` object.  Internally, `credential_path` will be converted to a `pathlib.Path` object. The pathlib documentation can be found [here](https://docs.python.org/3/library/pathlib.html#concrete-paths) and may be especially useful to Windows users specifying a credential path. 

It is not necessary to have credentials for both the `Reader` and the `Writer` in a profile.
If you would like to us an Oauth token, that can be included in the profile was well

```
[oauth-example]
oauth_token=xxxxxx
end_point=https://apiv2-us.devo.com/search/query
```
Multiple profiles can be stored in the `~/.devo_credentials` file as well

```
[profile-1]
api_key= ...

[profile-2]
api_key = ...
```
