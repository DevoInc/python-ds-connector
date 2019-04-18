[![LICENSE](https://img.shields.io/dub/l/vibe-d.svg)](https://github.com/devods/devodsconnector/blob/master/LICENSE)
![python](https://img.shields.io/badge/python-3+-blue.svg)
![version](https://img.shields.io/badge/version-1.0.0-blue.svg)
# devodsconnector

## Installing

The Devo DS Connector requires Python 3+

```
pip install git+https://github.com/devods/devodsconnector.git
```

## Usage

`import devodsconnector as devo`

## Querying Devo

### Creating a Reader object

To query Devo, create a `Reader` object found in [reader.py](https://github.com/devods/devodsconnector/blob/master/devodsconnector/reader.py)

Credentials must be specified when creating a Reader object in order to access the data in Devo.  In addition to credentials, an end point must be specified as well.  Credentials and end points can be specified in three ways

1. API key and secret: `devo_reader = devo.Reader(api_key={your api key}, api_secret={your api secret key}, end_point={your end point})`
2. OAuth Token: `devo_reader = devo.Reader(oauth_token={your oauth token}, end_point={your end point})`
3. Profile: `devo_reader = devo.Reader(profile={your profile})`

The API key and secret as well as the OAuth token can be found and generated from the Devo web UI in the Credentials section under the Administration tab.  These credentials are passed as strings.  A profile can be setup to store credential and end point information in one place.  See the section on credentials file for more information

The `end_point` for the US is `'https://apiv2-us.devo.com/search/query'` and
for the EU is `'https://apiv2-eu.devo.com/search/query'`

#### Methods

`Reader.query(linq_query, start, stop=None, output='dict')`  

`linq_query`: Linq query to run against Devo as a string

`start`: The start time (in UTC) to run the Linq query on.  start may be specified as a string, a datetime object, or as a unix timestamp in seconds.  Examples are valid strings are: `'2018-01-01'`,  `'Feb 10, 2019'`, or `'2019-01-01 10:05:00'`. Note that strings will be converted by [pandas.to_datetime](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html)

`stop`: The end time (in UTC) to run the Linq query on. stop may be None or specified in the same way as start.  Set stop to None for a continuous query.

`output`: Determines how the results of the Linq query will be returned.  Valid options are `'dict', 'list', 'namedtuple', or 'dataframe'`.  If output is `'dataframe'` the results will be returned in a `pandas.DataFrame`.  Note that a dataframe cannot be build from a continuous query.  For any other type of output a generator is returned.  Each element of the generator represents one row data in the results of the Linq query. That row will be stored in the data structure specified by output.  For example, an output of `'dict'` means rows will be represented as dictionaries where the keys are the column names corresponding to the values of that row.


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

`linq_query`, `start`, and `stop` are all specified in the same way as the `query` method above. Note that `randomSample` only returns dataframes and hence `stop` must be specified as a time and may not be left as None.

`sample_size`: The number of rows to be returned specified as an int

`Reader.population_sample(linq_query, start, stop, column, sample_size)`

Run a Linq query and return all the rows for a random subset of a population. For example   

`linq_query`, `start`, and `stop` are all specified in the same way as the `randomSample` method above. Similar to `randomSample`, `population_sample` only returns dataframes and hence `stop` must be specified.

`column`: Column name of the population to sample

`sample_size`: Size of the subset of the population to sample with

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

To load data, create a `Writer` object found in [writer.py](https://github.com/devods/devodsconnector/blob/master/devodsconnector/writer.py)

Credentials must also be specified when creating a Loader object in order to send data into Devo.  In addition to credentials, a relay must be specified as well.  Credentials and relays can be specified in two ways

1. Credentials: `devo_writer = devo.Writer(key={path_to_key}, crt={path_to_crt}, chain={path_to_chain}, relay={relay})`
2. Profile: `devo_writer = devo.Writer(profile={your_profile})`

The credentials of the writer are files and the paths to them are passed to the class as strings.  



#### Real Time vs historical

Both real time and historical data can be sent into Devo using the Loader.
The `historical` argument to any of the loading method is used to specify if
the data should be loaded in real time or with a historical timestamp.

For real time uploads, each record sent to Devo will be given an eventdate
equal to the time that it was received by Devo.  In the case of real time
uploads, no timestamp needs to be provided within the data itself.

For historical uploads, each record must have a timestamp.  The timestamp
should be specified using either `ts_index` or `ts_name` (see the description
of the methods for more information).  This timestamp should be in the
form of `YYYY-MM-DD hh:mm:ss` with the seconds having an optional fractional
component. Note that any object that has a string representation of this form
is a valid timestamp. For example, a `datetime.datetime` object meets this
criteria.  
Warning: historical data should be sent into Devo in order        


#### Methods

`Writer.load(data, tag, historical=True, ts_index=None, ts_name=None, columns=None)`

`data`: An iterable of lists or dictionaries.  Each element of the iterable should represent a row of the data to be uploaded.  If the iterable is of dictionaries, each dictionary should have the column names as keys and the data as values.

`tag`: Full name of the table to load the data into.

`historical`: Denotes if the data being uploaded has an associated historical timestamp.  If historical is false, all data is uploaded with the current timestamp.  If historical is True, either ts_index or ts_name must be specified.  

`ts_index`: Use when historical is True and data is an iterable of lists.  ts_index is an int that specifies the list index that contains the historical timestamp.

`ts_name`: Use when historical is True and data is an iterable of dictionaries.  ts_name specifies key of the dictionary that contains the historical timestamp.

`columns` If data is an iterable of lists, columns can optionally be specified to include column names in the generated Linq that parses the uploaded data.  See the section on accessing uploaded data

`Writer.load_file(file_path, tag, historical=True, ts_index=None, ts_name=None, header=False, columns=None)`

`file_path`: path to a csv file containing the data to be uploaded as a string

`header`: Denotes if the csv file contains a header row

`tag`, `historical` are specified the same as in the `load` method

`ts_index` Can be used when historical is True to specify the column in the csv containing the historical timestamp.

`ts_name` Can be used when both historical and header are True.  ts_name specifies the column in the csv containing the historical timestamp by column name.

`Writer.load_df(df, tag, ts_name)`

`df`: pandas DataFrame to be loaded into Devo

`tag`: Full name of the table to load the data into.

`ts_name`: The column name containing the historical timestamp.

Note that load_df can only be used for historical data uploads.

#### Accessing Uploaded Data

The Loader sends data into Devo by inserting a header and all of the data of each input row into the message column of the Devo table.  The Writer provides a Linq query that can be used to parse this message column to extract the data loaded into Devo.  

## Credential File

A credentials files can be used to store credentials for both the API and the Loader as well as end points and relays.

The credentials file needs to be stored at `~/.devo_credentials`

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

With the above stored in a text file located at `~/.devo_credentials` we can create API and Loader objects using the stored credentials

```
import devodsconnector as devo

devo_reader = devo.Reader(profile='example')
devo_writer = devo.Writer(profile='example')
```

It is not necessary to have credentials for both the Reader and the Writer in a profile.
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
