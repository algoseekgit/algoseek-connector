# algoseek-connector

A wrapper library for ORM-like SQL builder and executor.
The library provides a simple pythonic interface to algoseek datasets with custom data filtering/selection.

## Installing and Supported Versions

algoseek-connector is available on PyPI:

```
$ python -m pip install algoseek-connector
```

or alternatively

```
$ pip install algoseek-connector
```

Python versions 3.6+ are supported.
## Supported Features

The following query operations on datasets are supported:
- Selecting columns and arbitrary expressions based on columns
- Filtering by column value/column expression
- Grouping by column(s)
- Sorting by column(s)
- All common artithmetic, logical operations on dataset columns and function application
- Fetching query results as a pandas dataframe

## Getting Started


### Creating a session

A database connection is created with a `Session` object
with the DB host, username and password provided.
```
import algoseek_connector as aconnect

host = '123.123.123.123'
user = 'demo'
password ='secret-password-2000'

session = aconnect.Session(host, user, password)
```

Optionally a port number is provided unless it is a default value of 9000.

### Configuring a session with environment variables

You can make use of the following environment variables to set up the databse connection:

- AS_DATABASE_HOST
- AS_DATABASE_PORT
- AS_DATABASE_USER
- AS_DATABASE_PASSWORD

In this case an empty session is created with user credentials read from the environment.

```
session = aconnect.Session()
```

### Executing raw queries

A Session object can be used to execute a SQL query directly

```
session.execute('''
SELECT * FROM USEquityMarketData.TradeOnly
WHERE Ticker = 'IBM'
LIMIT 10''')
```

### Datagroups and datasets

All datasets available are grouped into data groups
Is structured into datagroups, e.g. USEquityMarketData, USFuturesMarketData, etc.

You can browse the list of available data groups with the `DataResource`

```
resource = DataResource(session)
for dgr in resource.datagroups.all():
    print(dgr.name)
```

Similarly, you can access the list of datasets of a specific data group:

```
datagroup = resource.datagroup('USEquityMarketData')
for dts in datagroup.datasets.all():
    print(dts.name)
```

Alternatively, getting a specific dataset directly:
```
dataset = aconnect.Dataset(
    'USEquityMarketData', 'TradeOnlyMinuteBar', session=session
)
```


### Selecting a subset of columns

To get specific columns the `Dataset.select` method is used:

```
ds = aconnect.Dataset(
    'USEquityMarketData', 'TradeOnly', session=session
)
ds.select(
    ds.EventDateTime, ds.Ticker, ds.Price
).head()
```

### Dataset filtering

Filtering expressions can be chained using `&` (AND) and `|` (OR) operators, a `~` is used for negation (NOT).

```
ds = aconnect.Dataset(
    'USEquityMarketData', 'TradeOnly', session=session
)
ds.select(
    ds.EventDateTime, ds.Ticker, ds.Price
).filter(
    ds.TradeDate.between('2022-01-01', '2022-01-31') &
    (ds.Ticker = 'TSLA') &
    (ds.Quantity < 100)
).head()
```

### Getting results

You can make use of `Dataset.fetch` method to execute the generated query and get results as a pandas dataframe:

```
ds = aconnect.Dataset(
    'USEquityMarketData', 'TradeOnlyMinuteBar', session=session
)

ds.select(
    ds.BarDateTime,
    ds.Ticker,
    ds.Volume
).filter(
    ds.Ticker,isin(['AAPL', 'FB']) &
    ds.TradeDate > '2022-05-01'
).fetch()
```

## TODO

- pandas dataframe parse date/time columns
