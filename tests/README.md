# algoseek-connector testing

The library tests are grouped in three different types: integration, learning and
unit.

## Tests setup

Testing requires access to Algoseek metadata API, ArdaDB and S3 buckets.


- **Metadata API**: Set user/pass using the environment variables
`ALGOSEEK_API_USERNAME` and `ALGOSEEK_API_PASSWORD`.

- **ArdaDB**: Set host address, port, user and password using the environment
variables `ALGOSEEK_ARDADB_HOST`, `ALGOSEEK_ARDADB_USER` and `ALGOSEEK_ARDADB_PASSWORD`.

- **S3**: Set access to dataset buckets setting the variables `ALGOSEEK_AWS_PROFILE`
to get the credentials from ~/.aws/credentials or set user/password using the
`ALGOSEEK_AWS_ACCESS_KEY_ID` and `ALGOSEEK_AWS_SECRET_ACCESS_KEY` environment
variables. Also, For writing to S3 buckets, access to the development buckets
needs to be set using the environment variables `ALGOSEEK_DEV_AWS_ACCESS_KEY_ID`
and `ALGOSEEK_DEV_AWS_SECRET_ACCESS_KEY`

## Unit tests

Check basic library component functionality. They do not connect to external
data sources such as DB or REST APIs. If necessary, external connections are
mocked.

Unit test are executed with the following command:

    make unit-tests


## Integration tests

Check library functionality in real-world scenarios, connecting to external
data sources. Usually they take longer to run and are not executed by default.

Integration tests are executed with the following command:

    make integration-tests

## Learning tests

This kind of tests do not test library functionality. They are created to
learn how third party library works and check their behavior in different
scenarios. They are not executed by default.

Learning tests are executed with the following command:

    make learning-tests


## Check code coverage

Code coverage is tested using the pytest-coverage plugin. Code coverage is
executed with the following command:

    make coverage

