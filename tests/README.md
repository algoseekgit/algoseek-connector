# algoseek-connector testing

The library tests are grouped in three different types: integration, learning and
unit.

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


