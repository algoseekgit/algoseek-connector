.. _sql:

Creating SQL queries
********************

Here we describe how to work with datasets. For detailed instructions to create
datasets from different data sources please see :ref:`here <datasets>`.

The Dataset class is built on top of SQLAlchemy columns and support most of
the standard SQL operations. Also, depending on the data source, extra
functionality may be available.

It is important to point out that Dataset objects do not store data and are
just a mean to retrieve data from a source by means of queries.

Each column of a dataset is represented as a SQLAlchemy column and is accessed
by using either attribute or dict-like access:

.. code-block:: python

  # access by attribute
  col = dataset.ASID

  # access by index
  col = dataset["ASID"]

the get_colu

The select method of Dataset allows to create select statements that are used
to retrieve data.

