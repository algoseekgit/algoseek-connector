import copy
import functools
from numbers import Number
from typing import List, Tuple, Iterable, Iterator, Optional, Union, Sequence, Any

from .sessions import Session
from .expressions import BaseExpression, Expression, Literal


@functools.lru_cache()
def list_datagroups(session: Session) -> Tuple[str]:
    return session.execute(
        "SHOW DATABASES", columnar=True
    )[0]


class DatagroupsMeta(object):

    def __init__(self, session: Optional[Session] = None):
        self.session = session or Session()
        self.names = list_datagroups(self.session)


@functools.lru_cache()
def list_datasets(datagroup_name: str, session: Session) -> Tuple[str]:

    # TODO:
    # datagroup_name in list_datagroups()
    return session.execute(
        f"SHOW TABLES FROM {datagroup_name}", columnar=True
    )[0]


class DatasetsMeta(object):

    def __init__(self, datagroup_name: str, session: Optional[Session] = None):
        self.session = session or Session()
        self.names = list_datasets(datagroup_name, self.session)


@functools.lru_cache()
def list_columns(
        datagroup_name: str,
        dataset_name: str,
        session: Session
        ) -> Tuple[Tuple[str], Tuple[str], Tuple[str]]:

    names, types, _, _, descriptions, _, _ = session.execute(
        f"DESCRIBE TABLE {datagroup_name}.{dataset_name}",
        columnar=True
    )
    return names, tuple(parse_column_type(x) for x in types), descriptions


def parse_column_type(type_name: str) -> str:
    # TODO
    if type_name.startswith('Enum'):
        return 'String'
    elif type_name.startswith('LowCardinality'):
        return type_name.split('(', 1)[1].rsplit(')', 1)[0]
    else:
        return type_name


def is_iterable_type(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, str)


class ColumnsMeta(object):

    def __init__(
            self,
            datagroup_name: str,
            dataset_name: str,
            session: Optional[Session] = None
            ):
        self.session = session or Session()
        self.names, self.types, self.descriptions = list_columns(
            datagroup_name, dataset_name, self.session
        )

    def __iter__(self) -> Iterator[Tuple[str, str, str]]:
        for triplet in zip(self.names, self.types, self.descriptions):
            yield triplet


class Datagroup(object):

    def __init__(
            self, name: str,
            session: Optional[Session] = None,
            meta: Optional[DatagroupsMeta] = None
            ):
        self.name = name
        self.session = session or Session()
        self.meta = meta or DatagroupsMeta(self.session)
        if name not in self.meta.names:
            raise ValueError(f"Cannot identify a datagroup with a name '{name}'")

    @property
    def datasets(self) -> 'DatasetSelector':
        return DatasetSelector(self, self.session)

    @property
    def _datasets_meta(self) -> 'DatasetsMeta':
        return DatasetsMeta(self.name, self.session)

    def __str__(self) -> str:
        return f"Datagroup({self.name})"


class DatagroupSelector(object):

    def __init__(self, session: Optional[Session] = None):

        self.session = session or Session()
        self.meta = DatagroupsMeta(self.session)
        self._datagroups = {
            name: Datagroup(
                name, session=self.session, meta=self.meta
            )
            for name in self.meta.names
        }
        for name, datagroup in self._datagroups.items():
            setattr(self, name, datagroup)

    def all(self) -> Iterable[Datagroup]:
        for datagroup in self._datagroups.values():
            yield datagroup


DatagroupIdentifier = Union[str, Datagroup]
ExprIdentifier = Union[str, BaseExpression]


class Dataset(object):

    def __init__(
            self,
            datagroup: DatagroupIdentifier,
            name: Optional[str] = None,
            session: Optional[Session] = None
            ):

        if isinstance(datagroup, str):
            if not name and '.' in datagroup:
                datagroup, name = datagroup.split('.', 1)
            self.datagroup = Datagroup(datagroup, session=session)
        elif isinstance(datagroup, Datagroup):
            self.datagroup = datagroup
        else:
            raise ValueError(f"Cannot cast {datagroup} into a datagroup")

        if name not in self.datagroup._datasets_meta.names:
            raise ValueError(f"Cannot identify a dataset with a name '{name}'")

        self.session = self.datagroup.session
        self.name = name
        self.meta = ColumnsMeta(self.datagroup.name, name, self.session)
        self._columns = {
            name: Column(name, dtype, desr, parent=self)
            for name, dtype, desr in self.meta
        }
        for name, column in self._columns.items():
            setattr(self, name, column)
        self.expression = DatasetExpression(self)

    def __str__(self) -> str:
        return f"Dataset({self.datagroup.name}.{self.name})"

    def select(self, *args: Any) -> 'Dataset':
        result = self.copy()
        result.expression.select(*args)
        return result

    def filter(self, expr: Expression) -> 'Dataset':
        result = self.copy()
        result.expression.filter(expr)
        return result

    def groupby(self, *args: Any) -> 'Dataset':
        result = self.copy()
        result.expression.groupby(*args)
        return result

    def sort(self, *args: Any) -> 'Dataset':
        result = self.copy()
        result.expression.sort(*args)
        return result

    def head(self, n: int = 10) -> 'Dataset':
        result = self.copy()
        result.expression.head(n)
        return result

    def fetch(self):
        df = self.session.query_dataframe(str(self.expression))
        return df

    def sql(self) -> str:
        return str(self.expression)

    def copy(self) -> 'Dataset':
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        result.expression = copy.copy(self.expression)
        result.expression._select = self.expression._select.copy()
        result.expression._groupby = self.expression._groupby.copy()
        result.expression._sort = self.expression._sort.copy()
        return result


class DatasetSelector(object):

    def __init__(
            self,
            datagroup: DatagroupIdentifier,
            session: Optional[Session] = None
            ):

        if isinstance(datagroup, str):
            self.datagroup = Datagroup(datagroup, session=session)
        elif isinstance(datagroup, Datagroup):
            self.datagroup = datagroup
        else:
            raise ValueError(f"Cannot cast {datagroup} into a datagroup")

        self.session = session or self.datagroup.session
        self.meta = self.datagroup._datasets_meta
        self._datasets = {
            name: Dataset(self.datagroup, name)
            for name in self.meta.names
        }
        for name, dataset in self._datasets.items():
            setattr(self, name, dataset)

    def all(self) -> Iterable['Dataset']:
        for dataset in self._datasets.values():
            yield dataset


class DatasetExpression(object):

    def __init__(self, parent: Dataset):
        self.parent = parent
        self._namespace = set(self.parent._columns.keys())
        self._select: List[BaseExpression] = []
        self._filter: Optional[Expression] = None
        self._groupby: List[ExprIdentifier] = []
        self._sort: List[ExprIdentifier] = []
        self._limit: Optional[int] = None

    @property
    def _table_name(self) -> str:
        return f"{self.parent.datagroup.name}.{self.parent.name}"

    def _parse_identifier(self, item) -> BaseExpression:
        if isinstance(item, BaseExpression):
            value = item
        elif isinstance(item, str) and item in self.parent._columns:
            value = self.parent._columns[item]
        elif isinstance(item, (Number, bool, str)):
            value = Literal.wrap_constant(item)
        else:
            raise ValueError(f"Cannot parse the value: {item}")
        return value

    def select(self, *args: Any):
        # columns will be added next or raise an error
        columns = []
        if not args:
            raise ValueError("Cannot select with an empty sequence")
        if len(args) == 1 and (isinstance(args[0], dict) or is_iterable_type(args[0])):
            targets = args[0]
        else:
            targets = args

        if isinstance(targets, Sequence):
            for item in targets:
                value = self._parse_identifier(item)
                columns.append(value)
        elif isinstance(targets, dict):
            for key, item in targets.items():
                value = self._parse_identifier(item)
                columns.append(value.alias(key))
        else:
            raise ValueError(f"Invalid type for select expression: {type(targets)}")

        self._select.clear()
        for item in columns:
            if isinstance(item, BaseExpression):
                self._select.append(item)
                if item.evaluated_name is not None:
                    self._namespace.add(item.evaluated_name)
                # TODO:
                # if item._parent is self:
                #     self._select.append(item)
                # else:
                #     raise ValueError("")
            elif isinstance(item, str):
                self._select.append(self.parent._columns[item])
            else:
                self._select.append(Literal.wrap_constant(item))

    def filter(self, expr: 'Expression'):
        # TODO: validate expr
        self._filter = expr if self._filter is None else self._filter & expr

    def head(self, n: int):
        if not isinstance(n, int):
            raise ValueError(f'An integer value is expected: got {type(n)} instead')
        if n <= 0:
            raise ValueError(f'A positive integer is expected: got {n} instead')
        self._limit = n

    def groupby(self, *args: Any):
        targets = args[0] if len(args) == 1 and is_iterable_type(args[0]) else args
        # TODO: validate columns
        for item in targets:
            if isinstance(item, str):
                if item not in self._namespace:
                    raise ValueError(f"{item} not in namespace")
                else:
                    self._groupby.append(item)
            else:
                self._groupby.append(item)

    def sort(self, *args: Any):
        targets = args[0] if len(args) == 1 and is_iterable_type(args[0]) else args
        # TODO: validate columns
        for item in targets:
            if isinstance(item, str):
                if item not in self._namespace:
                    raise ValueError(f"{item} not in namespace")
                else:
                    self._sort.append(item)
            else:
                self._sort.append(item)

    def __str__(self) -> str:
        select = '*'
        if self._select:
            select = ', '.join(col.sql() for col in self._select)
        sql_expr = ''
        sql_expr += f"SELECT {select}"
        sql_expr += f'\nFROM {self._table_name}'
        if self._filter:
            sql_expr += f'\nWHERE {self._filter.sql()}'
        if self._groupby:
            groupby_expr = ', '.join(map(str, self._groupby))
            sql_expr += f'\nGROUP BY {groupby_expr}'
        if self._sort:
            sort_expr = ', '.join(map(str, self._sort))
            sql_expr += f'\n ORDER BY {sort_expr}'
        if self._limit:
            sql_expr += f'\nLIMIT {self._limit}'
        return sql_expr


class Column(BaseExpression):
    """
    Represents an individual database column or column alias
    """

    def __init__(
            self,
            name: str,
            dtype: str,
            descr: Optional[str] = None,
            parent: Optional[Dataset] = None,
            alias_name: Optional[str] = None,
            cast_dtype: Optional[str] = None
            ):
        """

        Parameters
        ----------
        name
            The column name
        dtype
            The data type of values in the column
        descr
            A text describing the column contents
        alias_name
            A temporary alias name for the column
        cast_dtype
            The name of the data type to which column values are converted
        """
        body = self.name, self.dtype, self.description = name, dtype, descr
        super().__init__(body, parent, alias_name, cast_dtype)
        if self._parent and name not in self._parent.meta.names:
            raise ValueError(f"A dataset '{self._parent.name}' doesn't have a column '{name}'")

    @property
    def _body_str(self) -> str:
        return f'"{self.name}"'

    @property
    def evaluated_name(self) -> str:
        return self._alias or self.name

    @property
    def evaluated_dtype(self) -> str:
        return self._cast or self.dtype


class DataResource(object):

    def __init__(self, session: Optional[Session] = None):
        self.session = session or Session()

    @property
    def datagroups(self) -> DatagroupSelector:
        return DatagroupSelector(session=self.session)

    def datagroup(self, name: str) -> Datagroup:
        return Datagroup(name, session=self.session)
