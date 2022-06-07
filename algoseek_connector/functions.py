from typing import TYPE_CHECKING

from .operators import Operation

if TYPE_CHECKING:
    from .expressions import BaseExpression, FuncExpression


class Function(Operation):
    """
    """

    precedence = 90

    def __init__(self, name: str):
        self.name = name
        self.symbol = name

    def __call__(self, x: 'BaseExpression', *args, **kwargs) -> 'FuncExpression':
        # TODO: cannot apply a function to a constant
        # TODO: cannot apply a function to column by its string name
        # if not isinstance(x, BaseException):
        #     x = Literal.wrap_constant(x)
        return x.apply(self, *args)


class AggregateFunction(Function):
    pass


distinct = Function('distinct')
max2 = Function('max2')
min2 = Function('min2')

toInt = Function('toInt')
toUInt = Function('toUInt')
toFloat = Function('toFloat')
toDecimal = Function('toDecimal')

toDate = Function('toDate')
toDateTime = Function('toDateTime')
toTime = Function('toTime')
toString = Function('toString')

toIntervalSecond = Function('toIntervalSecond')
toIntervalMinute = Function('toIntervalMinute')
toIntervalHour = Function('toIntervalHour')
toIntervalDay = Function('toIntervalDay')
toIntervalWeek = Function('toIntervalWeek')
toIntervalMonth = Function('toIntervalMonth')
toIntervalQuarter = Function('toIntervalQuarter')
toIntervalYear = Function('toIntervalYear')

toUnixTimestamp64Milli = Function('toUnixTimestamp64Milli')
toUnixTimestamp64Micro = Function('toUnixTimestamp64Micro')
toUnixTimestamp64Nano = Function('toUnixTimestamp64Nano')
fromUnixTimestamp64Milli = Function('fromUnixTimestamp64Milli')
fromUnixTimestamp64Micro = Function('fromUnixTimestamp64Micro')
fromUnixTimestamp64Nano = Function('fromUnixTimestamp64Nano')

cast = Function('cast')
abs = Function('abs')

sum = AggregateFunction('sum')
avg = AggregateFunction('avg')
mean = AggregateFunction('mean')
count = AggregateFunction('count')
max = AggregateFunction('max')
min = AggregateFunction('min')
argMax = AggregateFunction('argMax')
argMin = AggregateFunction('argMin')
any = AggregateFunction('any')
avgWeighted = AggregateFunction('avgWeighted')
groupArray = AggregateFunction('groupArray')
uniq = AggregateFunction('uniq')
