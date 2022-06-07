import copy
import datetime
from typing import List, Any, Union, Iterable, Optional

from . import operators as op
from . import functions as fn


class BaseExpression(object):

    def __init__(
            self,
            body: Any,
            parent: Optional[Any] = None,
            alias_name: Optional[str] = None,
            cast_dtype: Optional[str] = None
            ):
        """
        Parameters
        ----------
        body
            The expression body
        alias_name
            A temporary alias name for the expression
        cast_dtype
            The name of the data type to which expression values are converted
        """
        self._body = body
        self._parent = parent
        self._alias = alias_name
        self._cast = cast_dtype

    @property
    def _body_str(self) -> str:
        """
        Overwritten by child classes

        Returns
        -------
        str
            String representation of the inner body
        """
        return str(self._body)

    @property
    def precedence(self) -> int:
        """
        A large number to ...
        """
        return 100

    @property
    def evaluated_name(self) -> Union[str, None]:
        return self._alias

    @property
    def evaluated_dtype(self) -> Union[str, None]:
        return self._cast

    @staticmethod
    def wrap_parenthesis(s: str) -> str:
        """
        Put parenthesis around the string or leave it unchanged if
        it already has the parenthesis

        Parameters
        ----------
        s
            The input string

        Returns
        -------
        str
            A string enclosed in parenthesis or
        """
        if not s.strip():  # empty string
            return s
        if s.startswith('(') and s.endswith(')'):  # already has parenthesis
            return s
        else:
            return f'({s})'

    def alias(self, alias_name: str) -> 'BaseExpression':
        """
        Old alias is overwritten

        Parameters
        ----------
        alias_name
            A temporary alias name for the column

        Returns
        -------
        Column
            A new instance of BaseExpression with the alias set
        """
        created = copy.copy(self)
        created._alias = alias_name
        return created

    def cast(self, dtype: str) -> 'BaseExpression':
        """
        Parameters
        ----------
        dtype
            A name of the data type

        Returns
        -------
        Column
            A new instance of BaseExpression with the cast data type set
        """
        created = copy.copy(self)
        created._cast = dtype
        return created

    def astype(self, dtype: str) -> 'BaseExpression':
        """
        Same as `cast`
        """
        return self.cast(dtype)

    def sql(self) -> str:
        """
        SQL representation of the expression
        """
        return str(self)

    def _format_str(self, s: str) -> str:
        """
        Apply alias and/or cast data type.
        """
        if self._cast:
            s = f'CAST({s} AS {self._cast})'
        if self._alias:
            s = f'{s} AS "{self._alias}"'
        return s

    def __copy__(self) -> 'BaseExpression':
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def __str__(self) -> str:
        return self._format_str(self._body_str)

    def __hash__(self) -> int:
        return hash(str(self))

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self._body})'

    def __and__(self, other: Any) -> 'BooleanExpression':
        return BooleanExpression(op.And(), self, Literal.wrap_constant(other))

    def __or__(self, other: Any) -> 'BooleanExpression':
        return BooleanExpression(op.Or(), self, Literal.wrap_constant(other))

    def __rand__(self, other: Any) -> 'BooleanExpression':
        return BooleanExpression(op.And(), Literal.wrap_constant(other), self)

    def __ror__(self, other: Any) -> 'BooleanExpression':
        return BooleanExpression(op.Or(), Literal.wrap_constant(other), self)

    def __lt__(self, other: Any) -> 'BooleanExpression':
        return BooleanExpression(op.Lt(), self, Literal.wrap_constant(other))

    def __le__(self, other: Any) -> 'BooleanExpression':
        return BooleanExpression(op.Le(), self, Literal.wrap_constant(other))

    def __eq__(self, other: Any) -> 'BooleanExpression':  # type: ignore[override]
        return BooleanExpression(op.Eq(), self, Literal.wrap_constant(other))

    def __ne__(self, other: Any) -> 'BooleanExpression':  # type: ignore[override]
        return BooleanExpression(op.Ne(), self, Literal.wrap_constant(other))

    def __gt__(self, other: Any) -> 'BooleanExpression':
        return BooleanExpression(op.Gt(), self, Literal.wrap_constant(other))

    def __ge__(self, other: Any) -> 'BooleanExpression':
        return BooleanExpression(op.Ge(), self, Literal.wrap_constant(other))

    def __add__(self, other: Any) -> 'ArithmeticExpression':
        return ArithmeticExpression(op.Add(), self, Literal.wrap_constant(other))

    def __sub__(self, other: Any) -> 'ArithmeticExpression':
        return ArithmeticExpression(op.Sub(), self, Literal.wrap_constant(other))

    def __mul__(self, other: Any) -> 'ArithmeticExpression':
        return ArithmeticExpression(op.Mul(), self, Literal.wrap_constant(other))

    def __truediv__(self, other: Any) -> 'ArithmeticExpression':
        return ArithmeticExpression(op.Div(), self, Literal.wrap_constant(other))

    def __mod__(self, other: Any) -> 'ArithmeticExpression':
        return ArithmeticExpression(op.Mod(), self, Literal.wrap_constant(other))

    def __radd__(self, other: Any) -> 'ArithmeticExpression':
        return ArithmeticExpression(op.Add(), Literal.wrap_constant(other), self)

    def __rsub__(self, other: Any) -> 'ArithmeticExpression':
        return ArithmeticExpression(op.Sub(), Literal.wrap_constant(other), self)

    def __rmul__(self, other: Any) -> 'ArithmeticExpression':
        return ArithmeticExpression(op.Mul(), Literal.wrap_constant(other), self)

    def __rdiv__(self, other: Any) -> 'ArithmeticExpression':
        return ArithmeticExpression(op.Div(), Literal.wrap_constant(other), self)

    def __rmod__(self, other: Any) -> 'ArithmeticExpression':
        return ArithmeticExpression(op.Mod(), Literal.wrap_constant(other), self)

    def __neg__(self) -> 'UnaryExpression':
        return UnaryExpression(op.USub(), self)

    def __pos__(self) -> 'BaseExpression':
        return self

    def __abs__(self) -> 'UnaryExpression':
        raise NotImplementedError

    def __invert__(self) -> 'UnaryExpression':
        return UnaryExpression(op.Not(), self)

    def lt(self, other: Any) -> 'BooleanExpression':
        return self < other

    def le(self, other: Any) -> 'BooleanExpression':
        return self <= other

    def eq(self, other: Any) -> 'BooleanExpression':
        return self == other

    def ne(self, other: Any) -> 'BooleanExpression':
        return self != other

    def gt(self, other: Any) -> 'BooleanExpression':
        return self > other

    def ge(self, other: Any) -> 'BooleanExpression':
        return self >= other

    def add(self, other: Any) -> 'ArithmeticExpression':
        return self + other

    def sub(self, other: Any) -> 'ArithmeticExpression':
        return self - other

    def mul(self, other: Any) -> 'ArithmeticExpression':
        return self * other

    def div(self, other: Any) -> 'ArithmeticExpression':
        return self / other

    def mod(self, other: Any) -> 'ArithmeticExpression':
        return ArithmeticExpression(op.Mod(), self, Literal.wrap_constant(other))

    def isin(self, other: Iterable[Any]) -> 'BooleanExpression':
        return BooleanExpression(op.In(), self, Literal.wrap_constant(tuple(other)))

    def like(self, other: str) -> 'BooleanExpression':
        return BooleanExpression(op.Like(), self, Literal.wrap_constant(other))

    def ilike(self, other: str) -> 'BooleanExpression':
        return BooleanExpression(op.Ilike(), self, Literal.wrap_constant(other))

    # TODO failing test extra parents
    def between(self, beg: Any, end: Any) -> 'BooleanExpression':
        other = Literal.wrap_constant(beg) & Literal.wrap_constant(end)
        return BooleanExpression(op.Between(), self, other)

    def apply(self, func: Union[str, fn.Function], *args) -> 'FuncExpression':
        if isinstance(func, str):
            return FuncExpression(fn.Function(func), [self] + list(args))
        else:
            return FuncExpression(func, [self] + list(args))


class Expression(BaseExpression):
    def __init__(
            self,
            operation: op.Operation,
            operands: List[BaseExpression],
            parent: Optional[Any] = None,
            alias_name: str = None,
            cast_dtype: str = None
            ):
        body = self.op, self.operands = operation, operands
        super().__init__(body, parent, alias_name, cast_dtype)

    @property
    def precedence(self) -> int:
        """
        """
        return self.op.precedence


class BinaryExpression(Expression):
    def __init__(
            self,
            operation: op.Operation,
            left: BaseExpression,
            right: BaseExpression,
            parent: Optional[Any] = None,
            alias_name: str = None,
            cast_dtype: str = None
            ):
        self.op, self.left, self.right = operation, left, right
        super().__init__(operation, [left, right], parent, alias_name, cast_dtype)

    def _infer_parent(self) -> Any:
        if self.left._parent is None or self.right._parent is None:
            return self.left._parent or self.right._parent
        elif self.left._parent is self.right._parent:
            return self.left._parent
        else:
            msg = (
                "Cannot use operands from distinct datasets: " +
                f"{self.left._parent} and {self.right._parent}"
            )
            raise ValueError(msg)

    @property
    def _body_str(self) -> str:
        left_str = str(self.left)
        right_str = str(self.right)
        if self.precedence > self.left.precedence:
            left_str = self.wrap_parenthesis(left_str)
        if self.precedence > self.right.precedence:
            right_str = self.wrap_parenthesis(right_str)
        if isinstance(self.right, UnaryExpression):
            right_str = self.wrap_parenthesis(right_str)
        if isinstance(self.left, BooleanExpression):
            left_str = self.wrap_parenthesis(left_str)
        if isinstance(self.right, BooleanExpression):
            right_str = self.wrap_parenthesis(right_str)
        return f"{left_str} {self.op.symbol} {right_str}"


class BooleanExpression(BinaryExpression):
    @property
    def _body_str(self) -> str:
        if isinstance(self.op, (op.Between, op.NotBetween)):
            left_str = str(self.left)
            right_str = str(self.right)
            if self.precedence > self.left.precedence:
                left_str = self.wrap_parenthesis(left_str)
            return f"{left_str} {self.op.symbol} {right_str}"
        else:
            return super()._body_str


class ArithmeticExpression(BinaryExpression):
    pass


class UnaryExpression(Expression):

    def __init__(
            self,
            operation: op.Operation,
            operand: BaseExpression,
            parent: Optional[Any] = None,
            alias_name: str = None,
            cast_dtype: str = None
            ):
        self.op, self.operand = operation, operand
        super().__init__(self.op, [self.operand], parent, alias_name, cast_dtype)

    @property
    def _body_str(self) -> str:
        operand_str = str(self.operand)
        if self.precedence > self.operand.precedence:
            operand_str = self.wrap_parenthesis(operand_str)
        return f"{self.op.symbol}{operand_str}"


class FuncExpression(Expression):

    def __init__(
            self,
            function: fn.Function,
            operand: List[BaseExpression],
            parent: Optional[Any] = None,
            alias_name: str = None,
            cast_dtype: str = None
            ):
        self.fn, self.operand = function, operand
        super().__init__(function, operand, parent, alias_name, cast_dtype)

    @property
    def _body_str(self) -> str:
        operand_str = ", ".join(str(x) for x in self.operand)
        return f"{self.fn.symbol}({operand_str})"


class Literal(BaseExpression):
    """
    Represents a constant value
    """
    @classmethod
    def wrap_constant(cls, value: Any) -> BaseExpression:
        """
        If not an Expression, make it a literal
        """
        return value if isinstance(value, BaseExpression) else Literal(value)

    @property
    def value(self):
        return self._body

    def _format_literal(self, value: Any) -> str:
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return str(value).upper()
        elif isinstance(value, (datetime.date, datetime.datetime)):
            return f"'{value.isoformat()}'"
        elif isinstance(value, (list, tuple, set)):
            return '(' + ", ".join(map(self._format_literal, value)) + ')'
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            return f"'{value}'"

    @property
    def _body_str(self) -> str:
        return self._format_literal(self.value)
