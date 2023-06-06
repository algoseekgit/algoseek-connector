
class Operation(object):
    """
    Operator precedence determines the grouping of terms
    in an expression and decides how an expression is evaluated.

    Within an expression, higher precedence operators will be evaluated first.
    """
    symbol: str = 'undefined'
    precedence: int = 0


class BooleanOperation(Operation):
    """
    A base class for operations evaluating to a boolean expression
    """
    pass


class LogicalOperation(BooleanOperation):
    pass


class Or(LogicalOperation):
    """
    TRUE if either Boolean expression is TRUE
    """
    symbol = 'OR'
    precedence = 11


class In(LogicalOperation):
    """
    TRUE if the operand is equal to one of a list of expressions
    """
    symbol = 'IN'
    precedence = 12


class NotIn(LogicalOperation):
    symbol = 'NOT IN'
    precedence = 12


class Between(LogicalOperation):
    """
    TRUE if the operand is within a range
    """
    symbol = 'BETWEEN'
    precedence = 13


class NotBetween(LogicalOperation):
    symbol = 'NOT BETWEEN'
    precedence = 13


class Like(LogicalOperation):
    """
    TRUE if the operand matches a pattern
    """
    symbol = 'LIKE'
    precedence = 14


class NotLike(LogicalOperation):
    symbol = 'NOT LIKE'
    precedence = 14


class Ilike(LogicalOperation):
    """
    TRUE if the operand matches a pattern (case-insensitive)
    """
    symbol = 'ILIKE'
    precedence = 14


class And(LogicalOperation):
    """
    TRUE if both Boolean expressions are TRUE
    """
    symbol = 'AND'
    precedence = 15


class EqualityOperation(BooleanOperation):
    pass


class Eq(EqualityOperation):
    """
    Equal to
    """
    symbol = '='
    precedence = 21


class Ne(EqualityOperation):
    """
    Not equal to
    """
    symbol = '!='
    precedence = 21


class RelationalOperation(BooleanOperation):
    pass


class Lt(RelationalOperation):
    """
    Less than
    """
    symbol = '<'
    precedence = 25


class Le(RelationalOperation):
    """
    Less than or equal to
    """
    symbol = '<='
    precedence = 25


class Ge(RelationalOperation):
    """
    Greater than or equal to
    """
    symbol = '>='
    precedence = 25


class Gt(RelationalOperation):
    """
    Greater than
    """
    symbol = '>'
    precedence = 25


class ArithmeticOperation(Operation):
    """
    A base class for operations evaluating to an arithmetic expression
    """
    pass


class AdditiveOperation(ArithmeticOperation):
    pass


class Add(AdditiveOperation):
    """
    Add
    """
    symbol = '+'
    precedence = 31


class Sub(AdditiveOperation):
    """
    Subtract
    """
    symbol = '-'
    precedence = 31


class MultiplicativeOperation(ArithmeticOperation):
    pass


class Mul(MultiplicativeOperation):
    """
    Multiply
    """
    symbol = '*'
    precedence = 35


class Div(MultiplicativeOperation):
    """
    Divide
    """
    symbol = '/'
    precedence = 35


class Mod(MultiplicativeOperation):
    """
    Modulo
    """
    symbol = '%'
    precedence = 35


class UnaryOperation(Operation):
    pass


class USub(UnaryOperation, ArithmeticOperation):
    """
    Negate
    """
    symbol = '-'
    precedence = 41


class Not(UnaryOperation, LogicalOperation):
    """
    Negate
    """
    symbol = 'NOT '
    precedence = 41
