from .__version__ import __title__, __description__, __version__
from .__version__ import __author__, __author_email__, __copyright__

from .sessions import Session, ExecutionError
from .resources import DataResource, Datagroup, Dataset

__all__ = [
    '__title__', '__description__', '__version__',
    '__author__', '__author_email__', '__copyright__',
    'Session', 'ExecutionError', 'DataResource', 'Datagroup', 'Dataset'
]
