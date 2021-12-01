from enum import Enum, auto


class _NoValue(Enum):
    """Enum subclass indicating the value is unimportant (https://docs.python.org/3/library/enum.html)"""

    def __repr__(self):
        return "<%s.%s>" % (self.__class__.__name__, self.name)


class ISBFormat(_NoValue):
    """Format parameter for JSON records"""

    ORIGINAL = "original"
    CORE = "core"
    FULL = "full"
    SOLR = "solr"
