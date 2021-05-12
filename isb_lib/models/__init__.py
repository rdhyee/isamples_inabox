import logging
import sqlalchemy.ext.declarative

_L = logging.getLogger("isb_lib.models")

Base = sqlalchemy.ext.declarative.declarative_base()
