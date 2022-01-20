import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.ext.declarative
import isb_web.config

DATABASE_URL = isb_web.config.Settings().database_url

engine = sqlalchemy.create_engine(DATABASE_URL)
SessionLocal = sqlalchemy.orm.sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = sqlalchemy.ext.declarative.declarative_base()
