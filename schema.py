import sys
import os
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import types, Column, create_engine

dbase = declarative_base()
metadata = dbase.metadata


class Reading(dbase):
    __tablename__ = 'readings'
    id = Column(types.Integer, primary_key=True)
    sensor_id = Column(types.String(20), nullable=False)
    cpm = Column(types.Integer, nullable=False)
    timestamp = Column(types.DateTime, nullable=False)
    usp = Column(types.Float, nullable=False)
    period_secs = Column(types.Integer, nullable=False)
    pcount = Column(types.Integer, nullable=False)
    as_test = Column(types.Boolean, default=lambda: True if os.getenv('LOCALDEV', None) else None)


#def get_session(options, config, allow_db=False):
#    engine = create_engine(config.get(options.section, 'uri'))
#
#    Session = sessionmaker(bind=engine)
#    session = None
#
#    if allow_db:
#        if options.create:
#            dbase.metadata.create_all(engine)
#            print("Created")
#
#        if options.delete or options.drop_all:
#            session = Session()
#            for table in dbase.metadata.sorted_tables:
#                if options.delete:
#                    session.execute(table.delete())
#                if options.drop_all:
#                    session.execute('drop table %s' % table.name)
#            session.commit()
#            if options.get('drop_all', None):
#                print("dropped")
#                sys.exit(1)
#            print("deleted")
#    return session or Session()
