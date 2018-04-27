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
    timestamp = Column(types.DateTime(timezone=True), nullable=False)
    usp = Column(types.Float, nullable=False)
    period_secs = Column(types.Integer, nullable=False)
    pcount = Column(types.Integer, nullable=False)
    as_test = Column(types.Boolean, default=lambda: True if os.getenv('LOCALDEV', None) else None)


class Rand(dbase):
    __tablename__ = 'randoms'
    id = Column(types.Integer, primary_key=True)
    value = Column(types.BigInteger, nullable=False)
    timestamp = Column(types.DateTime, nullable=False)


if __name__ == '__main__':
    import argparse
    import configparser

    config = configparser.ConfigParser()
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--config", dest="config", default='settings.ini', help="config file")
    parser.add_argument("-s", "--section", dest="section", default='beta', help="config file section")
    parser.add_argument("-c", "--create", dest="create", action="store_true", help="create database")
    options = parser.parse_args()
    config.readfp(open(options.config))
    engine = create_engine(config.get(options.section, 'uri'))

    Session = sessionmaker(bind=engine)
    session = None

    if options.create:
        dbase.metadata.create_all(engine)
        print("Created")
#
