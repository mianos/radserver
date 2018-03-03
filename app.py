import json
import configparser
import argparse

from flask import Flask, g, Response
from sqlalchemy import create_engine, null, func
from sqlalchemy.orm import sessionmaker

from schema import Reading

config_section = 'beta'
settings_file = 'settings.ini'

def get_app():
    config = configparser.ConfigParser()
    config.readfp(open(settings_file))
    engine = create_engine(config.get(config_section, 'uri'))
    Session = sessionmaker(bind=engine)

    app = Flask(__name__)

    @app.before_request
    def before_request():
        g.session = Session()

    @app.after_request
    def session_commit(response):
        session = getattr(g, 'session', None)
        if session is not None:
            if response.status_code // 200 == 1:
                g.session.commit()
            else:
                g.session.rollback()
        return response

    app.session = Session()
    return app


app = get_app()


@app.route('/')
def radiation():
    #SELECT sum(dt.pcount) / count(dt.id), sum(dt.pcount), count(dt.id), avg(dt.usp)
    #  FROM (select * from readings
    #  where period_secs = 60 and as_test is NULL
    #  ORDER BY id DESC
    #  LIMIT 60) as dt;

    minutes = 60
    recent = g.session.query(Reading).filter(Reading.period_secs == 60, Reading.as_test == null()).order_by(Reading.id.desc()).limit(minutes).subquery()
    result = g.session.query((func.sum(recent.c.pcount) / func.count(recent.c.id)).label('cpm'),
                              func.avg(recent.c.usp).label('usp'),
                              func.max(recent.c.timestamp).label('last_timestamp')) \
                      .one()
    return Response(json.dumps(result._asdict(), default=lambda tt: tt.isoformat() if hasattr(tt, 'isoformat') else tt),  mimetype='application/json')
