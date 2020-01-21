#!/usr/bin/env python
import os
import sys
import argparse
import configparser
import json
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import dateutil.parser
import paho.mqtt.client as mqtt

from schema import Reading, Rand
# import hec

session = None


app_config = dict()
app_sessions = dict()


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("+/radiation/+")
    client.subscribe("+/radrand")


def data_to_splunk(data):
    sd = {
        'time': time.mktime(data['timestamp'].timetuple()) + data['timestamp'].microsecond / 1E6,
        'event': 'metric',
        'source': 'radiation',
        'host': data['sensor_id'],
        'fields': {
            'cpm': data['cpm'],
            'pcount': data['pcount'],
            'value': data['usp'],
            'period_secs': data['period_secs'],
            'metric_name': "uS per hour"
        }
    }
    try:
        app_sessions['splunk'].send(sd)
    except Exception as e:
        print("splunk error '%s'" % str(e))


def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode('utf8'))
    except ValueError as e:
        print("json error '%s'" % (msg.payload), file=sys.stderr)
        return

    if msg.topic.split('/')[1] == 'radrand':
        print("Value %d\n" % data['value'])
        client.db_session.add(Rand(value=data['value'],
                              timestamp=dateutil.parser.parse((data['datetime']))))
    elif msg.topic.split('/')[1] == 'radiation':
        dbdata = dict(sensor_id=msg.topic.split('/')[0],
                      cpm=data['count'],
                      timestamp=dateutil.parser.parse((data['datetime'])),
                      pcount=data['this'],
                      usp=data['uSv_h'],
                      period_secs=data['period'])
        data_to_splunk(dbdata)
        client.db_session.add(Reading(**dbdata))
    client.db_session.commit()


def cstest(session):
    from scipy.stats import chisquare

    print(chisquare([val for val in session.query(Rand.value)]))


if __name__ == '__main__':
    config = configparser.ConfigParser()
    parser = argparse.ArgumentParser(description='check stuff and email')
    parser.add_argument("-f", "--config", dest="config", default='settings.ini', help="config file")
    parser.add_argument("-s", "--section", dest="section", default='beta', help="config file section")
    parser.add_argument("-c", "--chi", dest="chi", action='store_true', help="Chi Square test of random data")
    options = parser.parse_args()
    config.readfp(open(options.config))
    app_config.update(dict(config[options.section].items()))
#    app_sessions['splunk'] = hec.PyHEC(token=app_config['splunk_token'], uri=app_config['splunk_uri'])

    engine = create_engine(config.get(options.section, 'uri'))
    Session = sessionmaker(bind=engine)

    if options.chi:
        cstest(Session())
        sys.exit(0)

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.db_session = Session()
    client.connect("mqtt", 1883, 60)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_forever()
