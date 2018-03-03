#!/usr/bin/env python
import argparse
import configparser
import json

import dateutil.parser
import paho.mqtt.client as mqtt

from schema import Reading

session = None

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("+/radiation/+")


def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode('utf8'))
    rd = Reading(sensor_id=msg.topic.split('/')[0],
                 cpm=data['count'],
                 timestamp=dateutil.parser.parse((data['datetime'])),
                 pcount=data['this'],
                 usp=data['uSv_h'],
                 period_secs=data['period'])
    print("topic %s data '%r'" % (msg.topic, data))
    client.db_session.add(rd);
    client.db_session.commit()



if __name__ == '__main__':
    config = configparser.ConfigParser()
    parser = argparse.ArgumentParser(description='check stuff and email')
    parser.add_argument("-f", "--config", dest="config", default='settings.ini', help="config file")
    parser.add_argument("-s", "--section", dest="section", default='beta', help="config file section")
    parser.add_argument("-c", "--create", dest="create", action="store_true", default=None, help="Create Tables")
    parser.add_argument("-d", "--delete", dest="delete", action="store_true", default=None, help="delete data from 'data' tables")
    parser.add_argument("-r", "--drop-all", dest="drop_all", action="store_true", default=None, help="drop all 'data' tables")
    parser.add_argument("-t", "--test-data", dest="test_data", action="store_true", default=None, help="generate test data")
    options = parser.parse_args()
    config.readfp(open(options.config))

    engine = create_engine(config.get(options.section, 'uri'))
    Session = sessionmaker(bind=engine)

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
