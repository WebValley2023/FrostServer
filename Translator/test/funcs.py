import frost_sta_client as fsc
import pandas as pd
from datetime import datetime as dt
import dateutil.parser as dat

def insert_thing(service,name):
    thing=fsc.Thing(
            name=name,
            description="Appa "+name,
            properties={}
        )
    service.create(thing)
    return thing.id


def insert_sensor(service,packet, thing_id):
    for i in range(1,25):
        sensor=fsc.Sensor(
            name="S"+str(i),
            description="Appa "+packet['node_id']+" Sensor "+str(i),
            encodingType="application/json",
            metadata="http://www.appa.com",
            properties={"node_id":packet['node_id']}
        )
        service.create(sensor)
        sensor.thing_id=thing_id
        service.update(sensor)
        
def insert_observed_property(service,packet):
    for i in range(1,25):
        observed_property=fsc.ObservedProperty(
            name="S"+str(i),
            description="Appa "+packet['node_id']+" Sensor "+str(i),
            definition="http://www.appa.com",
            properties={"node_id":packet['node_id']}
        )
        service.create(observed_property)

def insert_datastream():
    
