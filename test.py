from dataclasses import dataclass, fields
from geojson import Point
from typing import Iterable
import frost_sta_client as fsc
import pandas as pd
from datetime import datetime as dt
import dateutil.parser as datP

FROST_SERVER = "http://localhost:8080/FROST-Server/v1.1"
service = fsc.SensorThingsService(FROST_SERVER)

packet = {'S1_R1': 500, 'S1_R2': 128000000, 'S1_Voltageage': 3.91,
 'S2_R1': 111.1, 'S2_R2': 790123, 'S2_Voltageage': 3.64,
 'S3_R1': 135.3, 'S3_R2': 50000000, 'S3_Voltageage': 2.39,
 'S4_R1': 147.7, 'S4_R2': 355555552, 'S4_Voltageage': 2.57,
 'S5_R1': 128.3, 'S5_R2': 499000000, 'S5_Voltageage': 3.89,
 'S6_R1': 128.9, 'S6_R2': 499000000, 'S6_Voltageage': 4.02,
 'S7_R1': 103.4, 'S7_R2': 26876640, 'S7_Voltageage': 3.95,
 'S8_R1': 108.6, 'S8_R2': 373333344, 'S8_Voltageage': 4.33,
 'T': 33.5,'RH': 32.7, 'P': 988, 'timestamp': 1659106708162, 'node_id': 'appa1-debug'}

# @dataclass
# class Node:
#     id: str
#     node_name: str
#     description: str


@dataclass
class Sensor:
    id: int
    node_id: int
    name: str
    description: str

@dataclass
class Node:
    id: int
    name: str
    description: str
    properties: str

@dataclass
class Location:
    id: int
    name: str
    description: str
    encoding_type: str


@dataclass
class HistoricalLocation:
    id: int
    time:str

@dataclass
class Datastream:
    id: int
    name: str
    description: str
    #todo understand observation_type
    unit_of_measurement: str
    observated_area: str

@dataclass
class ObservedProperty:
    id:int
    name: str
    definition: str
    description: str








# @dataclass(frozen=True)
# class ObservedProperty:
#     name: str
#     definition: str
#     description: str
#     feature_of_interest: str  # what is it observing??

def create_node(service, nSensors):
    things = {}
    
    for node in range(0, nSensors):
        thing = fsc.Thing(
            id = 1,
            name = "Node_Name",
            description = "Node_Description",
        )
        things[node.id] = thing
        # service.create(thing)
        # print(f"Inserted {node=}")

def create_observation(service, sensors, parameters):
    observations = {}
    
    for i in range(0, len(sensors)):
        observation = fsc.Observation(
            id = i,
            result = "any",
            phenomenon_time = "?",
            result_time = "?",
            valid_time = "?",
            result_quality = "?",
            parameters = parameters
        )
        observations[observation.id] = observation
        # service.create(observation)
        # print(f"Inserted {observation=}")
    return observations

def create_sensor(service):
    sensors_map = {}
    
    for i in range(1,9):
        entity = fsc.Sensor(
            id = i,
            name = "S" + str(i),
            description = "S"  + str(i) + "_Description",
            properties = {"node_id": "S"  + str(i) + "_Node"},
            encoding_type = 'application/json',
            metadata = "any",
        )
        sensors_map[entity.id] = entity
        # service.create(entity)
        # print(f"Inserted {sensor=}")
    return sensors_map

def convert_to_isoformat(dateInMilllis):
    convertToDayFormat = dt.fromtimestamp(dateInMilllis / 1000.0)
    return convertToDayFormat.isoformat() + 'Z'



def isolate_parameters(packet):
    l = ['T', 'RH', 'P', 'timestamp', 'node_id']
    parameters = {key: item for key, item in packet.items() if key in l}
    return parameters

sensors = create_sensor(service=service)
parameters = isolate_parameters(packet)
create_observation(service, sensors, parameters)
#create_node(service, sensors)