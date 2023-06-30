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
    id: str
    node_id: str
    name: str
    description: str


# @dataclass
# class Packet:
#     id: str
#     node_id: str
#     insert_ts: str
#     sensor_ts: str


# @dataclass
# class PacketData:
#     id: str
#     packet_id: str
#     sensor_id: str
#     r1: str
#     r2: str
#     volt: str


# @dataclass(frozen=True)
# class ObservedProperty:
#     name: str
#     definition: str
#     description: str
#     feature_of_interest: str  # what is it observing??



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
        service.create(entity)
        print(f"Inserted {sensor=}")




