from dataclasses import dataclass, fields
import frost_sta_client as fsc
import pandas as pd
from datetime import datetime as dt
import dateutil.parser as datP

FROST_SERVER = "http://localhost:8080/FROST-Server/v1.1"
service = fsc.SensorThingsService(FROST_SERVER)

packet = {'S1_R1': 500, 'S1_R2': 128000000, 'S1_Voltage': 3.91,
 'S2_R1': 111.1, 'S2_R2': 790123, 'S2_Voltage': 3.64,
 'S3_R1': 135.3, 'S3_R2': 50000000, 'S3_Voltage': 2.39,
 'S4_R1': 147.7, 'S4_R2': 355555552, 'S4_Voltage': 2.57,
 'S5_R1': 128.3, 'S5_R2': 499000000, 'S5_Voltage': 3.89,
 'S6_R1': 128.9, 'S6_R2': 499000000, 'S6_Voltage': 4.02,
 'S7_R1': 103.4, 'S7_R2': 26876640, 'S7_Voltage': 3.95,
 'S8_R1': 108.6, 'S8_R2': 373333344, 'S8_Voltage': 4.33,
 'T': 33.5,'RH': 32.7, 'P': 988, 'timestamp': 1659106708162, 'node_id': 'appa1-debug'}

@dataclass
class Sensor:
    id: int
    node_id: str
    name: str
    description: str

@dataclass
class Node:
    id: id
    node_name: str
    description: str
    properties: str

@dataclass
class Observation:
    id: int
    result: str
    phenomenon_time: str
    result_time: str
    valid_time: str
    result_quality: str
    parameters: str

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

@dataclass(frozen=True)
class ObservedProperty:
    id: int
    name: str
    definition: str
    description: str
    properties: str  # what is it observing??

def isolate_parameters(packet):
     l = ['T', 'RH', 'P', 'timestamp', 'node_id']
     parameters = {key: item for key, item in packet.items() if key in l}
     parameters['timestamp'] = convert_to_isoformat(parameters['timestamp'])
     return parameters

def convert_structure(packet):
     colonne = ['descr', 'value']
     dataFrame = pd.DataFrame(list(packet.items()), columns=colonne)
     convertedData = convert_to_isoformat(dataFrame['value'].iloc[27])
     dataFrame['value'].iloc[27] = convertedData
     return dataFrame


def match_observated_properties(definition_to_match):
    definition = ''
    description = ''
    if definition_to_match not in ['T', 'RH', 'P', 'timestamp', 'node_id']:
        definition = "_Resistance" if "R" in definition_to_match else  "_Voltage"
        description = "Resistance" if "R" in definition_to_match else "Voltage"
    else:
        match definition_to_match:
            case 'T':
                definition = 'T'
                description = 'Temperature'
            case 'RH':
                definition = 'RH'
                description = 'Humidity'
            case 'P':
                definition = 'P'
                description = 'Pressure'
            case 'timestamp':
                definition = 'timestamp'
                description = 'Time_stamp'
            case 'node_id':
                definition = 'node_id'
                description = 'Node_id'
                
    return definition, description

def create_observated_property(service, packet):
    observatedProperties = {}
    
    for i in range(len(packet)):
        definition, description = match_observated_properties(list(packet.keys())[i])
        observatedProperty = fsc.ObservedProperty(
            id = i,
            name = list(packet.keys())[i],
            definition = definition,
            description = description,
            properties = {}
        )
        observatedProperties[observatedProperty.id] = observatedProperty
        # service.create(observation)
        print(f"Inserted {observatedProperty=}")
    
    return observatedProperties

def create_observation(service, packet):
    observations = {}
    
    for i in range(len(packet)):
        observation = fsc.Observation(
            id = i,
            result = "any",
            phenomenon_time = packet['timestamp'],
            result_quality = list(packet.values())[i],
            parameters = {}
        )
        observations[observation.id] = observation
        # service.create(observation)
        print(f"Inserted {observation=}")

    return observations



def create_node(service):
    things = {}
    
    for i in range(1, 2):
        thing = fsc.Thing(
            id = i,
            name = "Node_Name",
            description = "Node_Description",
            properties = {} 
        )
        things[thing.id] = thing
        #service.create(thing)
        print(f"Inserted {thing=}")

    return things

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
        print(f"Inserted {entity=}")
    
    return sensors_map


def convert_to_isoformat(dateInMilllis):
    convertToDayFormat = dt.fromtimestamp(dateInMilllis / 1000.0)
    return convertToDayFormat.isoformat() + 'Z'

packet['timestamp'] = convert_to_isoformat(packet['timestamp'])
sensors = create_sensor(service=service)
create_node(service=service)
create_observation(service=service, packet=packet)
create_observated_property(service=service, packet=packet)

parameters = isolate_parameters(packet)

