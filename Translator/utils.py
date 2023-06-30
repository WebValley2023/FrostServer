import frost_sta_client as fsc
import pandas as pd
from datetime import datetime as dt
import dateutil.parser as datP

def match_observated_properties(definition_to_match):
    definition = ''
    description = ''
    if definition_to_match not in ['T', 'RH', 'P', 'timestamp', 'node_id']:
        definition = "Resistance" if "R" in definition_to_match else  "Voltage"#todo fix
        description = "Resistance" if "R" in definition_to_match else "Voltage"
    else:
        match definition_to_match:
            case 'T':
                definition = 'T'
                description = 'Temperature°C'
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
        service.create(observatedProperty)
        print(f"Inserted {observatedProperty=}")
    
    return observatedProperties


def create_observation(service, packet):
    observations = {}
    
    for i in range(len(packet)):
        packet['timestamp'] = "2016-06-22T13:21:31.144Z"
        observation = fsc.Observation(
            # id = i,
            result = 5,
            phenomenon_time = packet['timestamp'],
            # result_time = packet['timestamp'],
            # valid_time = packet['timestamp'],
            # result_quality = list(packet.values())[i],
            datastream= fsc.Datastream(id=1),
            parameters = {}
        )
        observations[observation.id] = observation
        service.create(observation)
        print(f"Inserted {observation=}")

    return observations

def create_location(service, packet):
    locations = {}
    
    for i in range(len(packet)): # numero di elementi da definire
        location = fsc.Location(
            id = i,
            name = "Location_name",
            description = "Location_description",
            properties = {},
            encoding_type = "?",
            location = "any",
        )
        locations[location.id] = location
        service.create(location)
        print(f"Inserted {location=}")
    
    return locations

def create_node(service):
    things = {}
    
    for i in range(1, 2): # numero di elementi da definire
        thing = fsc.Thing(
            id = i,
            name = "Node_Name",
            description = "Node_Description",
            properties = {} 
        )
        things[thing.id] = thing
        service.create(thing)
        print(f"Inserted {thing=}")

    return things

def create_sensor(service):
    sensors_map = {}
    
    for i in range(1,9):
        sensor = fsc.Sensor(
            id = i,
            name = "S" + str(i),
            description = "S"  + str(i) + "_Description",
            properties = {"node_id": "S"  + str(i) + "_Node"},
            encoding_type = 'application/json',
            metadata = "any",
        )
        sensors_map[sensor.id] = sensor
        service.create(sensor)
        print(f"Inserted {sensor=}")
    
    return sensors_map

def create_datastream(service, packet):




def convert_to_isoformat(dateInMilllis):
    convertToDayFormat = dt.fromtimestamp(dateInMilllis / 1000.0)
    return convertToDayFormat.isoformat() + 'Z'
