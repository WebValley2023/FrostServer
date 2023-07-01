import frost_sta_client as fsc
import pandas as pd
from datetime import datetime as datetime, timedelta
import dateutil.parser as datP
from dataclasses import dataclass, fields


# def match_observated_properties and def create_location still TBD

# def match_observated_properties(definition_to_match):
#     definition = ''
#     description = ''
#     if definition_to_match not in ['T', 'RH', 'P', 'timestamp', 'node_id']:
#         definition = "Resistance" if "R" in definition_to_match else  "Voltage"#todo fix
#         description = "Resistance" if "R" in definition_to_match else "Voltage"
#     else:
#         match definition_to_match:
#             case 'T':
#                 definition = 'T'
#                 description = 'Temperature°C'
#             case 'RH':
#                 definition = 'RH'
#                 description = 'Humidity'
#             case 'P':
#                 definition = 'P'
#                 description = 'Pressure'
#             case 'timestamp':
#                 definition = 'timestamp'
#                 description = 'Time_stamp'
#             case 'node_id':
#                 definition = 'node_id'
#                 description = 'Node_id'
                
#     return definition, description


# def create_location(service, packet):
#     locations = {}
    
#     for i in range(len(packet)): # numero di elementi da definire
#         location = fsc.Location(
#             id = i,
#             name = "Location_name",
#             description = "Location_description",
#             properties = {},
#             encoding_type = "?",
#             location = "any",
#         )
#         locations[location.id] = location
#         service.create(location)
#         print(f"Inserted {location=}")
    
#     return locations

def create_node(service):
    thing = fsc.Thing(
        id = 1,
        name = "Node_Name",
        description = "Node_Description",
        properties = {"location": "42.1234,-71.5678"} 
    )
    service.create(thing)
    return thing

def create_sensor(service):
    sensor = fsc.Sensor(
        id = 1,
        name = "Sensor_",
        description="Sensor_for_",
        properties = {"node_id": "appa1_debug", "active": True},
        encoding_type="http://www.opengis.net/doc/IS/SensorML/2.0/GEOMETRY_XYZ",
        metadata = "any"
    )
    service.create(sensor)
    return sensor

def create_feature_of_interest(service):
    feature_of_interest = fsc.FeatureOfInterest(
            id=1,
            properties = {"prova": "prova"},
            name = "Feature",
            description = "Feature of Interest Description",
            encoding_type = "application/vnd.geo+json",
            feature = {"type": "Point", "coordinates": [42.1234, -71.5678]}
    )
    service.create(feature_of_interest)
    return feature_of_interest

def observed_property(service):
    observed_property = fsc.ObservedProperty(
        name = "Temperature",
        definition = "http://www.opengis.net/def/property/OGC/0/Sensor/temperature",
        description = "Temperature observed by sensor"
    )
    service.create(observed_property)
    return observed_property

def create_datastream(service, packet):
    # unit of merurement has to be created next to datastream (same function!!!)
    unit_of_measurement = fsc.UnitOfMeasurement(
        name="degree Celsius",
        symbol="°C",
        definition="http://unitsofmeasure.org/ucum.html#para-30"
    )
    datastream = fsc.Datastream(
        id = 1,
        name = "Datastream_",
        description = "Datastream_for_",
        unit_of_measurement = unit_of_measurement,
        phenomenon_time = packet['timestamp'],
        result_time = packet['timestamp'],
        observation_type = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        observed_property = observed_property(service=service),
        sensor = create_sensor(service=service),
        thing = create_node(service=service)
    )
    service.create(datastream)
    return datastream

def create_observation(service, packet):
    
    feature_of_interest = create_feature_of_interest(service=service)
    datastream = create_datastream(service=service, packet=packet)
    observation = fsc.Observation(
        phenomenon_time = packet['timestamp'],
        result = packet['T'],
        feature_of_interest = feature_of_interest,
        datastream = datastream
    )

    service.create(observation)
    
def convert_to_isoformat(timestamp):
    start_time = datetime.utcfromtimestamp(timestamp / 1000)
    end_time = start_time + timedelta(seconds=60)

    interval = f"{start_time.isoformat()}Z/{end_time.isoformat()}Z"
    
    return interval