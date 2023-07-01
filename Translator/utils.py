import frost_sta_client as fsc
import pandas as pd
from datetime import datetime as datetime, timedelta
import dateutil.parser as datP
from dataclasses import dataclass, fields
from dateutil import parser




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


# def create_observation(service, packet):
#     observations = {}

#     for i in range(len(packet)):

#         observation = fsc.Observation(
#             # id = i,
#             result = 5,
#             phenomenon_time = packet['timestamp'],
#             result_time = packet['timestamp'],
#             valid_time = packet['timestamp'],
#             result_quality = list(packet.values())[i],
#             datastream= fsc.Datastream(id=i),
#             parameters = {}
#         )
#         observations[observation.id] = observation
#         service.create(observation)
#         print(f"Inserted {observation=}")

#     return observations

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
            metadata = "any"
        )

        
        sensors_map[sensor.id] = sensor
        service.create(sensor)
        print(f"Inserted {sensor=}")
    
    #create a datastream for each sensor
    
    return sensors_map


# def create_sensor_with_datastream(service, packet):

#     sensors= {}
#     for i in range(len(packet)):
#         sensor = fsc.Sensor(
#             id = i,
#             name = "S" + str(i),
#             description = "S"  + str(i) + "_Description",
#             properties = {"node_id": "S"  + str(i) + "_Node"},
#             encoding_type = 'application/json',
#             metadata = "any",
#         )
#         sensors[sensor.id] = sensor
#         service.create(sensor)
#         print(f"Inserted {sensor=}")

#         # datastream = fsc.Datastream(
#         #     id = i,
#         #     name = "Datastream_"+"S" + str(i),
#         #     description = "Datastream_for_"+"S" + str(i),
#         #     phenomenon_time= packet['timestamp'],
#         #     sensor_id = sensor.id,
#         #     thing_id = sensor.properties['node_id'],
#         #     observed_property_id = i,
#         #     observations = {}
#         # )
#         # service.create(datastream)
#         # print(f"Inserted {datastream=}")
        
#     return sensors

def create_observation(service, packet):
    
    dav = fsc.model.ext.data_array_value.DataArrayValue()
    unit_of_measurement = fsc.UnitOfMeasurement(
        name="degree Celsius",
        symbol="°C",
        definition="http://unitsofmeasure.org/ucum.html#para-30"
    )
    feature_of_interest = fsc.FeatureOfInterest(
        id=1,
        properties = {"prova": "prova"},
        name="Feature",
        description="Feature of Interest Description",
        encoding_type="application/vnd.geo+json",
        feature={"type": "Point", "coordinates": [42.1234, -71.5678]}
    )
    observed_property = fsc.ObservedProperty(
        name="Temperature",
        definition="http://www.opengis.net/def/property/OGC/0/Sensor/temperature",
        description="Temperature observed by sensor"
    )
    sensor = fsc.Sensor(
        id = 1,
        name = "Sensor_",
        description="Sensor_for_",
        properties = {"node_id": "S"},
        encoding_type="http://www.opengis.net/doc/IS/SensorML/2.0/GEOMETRY_XYZ",
        metadata = "any"
    )
    thing = fsc.Thing(
        id = 1,
        name = "Node_Name",
        description = "Node_Description",
        properties = {"location": "42.1234,-71.5678"} 
    )
    datastream = fsc.Datastream(
        id=1,
        name="Datastream_",
        description="Datastream_for_",
        unit_of_measurement=unit_of_measurement,
        phenomenon_time=packet['timestamp'],
        result_time=packet['timestamp'],
        #observations=[observation],
        observation_type="http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        observed_property=observed_property,
        sensor=sensor,
        thing=thing
    )
    observation = fsc.Observation(
        phenomenon_time=packet['timestamp'],
        result=packet['T'],
        feature_of_interest=feature_of_interest,
        datastream=datastream
    )
    
    service.create(feature_of_interest)
    service.create(observed_property)
    service.create(sensor)
    service.create(thing)
    service.create(datastream)
    service.create(observation)
    


def convert_to_isoformat(timestamp):
    start_time = datetime.utcfromtimestamp(timestamp / 1000)
    end_time = start_time + timedelta(seconds=60)

    interval = f"{start_time.isoformat()}Z/{end_time.isoformat()}Z"
    
    return interval

