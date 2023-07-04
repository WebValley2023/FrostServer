import frost_sta_client as fsc
from geojson import Point
import pandas as pd
from datetime import datetime as datetime, timedelta

def set_unit_of_measure(unit_of_measurement_to_match):
    unit_of_measurement = ""
    if unit_of_measurement_to_match not in ['T', 'RH', 'P']:
        unit_of_measurement = "Ω" if "R" in unit_of_measurement_to_match else  "V"
    else:
        match unit_of_measurement_to_match:
                case 'T':
                    unit_of_measurement = '°C'
                case 'RH':
                    unit_of_measurement = '%'
                case 'P':
                    unit_of_measurement = 'mmHg'
    return unit_of_measurement
    

def match_observated_properties(definition_to_match, name_or_definition):
    if name_or_definition:
        name = ''
        if definition_to_match not in ['T', 'RH', 'P', 'timestamp', 'node_id']:
            name = "Resistance" if "R" in definition_to_match else  "Voltage"
        else:
            match definition_to_match:
                case 'T':
                    name = 'T'
                case 'RH':
                    name = 'RH'
                case 'P':
                    name = 'P'
                case 'timestamp':
                    name = 'timestamp'
                case 'node_id':
                    name = 'node_id'
        return name
    else:
        description = ''
        if definition_to_match not in ['T', 'RH', 'P', 'timestamp', 'node_id']:
            description = "Resistance" if "R" in definition_to_match else "Voltage"
        else:
            match definition_to_match:
                case 'T':
                    description = 'Temperature °C'
                case 'RH':
                    description = 'Humidity'
                case 'P':
                    description = 'Pressure'
                case 'timestamp':
                    description = 'Time_stamp'
                case 'node_id':
                    description = 'Node_id'
        return description
    
def create_location(service):
    coordinates = [{"type":"Point","coordinates":[11.11022, 11.1262]}, {"type":"Point","coordinates":[46.10433, 46.06292]}]
    locations = []
    for i in range(0,2):
        if i < 1:
            location = fsc.Location(
                name = "Via Bolzano", 
                description = "Via Bolzano",
                properties = None,
                encoding_type = "application/vnd.geo+json",
                location = coordinates[i],
            )
        else:
            location = fsc.Location(
                name = "S. Chiara", 
                description = "S. Chiara Park",
                properties = None,
                encoding_type = "application/vnd.geo+json",
                location = coordinates[i],
            )
        service.create(location)
        print(f"{i=} Inserted {location=}")
        locations.append(location)
    return locations

def create_node(service, packet): ##create thing
    things = []
    locations = create_location(service=service, packet=packet)
    for i in range(0,2):
        thing = fsc.Thing(
            name = "Stazione " + locations[i].name,
            description = "Node_Description",
            properties = {}
        )
        service.create(thing)
        thing.locations = [locations[i]]
        print(f"{i=} Inserted {thing=}")
        things.append(thing)
    sensors = create_sensor(service=service, packet=packet, thing=thing)
    return sensors, things

def create_sensor(service, packet, thing):##TODO understand if it is right
    sensors = []
    for i in range(0, 11):
        sensor = fsc.Sensor(
            name = list(packet)[i],
            description = match_observated_properties(list(packet)[i], False),
            properties = {"node_id": thing.id, "active": True},
            encoding_type = "application/vnd.geo+json",
            metadata = "any"
        )
        service.create(sensor)
        print(f"{i=} Inserted {sensor=}")
        sensors.append(sensor)
    
    return sensors



def create_feature_of_interest(service, packet):
    features_of_interest = []
    for i in range(len(packet)):
        feature_of_interest = fsc.FeatureOfInterest(
            properties = {"prova": "prova"},
            name = list(packet)[i],
            description = match_observated_properties(list(packet)[i], False),
            encoding_type = "application/vnd.geo+json",
            feature = {"type": "Point", "coordinates": ["prova", "prova"]}
        )
        service.create(feature_of_interest)
        print(f"{i=} Inserted {feature_of_interest=}")
        features_of_interest.append(feature_of_interest)
    
    return features_of_interest

def observed_property(service, packet): 
    observed_properties = []
    for i in range(len(packet)):
        observed_property = fsc.ObservedProperty(
            name = list(packet)[i],
            definition = "https://unitsofmeasure.org/ucum#para-30",
            description = match_observated_properties(list(packet)[i], False)
        )
        service.create(observed_property)
        print(f"{i=} Inserted {observed_property=}")
        observed_properties.append(observed_property)
    return observed_properties

def create_datastream(service, packet): ##TODO understand if it is right
    # unit of merurement has to be created next to datastream (same function!!!)
    datastreams = []
    sensors, things = create_node(service=service, packet=packet)
    observed_properties = observed_property(service=service, packet=packet)
    for i in range(len(packet) - 2):
        unit_of_measurement = fsc.UnitOfMeasurement(
            name = list(packet)[i],
            symbol = set_unit_of_measure(list(packet)[i]),
            definition = "http://unitsofmeasure.org/ucum.html#para-30"
        )
        if i < 24:
            datastream = fsc.Datastream(
                name = list(packet)[i],
                description = match_observated_properties(list(packet)[i], False),
                unit_of_measurement = unit_of_measurement,
                phenomenon_time = None,
                result_time = None,
                observation_type = "https://unitsofmeasure.org/ucum#para-30",
                observed_property = observed_properties[i],
                thing = things[0],
                sensor = sensors[i]
            )
        else:
            datastream = fsc.Datastream(
                name = list(packet)[i],
                description = match_observated_properties(list(packet)[i], False),
                unit_of_measurement = unit_of_measurement,
                phenomenon_time = None,
                result_time = None,
                observation_type = "https://unitsofmeasure.org/ucum#para-30",
                observed_property = observed_properties[i],
                thing = things[0],
                sensor = sensors[i]
            )
        service.create(datastream)
        print(f"{i=} Inserted {datastream=}")
        datastreams.append(datastream)
    return datastreams

def create_observation(service, packet):
    observations = []
    features_of_interest = create_feature_of_interest(service=service, packet=packet)
    datastreams = create_datastream(service=service, packet=packet)
    for i in range(len(packet) - 2):
        observation = fsc.Observation(
            phenomenon_time = packet['timestamp'],
            result = packet[list(packet)[i]],
            feature_of_interest = features_of_interest[i],
            datastream = datastreams[i]
        )
        service.create(observation)
        print(f"{i=} Inserted {observation=}")
        observations.append(observation)
    return observations
    
def convert_to_isoformat(timestamp):
    start_time = datetime.utcfromtimestamp(timestamp / 1000)
    end_time = start_time + timedelta(seconds=60)

    interval = f"{start_time.isoformat()}Z/{end_time.isoformat()}Z"
    return interval