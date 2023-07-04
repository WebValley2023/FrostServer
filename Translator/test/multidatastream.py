import frost_sta_client as fsc
from geojson import Point
import pandas as pd
import json
import requests
from datetime import datetime as datetime, timedelta

def create_multidatastream_unit_of_measurement(datastreams):
    units_of_measurement = []
    for i in range(2, len(datastreams), 3):
        l = []
        l.append(datastreams[i - 2].unit_of_measurement)
        l.append(datastreams[i - 1].unit_of_measurement)
        l.append(datastreams[i].unit_of_measurement)
        units_of_measurement.append(l)
    return units_of_measurement
        

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
    points = [Point((11.11022, 11.1262)), Point((46.10433, 46.06292))]
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

def create_node(service, packet):
    things = []
    locations = create_location(service=service)
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

def check_already_extisting_sensors(node_id): #check if the sensors in thing with id 'node_id' already exist
    filtered_sensors = []
    node_to_search = node_id

    to_encode = "http://localhost:8080/FROST-Server/v1.1/Sensors" 
    #to_encode = to_encode.replace(" ","+")

    sensors_list = json.loads(requests.get(to_encode).text)

    #sensors_count = sensors_list["@iot.count"]
    sensors_list = sensors_list["value"]

    #filtering sensors by node
    for sensor in sensors_list:
        try:
            if sensor["properties"]["node_id"] == node_to_search:
                filtered_sensors.append(sensor['name'])   
        except:
            pass
    return filtered_sensors

def create_sensor(service, packet, thing):
    already_extisting_sensors = check_already_extisting_sensors(thing.id)
    sensors = []
    
    for i in range(0, 11):
        if i < 8:
            sensor = fsc.Sensor(
                name = "S" + str(i + 1),
                description = "R1, R2, V of S" + str(i + 1),
                properties = {"node_id": thing.id, "active": True},
                encoding_type = "application/vnd.geo+json",
                metadata = "any"
            )
        else:
            sensor = fsc.Sensor(
                name = list(packet)[i + 16],
                description = match_observated_properties(list(packet)[i + 16], False),
                properties = {"node_id": thing.id, "active": True},
                encoding_type = "application/vnd.geo+json",
                metadata = "any"
            )
        if sensor.name not in already_extisting_sensors:
            service.create(sensor)
            print(f"{i=} Inserted {sensor=}")
            sensors.append(sensor)
        else:
            print("Sensor " + sensor.name + " Skipped")
        
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

def download_sensors(node_id): # download the sensors of a certain node_id
    downloaded_sensors = []
    node_to_search = node_id
    to_encode = "http://localhost:8080/FROST-Server/v1.1/Sensors"
    sensors_list = json.loads(requests.get(to_encode).text)
    sensors_list = sensors_list["value"]
    for sensor in sensors_list:
        try:
            if sensor["properties"]["node_id"] == node_to_search:
                downloaded_sensors.append(sensor)   
        except:
            pass
    return downloaded_sensors

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

def create_datastream(service, packet):
    # unit of merurement has to be created next to datastream (same function!!!)
    j = 0
    datastreams = []
    sensors, things = create_node(service=service, packet=packet)
    observed_properties = observed_property(service=service, packet=packet)
    if len(sensors) == 0:
        sensors = download_sensors(things[0].id)

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
                sensor = sensors[j]
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
                sensor = sensors[j]
            )
        if ((i + 1) % 3) == 0:
           j += 1
        service.create(datastream)
        print(f"{i=} Inserted {datastream=}")
        datastreams.append(datastream)
    return datastreams

def create_multidatastream(service, packet):
    datastreams = create_datastream(service=service, packet=packet)
    units_of_measurement = create_multidatastream_unit_of_measurement(datastreams=datastreams)
    multidatastreams = []
    j = 0
    for i in range(len(datastreams)):

        
        multidatastream = fsc.MultiDatastream(
            name = "S" + str(j) + " Datastreams",
            description = "S" + str(j) + " Datastreams",
            properties={},
            ##TODO fix this
            observed_properties =[datastreams[i].observed_property, datastreams[i].observed_property, datastreams[i].observed_property],
            observation_type = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_ComplexObservation",
            observed_area=None,
            ##TODO fix this
            unit_of_measurements = units_of_measurement[j],
            phenomenon_time = None,
            multi_observation_data_types = ["http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement","http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement","http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"],    
            result_time = None,
            thing = datastreams[i].thing,
            sensor = datastreams[i].sensor,

        )
        if ((i + 1) % 3) == 0:
            j += 1
        service.create(multidatastream)
        print(f"{i=} Inserted {multidatastream=}")
        multidatastreams.append(multidatastream)
    return multidatastreams

def create_observation(service, packet):
    observations = []
    features_of_interest = create_feature_of_interest(service=service, packet=packet)
    multidatastreams = create_multidatastream(service=service, packet=packet)
    for i in range(len(packet) - 2):
        observation = fsc.Observation(
            phenomenon_time = packet['timestamp'],
            result =[packet[list(packet)[i]],1,2],
            feature_of_interest = features_of_interest[i],
            multi_datastream = multidatastreams[i]
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






