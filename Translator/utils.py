import frost_sta_client as fsc
from geojson import Point
import pandas
import json
import requests
import math
from datetime import datetime as datetime, timedelta       

def check_already_extisting_location(service, node_id):
    node_to_search = node_id # node_id has to be the same as the one you want to serch
    locations = service.locations().query().filter("name eq " + "'" + node_to_search + "'").list()
    if len(locations.entities) == 0:
        return []
    else:
        return locations.entities[0]

def check_already_extisting_thing(service, node_id): #check if the thing in thing with id 'node_id' already exist
    node_to_search = node_id # node_id has to be the same as the one you want to serch
    things = service.things().query().filter("name eq " + "'" + node_to_search + "'").list()
    if len(things.entities) == 0:
        return []
    else:
        return things.entities[0]

def check_already_extisting_sensors(service, node_id): #check if the sensors in thing with id 'node_id' already exist
    l = []
    node_to_search = node_id # node_id has to be the same as the one you want to serch
    filtered_sensors = service.sensors().query().filter("properties/node_id eq " + "'" + node_to_search + "'").list()
    for i in range(len(filtered_sensors.entities)):
        l.append(filtered_sensors.entities[i])
    return l

def check_already_extisting_datastream(service, name): #check if the sensors in thing with id 'node_id' already exist
    l = []
    filtered_datastreams = service.datastreams().query().filter("name eq " + "'" + name + "'").list()
    for i in range(len(filtered_datastreams.entities)):
        l.append(filtered_datastreams.entities[i])
    return l

def check_already_extisting_multidatastream(service, name): #check if the sensors in thing with id 'node_id' already exist
    l = []
    filtered_multidatastreams = service.multi_datastreams().query().filter("name eq " + "'" + name + "'").list()
    for i in range(len(filtered_multidatastreams.entities)):
        l.append(filtered_multidatastreams.entities[i])
    return l

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
        unit_of_measurement = "Ω" if "resistance" in unit_of_measurement_to_match else  "V"
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
            name = "Resistance" if "resistance" in definition_to_match else  "Voltage"
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
            description = "Resistance" if "resistance" in definition_to_match else "Voltage"
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
   
def create_location(service, packet):
    loc = check_already_extisting_location(service=service, node_id=packet['node_id'])
    if loc == []:
        location = fsc.Location(
            name = packet['node_id'],
            description = packet['node_id'],
            properties = None,
            encoding_type = "application/vnd.geo+json",
            location = {"type":"Point","coordinates":[0, 0]}
        )
        service.create(location)
        print(f"Inserted {location=}")
        return location
    else:
        return loc

def create_node(service, packet):
    node = check_already_extisting_thing(service=service, node_id=packet['node_id'])
    location = create_location(service=service, packet=packet)
    if node == []:
        thing = fsc.Thing(
            name = packet['node_id'],
            description = packet['node_id'],
            properties = {}
        )
        service.create(thing)
        thing.locations = location
        print(f"Inserted {thing=}")
        sensors = create_sensor(service=service, packet=packet, thing=thing)
        return sensors, thing
    else:
        sensors = create_sensor(service=service, packet=packet, thing=node)
        return sensors, node
    

def create_sensor(service, packet, thing):
    sens = check_already_extisting_sensors(service=service, node_id=packet['node_id'])
    if sens is None or len(sens) == 0:
        sensors = []
        k = 0
        for i in range(0, 11):
            if i < 8:
                sensor = fsc.Sensor(
                    name = "S" + str(i + 1) + "_ID",
                    description = "R1, R2, V of S" + str(i + 1),
                    properties = {"node_id": packet['node_id'], "active": True},
                    encoding_type = "application/vnd.geo+json",
                    metadata = "any"
                )
            else:
                l = ['T', 'RH', 'P']
                sensor = fsc.Sensor(
                    name = l[(i - i) + k],
                    description = match_observated_properties(l[(i - i) + k], False),
                    properties = {"node_id": packet['node_id'], "active": True},
                    encoding_type = "application/vnd.geo+json",
                    metadata = "any"
                )
                k += 1
            service.create(sensor)
            print(f"{i=} Inserted {sensor=}")
            sensors.append(sensor)
        return sensors
    else:
        return sens

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

def create_datastream(service, packet):
    # unit of merurement has to be created next to datastream (same function!!!)
    j = 0
    sensors, thing = create_node(service=service, packet=packet)
    observed_properties = observed_property(service=service, packet=packet)
    data = check_already_extisting_datastream(service=service, name=packet['node_id'])
    datastreams = []
    if data == []:
        for i in range(len(packet) - 2):
            unit_of_measurement = fsc.UnitOfMeasurement(
                name = list(packet)[i],
                symbol = set_unit_of_measure(list(packet)[i]),
                definition = "http://unitsofmeasure.org/ucum.html#para-30"
            )
            if i < 24:
                datastream = fsc.Datastream(
                    name = packet['node_id'],
                    description = match_observated_properties(list(packet)[i], False),
                    unit_of_measurement = unit_of_measurement,
                    phenomenon_time = None,
                    result_time = None,
                    observation_type = "https://unitsofmeasure.org/ucum#para-30",
                    observed_property = observed_properties[i],
                    thing = thing,
                    sensor = sensors[j]
                )
            else:
                datastream = fsc.Datastream(
                    name = packet['node_id'],
                    description = match_observated_properties(list(packet)[i], False),
                    unit_of_measurement = unit_of_measurement,
                    phenomenon_time = None,
                    result_time = None,
                    observation_type = "https://unitsofmeasure.org/ucum#para-30",
                    observed_property = observed_properties[i],
                    thing = thing,
                    sensor = sensors[j]
                )
            if ((i + 1) % 3) == 0:
                j += 1
            service.create(datastream)
            print(f"{i=} Inserted {datastream=}")
            datastreams.append(datastream)
        return datastreams
    else:
        return data

def create_multidatastream(service, packet):
    datastreams = create_datastream(service=service, packet=packet)
    units_of_measurement = create_multidatastream_unit_of_measurement(datastreams=datastreams)
    multidatastreams = []
    multidata = check_already_extisting_multidatastream(service=service, name=packet['node_id'])
    j = 0
    if multidata == []:
        for i in range(len(datastreams)):
            multidatastream = fsc.MultiDatastream(
                name = packet['node_id'],
                description = packet['node_id'] + " Datastreams",
                properties = {},
                observed_properties = [datastreams[i].observed_property, datastreams[i].observed_property, datastreams[i].observed_property],
                observation_type = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_ComplexObservation",
                observed_area=None,
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
    else:
        return multidata

def create_observation(service, packet):
    observations = []
    features_of_interest = create_feature_of_interest(service=service, packet=packet)
    multidatastreams = create_multidatastream(service=service, packet=packet)
    j = 0
    k = 0
    for i in range(len(packet) - 2):
        if i < 24:
            if not(math.isnan(packet[list(packet)[i]])):
                observation = fsc.Observation(
                    phenomenon_time = packet['timestamp'],
                    result = [packet['S' + str(k + 1) + '_ID_heater_resistance'], packet['S' + str(k + 1) + '_ID_signal_resistance'], packet['S' + str(k + 1) + '_ID_voltage']],
                    feature_of_interest = features_of_interest[i],
                    multi_datastream = multidatastreams[i]
                )
                service.create(observation)
                print(f"{i=} Inserted {observation=}")
                observations.append(observation)
        else:
            if not(math.isnan(packet[list(packet)[i]])):
                observation = fsc.Observation(
                    phenomenon_time = packet['timestamp'],
                    result = [packet[list(packet)[i]], packet[list(packet)[i]], packet[list(packet)[i]]],
                    feature_of_interest = features_of_interest[i],
                    multi_datastream = multidatastreams[i]
                )
                service.create(observation)
                print(f"{i=} Inserted {observation=}")
                observations.append(observation)
        k += 1
        if k == 8:
            k = 0
            
        if ((i + 1) % 3) == 0:
            j += 1
        
    return observations
   
def convert_to_isoformat(timestamp):
    start_time = datetime.utcfromtimestamp(timestamp / 1000)
    end_time = start_time + timedelta(seconds=60)
    interval = f"{start_time.isoformat()}Z/{end_time.isoformat()}Z"
    return interval

def clean_packet(packet):
    packet_keys = ['S1_R1', 'S1_R2', 'S1_Voltage', 'S2_R1', 'S2_R2', 'S2_Voltage', 'S3_R1', 'S3_R2', 'S3_Voltage', 'S4_R1', 'S4_R2', 'S4_Voltage', 'S5_R1', 'S5_R2', 'S5_Voltage', 'S6_R1', 'S6_R2', 'S6_Voltage', 'S7_R1', 'S7_R2', 'S7_Voltage', 'S8_R1', 'S8_R2', 'S8_Voltage', 'T', 'RH', 'P', 'timestamp', 'node_id']
    for i in list(packet.keys()):
        if i not in packet_keys:
            del packet[i]
    return packet

def read_all_data(service):
    all_data_db = 'all_data.csv'
    db = pandas.read_csv(all_data_db)
    grouped = db.groupby(['node_description', 'ts'])
    for key, item in grouped:
        l = grouped.get_group(key).index[0]
        rows = []
        packet = {}
        k = 0
        while k < 8:
            rows.append(db.loc[l + k].to_dict())
            packet[str(rows[k]['sensor_description']) + '_heater_resistance'] = rows[k]['heater_res']
            packet[str(rows[k]['sensor_description']) + '_signal_resistance'] = rows[k]['signal_res']
            packet[str(rows[k]['sensor_description']) + '_voltage'] = rows[k]['volt']
            if k == 7:
                packet['T'] = rows[0]['t']
                packet['RH'] = rows[0]['rh']
                packet['P'] = rows[0]['p']
                packet['timestamp'] = rows[0]['ts']
                packet['node_id'] = rows[0]['node_description']
            k += 1
        create_observation(service=service, packet=packet)
        print("-------------------SEPARATORE-------------------")
