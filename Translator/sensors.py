from dataclasses import dataclass, fields
from typing import Iterable
import json
import frost_sta_client as fsc
import pandas as pd
import numpy as np

DATA_FOLDER = ""
FROST_SERVER = "http://localhost:8080/FROST-Server/v1.1"


@dataclass
class Node:
    id: str
    node_name: str
    description: str


@dataclass
class Sensor:
    id: str
    node_id: str
    name: str
    description: str
    active : bool
    attrs: dict


@dataclass
class Packet:
    id: str
    node_id: str
    insert_ts: str
    sensor_ts: str


@dataclass
class PacketData:
    id: str
    packet_id: str
    sensor_id: str
    r1: str
    r2: str
    volt: str


@dataclass(frozen=True)
class ObservedProperty:
    name: str
    definition: str
    description: str
    feature_of_interest: str  # what is it observing??


OBSERVED_PROPS = {
    "r1": ObservedProperty(name="r1", definition="r1", description="r1", feature_of_interest="air"),
    "r2": ObservedProperty(name="r2", definition="r2", description="r2", feature_of_interest="air"),
    "volt": ObservedProperty(name="volt", definition="volt", description="volt", feature_of_interest="voltage"),
}

UNIT_OF_MEASUREMENT = fsc.UnitOfMeasurement(
    name="nome unita misura", symbol="simbolo", definition="definition"
)


def csv_to_dataclass(csv_file, output_class):
    columns = [field.name for field in fields(output_class)]
    dataframe = pd.read_csv(DATA_FOLDER + csv_file, usecols=columns)

    for row in dataframe.itertuples():
        yield output_class(**{column: getattr(row, column) for column in columns})


def load_nodes(csv_file="node_db.csv"):
    yield from csv_to_dataclass(csv_file=csv_file, output_class=Node)


def load_sensors(csv_file="sensor_db.csv"):
    yield from csv_to_dataclass(csv_file=csv_file, output_class=Sensor)


def load_packets(csv_file="packet_db.csv", packet_ids=None):
    """
    Only load specific packets
    """
    output_class = Packet
    columns = [field.name for field in fields(output_class)]
    dataframe = pd.read_csv(DATA_FOLDER + csv_file, usecols=columns, index_col="id", parse_dates=["insert_ts", "sensor_ts"])

    # remove 'empty' packets (i.e.: they have no packetData associated with them)
    return dataframe[dataframe.index.isin(packet_ids)]


def load_packet_data(csv_file="packet_data_db.csv"):
    output_class = PacketData
    columns = [field.name for field in fields(output_class)]
    # TODO: non caricare in memoria tutto il file maybe?
    dataframe = pd.read_csv(DATA_FOLDER + csv_file, usecols=columns)

    return dataframe


def upsert_sensors(service, sensors: Iterable[Sensor]):
    already_inserted_sensors = service.sensors().query().list()

    inserted_sensors = {sensor.name: sensor for sensor in already_inserted_sensors}

    print("Upserting sensors...")

    # "old_id" => sensor obj
    sensors_map = {}

    for sensor in sensors:
        sensor.active = str(sensor.active)
        sensor.attrs = json.loads(sensor.attrs)
        if sensor.name not in inserted_sensors:
            entity = fsc.Sensor(
                name=sensor.name,
                description=sensor.description,
                properties={
                    "node_id": sensor.node_id,
                    "active": True if sensor.active == 'nan' else False,
                    "install_ts": sensor.attrs['active since'] if 'active since' in sensor.attrs else '',
                    "removed_ts": "",
                    "synthesis": "",
                    "layout": "",
                    "n_wafer": "",
                    "year": "",
                    
                    },
                encoding_type='application/json',
                metadata="",
            )
            sensors_map[sensor.id] = entity
            service.create(entity)

            print(f"Inserted {sensor=}")
        else:
            sensors_map[sensor.id] = inserted_sensors[sensor.name]
            print(f"Skipped {sensor=}")

    return sensors_map


def fetch_all_nodes(service):
    nodes = service.things().query().list()
    return {node.name: node for node in nodes}


def upsert_nodes(service, nodes: Iterable[Node]):
    inserted_nodes = fetch_all_nodes(service)

    print("Upserting things/nodes")

    things = {}

    for node in nodes:
        if node.node_name not in inserted_nodes:
            thing = fsc.Thing(
                name=node.node_name,
                description=node.description
            )
            service.create(thing)
            print(f"Inserted {node=}")
            things[node.id] = thing
        else:
            print(f"Skipped {node=}")
            things[node.id] = inserted_nodes[node.node_name]

    return things


def upsert_features_of_interest(service: fsc.SensorThingsService):
    names = ("air", "voltage")

    inserted_features = {feature.name: feature for feature in service.features_of_interest().query().list()}
    features = {}

    for name in names:

        if feature := inserted_features.get(name):
            print(f"Feature {name} was already created")
            features[name] = feature
        else:
            entity = fsc.FeatureOfInterest(name=name, description=name, encoding_type="application/json", feature="any")
            print(f"Created Feature {name}")
            service.create(entity)
            features[name] = entity

    return features


def upsert_observed_properties(service: fsc.SensorThingsService):
    inserted_properties = {
        obs_prop.name: obs_prop for obs_prop in service.observed_properties().query().list()
    }

    props = {}

    for prop_name in OBSERVED_PROPS:
        if prop := inserted_properties.get(prop_name):
            props[prop_name] = prop
        else:
            entity = fsc.ObservedProperty(
                name=prop_name, definition=prop_name, description=prop_name
            )
            props[prop_name] = entity

            service.create(entity)

    return props


def upsert_datastreams(service: fsc.SensorThingsService):
    """
    Makes sure there is a datastream for each combination of sensor and observed property
    """
    ...


if __name__ == "__main__":
    service = fsc.SensorThingsService(FROST_SERVER)

    nodes = load_nodes()
    things: dict[str, fsc.Thing] = upsert_nodes(service, nodes)

    csv_sensors = load_sensors()
    #print(csv_sensors)
    sensors: dict[str, fsc.Sensor] = upsert_sensors(service, csv_sensors)
"""
    packet_data: pd.DataFrame = load_packet_data()
    packet_ids = set(packet_data.packet_id.unique())

    packets = load_packets(packet_ids=packet_ids)

    foi = upsert_features_of_interest(service)

    # todo
    upsert_datastreams(service)

    # todo: fetch datastreams from frost to make sure they are not recreated maybe?
    # (old_sensor_id, observed_property_name) => datastream
    datastreams = {}

    observed_properties = upsert_observed_properties(service)

    for packet_data in packet_data.itertuples():
        packet_id = packet_data.packet_id
        packet = packets.loc[packet_id]

        sensor_id = packet_data.sensor_id
        sensor = sensors[sensor_id]

        for prop_name, prop in observed_properties.items():
            if datastream := datastreams.get((sensor_id, prop_name)):
                print(f"Reusing datastream for observation of {sensor_id=} and {prop_name=}")
            else:
                print(f"Creating datastream for observation of {sensor_id=} and {prop_name=}")
                datastream = fsc.Datastream(
                    name="boh",
                    description="boh",
                    observation_type="boh",
                    unit_of_measurement=UNIT_OF_MEASUREMENT,  # todo: maybe each observed prop should have its own uoi?
                    observed_property=prop,
                    sensor=sensor,
                    thing=things[sensor.properties['node_id']]
                )

                service.create(datastream)
                datastreams[(sensor_id, prop_name)] = datastream

            observation = fsc.Observation(
                phenomenon_time=packet.sensor_ts,
                result=getattr(packet_data, prop_name),
                datastream=datastream,
                feature_of_interest=foi[OBSERVED_PROPS[prop_name].feature_of_interest]
            )

            service.create(observation)"""
