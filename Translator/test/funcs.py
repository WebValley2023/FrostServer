import frost_sta_client as fsc
from datetime import datetime as dt, timedelta

def convert_to_isoformat(timestamp):
    start_time = dt.utcfromtimestamp(timestamp / 1000)
    end_time = start_time + timedelta(seconds=60)

    interval = f"{start_time.isoformat()}Z/{end_time.isoformat()}Z"
    
    return interval

def insert_thing(service, name):
    thing = fsc.Thing(
        name=name,
        description="Appa " + name,
        properties={}
    )
    service.create(thing)
    return thing.id

def insert_sensor(service, packet, thing_id):
    for i in range(1, 25):
        sensor = fsc.Sensor(
            name="S" + str(i),
            description="Appa " + packet['node_id'] + " Sensor " + str(i),
            encoding_type="application/json",
            metadata="http://www.appa.com",
            properties={"node_id": packet['node_id']}
        )
        service.create(sensor)
        sensor.thing_id = thing_id
        service.update(sensor)

def insert_observed_property(service, packet):
    for i in range(1, 25):
        observed_property = fsc.ObservedProperty(
            name="S" + str(i),
            description="Appa " + packet['node_id'] + " Sensor " + str(i),
            definition="http://www.appa.com",
            properties={"node_id": packet['node_id']}
        )
        service.create(observed_property)

def insert_datastream(service, packet, thing_id):

    

    unitOfMeasurement = fsc.UnitOfMeasurement(
        name="Degree Celsius",
        symbol="degC",
        definition="http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius"
    )


    #convert to a printable list
    
    sensors = service.sensors().query().list()
    #convert to a printable list

    

    sensors_list = []
    for sensor in sensors:
        sensors_list.append(sensor.name)

    

   





    for i in range(1, 25):
        
        
        datastream = fsc.Datastream(
            name="datastream_" + sensors_list[i][0],
            description="Appa " + sensors_list[i][0],
            observation_type="http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            observed_property=  fsc.ObservedProperty(
            name="S" + str(i),
            description="Appa " + packet['node_id'] + " Sensor " + str(i),
            definition="http://www.appa.com",
            properties={"node_id": packet['node_id']}
        ),
        
        phenomenon_time=convert_to_isoformat(packet['timestamp']),
        unit_of_measurement=unitOfMeasurement,
        properties={"node_id": packet['node_id'], "sensor": sensors_list[i][0]},
        )
        service.create(datastream)
        datastream.add_thing_id(thing_id)
        datastream.add_sensor_name(sensor_name)
        datastream.add_observed_property_name(observed_property_name)
        service.update(datastream)

