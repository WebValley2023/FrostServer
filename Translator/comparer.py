from utils import *
import requests
import json
from urllib.parse import quote
import gc
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
 'T': 33.5,'RH': 32.7, 'P': 988, 'timestamp': 1659106708162, 'node_id': 250 }

url_sensors = "http://localhost:8080/FROST-Server/v1.1/Sensors"
sensor_name = "S1"
node_tosearch = packet["node_id"]


#search for the datastream of the sensor

#getting the possible sensors list

#filter the sensors that in the properties field have the node_id equal to the node_id of the packet and given the sensor name

#make a request to the server and list all sensors










filtered_sensors = []


##filter sensors by name
name="" ##TODO insert name
to_encode = "http://localhost:8080/FROST-Server/v1.1/Sensors?$filter=name+eq+\'"+name+"\'" 


to_encode = to_encode.replace(" ","+")

sensors_list = json.loads(requests.get(to_encode).text)

sensors_count = sensors_list["@iot.count"]
sensors_list = sensors_list["value"]


#filtering sensors by node
for sensor in sensors_list:
    try:
        
        if sensor["properties"]["node_id"] == node_tosearch:
            filtered_sensors.append(sensor["@iot.id"])    
            
                     
    except:
        pass

print(len(filtered_sensors))
# now in filtered_sensors there are all the ids of 

#now getting datastreams

link_datastreams = "http://localhost:8080/FROST-Server/v1.1/Datastreams"


list_datastreams = json.loads(requests.get(link_datastreams).text)
datastreams_count= list_datastreams["@iot.count"]
datastreams_list= list_datastreams["value"]


found_sensor = None

for datastream in datastreams_list:
    datastream_sensor = json.loads(requests.get(datastream["Sensor@iot.navigationLink"]).text)
    datastream_node = json.loads(requests.get(datastream["Thing@iot.navigationLink"]).text)

    if datastream_sensor["@iot.id"] in filtered_sensors:
        print("sensore corrispondente")
        found_sensor = datastream_sensor



        
    

    



    
















'''

tosearch = "Sensor_"


sensors_filtered_list = []
for sensor in sensors_list:


    try:
        if sensor.properties['node_id'] == packet['node_id'] and sensor.name == tosearch:
            sensors_filtered_list.append(sensor)
            m= sensor.id
            

    except:
        pass

# search the datastream 

datastream = service.datastreams().find(90)
print(datastream._id)
print(datastream.id)
print(datastream.properties)
print(datastream.thing)

'''







#print("dovevo cercare il datastream del sensore chiamato "+ tosearch + "sulla linea "+ packet['node_id']+", il suo datastream é quello con identificativo "+ datastream.id)










