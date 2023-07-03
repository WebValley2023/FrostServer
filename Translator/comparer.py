from utils import *
import json
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
 'T': 33.5,'RH': 32.7, 'P': 988, 'timestamp': 1659106708162, 'node_id': 'appa1_debug'}



#search for the datastream of the sensor

#getting the possible sensors list

#filter the sensors that in the properties field have the node_id equal to the node_id of the packet

sensors_list = service.sensors().query().list()



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

datastreams = service.datastreams().query().list()

for datastream in datastreams:
    print(datastream.sensor)
    '''
    try:
        if datastream.sensor.id == 1224:
            print("iddd"+datastream.id)
    except:
        pass
    '''
    



#print("dovevo cercare il datastream del sensore chiamato "+ tosearch + "sulla linea "+ packet['node_id']+", il suo datastream é quello con identificativo "+ datastream.id)










