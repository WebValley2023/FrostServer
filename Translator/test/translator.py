from funcs import *


packet = {'S1_R1': 500, 'S1_R2': 128000000, 'S1_Voltage': 3.91,
 'S2_R1': 111.1, 'S2_R2': 790123, 'S2_Voltage': 3.64,
 'S3_R1': 135.3, 'S3_R2': 50000000, 'S3_Voltage': 2.39,
 'S4_R1': 147.7, 'S4_R2': 355555552, 'S4_Voltage': 2.57,
 'S5_R1': 128.3, 'S5_R2': 499000000, 'S5_Voltage': 3.89,
 'S6_R1': 128.9, 'S6_R2': 499000000, 'S6_Voltage': 4.02,
 'S7_R1': 103.4, 'S7_R2': 26876640, 'S7_Voltage': 3.95,
 'S8_R1': 108.6, 'S8_R2': 373333344, 'S8_Voltage': 4.33,
 'T': 33.5,'RH': 32.7, 'P': 988, 'timestamp': 1659106708162, 'node_id': 'appa1-debug'}

FROST_SERVER = "http://localhost:8080/FROST-Server/v1.1"
service = fsc.SensorThingsService(FROST_SERVER)

print(convert_to_isoformat(packet['timestamp']))
thing_id=insert_thing(service, packet['node_id'])
insert_sensor(service, packet, thing_id)
observed_property_id=insert_observed_property(service, packet)
insert_datastream(service, packet,thing_id)





    
