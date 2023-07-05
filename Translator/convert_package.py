from utils import *

FROST_SERVER = "http://localhost:8080/FROST-Server/v1.1"
service = fsc.SensorThingsService(FROST_SERVER)

""" by calling 'create_observation' it should create the following objects:
    29 feature_of_interest
    2 location
    2 thing 
    11 sensors (if not already exist)
    29 observed_property
    27 datastream
    27 multidatastream
    27 observation"""
    

#packet['timestamp'] = convert_to_isoformat(packet['timestamp'])
#packet = clean_packet(packet=packet)
#create_observation(service=service, packet=packet)
read_all_data(service=service)
print('ciao')