from utils import *
import json
FROST_SERVER = "http://localhost:8080/FROST-Server/v1.1"
service = fsc.SensorThingsService(FROST_SERVER)

#print the list of sensors
sensors_list = service.sensors().query().list()

#parse things_list to get a dictionary of sensors

sensors = {}
for thing in sensors_list:
    sensors[thing.name] = thing.id

sensors_nodes = {}
for thing in sensors_list:
    try:
        print("node id: "+str(thing.properties["node_id"])+" sensor id: " + str(thing.id))
    except KeyError:
        print("no year")




'''
print("List of sensors:")
print(t)
print("List of nodes:")
print(things_nodes)
'''