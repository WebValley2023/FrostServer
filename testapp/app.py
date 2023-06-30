import json
import logging
from datetime import datetime

from flask import Flask, request
# from psycopgwrapper import PG
from waitress import serve

app = Flask(__name__)


@app.route('/sensordata', methods=['GET', 'POST'])
def add_sensor_data():
    # pg = PG.get_default_postgres()

    # Get request for debugging
    if request.method == 'GET':
        logging.warning("Debug: GET request")
        packet = {'S1_R1': 500, 'S1_R2': 128000000, 'S1_Voltage': 3.91,
        'S2_R1': 111.1, 'S2_R2': 790123, 'S2_Voltage': 3.64,
        'S3_R1': 135.3, 'S3_R2': 50000000, 'S3_Voltage': 2.39,
        'S4_R1': 147.7, 'S4_R2': 355555552, 'S4_Voltage': 2.57,
        'S5_R1': 128.3, 'S5_R2': 499000000, 'S5_Voltage': 3.89,
        'S6_R1': 128.9, 'S6_R2': 499000000, 'S6_Voltage': 4.02,
        'S7_R1': 103.4, 'S7_R2': 26876640, 'S7_Voltage': 3.95,
        'S8_R1': 108.6, 'S8_R2': 373333344, 'S8_Voltage': 4.33,
        'CFG': 0, 'T': 33.5, 'TH': 35.5, 'H': 33.9, 'RH': 32.7,
        'P': 988, 'G': 3172.33, 'IAQ': 56.4, 'CO2': 631.96, 'VOC': 0.82, 'IAC_comp': 3,
        'timestamp': 1659106708162, 'node_id': 'appa1-debug'}
    else:
        packet = request.json

    # TODO: add attrs that do not "key.startswith("S") and key.endswith(("_R1", "_R2", "_Voltage"))"
    attrs_dict = {
        k: v
        for k, v in packet.items()
        if not k.startswith("S")
        and not k.endswith(("_R1", "_R2", "_Voltage"))
        and k not in ["timestamp", "node_id"]
    }    
    
    logging.warning("got packet!")

    return packet



if __name__ == '__main__':
    serve(app, host="0.0.0.0", port=5000) 