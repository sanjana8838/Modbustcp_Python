from pymodbus.client.sync import ModbusTcpClient
import requests
import time
import datetime
import logging
import csv
from os.path import exists as file_exists
import json


#backend url
#url = 'http://192.168.1.36:4000/data'

#json payload
ploads = {
    "timestamp":0,
    "dc_main_contactor":0,
    "stk1_contactor":0,
    "primary_positive_pump":0,
    "primary_negative_pump":0,
    "system_mode":0,
    "system_alarm_status":0,
    "balancing_valve":0,
    "positive_valve":0,
    "negative_valve":0,
    "state_of_charge":0,
    "bcu_mode_status":0,
    "bcu_voltage":0,
    "bcu_current":0,
    "bcu_power":0,
    "bcu_state_of_charge":0,
    "bcu_hydrogen_sensor":0,
    "bcu_leakage_sensor":0,
    "smoke_sensor":0,
    "bcu_ocv":0,
    "bcu_positive_tank_temp":0,
    "bcu_negative_tank_temp":0,
    "positive_tank_high_level_float":0,
    "negative_tank_high_level_float":0,
    "positive_tank_low_level_float":0,
    "negative_tank_low_level_float":0,
    "primary_stack_voltage":0,
    "primary_stack_current":0,
    "primary_stack_positive_pressure_sensor":0,
    "primary_stack_negative_pressure_sensor":0,
    "positive_stack_pressure_delta":0,
    "b1_primary_stack_pressure_delta":0,
    "sensor_temp":0,
    "humidity":0,
    "bms_timestamp":0,
    "plc_alarm_status_1":0
}

#root logging format and level
FORMAT = ( '%(asctime)s %(name)s - %(levelname)s - %(message)s' )
logging.basicConfig(filename='app.log', filemode='w', format=FORMAT, level=logging.DEBUG)

#non root logging format and level
def setup_logger(logger_name, log_file, level=logging.INFO, format='%(message)s'):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter(format)
    fileHandler = logging.FileHandler(log_file, mode='w')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)  

#modbus connection
def ModbusConnect(ip, port=502):
    global client 
    logger1.debug('Connecting to Modbus...')
    client = ModbusTcpClient(ip, port=port)
    connect = client.connect()
    logging.debug(connect)
    logger1.debug('Modbus Connection Status: %s', connect)

#reading registers
def ReadRegister():
    logging.debug('Reading Registers')
    logger1.debug('Reading Registers')
    #ploads['timestamp'] = str(datetime.datetime.now())
    ploads['timestamp'] = int(time.time())
    ploads["dc_main_contactor"] = float((client.read_holding_registers(0, 2, unit=2)).registers[1])
    ploads["stk1_contactor"] = float(client.read_holding_registers(2, 2, unit=2).registers[1])    
    ploads["primary_positive_pump"] = float(client.read_holding_registers(4, 2, unit=2).registers[1]) 
    ploads["primary_negative_pump"]= float(client.read_holding_registers(6, 2, unit=2).registers[1]) 
    ploads["balancing_valve"] = float(client.read_holding_registers(12, 2, unit=2).registers[1]) 
    ploads["positive_valve"] = float(client.read_holding_registers(14, 2, unit=2).registers[1]) 
    ploads["negative_valve"] = float(client.read_holding_registers(16, 2, unit=2).registers[1]) 
    ploads["state_of_charge"] = float(client.read_holding_registers(18, 2, unit=2).registers[1]) 
    ploads["bcu_mode_status"] = float(client.read_holding_registers(20, 2, unit=2).registers[1]) 
    ploads["bcu_voltage"] = float(client.read_holding_registers(22, 2, unit=2).registers[1])
    ploads["bcu_current"] = float(client.read_holding_registers(24, 2, unit=2).registers[1]) 
    ploads["bcu_power"] = float(client.read_holding_registers(26, 2, unit=2).registers[1]) 
    ploads["bcu_state_of_charge"] = float(client.read_holding_registers(28, 2, unit=2).registers[1])
    ploads["bcu_hydrogen_sensor"] = float(client.read_holding_registers(30, 2, unit=2).registers[1]) 
    ploads["bcu_leakage_sensor"] = float(client.read_holding_registers(32, 2, unit=2).registers[1]) 
    ploads["smoke_sensor"] = float(client.read_holding_registers(34, 2, unit=2).registers[1]) 
    ploads["bcu_ocv"] = float(client.read_holding_registers(36, 2, unit=2).registers[1]) 
    ploads["bcu_positive_tank_temp"] = float(client.read_holding_registers(38, 2, unit=2).registers[1]) 
    ploads["bcu_negative_tank_temp"] = float(client.read_holding_registers(40, 2, unit=2).registers[1]) 
    ploads["positive_tank_high_level_float"] = float(client.read_holding_registers(42, 2, unit=2).registers[1]) 
    ploads["negative_tank_high_level_float"] = float(client.read_holding_registers(44, 2, unit=2).registers[1]) 
    ploads["positive_tank_low_level_float"] = float(client.read_holding_registers(46, 2, unit=2).registers[1]) 
    ploads["negative_tank_low_level_float"] = float(client.read_holding_registers(48, 2, unit=2).registers[1]) 
    ploads["primary_stack_voltage"] = float(client.read_holding_registers(50, 2, unit=2).registers[1]) 
    ploads["primary_stack_current"] = float(client.read_holding_registers(52, 2, unit=2).registers[1]) 
    ploads["primary_stack_positive_pressure_sensor"] = float(client.read_holding_registers(54, 2, unit=2).registers[1])
    ploads["primary_stack_negative_pressure_sensor"] = float(client.read_holding_registers(56, 2, unit=2).registers[1]) 
    ploads["positive_stack_pressure_delta"] = float(client.read_holding_registers(58, 2, unit=2).registers[1]) 
    ploads["b1_primary_stack_pressure_delta"] = float(client.read_holding_registers(60, 2, unit=2).registers[1]) 
    ploads["sensor_temp"] = float(client.read_holding_registers(64, 2, unit=2).registers[1]) 
    ploads["humidity"] = float(client.read_holding_registers(62, 2, unit=2).registers[1]) 
#    ploads["bms_timestamp"] = float(client.read_holding_registers(102, 2, unit=2).registers[1]) 
#    ploads["plc_alarm_status_1"] = float(client.read_holding_registers(104, 2, unit=2).registers[1]) 
    logger1.debug('Reading Register Complete')
    csv_w(ploads)
 
def SendData(url):
    logger1.debug('Posting Data to DB')
    t = http_client.post(url, json=ploads)
    logging.debug((str(t.text)))
    logger1.debug(ploads)
    logger1.debug('POST Complete')

def PrintData():
    print(ploads)

def csv_w(pload):
    with open('Parameters.csv', 'a', newline='') as f:
        w = csv.writer(f)
        w.writerow(pload.values())
    f.close()

if __name__ == "__main__":

    global logger2
    global logger1
    setup_logger('log1', 'run.log', level=logging.DEBUG, format='%(asctime)s - %(message)s')
    #setup_logger('log2', 'parameter.log')  
    logger1 = logging.getLogger('log1')
    #logger2 = logging.getLogger('log2')  

    #Config file read
    global config1
    with open('config.json', 'r') as f2:
        config1 = json.load(f2)
        f2.close()

    #CSV Setup
    if not file_exists('Parameters.csv'):
        with open('Parameters.csv', 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(ploads.keys())
        f.close()

    #Modbus Connect
    ModbusConnect(config1['modbus_ip'])

    #http request
    global http_client 
    http_client = requests.session()

    while True:
        #Read Registers
        ReadRegister()

        #Send Data
        SendData(config1['db_url'])

        #Print Data
        PrintData()

        #delay
        time.sleep(5)

    #Close Connections
    #http_client.delete(url)
    #client.close()