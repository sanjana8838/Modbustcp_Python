from pymodbus.client.sync import ModbusTcpClient
import requests
import time
import datetime
import logging
import csv
from os.path import exists as file_exists
import json
import ctypes
import pymongo
import paho.mqtt.client as mqtt

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
    "plc_alarm_status_1":0,
    "system_state":0,
    "powerflow_state":0,
    "alarm_status":"Alarm_"
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

#Negative Number Read
def NegativeValue(n):
    num = int((int(n[1])))
    num = num & 0xFFFF
    num = ctypes.c_int16(num).value
    return num

def ReadBlock(word):
    num = int((int(word[0]) << (16)) + (int(word[1])))
    return num


def decode_alrm(status):
    alarm_st = "Alarm_"
    if (status & 0x1):
        alarm_st = alarm_st + "ss_"
    elif (status & 0x2):
        alarm_st = alarm_st + "lvlf_"
    elif (status & 0x4):
        alarm_st = alarm_st + "lka_"
    elif (status & 0x8):
        alarm_st = alarm_st + "highetmp_"  
    elif (status & 0x10):
        alarm_st = alarm_st + "lowprs_" 
    elif (status & 0x20):
        alarm_st = alarm_st + "hightmp_" 
    elif (status & 0x40):
        alarm_st = alarm_st + "hs_"
    elif (status & 0x80):
        alarm_st = alarm_st + "hc_"
    elif (status & 0x100):
        alarm_st = alarm_st + "sc_"
    elif (status & 0x200):
        alarm_st = alarm_st + "hv_"
    elif (status & 0x400):
        alarm_st = alarm_st + "lsoc_"
    elif (status & 0x800):
        alarm_st = alarm_st + "hsoc_"
    elif (status & 0x1000):
        alarm_st = alarm_st + "hprs_"
    elif (status & 0x2000):
        alarm_st = alarm_st + "dprs_"
    elif (status & 0x4000):
        alarm_st = alarm_st + "invcomm_"
    elif (status & 0x8000):
        alarm_st = alarm_st + "iot_"
    return alarm_st

#reading registers
def ReadRegister():
    logging.debug('Reading Registers')
    logger1.debug('Reading Registers')
    #ploads['timestamp'] = str(datetime.datetime.now())

    timestamp1 = int(time.time())
    ploads['timestamp'] = timestamp1

    dc_main_contactor = (client.read_holding_registers(0, 2, unit=2).registers)
    dc_main_contactor = ReadBlock(dc_main_contactor)
    ploads["dc_main_contactor"] = dc_main_contactor

    stk1_contactor = (client.read_holding_registers(2, 2, unit=2).registers) 
    stk1_contactor = ReadBlock(stk1_contactor)
    ploads["stk1_contactor"] = stk1_contactor

    primary_positive_pump = (client.read_holding_registers(4, 2, unit=2).registers) 
    primary_positive_pump = ReadBlock(primary_positive_pump)
    ploads["primary_positive_pump"] = primary_positive_pump

    primary_negative_pump = (client.read_holding_registers(6, 2, unit=2).registers) 
    primary_negative_pump = ReadBlock(primary_negative_pump)
    ploads["primary_negative_pump"] = primary_negative_pump

    balancing_valve = (client.read_holding_registers(12, 2, unit=2).registers)
    balancing_valve = ReadBlock(balancing_valve)
    ploads["balancing_valve"] = balancing_valve

    positive_valve = (client.read_holding_registers(14, 2, unit=2).registers)
    positive_valve = ReadBlock(positive_valve)
    ploads["positive_valve"] = positive_valve

    negative_valve = (client.read_holding_registers(16, 2, unit=2).registers)
    negative_valve = ReadBlock(negative_valve)
    ploads["negative_valve"] = negative_valve

    state_of_charge = (client.read_holding_registers(18, 2, unit=2).registers)
    state_of_charge = ReadBlock(state_of_charge)
    ploads["state_of_charge"] = (state_of_charge)/100

    bcu_mode_status = (client.read_holding_registers(20, 2, unit=2).registers)
    bcu_mode_status = ReadBlock(bcu_mode_status)
    ploads["bcu_mode_status"] =  bcu_mode_status

    bcu_voltage = (client.read_holding_registers(22, 2, unit=2).registers)
    bcu_voltage = ReadBlock(bcu_voltage)
    ploads["bcu_voltage"] = bcu_voltage

    bcu_current = (client.read_holding_registers(24, 2, unit=2).registers)
    bcur = NegativeValue(bcu_current)
    ploads["bcu_current"] =  bcur

    bcu_power = (client.read_holding_registers(26, 2, unit=2).registers) 
    bpow = NegativeValue(bcu_power)
    ploads["bcu_power"] = bpow

    bcu_state_of_charge = (client.read_holding_registers(28, 2, unit=2).registers)
    bcu_state_of_charge = ReadBlock(bcu_state_of_charge)
    ploads["bcu_state_of_charge"] = bcu_state_of_charge

    bcu_hydrogen_sensor = (client.read_holding_registers(30, 2, unit=2).registers)
    bcu_hydrogen_sensor = ReadBlock(bcu_hydrogen_sensor)
    ploads["bcu_hydrogen_sensor"] =  bcu_hydrogen_sensor

    bcu_leakage_sensor = (client.read_holding_registers(32, 2, unit=2).registers)
    bcu_leakage_sensor = ReadBlock(bcu_leakage_sensor)
    ploads["bcu_leakage_sensor"] =  bcu_leakage_sensor

    smoke_sensor = (client.read_holding_registers(34, 2, unit=2).registers) 
    smoke_sensor = ReadBlock(smoke_sensor)
    ploads["smoke_sensor"] = smoke_sensor

    bcu_ocv = (client.read_holding_registers(36, 2, unit=2).registers) 
    bcu_ocv = ReadBlock(bcu_ocv)
    ploads["bcu_ocv"] = bcu_ocv

    bcu_positive_tank_temp = (client.read_holding_registers(38, 2, unit=2).registers)
    bcu_positive_tank_temp = ReadBlock(bcu_positive_tank_temp)
    ploads["bcu_positive_tank_temp"] =  bcu_positive_tank_temp

    bcu_negative_tank_temp = (client.read_holding_registers(40, 2, unit=2).registers) 
    bcu_negative_tank_temp = ReadBlock(bcu_negative_tank_temp)
    ploads["bcu_negative_tank_temp"] = bcu_negative_tank_temp

    positive_tank_high_level_float = (client.read_holding_registers(42, 2, unit=2).registers) 
    positive_tank_high_level_float = ReadBlock(positive_tank_high_level_float)
    ploads["positive_tank_high_level_float"] = positive_tank_high_level_float

    negative_tank_high_level_float = (client.read_holding_registers(44, 2, unit=2).registers) 
    negative_tank_high_level_float = ReadBlock(negative_tank_high_level_float)
    ploads["negative_tank_high_level_float"] = negative_tank_high_level_float

    positive_tank_low_level_float = (client.read_holding_registers(46, 2, unit=2).registers) 
    positive_tank_low_level_float = ReadBlock(positive_tank_low_level_float)
    ploads["positive_tank_low_level_float"] = positive_tank_low_level_float

    negative_tank_low_level_float = (client.read_holding_registers(48, 2, unit=2).registers) 
    negative_tank_low_level_float = ReadBlock(negative_tank_low_level_float)
    ploads["negative_tank_low_level_float"] = negative_tank_low_level_float

    primary_stack_voltage = (client.read_holding_registers(50, 2, unit=2).registers) 
    primary_stack_voltage = ReadBlock(primary_stack_voltage)
    ploads["primary_stack_voltage"] = primary_stack_voltage

    primary_stack_current = (client.read_holding_registers(52, 2, unit=2).registers) 
    primary_stack_current = NegativeValue(primary_stack_current)
    ploads["primary_stack_current"] = primary_stack_current

    primary_stack_positive_pressure_sensor = (client.read_holding_registers(54, 2, unit=2).registers)
    primary_stack_positive_pressure_sensor = ReadBlock(primary_stack_positive_pressure_sensor)
    ploads["primary_stack_positive_pressure_sensor"] = primary_stack_positive_pressure_sensor

    primary_stack_negative_pressure_sensor = (client.read_holding_registers(56, 2, unit=2).registers) 
    primary_stack_negative_pressure_sensor = ReadBlock(primary_stack_negative_pressure_sensor)
    ploads["primary_stack_negative_pressure_sensor"] = primary_stack_negative_pressure_sensor

    positive_stack_pressure_delta = (client.read_holding_registers(58, 2, unit=2).registers) 
    positive_stack_pressure_delta = ReadBlock(positive_stack_pressure_delta)
    ploads["positive_stack_pressure_delta"] = positive_stack_pressure_delta

    b1_primary_stack_pressure_delta = (client.read_holding_registers(60, 2, unit=2).registers) 
    b1_primary_stack_pressure_delta = ReadBlock(b1_primary_stack_pressure_delta)
    ploads["b1_primary_stack_pressure_delta"] = b1_primary_stack_pressure_delta

    sensor_temp = (client.read_holding_registers(64, 2, unit=2).registers) 
    sensor_temp = ReadBlock(sensor_temp)
    ploads["sensor_temp"] = sensor_temp

    humidity = (client.read_holding_registers(62, 2, unit=2).registers) 
    humidity = ReadBlock(humidity)
    ploads["humidity"] = humidity

    bms_timestamp = (client.read_holding_registers(102, 2, unit=2).registers) 
    bms_timestamp = ReadBlock(bms_timestamp)
    ploads["bms_timestamp"] = bms_timestamp

    plc_alarm_status_1 = (client.read_holding_registers(104, 2, unit=2).registers) 
    plc_alarm_status_1 = ReadBlock(plc_alarm_status_1)
    ploads["plc_alarm_status_1"] = hex(plc_alarm_status_1)

    ploads["alarm_status"] = decode_alrm(plc_alarm_status_1) 

    system_state = (client.read_holding_registers(110, 2, unit=2).registers)
    system_state = ReadBlock(system_state)
    ploads["system_state"] =  system_state

    powerflow_state = (client.read_holding_registers(112, 2, unit=2).registers)
    powerflow_state = ReadBlock(powerflow_state)
    ploads["powerflow_state"] = powerflow_state
    
    logger1.debug('Reading Register Complete')
    csv_w(ploads)
 
def SendData(url):
    logger1.debug('Posting Data to DB')
    t = http_client.post(url, json=ploads)
    logging.debug((str(t.text)))
    logger1.debug('POST Complete')
    logger1.debug('Sending to Local DB')
    global x
    x = col_db.insert_one(ploads)
    ploads.pop("_id", None)
    logger1.debug("Local DB document inserted")

def PrintData():
    print(ploads)

def csv_w(pload):
    with open('Parameters.csv', 'a', newline='') as f:
        w = csv.writer(f)
        w.writerow(pload.values())
    f.close()
    
    
def mqtt_pub(msg, topic):
    try:
        if 1:
            #msg = (json.dumps(msg))
            msg = str(msg)
            logger1.debug('MQTT Publish Start')
            client_mqtt.publish(topic, msg)
            print(msg)
            logger1.debug('MQTT Publish Complete')
    except OSError:
        logger1.debug('Publish Failure')


if __name__ == "__main__":

    global logger2
    global logger1
    setup_logger('log1', 'run.log', level=logging.DEBUG, format='%(asctime)s - %(message)s')
    #setup_logger('log2', 'parameter.log')  
    logger1 = logging.getLogger('log1')
    #logger2 = logging.getLogger('log2')  

    #Config file read
    global config1
    with open('setup.json', 'r') as f2:
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

    #MongoDB setup
    client_db = pymongo.MongoClient(config1['mongodb_url'])
    global m_db
    m_db = client_db["local"]
    global col_db
    col_db = m_db["post_python"]

    #MQTT Connection setup
    global client_mqtt
    client_mqtt = mqtt.Client("P1_IOT")
    client_mqtt.connect(str(config1['broker_addr']))

    while True:
        #Read Registers
        ReadRegister()

        #Send Data
        SendData(config1['db_url'])

        #Print Data
        PrintData()

        #Publish MQTT
        mqtt_pub(ploads["bcu_voltage"], config1['mqtt_topic'])

        #delay
        time.sleep(5)

    #Close Connections
    #http_client.delete(url)
    #client.close()