from pymodbus.client.sync import ModbusTcpClient
import requests
import time
import datetime
from random import randint
import logging

#backend url
url = 'http://192.168.1.36:4000/data'

#json payload
ploads = {
    "timestamp":0,
    "primary_charging_relay":0,
    "primary_discharge_relay":0,
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
    "pcs1_dc_volts":0,
    "pcs1_dc_batt_current":0,
    "pcs1_dc_inverter_power":0,
    "pcs1_voltage":0,
    "pcs1_current":0, 
    "pcs1_reactive_power":0,
    "pcs1_load_power":0,
    "pcs1_ac_supply_power":0,
    "pcs1_ac_out_status":0,
    "pcs1_fault_status":0,
    "pcs1_fan_speed":0,
    "system0PVEnable":0,
    "system0PVChargePower":0,
    "system0PVTotalPower":0,
    "pcs1InvFreq":0,
    "pcs1InternalTemperature":0
}

#logging format and level
FORMAT = ( '%(asctime)s %(name)s - %(levelname)s - %(message)s' )
logging.basicConfig(filename='app.log', filemode='w', format=FORMAT, level=logging.DEBUG)

#modbus connection
def ModbusConnect():
    global client 
    client = ModbusTcpClient('192.168.1.36', port=502)
    connect = client.connect()
    logging.debug(connect)

#reading registers
def ReadRegister():
    logging.debug('Reading Registers')
    ploads['timestamp'] = str(datetime.datetime.now())
    ploads["primary_charging_relay"] = float((client.read_holding_registers(0, 2)).registers[1])
    ploads["primary_discharge_relay"] = float(client.read_holding_registers(2, 2).registers[1])    
    ploads["primary_positive_pump"] = float(client.read_holding_registers(4, 2).registers[1]) 
    ploads["primary_negative_pump"]= float(client.read_holding_registers(6, 2).registers[1]) 
    ploads["system_mode"] = float(client.read_holding_registers(8, 2).registers[1]) 
    ploads["system_alarm_status"] = float(client.read_holding_registers(10, 2).registers[1]) 
    ploads["balancing_valve"] = float(client.read_holding_registers(12, 2).registers[1]) 
    ploads["positive_valve"] = float(client.read_holding_registers(14, 2).registers[1]) 
    ploads["negative_valve"] = float(client.read_holding_registers(16, 2).registers[1]) 
    ploads["state_of_charge"] = float(client.read_holding_registers(18, 2).registers[1]) 
    ploads["bcu_mode_status"] = float(client.read_holding_registers(20, 2).registers[1]) 
    ploads["bcu_voltage"] = float(client.read_holding_registers(22, 2).registers[1])
    ploads["bcu_current"] = float(client.read_holding_registers(24, 2).registers[1]) 
    ploads["bcu_power"] = float(client.read_holding_registers(26, 2).registers[1]) 
    ploads["bcu_state_of_charge"] = float(client.read_holding_registers(28, 2).registers[1])
    ploads["bcu_hydrogen_sensor"] = float(client.read_holding_registers(30, 2).registers[1]) 
    ploads["bcu_leakage_sensor"] = float(client.read_holding_registers(32, 2).registers[1]) 
    ploads["smoke_sensor"] = float(client.read_holding_registers(34, 2).registers[1]) 
    ploads["bcu_ocv"] = float(client.read_holding_registers(36, 2).registers[1]) 
    ploads["bcu_positive_tank_temp"] = float(client.read_holding_registers(38, 2).registers[1]) 
    ploads["bcu_negative_tank_temp"] = float(client.read_holding_registers(40, 2).registers[1]) 
    ploads["positive_tank_high_level_float"] = float(client.read_holding_registers(42, 2).registers[1]) 
    ploads["negative_tank_high_level_float"] = float(client.read_holding_registers(44, 2).registers[1]) 
    ploads["positive_tank_low_level_float"] = float(client.read_holding_registers(48, 2).registers[1]) 
    ploads["positive_tank_low_level_float"] = float(client.read_holding_registers(46, 2).registers[1]) 
    ploads["positive_tank_low_level_float"] = float(client.read_holding_registers(48, 2).registers[1]) 
    ploads["primary_stack_voltage"] = float(client.read_holding_registers(50, 2).registers[1]) 
    ploads["primary_stack_current"] = float(client.read_holding_registers(52, 2).registers[1]) 
    ploads["primary_stack_positive_pressure_sensor"] = float(client.read_holding_registers(54, 2).registers[1])
    ploads["primary_stack_negative_pressure_sensor"] = float(client.read_holding_registers(56, 2).registers[1]) 
    ploads["positive_stack_pressure_delta"] = float(client.read_holding_registers(58, 2).registers[1]) 
    ploads["b1_primary_stack_pressure_delta"] = float(client.read_holding_registers(60, 2).registers[1]) 
    ploads["sensor_temp"] = float(client.read_holding_registers(62, 2).registers[1]) 
    ploads["humidity"] = float(client.read_holding_registers(64, 2).registers[1]) 
    ploads["pcs1_dc_volts"] = float(client.read_holding_registers(66, 2).registers[1])
    ploads["pcs1_dc_batt_current"] = float(client.read_holding_registers(68, 2).registers[1]) 
    ploads["pcs1_dc_inverter_power"] = float(client.read_holding_registers(70, 2).registers[1]) 
    ploads["pcs1_voltage"] = float(client.read_holding_registers(72, 2).registers[1]) 
    ploads["pcs1_current"] = float(client.read_holding_registers(74, 2).registers[1]) 
    ploads["pcs1_reactive_power"] = float(client.read_holding_registers(76, 2).registers[1]) 
    ploads["pcs1_load_power"] = float(client.read_holding_registers(78, 2).registers[1]) 
    ploads["pcs1_ac_supply_power"] = float(client.read_holding_registers(80, 2).registers[1]) 
    ploads["pcs1_ac_out_status"] = float(client.read_holding_registers(82, 2).registers[1]) 
    ploads["pcs1_fault_status"] = float(client.read_holding_registers(84, 2).registers[1]) 
    ploads["pcs1_fan_speed"] = float(client.read_holding_registers(86, 2).registers[1]) 
    ploads["system0PVEnable"] = float(client.read_holding_registers(88, 2).registers[1]) 
    ploads["system0PVChargePower"] = float(client.read_holding_registers(90, 2).registers[1]) 
    ploads["system0PVTotalPower"] = float(client.read_holding_registers(92, 2).registers[1]) 
    ploads["pcs1InvFreq"] = float(client.read_holding_registers(94, 2).registers[1]) 
    ploads["pcs1InternalTemperature"] = float(client.read_holding_registers(96, 2).registers[1]) 

def SendData():
    t = http_client.post(url, json=ploads)
    logging.debug((str(t.text)))

def PrintData():
    print(ploads)

if __name__ == "__main__":
    

    #Modbus Connect
    ModbusConnect()

    #http request
    global http_client 
    http_client = requests.session()

    while True:
        #Read Registers
        ReadRegister()

        #Send Data
        SendData()

        #Print Data
        #PrintData()

        #delay
        time.sleep(5)

    #Close Connections
    http_client.delete(url)
    client.close()