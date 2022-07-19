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
    "_id":"62c65f43980d8d04c1c66251",
    "timestamp":datetime.datetime.now().isoformat(),
    "pcs1_reactive_power":0,
    "pcs1_current":0,
    "pcs1_voltage":0,
    "pri_stack_neg_pressure":0,
    "pri_stack_pos_pressure":0,
    "bcu_OCV":0,
    "bcu_state_of_charge":0,
    "bcu_power":0,
    "bcu_current":0,
    "bcu_voltage":0
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
    global reactive_power 
    reactive_power = client.read_holding_registers(0, 2)
    global soc 
    soc= client.read_holding_registers(2, 2)

def SendData():
    # register value entered in dictionary and posted
    for i in range(1):
        ploads['timestamp'] = str(datetime.datetime.now())
        ploads['pcs1_reactive_power']=reactive_power.registers[1]
        ploads['bcu_state_of_charge']=soc.registers[1]
        ploads['bcu_power']=randint(-5, 5)
        ploads['pri_stack_neg_pressure']=randint(300,400)
        ploads['pri_stack_pos_pressurer']=randint(300,400)
        ploads['pcs1_current'] = randint(-5, 5)
        ploads['pcs1_voltage']=randint(230, 240)
        t = http_client.post(url, json=ploads)
        print(str(t.text))

if __name__ == "__main__":
    #Modbus Connect
    ModbusConnect()
    #http request
    global http_client 
    http_client = requests.session()

    #Read Registers
    ReadRegister()

    #Send Data
    SendData()

    #Close Connections
    http_client.delete(url)
    client.close()

    #delay
    time.sleep(2)