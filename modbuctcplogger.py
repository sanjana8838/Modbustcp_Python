from pymodbus.client.sync import ModbusTcpClient
import requests
import time
import datetime
from random import randint

#backend url
url = 'http://192.168.1.36:4000/data'

#json payload
ploads = {
    "_id":"62c65f43980d8d04c1c66251",
    "timestamp":datetime.datetime.now().isoformat(),
    "pcs1_reactive_power":123,
    "pcs1_current":33,
    "pcs1_voltage":230,
    "pri_stack_neg_pressure":-300.419999838,
    "pri_stack_pos_pressure":256.51333,
    "bcu_OCV":1.34524,
    "bcu_state_of_charge":90.93999863,
    "bcu_power":2,
    "bcu_current":1.23,
    "bcu_voltage":5.747000217
}

#http request
s = requests.session()

#modbus connection
client = ModbusTcpClient('192.168.1.36')

#reading registers
reactive_power = client.read_holding_registers(0, 2)
soc = client.read_holding_registers(2, 2)

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
    t = s.post(url, json=ploads)
    print(str(t.text))


#delete sessions
s.delete(url)
client.close()

#delay
time.sleep(2)