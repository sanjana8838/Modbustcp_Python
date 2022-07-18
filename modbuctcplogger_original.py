from pymodbus.client.sync import ModbusTcpClient

client = ModbusTcpClient('192.168.1.36')
#client.write_coil(1, True)
#result = client.read_coils(1,1)
rs = client.read_holding_registers(0,)
print(rs.registers)
print(type(rs.registers))
client.close()
