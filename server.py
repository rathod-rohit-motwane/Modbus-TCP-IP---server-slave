import json
import struct
import time
import threading
import os
import logging
import time
from typing import Dict, List, Any
from dataclasses import dataclass
from pymodbus.server import StartTcpServer
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusServerContext,ModbusSlaveContext


# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

slave_file="sample.json"
@dataclass
class RegisterConfig:
    address: int
    function_code: int
    data_type: int  #3=float32, 1,2=int,unsign, 5=bool (coil)
    value: Any

@dataclass
class SlaveContext:
    slave_address: int
    registers: List[RegisterConfig]
    coils: List[int]
    discrete_inputs: List[int]
    holding_registers: List[int]
    input_registers: List[int]

class ModbusTCPSlave:
    def __init__(self, json_file=slave_file, port=1502, max_slaves=20, max_registers=2000, poll_interval=30):
        self.json_file = json_file
        self.port = port
        self.max_slaves = max_slaves
        self.max_registers = max_registers
        self.poll_interval = poll_interval
        self.slaves: Dict[int, SlaveContext] = {}
        self.rwlock = threading.RLock()
        self.running = True
        self.last_modified = 0

        # Initialize empty mapping
        self._initialize_empty_mapping()
        # Load JSON initially
        self.parse_json()
        # Start JSON polling thread
        threading.Thread(target=self._poll_json_changes, daemon=True).start()
        # Start TCP server thread
        threading.Thread(target=self._start_server, daemon=True).start()

    def _initialize_empty_mapping(self):
        reg_empty = [0] * self.max_registers
        self.slave_contexts = {}
        for slave_id in range(1, self.max_slaves + 1):
            self.slave_contexts[slave_id] = ModbusSlaveContext(
                di=ModbusSequentialDataBlock(0, reg_empty[:]),
                co=ModbusSequentialDataBlock(0, reg_empty[:]),
                hr=ModbusSequentialDataBlock(0, reg_empty[:]),
                ir=ModbusSequentialDataBlock(0, reg_empty[:])
            )
        self.context = ModbusServerContext(slaves=self.slave_contexts, single=False)

##############helper##################################3
    def float_to_registers(self, value: float):
        return struct.unpack(">HH", struct.pack(">f", value))

    def int32_to_registers(self, value: int):
        return struct.unpack(">HH", struct.pack(">i", value))

##########################################################
    def get_file_mtime(self):
        try:
            return os.path.getmtime(self.json_file)
        except OSError:
            print("Json-File not found")
            return 0

    def parse_json(self):
        try:
            if not os.path.exists(self.json_file):
                logger.error(f"JSON file {self.json_file} not found")
                return False

            with open(self.json_file, 'r') as f:
                data = json.load(f)

            new_slaves = {}
            for slave_idx, slave_data in enumerate(data):
                slave_id = slave_data.get("id", slave_idx) + 1  # Slave addresses start at 1
                slave_ctx = SlaveContext(
                    slave_address=slave_id,
                    registers=[],
                    coils=[0]*self.max_registers,
                    discrete_inputs=[0]*self.max_registers,
                    holding_registers=[0]*self.max_registers,
                    input_registers=[0]*self.max_registers)

                for reg_data in slave_data.get("registers", []):
                    address = reg_data.get("address", 0)
                    fn_code = reg_data.get("fn_code", 3)
                    data_type = reg_data.get("data_type", 3)
                    value = reg_data.get("value", 0)
                    reg_config = RegisterConfig(address, fn_code, data_type, value)

                    mapping_fn_code={1:"coil", 2:"discreat coil" ,3:"holding register", 4:"input register"}
                    if fn_code not in mapping_fn_code:
                        logger.error(f"[Slave {slave_id}] Invalid function code: {fn_code} address{address}")
                        continue
                    data_type_mapping={3:"float", 1:"signed", 2:"unsign",5:"bool"}
                    if data_type not in data_type_mapping:
                        logger.error(f"[Slave {slave_id}] Invalid data-type={data_type},fn={fn_code} address{address}")
                        continue
                # Convert the value into 16-bit register words
                    hex_values = [] #useful for logging/debugging the value as Modbus “raw register words”
                    if fn_code in (3,4):  # Holding / Input Registers
                        if data_type == 3:  # float32
                            reg2, reg1 = self.float_to_registers(float(value))
                            hex_values = [reg2, reg1]
                             #max_registers should not exceed max count 2000(checking)
                            if fn_code == 3: #[HR]
                                if address < self.max_registers:
                                    slave_ctx.holding_registers[address] = reg1
                                if address+1 < self.max_registers:
                                    slave_ctx.holding_registers[address+1] = reg2
                            else: #[IR]
                                if address < self.max_registers:
                                    slave_ctx.input_registers[address] = reg1
                                if address+1 < self.max_registers:
                                    slave_ctx.input_registers[address+1] = reg2

                        elif data_type in (1,2):  # int and unsign
                            reg2, reg1 = self.int32_to_registers(int(value))
                            hex_values = [reg2, reg1]
                            if fn_code == 3: #[HR]
                                if address < self.max_registers:
                                    slave_ctx.holding_registers[address] = reg1
                                if address+1 < self.max_registers:
                                    slave_ctx.holding_registers[address+1] = reg2
                            else:
                                if address < self.max_registers:
                                    slave_ctx.input_registers[address] = reg1
                                if address+1 < self.max_registers:
                                    slave_ctx.input_registers[address+1] = reg2
#####################coil fc=1
                    elif (fn_code == 1) :  # Coil
                        hex_values = [1 if value else 0] #zero or one
                        if (fn_code == 1 or data_type == 5) and address < self.max_registers:
                            slave_ctx.coils[address] = 1 if value else 0
                    elif fn_code ==2: #discreate
                        if address < self.max_registers:
                            slave_ctx.discrete_inputs[address] = 1 if value else 0
                    else:
                        logger.error(f"Invalid Function code {fn_code}, with data type {data_type}")
                        return False

                # Log in Modbus TCP PDU style: [Slave][Func][Addr][Value(s)]
                    hex_str = ' '.join(f"{v:04X}" for v in hex_values)
                    logger.info(f" DATA type: {data_type}| PUD:[Slave {slave_id:02}] | FC={fn_code} | Addr={address} | Value=0x{hex_str}")

                    slave_ctx.registers.append(reg_config)

                new_slaves[slave_id] = slave_ctx

            with self.rwlock:
                self.slaves = new_slaves
                self._update_modbus_context()

            logger.info(f"Loaded {len(new_slaves)} slaves from JSON")
            return True
        except Exception as e:
            logger.error(f"Failed to parse JSON: {e}")
            return False

    def _update_modbus_context(self):
        for slave_id, slave_ctx in self.slaves.items():
            if slave_id not in self.slave_contexts:
                logger.error(f"inavlid slave address {slave_id}")
                continue
            store = self.slave_contexts[slave_id]
            for addr, val in enumerate(slave_ctx.coils): #coil
                if addr < self.max_registers:
                    store.setValues(1, addr, [val])

            for addr, val in enumerate(slave_ctx.discrete_inputs):
                if addr < self.max_registers:
                    store.setValues(2, addr, [val])

            for addr, val in enumerate(slave_ctx.holding_registers): #holding registor
                if addr < self.max_registers:
                    store.setValues(3, addr, [val])

            for addr, val in enumerate(slave_ctx.input_registers):
                if addr < self.max_registers:
                    store.setValues(4, addr, [val])


    def _poll_json_changes(self):
        self.last_modified = self.get_file_mtime()
        while self.running:
            try:
                current_mtime = self.get_file_mtime()
                if current_mtime > self.last_modified:
                    logger.info("JSON changed, reloading...")
                    if self.parse_json():
                        self.last_modified = current_mtime
                        logger.info("JSON hot-reloaded successfully")
                time.sleep(self.poll_interval)
            except Exception as e:
                logger.error(f"Polling error: {e}")
                time.sleep(self.poll_interval)


    def _start_server(self):
        self.context = ModbusServerContext(slaves=self.slave_contexts, single=False)
        StartTcpServer(self.context, address=("0.0.0.0", self.port),ignore_missing_slaves=True)
        logger.info(f"Starting Modbus TCP server on port {self.port}")

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    slave_server = ModbusTCPSlave(json_file=slave_file, port=1502, poll_interval=30)
    logger.info("Modbus TCP slave is running. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(3)
    except KeyboardInterrupt:
        logger.info("Stopping Modbus TCP slave...")
        slave_server.running = False
