package modbus

import (
	"fmt"
	"hash/fnv"
	"net"
	"sync"

	"github.com/goburrow/modbus"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
)

type Modbus struct {
	Address string `toml:"address"`

	server *modbus.TCPServer
	mu     sync.Mutex

	coils            map[uint16]bool
	discreteInputs   map[uint16]bool
	inputRegisters   map[uint16]uint16
	holdingRegisters map[uint16]uint16
}

func (m *Modbus) SampleConfig() string {
	return `
  ## Address of the Modbus server
  address = "0.0.0.0:502"
`
}

func (m *Modbus) Description() string {
	return "A Modbus server that outputs Telegraf metrics"
}

func (m *Modbus) StartServer() {
	listener, err := net.Listen("tcp", m.Address)
	if err != nil {
		fmt.Println("Error starting Modbus server:", err)
		return
	}
	defer listener.Close()

	handler := modbus.NewTCPServerHandler(listener)
	m.server = modbus.NewServer(handler)
	fmt.Println("Modbus server listening on", m.Address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go m.ServeTCP(conn)
	}
}

func (m *Modbus) Connect() error {
	m.coils = make(map[uint16]bool)
	m.discreteInputs = make(map[uint16]bool)
	m.inputRegisters = make(map[uint16]uint16)
	m.holdingRegisters = make(map[uint16]uint16)

	go m.StartServer()
	return nil
}

func (m *Modbus) Close() error {
	if m.server != nil {
		m.server.Close()
	}
	return nil
}

func (m *Modbus) Write(metrics []telegraf.Metric) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, metric := range metrics {
		for _, field := range metric.FieldList() {
			key := fmt.Sprintf("%s_%s", metric.Name(), field.Key)
			address := hashToUint16(key)
			value := uint16(field.Value.(float64))

			// Example logic to distribute metrics across different types
			if address < 10000 {
				m.coils[address] = value != 0
			} else if address < 20000 {
				m.discreteInputs[address-10000] = value != 0
			} else if address < 30000 {
				m.inputRegisters[address-20000] = value
			} else {
				m.holdingRegisters[address-30000] = value
			}
		}
	}
	return nil
}

func hashToUint16(s string) uint16 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return uint16(h.Sum32() % 65536)
}

func (m *Modbus) ServeTCP(conn net.Conn) {
	handler := modbus.NewTCPHandler()

	handler.FuncReadCoils = func(address uint16, quantity uint16) ([]byte, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		result := make([]byte, quantity/8+1)
		for i := uint16(0); i < quantity; i++ {
			if m.coils[address+i] {
				result[i/8] |= 1 << (i % 8)
			}
		}
		return result, nil
	}

	handler.FuncReadDiscreteInputs = func(address uint16, quantity uint16) ([]byte, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		result := make([]byte, quantity/8+1)
		for i := uint16(0); i < quantity; i++ {
			if m.discreteInputs[address+i] {
				result[i/8] |= 1 << (i % 8)
			}
		}
		return result, nil
	}

	handler.FuncReadHoldingRegisters = func(address uint16, quantity uint16) ([]byte, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		result := make([]byte, quantity*2)
		for i := uint16(0); i < quantity; i++ {
			modbus.Uint16ToBytes(m.holdingRegisters[address+i], result[i*2:])
		}
		return result, nil
	}

	handler.FuncReadInputRegisters = func(address uint16, quantity uint16) ([]byte, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		result := make([]byte, quantity*2)
		for i := uint16(0); i < quantity; i++ {
			modbus.Uint16ToBytes(m.inputRegisters[address+i], result[i*2:])
		}
		return result, nil
	}

	handler.FuncWriteSingleCoil = func(address uint16, value uint16) (bool, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.coils[address] = (value == 0xFF00)
		return true, nil
	}

	handler.FuncWriteSingleRegister = func(address uint16, value uint16) (bool, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.holdingRegisters[address] = value
		return true, nil
	}

	handler.Serve(conn)
}

func init() {
	outputs.Add("modbus", func() telegraf.Output {
		return &Modbus{}
	})
}
