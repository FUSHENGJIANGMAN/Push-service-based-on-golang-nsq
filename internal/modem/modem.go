package modem

import (
	"time"

	"github.com/tarm/serial"
)

// Modem 表示一个打开的 4G 模块串口会话：
//
//	port: 串口句柄
//	config: 初始化时传入的配置（端口号、波特率、队列尺寸等）
//	operator: 已检测到的运营商类型（可能为 Unknown, 需调用 DetectOperator）
type Modem struct {
	port     *serial.Port
	config   ModemConfig
	operator OperatorType
}

// NewModem 打开串口并创建 Modem 实例
func NewModem(config ModemConfig) (*Modem, error) {
	c := &serial.Config{
		Name:        config.PortName,
		Baud:        config.BaudRate,
		ReadTimeout: 200 * time.Millisecond,
	}
	p, err := serial.OpenPort(c)
	if err != nil {
		return nil, err
	}

	m := &Modem{
		port:   p,
		config: config,
	}

	return m, nil
}

// Close 关闭串口（若已打开）
func (m *Modem) Close() {
	if m.port != nil {
		m.port.Close()
	}
}
