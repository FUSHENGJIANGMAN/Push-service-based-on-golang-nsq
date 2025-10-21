package modem

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

//
// ========== 可调常量与内部错误（不引入新结构体，保持无感替换） ==========
//

const (
	defaultResponseTimeout   = 10 * time.Second
	recoverableBackoff       = 100 * time.Millisecond
	interLinePause           = 50 * time.Millisecond
	initializeCommandSpacing = 500 * time.Millisecond
)

//
// ========== 内部小工具函数（拆分职责，函数均 ≤ 30 行） ==========
//

// writeCommand 负责写入命令与基本日志。
// “为什么”：将 I/O 与业务收敛在小函数，SendCommand 保持单一职责与浅层逻辑。
func (m *Modem) writeCommand(cmd string) error {
	fmt.Printf("📤 发送命令: %s", strings.TrimRight(cmd, "\r\n"))
	_, err := m.port.Write([]byte(cmd))
	if err != nil {
		return fmt.Errorf("写入命令失败: %w", err)
	}
	return nil
}

// readNextLine 读取一行，遇到临时性可恢复错误（超时/EOF）返回空串与 nil，驱动上层继续轮询。
// “为什么”：把边界条件集中在一处，避免重复且深层嵌套。
func readNextLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err == nil {
		return line, nil
	}
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		time.Sleep(recoverableBackoff)
		return "", nil
	}
	if err == io.EOF {
		time.Sleep(recoverableBackoff)
		return "", nil
	}
	return "", fmt.Errorf("读取响应失败: %w", err)
}

// isTerminalLine 判断是否为结束标记（OK/ERROR/+CME ERROR 等）或同行携带关键字。
// “为什么”：模块厂商实现不一，统一终止条件，减少上层分支复杂度。
func isTerminalLine(line string) bool {
	t := strings.TrimSpace(line)
	if t == "OK" || t == "ERROR" || strings.HasPrefix(t, "ERROR:") {
		return true
	}
	if strings.HasPrefix(t, "+CME ERROR") || strings.HasPrefix(t, "+CMS ERROR") {
		return true
	}
	return strings.Contains(t, "OK") || strings.Contains(t, "ERROR")
}

//
// ========== 对外方法（签名保持不变：无感替换） ==========
//

// SendCommand 发送单条 AT 指令并收集直到出现 OK/ERROR 结束的响应文本。
// 采用总超时保护与卫语句提前返回，避免深层嵌套。
func (m *Modem) SendCommand(cmd string) (string, error) {
	if err := m.writeCommand(cmd); err != nil {
		return "", err
	}

	reader := bufio.NewReader(m.port)
	var response strings.Builder
	timeout := time.After(defaultResponseTimeout)

	for {
		select {
		case <-timeout:
			return "", fmt.Errorf("AT命令响应超时(%s)", defaultResponseTimeout)
		default:
			line, err := readNextLine(reader)
			if err != nil {
				return "", err
			}
			if line == "" { // 可恢复情况：继续等
				continue
			}

			fmt.Printf("📥 收到: %s", line)
			response.WriteString(line)

			if isTerminalLine(line) {
				return response.String(), nil
			}
			time.Sleep(interLinePause)
		}
	}
}

// Initialize 执行一组基础 AT 指令进行模块初始化（签名不变）。
// “为什么集中在一起”：不同调用点不必重复样板，保证流程一致与可读性。
func (m *Modem) Initialize() error {
	fmt.Println("正在初始化设备...")

	commands := []string{
		CMD_RESET,
		CMD_ECHO_OFF,
		CMD_ENABLE_TTS,
		CMD_SAVE,
	}

	for _, cmd := range commands {
		if _, err := m.SendCommand(cmd); err != nil {
			return fmt.Errorf("初始化命令 %s 失败: %w", strings.TrimSpace(cmd), err)
		}
		time.Sleep(initializeCommandSpacing)
	}

	fmt.Println("✅ 设备初始化完成")
	return nil
}
