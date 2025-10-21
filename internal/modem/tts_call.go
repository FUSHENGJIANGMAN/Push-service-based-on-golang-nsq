package modem

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"golang.org/x/text/encoding/simplifiedchinese"
)

// ==================== 常量定义 ====================

const (
	maxTTSBytesLength      = 1600
	defaultBufferSize      = 512
	maxLineBufferSize      = 400
	readTimeout            = 20 * time.Millisecond
	portDrainDuration      = 50 * time.Millisecond
	commandTimeout         = 4 * time.Second
	minTTSDuration         = 4 * time.Second
	maxTTSDuration         = 40 * time.Second
	ttsCharactersPerSecond = 8
)

// ==================== 错误定义 ====================

type CallError struct {
	Operation string
	Reason    string
	Err       error
}

func (e *CallError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s失败: %s (%v)", e.Operation, e.Reason, e.Err)
	}
	return fmt.Sprintf("%s失败: %s", e.Operation, e.Reason)
}

func newCallError(operation, reason string, err error) *CallError {
	return &CallError{Operation: operation, Reason: reason, Err: err}
}

// ==================== 核心业务逻辑 ====================

// MakeCallAndSpeak 拨号并播报 TTS 文本(完整流程)
func (m *Modem) MakeCallAndSpeak(phoneNumber, text string) error {
	if err := m.prepareForNewCall(); err != nil {
		return err
	}

	if err := m.dialPhoneNumber(phoneNumber); err != nil {
		return err
	}

	if err := m.configureAudioSettings(); err != nil {
		return err
	}

	if err := m.speakTextOverCall(text); err != nil {
		return err
	}

	return m.hangUpCall()
}

// MakeVoiceCall 带 context 的语音呼叫接口
func (m *Modem) MakeVoiceCall(ctx context.Context, phoneNumber, text string) error {
	return m.MakeCallAndSpeak(phoneNumber, text)
}

// ==================== 呼叫流程子步骤 ====================

// prepareForNewCall 准备新呼叫(确保上次呼叫已挂断)
func (m *Modem) prepareForNewCall() error {
	if err := ensureCallTerminated(m.port); err != nil {
		return newCallError("准备呼叫", "清理旧连接失败", err)
	}
	drainPortBuffer(m.port, portDrainDuration)
	return nil
}

// dialPhoneNumber 拨打电话号码
func (m *Modem) dialPhoneNumber(phoneNumber string) error {
	dialCommand := m.buildDialCommand(phoneNumber)
	fmt.Printf("📞 拨号 %s ...\n", phoneNumber)

	if err := writeCommandToPort(m.port, dialCommand); err != nil {
		return newCallError("拨号", "发送拨号指令失败", err)
	}

	if !waitForCallEstablished(m.port, 30*time.Second) {
		return newCallError("拨号", "未接通或被拒绝", nil)
	}

	fmt.Println("✅ 已接通")
	time.Sleep(portDrainDuration)
	return nil
}

// buildDialCommand 根据运营商构建拨号指令
func (m *Modem) buildDialCommand(phoneNumber string) string {
	if m.operator == OperatorChinaTelecom {
		return fmt.Sprintf("AT+CDV%s;\r", phoneNumber)
	}
	return fmt.Sprintf("ATD%s;\r", phoneNumber)
}

// configureAudioSettings 配置通话音频参数
func (m *Modem) configureAudioSettings() error {
	commands := []string{
		"ATE0\r",       // 关闭回显
		"AT+CHFA=1\r",  // 启用免提
		"AT+CLVL=10\r", // 设置音量
		"AT+CMUT=0\r",  // 取消静音
		"AT+CTTS=0\r",  // 停止之前的 TTS
	}

	for _, command := range commands {
		if err := writeCommandToPort(m.port, command); err != nil {
			return newCallError("配置音频", "发送配置指令失败", err)
		}
	}

	drainPortBuffer(m.port, portDrainDuration)
	waitForTTSCompletion(m.port, 1*time.Second)
	return nil
}

// speakTextOverCall 在通话中播报 TTS 文本
func (m *Modem) speakTextOverCall(text string) error {
	cleanedText := sanitizeTTSText(text)
	if cleanedText == "" {
		return newCallError("TTS 播报", "文本为空", nil)
	}

	encodedBytes, err := encodeTextToGBK(cleanedText)
	if err != nil {
		return newCallError("TTS 播报", "GBK 编码失败", err)
	}

	encodedBytes = limitTTSBytesLength(encodedBytes)
	fmt.Printf("🔊 播报: %s\n", cleanedText)

	if err := sendTTSCommand(m.port, encodedBytes); err != nil {
		return newCallError("TTS 播报", "发送 TTS 指令失败", err)
	}

	waitDuration := calculateTTSWaitDuration(encodedBytes)
	if waitForTTSCompletion(m.port, waitDuration) {
		fmt.Println("✅ 播报结束,自动挂断")
	} else {
		fmt.Println("⚠️ 未检测到播报完成信号,按估算时间结束")
	}

	return nil
}

// hangUpCall 挂断电话
func (m *Modem) hangUpCall() error {
	if err := writeCommandToPort(m.port, "AT+CHUP\r"); err != nil {
		return newCallError("挂断", "发送挂断指令失败", err)
	}
	waitForPortResponse(m.port, "NO CARRIER", "ERROR", 3*time.Second)
	return nil
}

// ==================== TTS 处理函数 ====================

// sendTTSCommand 发送 TTS 播报指令(支持模式2和模式1降级)
func sendTTSCommand(port io.ReadWriter, encodedBytes []byte) error {
	if trySendTTSMode(port, 2, encodedBytes) {
		return nil
	}

	fmt.Println("⚠️ 模式2未响应,尝试模式1")
	if trySendTTSMode(port, 1, encodedBytes) {
		return nil
	}

	return fmt.Errorf("TTS 指令启动失败")
}

// trySendTTSMode 尝试使用指定模式发送 TTS 指令
func trySendTTSMode(port io.ReadWriter, mode int, encodedBytes []byte) bool {
	var commandBuffer bytes.Buffer
	commandBuffer.WriteString(fmt.Sprintf("AT+CTTS=%d,\"", mode))
	commandBuffer.Write(encodedBytes)
	commandBuffer.WriteString("\"\r")

	if err := writeCommandToPort(port, commandBuffer.String()); err != nil {
		return false
	}

	return waitForPortResponse(port, "OK", "ERROR", commandTimeout)
}

// calculateTTSWaitDuration 根据文本长度估算播报时长
func calculateTTSWaitDuration(encodedBytes []byte) time.Duration {
	characterCount := len(encodedBytes) / 2
	estimatedDuration := time.Duration(characterCount/ttsCharactersPerSecond+2) * time.Second

	if estimatedDuration < minTTSDuration {
		return minTTSDuration + 3*time.Second
	}
	if estimatedDuration > maxTTSDuration {
		return maxTTSDuration + 3*time.Second
	}

	return estimatedDuration + 3*time.Second
}

// sanitizeTTSText 清理和规范化 TTS 文本
func sanitizeTTSText(text string) string {
	text = strings.ReplaceAll(text, "\r", " ")
	text = strings.ReplaceAll(text, "\n", " ")
	text = strings.ReplaceAll(text, "\t", " ")
	text = strings.ReplaceAll(text, `"`, "")
	return strings.TrimSpace(text)
}

// limitTTSBytesLength 限制 TTS 字节长度
func limitTTSBytesLength(data []byte) []byte {
	if len(data) > maxTTSBytesLength {
		return data[:maxTTSBytesLength]
	}
	return data
}

// ==================== 编码函数 ====================

// EncodeTTSUCS2Hex 将文本转为字节对调形式的 HEX 编码(用于 TTS)
func EncodeTTSUCS2Hex(text string) string {
	var hexBuilder strings.Builder

	for _, runeValue := range text {
		if runeValue > 0xFFFF {
			continue
		}

		lowByte := byte(runeValue & 0xFF)
		highByte := byte((runeValue >> 8) & 0xFF)
		hexBuilder.WriteString(fmt.Sprintf("%02X%02X", lowByte, highByte))
	}

	return hexBuilder.String()
}

// encodeTextToGBK 将字符串编码为 GBK 字节
func encodeTextToGBK(text string) ([]byte, error) {
	encoder := simplifiedchinese.GBK.NewEncoder()
	return encoder.Bytes([]byte(text))
}

// ==================== 串口通信辅助函数 ====================

// writeCommandToPort 向串口写入指令
func writeCommandToPort(port io.ReadWriter, command string) error {
	_, err := port.Write([]byte(command))
	return err
}

// drainPortBuffer 清空串口读缓冲区
func drainPortBuffer(port io.ReadWriter, duration time.Duration) {
	deadline := time.Now().Add(duration)
	tempBuffer := make([]byte, 128)

	for time.Now().Before(deadline) {
		port.Read(tempBuffer)
		time.Sleep(readTimeout)
	}
}

// ensureCallTerminated 确保上一次通话已挂断
func ensureCallTerminated(port io.ReadWriter) error {
	if err := writeCommandToPort(port, "AT+CHUP\r"); err != nil {
		return err
	}
	drainPortBuffer(port, 800*time.Millisecond)
	return nil
}

// ==================== 串口响应等待函数 ====================

// waitForPortResponse 等待串口返回指定关键字
func waitForPortResponse(port io.ReadWriter, successKeyword, failureKeyword string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	responseBuffer := make([]byte, 0, defaultBufferSize)
	singleByte := make([]byte, 1)

	for time.Now().Before(deadline) {
		bytesRead, _ := port.Read(singleByte)
		if bytesRead == 0 {
			time.Sleep(readTimeout)
			continue
		}

		responseBuffer = append(responseBuffer, singleByte[0])
		response := string(responseBuffer)

		if strings.Contains(response, successKeyword) {
			return true
		}
		if strings.Contains(response, failureKeyword) {
			return false
		}

		responseBuffer = limitBufferSize(responseBuffer, 1024, defaultBufferSize)
	}

	return false
}

// waitForCallEstablished 等待通话建立
func waitForCallEstablished(port io.ReadWriter, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	lineBuffer := make([]byte, 0)
	singleByte := make([]byte, 1)
	nextQueryTime := time.Now()
	queryInterval := 250 * time.Millisecond

	for time.Now().Before(deadline) {
		if time.Now().After(nextQueryTime) {
			writeCommandToPort(port, "AT+CLCC\r")
			nextQueryTime = time.Now().Add(queryInterval)
		}

		bytesRead, _ := port.Read(singleByte)
		if bytesRead == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		currentByte := singleByte[0]
		if isLineTerminator(currentByte) {
			if isCallEstablishedLine(lineBuffer) {
				return true
			}
			if isCallFailedLine(lineBuffer) {
				return false
			}
			lineBuffer = lineBuffer[:0]
			continue
		}

		lineBuffer = append(lineBuffer, currentByte)
		lineBuffer = limitBufferSize(lineBuffer, maxLineBufferSize, 200)
	}

	return false
}

// isCallEstablishedLine 检查是否为通话建立的响应行
func isCallEstablishedLine(lineBuffer []byte) bool {
	line := strings.TrimSpace(string(lineBuffer))
	if line == "" {
		return false
	}

	upperLine := strings.ToUpper(line)
	if !strings.HasPrefix(upperLine, "+CLCC:") {
		return false
	}

	parts := strings.Split(line, ",")
	if len(parts) < 3 {
		return false
	}

	callState := strings.TrimSpace(parts[2])
	return strings.HasPrefix(callState, "0")
}

// isCallFailedLine 检查是否为通话失败的响应行
func isCallFailedLine(lineBuffer []byte) bool {
	line := strings.TrimSpace(string(lineBuffer))
	if line == "" {
		return false
	}

	upperLine := strings.ToUpper(line)
	failureKeywords := []string{"NO CARRIER", "BUSY", "NO ANSWER"}

	for _, keyword := range failureKeywords {
		if strings.Contains(upperLine, keyword) {
			return true
		}
	}

	return false
}

// waitForTTSCompletion 等待 TTS 播报完成
func waitForTTSCompletion(port io.ReadWriter, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	lineBuffer := make([]byte, 0)
	singleByte := make([]byte, 1)
	hasSeenNonZeroStatus := false

	for time.Now().Before(deadline) {
		bytesRead, _ := port.Read(singleByte)
		if bytesRead == 0 {
			time.Sleep(15 * time.Millisecond)
			continue
		}

		currentByte := singleByte[0]
		if isLineTerminator(currentByte) {
			status, found := extractTTSStatus(lineBuffer)
			if found {
				if status != "0" {
					hasSeenNonZeroStatus = true
				} else if hasSeenNonZeroStatus {
					return true
				}
			}

			if isCallTerminatedLine(lineBuffer) {
				return false
			}

			lineBuffer = lineBuffer[:0]
			continue
		}

		lineBuffer = append(lineBuffer, currentByte)
		lineBuffer = limitBufferSize(lineBuffer, maxLineBufferSize, 200)
	}

	return false
}

// extractTTSStatus 从响应行中提取 TTS 状态
func extractTTSStatus(lineBuffer []byte) (status string, found bool) {
	line := strings.TrimSpace(string(lineBuffer))
	if line == "" {
		return "", false
	}

	upperLine := strings.ToUpper(line)
	if !strings.HasPrefix(upperLine, "+CTTS:") {
		return "", false
	}

	parts := strings.Split(line, ":")
	if len(parts) < 2 {
		return "", false
	}

	status = strings.TrimSpace(parts[1])
	return status, true
}

// isCallTerminatedLine 检查是否为通话终止的响应行
func isCallTerminatedLine(lineBuffer []byte) bool {
	line := strings.TrimSpace(string(lineBuffer))
	upperLine := strings.ToUpper(line)
	return strings.Contains(upperLine, "NO CARRIER")
}

// ==================== 通用辅助函数 ====================

// isLineTerminator 判断字节是否为行终止符
func isLineTerminator(b byte) bool {
	return b == '\n' || b == '\r'
}

// limitBufferSize 限制缓冲区大小,防止内存溢出
func limitBufferSize(buffer []byte, maxSize, keepSize int) []byte {
	if len(buffer) > maxSize {
		return buffer[len(buffer)-keepSize:]
	}
	return buffer
}
