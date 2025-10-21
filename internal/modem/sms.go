package modem

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"push-gateway/internal/utils"
)

// ListSMS 列出并智能合并拆分的长短信，返回结构化列表
func (m *Modem) ListSMS() ([]SMS, error) {
	if err := m.ensureTextMode(); err != nil {
		return nil, fmt.Errorf("设置短信文本模式失败: %w", err)
	}

	response, err := m.SendCommand(CMD_LIST_SMS)
	if err != nil {
		return nil, fmt.Errorf("查询短信列表失败: %w", err)
	}

	smsList := m.parseSMSList(response)
	sort.Slice(smsList, func(i, j int) bool {
		return smsList[i].Index < smsList[j].Index
	})

	mergedList := m.mergeConcatenatedSMS(smsList)

	sort.Slice(mergedList, func(i, j int) bool {
		return mergedList[i].Index < mergedList[j].Index
	})

	fmt.Printf("📬 共收到 %d 条短信（合并后）\n", len(mergedList))
	for i, sms := range mergedList {
		fmt.Printf("[%d] 索引:%d 发件人:%s 时间:%s\n    内容: %s\n",
			i+1, sms.Index, sms.Sender, sms.Timestamp, sms.Text)
	}

	return mergedList, nil
}

// ensureTextMode 确保调制解调器处于短信文本模式
func (m *Modem) ensureTextMode() error {
	_, err := m.SendCommand(CMD_SMS_TEXT_MODE)
	return err
}

// parseSMSList 从原始 AT 响应中解析出 SMS 列表
func (m *Modem) parseSMSList(response string) []SMS {
	var smsList []SMS
	lines := strings.Split(response, "\n")
	inMessage := false
	var currentSMS SMS
	var messageText strings.Builder

	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" {
			continue
		}

		if strings.HasPrefix(trimmedLine, "+CMGL:") {
			if inMessage {
				currentSMS.RawHex = messageText.String()
				smsList = append(smsList, currentSMS)
				messageText.Reset()
			}
			inMessage = true
			currentSMS = m.parseCMGLLine(trimmedLine)
			continue
		}

		if trimmedLine == "OK" {
			if inMessage {
				currentSMS.RawHex = messageText.String()
				smsList = append(smsList, currentSMS)
				inMessage = false
				messageText.Reset()
			}
			continue
		}

		if inMessage && !strings.HasPrefix(trimmedLine, "AT") {
			if messageText.Len() > 0 {
				messageText.WriteString("\n")
			}
			messageText.WriteString(trimmedLine)
		}
	}

	// 处理最后一段未闭合的消息
	if inMessage {
		currentSMS.RawHex = messageText.String()
		smsList = append(smsList, currentSMS)
	}

	return smsList
}

// parseCMGLLine 解析 +CMGL: 开头的行，填充 SMS 基础字段
func (m *Modem) parseCMGLLine(line string) SMS {
	var sms SMS
	parts := strings.SplitN(strings.TrimPrefix(line, "+CMGL: "), ",", 5)
	if len(parts) < 1 {
		return sms
	}

	if index, err := strconv.Atoi(strings.Trim(parts[0], " \"")); err == nil {
		sms.Index = index
	}
	if len(parts) > 1 {
		sms.Status = strings.Trim(parts[1], " \"")
	}
	if len(parts) > 2 {
		senderHex := strings.Trim(parts[2], " \"")
		sms.Sender = decodeIfNeeded(senderHex)
	}
	if len(parts) > 4 {
		sms.Timestamp = strings.Trim(parts[4], " \"")
	}
	return sms
}

// mergeConcatenatedSMS 合并被拆分的长短信
func (m *Modem) mergeConcatenatedSMS(smsList []SMS) []SMS {
	var mergedList []SMS
	i := 0
	for i < len(smsList) {
		current := smsList[i]

		if isCompleteMessage(current.RawHex) {
			current.Text = decodeIfNeeded(current.RawHex)
			mergedList = append(mergedList, current)
			i++
			continue
		}

		// 收集连续的、来自同一发件人的片段
		group := []SMS{current}
		j := i + 1
		for j < len(smsList) &&
			smsList[j].Sender == current.Sender &&
			!isCompleteMessage(smsList[j-1].RawHex) {
			group = append(group, smsList[j])
			j++
		}

		var fullHex strings.Builder
		for _, sms := range group {
			fullHex.WriteString(sms.RawHex)
		}

		merged := group[0]
		merged.Text = decodeIfNeeded(fullHex.String())
		merged.Index = group[len(group)-1].Index
		merged.Timestamp = group[len(group)-1].Timestamp
		mergedList = append(mergedList, merged)

		i = j
	}
	return mergedList
}

// SendSMS 采用 PDU 方式发送 UCS2 编码短信
func (m *Modem) SendSMS(ctx context.Context, phoneNumber, message string) error {
	fmt.Printf("📤 正在发送短信到 %s...\n", phoneNumber)

	smscNumber, err := m.getSMSCNumber()
	if err != nil {
		return fmt.Errorf("获取短信中心失败: %w", err)
	}

	if err := m.switchToPDUCharset(); err != nil {
		return fmt.Errorf("设置PDU字符集失败: %w", err)
	}

	unicodeHex, err := utils.ToUnicodeHex(message)
	if err != nil {
		return fmt.Errorf("消息编码失败: %w", err)
	}

	pdu, bodyLength, err := m.buildPDUSMS(phoneNumber, unicodeHex, smscNumber)
	if err != nil {
		return fmt.Errorf("构造PDU失败: %w", err)
	}

	if err := m.sendPDUCommandAndWaitForPrompt(bodyLength); err != nil {
		return fmt.Errorf("发送PDU命令失败: %w", err)
	}

	if err := m.sendPDUDataAndConfirm(pdu); err != nil {
		return fmt.Errorf("发送PDU数据失败: %w", err)
	}

	fmt.Printf("✅ 短信已成功发送至 %s\n", phoneNumber)
	return nil
}

// getSMSCNumber 获取短信中心号码
func (m *Modem) getSMSCNumber() (string, error) {
	resp, err := m.SendCommand(CMD_GET_SMS_CENTER)
	if err != nil {
		return "", err
	}

	lines := strings.Split(resp, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "+CSCA:") {
			parts := strings.Split(trimmed, "\"")
			if len(parts) < 2 {
				continue
			}
			ucs2Hex := parts[1]
			if decoded, err := decodeUCS2(ucs2Hex); err == nil {
				fmt.Printf("📍 短信中心: %s (原始UCS2: %s)\n", decoded, ucs2Hex)
				return decoded, nil
			}
			fmt.Printf("⚠️ UCS2解码失败，使用原始值: %s\n", ucs2Hex)
			return ucs2Hex, nil
		}
	}
	fmt.Println("⚠️ 未获取到短信中心，使用默认配置")
	return "", nil
}

// switchToPDUCharset 切换到PDU模式并设置字符集
func (m *Modem) switchToPDUCharset() error {
	if _, err := m.SendCommand(CMD_SET_CHARSET); err != nil {
		return err
	}
	if _, err := m.SendCommand(CMD_SMS_PDU_MODE); err != nil {
		return err
	}
	return nil
}

// sendPDUCommandAndWaitForPrompt 发送 AT+CMGS 命令并等待 '>' 提示符
func (m *Modem) sendPDUCommandAndWaitForPrompt(length int) error {
	cmd := fmt.Sprintf("AT+CMGS=%d\r", length)
	fmt.Printf("📡 发送命令: %s", strings.TrimRight(cmd, "\r"))

	if _, err := m.port.Write([]byte(cmd)); err != nil {
		return fmt.Errorf("写入CMGS命令失败: %w", err)
	}

	buffer := make([]byte, 0, 100)
	timeout := time.After(15 * time.Second)

	for {
		select {
		case <-timeout:
			return fmt.Errorf("等待 '>' 提示符超时(15秒)")
		default:
			b := make([]byte, 1)
			n, err := m.port.Read(b)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				if err == io.EOF {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				return fmt.Errorf("读取提示符失败: %w", err)
			}

			if n > 0 {
				buffer = append(buffer, b[0])
				bufferStr := string(buffer)
				if strings.Contains(bufferStr, ">") {
					fmt.Println("✅ 收到提示符，准备发送PDU数据")
					return nil
				}
				if strings.Contains(bufferStr, "ERROR") {
					return fmt.Errorf("CMGS命令返回错误: %s", bufferStr)
				}
				if len(buffer) > 100 {
					buffer = buffer[len(buffer)-50:]
				}
			}
		}
	}
}

// sendPDUDataAndConfirm 发送PDU数据并等待发送确认
func (m *Modem) sendPDUDataAndConfirm(pdu string) error {
	pduData := pdu + string(rune(0x1A)) // Ctrl+Z
	fmt.Printf("📦 发送PDU数据 (%d字符) + Ctrl+Z\n", len(pdu))

	if _, err := m.port.Write([]byte(pduData)); err != nil {
		return fmt.Errorf("写入PDU数据失败: %w", err)
	}

	fmt.Println("⏳ 等待发送确认...")
	reader := bufio.NewReader(m.port)
	timeout := time.After(30 * time.Second)

	for {
		select {
		case <-timeout:
			return fmt.Errorf("等待短信发送确认超时(30秒)")
		default:
			line, err := reader.ReadString('\n')
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			fmt.Printf("📡 Modem: %s", line)

			if strings.Contains(line, "ERROR") {
				return fmt.Errorf("短信发送失败: %s", strings.TrimSpace(line))
			}
			if strings.Contains(line, "OK") || strings.Contains(line, "+CMGS:") {
				return nil
			}
		}
	}
}

// buildPDUSMS 构造完整 PDU 字符串及其在 CMGS 中需要的长度
func (m *Modem) buildPDUSMS(phoneNumber, unicodeHex, smscNumber string) (string, int, error) {
	encodedPhone := encodePhoneNumber(phoneNumber)
	smscPart := m.buildSMSCPart(smscNumber)

	unicodeByteCount := len(unicodeHex) / 2
	pduBody := "31" + // First octet (SMS-SUBMIT, no validity, no reply path)
		"00" + // TP-MR (Message Reference)
		fmt.Sprintf("%02X", len(phoneNumber)) +
		"81" + // Type of address (international, unknown)
		encodedPhone +
		"00" + // TP-PID (Protocol Identifier)
		"08" + // TP-DCS (UCS2)
		"A7" + // TP-VP (7 days validity)
		fmt.Sprintf("%02X", unicodeByteCount) +
		unicodeHex

	pdu := smscPart + pduBody
	pduBodyLen := len(pduBody) / 2

	fmt.Printf("🔍 PDU调试信息:\n")
	fmt.Printf("   短信中心: %s -> 编码: %s\n", smscNumber, smscPart)
	fmt.Printf("   手机号: %s -> 编码: %s\n", phoneNumber, encodedPhone)
	fmt.Printf("   Unicode字符数: %d\n", len(unicodeHex)/4)
	fmt.Printf("   Unicode内容: %s\n", unicodeHex)
	fmt.Printf("   PDU主体长度: %d字节 (用于CMGS命令)\n", pduBodyLen)
	fmt.Printf("   完整PDU长度: %d字节\n", len(pdu)/2)

	return pdu, pduBodyLen, nil
}

// buildSMSCPart 构建短信中心部分的PDU
func (m *Modem) buildSMSCPart(smscNumber string) string {
	if smscNumber == "" {
		return "00"
	}

	cleanSMSC := strings.TrimPrefix(smscNumber, "+86")
	cleanSMSC = strings.TrimPrefix(cleanSMSC, "+")
	if len(cleanSMSC)%2 != 0 {
		cleanSMSC += "F"
	}
	encodedSMSC := encodePhoneNumber(cleanSMSC)
	smscLen := len(cleanSMSC)/2 + 1
	return fmt.Sprintf("%02X91%s", smscLen, encodedSMSC)
}

// encodePhoneNumber 对电话号码做 3GPP 半字节交换 (奇数位补 F)
func encodePhoneNumber(phone string) string {
	phone = strings.TrimPrefix(phone, "+86")
	if len(phone)%2 != 0 {
		phone += "F"
	}
	var encoded strings.Builder
	for i := 0; i < len(phone); i += 2 {
		encoded.WriteByte(phone[i+1])
		encoded.WriteByte(phone[i])
	}
	return encoded.String()
}

// decodeIfNeeded 尝试解码十六进制字符串，若为纯ASCII则直接返回
func decodeIfNeeded(hexStr string) string {
	if decoded, err := decodeUCS2(hexStr); err == nil {
		return decoded
	}
	return hexStr
}
