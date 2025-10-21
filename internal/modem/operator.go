package modem

import (
	"fmt"
	"log"
	"strings"
	"time"
)

const (
	imsiMinLength   = 15
	imsiPrefixLen   = 5
	logPrefixDetect = "[MODEM]"
)

// DetectOperator 读取 IMSI 前缀并推断运营商类型（对外行为与签名保持不变）
func (m *Modem) DetectOperator() (OperatorType, error) {
	response, err := m.SendCommand(CMD_GET_IMSI)
	if err != nil {
		return OperatorUnknown, fmt.Errorf("获取IMSI失败: %w", err)
	}

	imsi, err := parseIMSIFromResponse(response)
	if err != nil {
		return OperatorUnknown, err
	}

	operator := operatorFromIMSI(imsi)
	m.operator = operator

	log.Printf("%s 运营商: %s (IMSI前缀: %s)", logPrefixDetect, operator.String(), imsi[:imsiPrefixLen])
	return operator, nil
}

// detectOperatorWithRetry 带重试的运营商检测（用于启动阶段 SIM 尚未就绪的情况）
func detectOperatorWithRetry(m *Modem, attempts int, interval time.Duration) (OperatorType, error) {
	var lastErr error
	for i := 1; i <= attempts; i++ {
		operator, err := m.DetectOperator()
		if err == nil && operator != OperatorUnknown {
			return operator, nil
		}
		lastErr = err
		log.Printf("%s 检测运营商第 %d/%d 次失败: %v", logPrefixDetect, i, attempts, err)
		if i < attempts {
			time.Sleep(interval)
		}
	}
	return OperatorUnknown, lastErr
}

// GetOperator 返回当前检测到的运营商类型（对外行为与签名保持不变）
func (m *Modem) GetOperator() OperatorType {
	return m.operator
}

// --------------------------- 内部通用逻辑（提取公共函数，简化主流程） ---------------------------

// parseIMSIFromResponse 从 AT 返回中提取 IMSI（首个长度≥15且全为数字的行）
func parseIMSIFromResponse(response string) (string, error) {
	for _, rawLine := range strings.Split(response, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "AT") || line == "OK" {
			continue
		}
		if len(line) >= imsiMinLength && isAllDigits(line) {
			return line, nil
		}
	}
	return "", fmt.Errorf("未获取到IMSI")
}

// operatorFromIMSI 根据 IMSI 前缀推断运营商（未知返回 OperatorUnknown）
func operatorFromIMSI(imsi string) OperatorType {
	if len(imsi) < imsiPrefixLen {
		return OperatorUnknown
	}

	prefix := imsi[:imsiPrefixLen]
	switch prefix {
	case "46000", "46002", "46007", "46008": // 中国移动（含部分物联网）
		return OperatorChinaMobile
	case "46001", "46006", "46009": // 中国联通
		return OperatorChinaUnicom
	case "46003", "46005", "46011": // 中国电信
		return OperatorChinaTelecom
	case "46020": // 中国铁通
		return OperatorChinaTietong
	default:
		return OperatorUnknown
	}
}

// isAllDigits 判断字符串是否全为数字（保留原函数名与签名，便于无感替换）
func isAllDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
