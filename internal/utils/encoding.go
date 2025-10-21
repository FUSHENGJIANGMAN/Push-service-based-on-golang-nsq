package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/text/encoding/simplifiedchinese"
)

// ToUnicodeHex 将字符串转为UNICODE编码（字节互换）的Hex字符串
// ToUnicodeHexRaw 将原始字节按 UTF-16LE 编码为 Hex（支持\0等控制字符）
func ToUnicodeHexRaw(data []byte) (string, error) {
	var buf bytes.Buffer
	for _, b := range data {
		// 强制转为 uint16，低字节为 b，高字节为 0（兼容 ASCII 和控制字符）
		err := binary.Write(&buf, binary.BigEndian, uint16(b))
		if err != nil {
			return "", err
		}
	}

	// 字节互换：UTF-16BE → UTF-16LE 样式（Modem 要求）
	out := buf.Bytes()
	for i := 0; i < len(out); i += 2 {
		if i+1 < len(out) {
			out[i], out[i+1] = out[i+1], out[i]
		}
	}

	return hex.EncodeToString(out), nil
}

// ToUnicodeHex 将字符串转为正确的UCS2编码Hex字符串
func ToUnicodeHex(s string) (string, error) {
	// 将字符串转为UTF-16编码的rune切片
	runes := []rune(s)
	var result strings.Builder

	for _, r := range runes {
		// 将每个rune转为2字节的UTF-16编码（大端序）
		high := byte(r >> 8)
		low := byte(r & 0xFF)

		// 按照设备手册要求的格式输出
		result.WriteString(fmt.Sprintf("%02X%02X", high, low))
	}

	return result.String(), nil
}

// ToGBKHex 将字符串转为GBK编码的Hex字符串
func ToGBKHex(s string) (string, error) {
	gbkBytes, err := simplifiedchinese.GBK.NewEncoder().Bytes([]byte(s))
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(gbkBytes), nil
}

// HexToBytes 将Hex字符串转为[]byte
func HexToBytes(hexStr string) ([]byte, error) {
	hexStr = strings.ReplaceAll(hexStr, " ", "")
	if len(hexStr)%2 != 0 {
		return nil, errors.New("hex string must have even length")
	}
	return hex.DecodeString(hexStr)
}
