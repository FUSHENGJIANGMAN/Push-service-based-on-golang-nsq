package modem

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// ==================== 常量定义 ====================

const (
	// UTF-16 代理对范围常量
	surrogatePairHighStart = 0xD800  // 高位代理对起始
	surrogatePairHighEnd   = 0xDBFF  // 高位代理对结束
	surrogatePairLowStart  = 0xDC00  // 低位代理对起始
	surrogatePairLowEnd    = 0xDFFF  // 低位代理对结束
	surrogatePairBase      = 0x10000 // 代理对基础偏移量

	// UTF-16 编码单元大小
	utf16BytesPerCharacter = 2
	utf16HexCharsPerUnit   = 4

	// UCS2 编码单元大小
	ucs2HexCharsPerUnit = 4
)

// ==================== 句末标点判断 ====================

var sentenceEndingPunctuations = []rune{'。', '！', '？', '】', '>', '.'}

// isCompleteMessage 判断消息内容是否以句末标点结束（认为是完整消息）
func isCompleteMessage(hexString string) bool {
	decodedText := decodeUTF16BEHexIfNeeded(hexString)
	decodedText = strings.TrimSpace(decodedText)

	if decodedText == "" {
		return true
	}

	return endsWithSentencePunctuation(decodedText)
}

// endsWithSentencePunctuation 检查文本是否以句末标点结束
func endsWithSentencePunctuation(text string) bool {
	runes := []rune(text)
	if len(runes) == 0 {
		return false
	}

	lastCharacter := runes[len(runes)-1]
	for _, punctuation := range sentenceEndingPunctuations {
		if lastCharacter == punctuation {
			return true
		}
	}

	return false
}

// ==================== UTF-16BE HEX 自动检测与解码 ====================

// decodeUTF16BEHexIfNeeded 自动检测字符串是否为 UTF-16BE HEX 格式并尝试解码
func decodeUTF16BEHexIfNeeded(input string) string {
	cleanedInput := cleanHexString(input)

	if !isValidUTF16BEHex(cleanedInput) {
		return input
	}

	decodedText, err := decodeUTF16BEHex(cleanedInput)
	if err != nil {
		return input
	}

	return decodedText
}

// cleanHexString 清理 HEX 字符串（移除空格和换行）
func cleanHexString(input string) string {
	cleaned := strings.ReplaceAll(input, "\n", "")
	cleaned = strings.ReplaceAll(cleaned, " ", "")
	return cleaned
}

// isValidUTF16BEHex 验证字符串是否为有效的 UTF-16BE HEX 格式
func isValidUTF16BEHex(input string) bool {
	// 长度必须是 4 的倍数（每个 UTF-16 单元占 4 个 HEX 字符）
	if len(input)%utf16HexCharsPerUnit != 0 {
		return false
	}

	// 检查是否所有字符都是有效的 HEX 字符
	return isHexString(input)
}

// isHexString 检查字符串是否只包含 HEX 字符（0-9, A-F, a-f）
func isHexString(input string) bool {
	for _, char := range input {
		if !isHexCharacter(char) {
			return false
		}
	}
	return true
}

// isHexCharacter 判断单个字符是否为有效的 HEX 字符
func isHexCharacter(char rune) bool {
	return (char >= '0' && char <= '9') ||
		(char >= 'A' && char <= 'F') ||
		(char >= 'a' && char <= 'f')
}

// ==================== UTF-16BE HEX 解码核心逻辑 ====================

// decodeUTF16BEHex 将 UTF-16BE HEX 字符串解码为普通文本
func decodeUTF16BEHex(hexString string) (string, error) {
	decodedBytes, err := hex.DecodeString(hexString)
	if err != nil {
		return "", fmt.Errorf("HEX 解码失败: %v", err)
	}

	if len(decodedBytes)%utf16BytesPerCharacter != 0 {
		return "", fmt.Errorf("UTF-16BE 字节长度必须是偶数")
	}

	runes := decodeUTF16BEBytes(decodedBytes)
	return string(runes), nil
}

// decodeUTF16BEBytes 将 UTF-16BE 字节数组解码为 rune 切片
func decodeUTF16BEBytes(data []byte) []rune {
	var runes []rune
	index := 0

	for index < len(data) {
		if index+1 >= len(data) {
			break
		}

		codeUnit := readUTF16CodeUnit(data, index)

		// 检查是否为高位代理对（需要与低位代理对组合）
		if isHighSurrogatePair(codeUnit) {
			runeValue, consumed := decodeSurrogatePair(data, index)
			if consumed > 0 {
				runes = append(runes, runeValue)
				index += consumed
				continue
			}
		}

		// 普通 BMP 字符（基本多文种平面）
		runes = append(runes, rune(codeUnit))
		index += utf16BytesPerCharacter
	}

	return runes
}

// readUTF16CodeUnit 从字节数组中读取一个 UTF-16 编码单元（大端序）
func readUTF16CodeUnit(data []byte, offset int) uint16 {
	highByte := data[offset]
	lowByte := data[offset+1]
	return uint16(highByte)<<8 | uint16(lowByte)
}

// isHighSurrogatePair 判断是否为高位代理对
func isHighSurrogatePair(codeUnit uint16) bool {
	return codeUnit >= surrogatePairHighStart && codeUnit <= surrogatePairHighEnd
}

// isLowSurrogatePair 判断是否为低位代理对
func isLowSurrogatePair(codeUnit uint16) bool {
	return codeUnit >= surrogatePairLowStart && codeUnit <= surrogatePairLowEnd
}

// decodeSurrogatePair 解码 UTF-16 代理对为完整的 Unicode 码点
// 返回：解码后的 rune 值和消耗的字节数
func decodeSurrogatePair(data []byte, offset int) (rune, int) {
	// 确保有足够的字节读取低位代理对
	if offset+3 >= len(data) {
		return 0, 0
	}

	highSurrogate := readUTF16CodeUnit(data, offset)
	lowSurrogate := readUTF16CodeUnit(data, offset+utf16BytesPerCharacter)

	// 验证低位代理对的有效性
	if !isLowSurrogatePair(lowSurrogate) {
		return 0, 0
	}

	// 计算完整的 Unicode 码点
	// 公式: 0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00)
	highValue := uint32(highSurrogate - surrogatePairHighStart)
	lowValue := uint32(lowSurrogate - surrogatePairLowStart)
	runeValue := surrogatePairBase + (highValue << 10) + lowValue

	return rune(runeValue), utf16BytesPerCharacter * 2
}

// ==================== UCS2 解码 ====================

// decodeUCS2 将 UCS2 HEX 字符串（4字节一组的 UTF-16BE）解码为文本
func decodeUCS2(ucs2HexString string) (string, error) {
	if err := validateUCS2Format(ucs2HexString); err != nil {
		return "", err
	}

	var resultBuilder strings.Builder
	for offset := 0; offset < len(ucs2HexString); offset += ucs2HexCharsPerUnit {
		hexChunk := ucs2HexString[offset : offset+ucs2HexCharsPerUnit]

		character, err := parseUCS2Character(hexChunk)
		if err != nil {
			return "", fmt.Errorf("解析 UCS2 字符失败(位置 %d): %v", offset, err)
		}

		resultBuilder.WriteRune(character)
	}

	return resultBuilder.String(), nil
}

// validateUCS2Format 验证 UCS2 HEX 字符串格式
func validateUCS2Format(ucs2HexString string) error {
	if len(ucs2HexString)%ucs2HexCharsPerUnit != 0 {
		return fmt.Errorf("UCS2 字符串长度必须是 %d 的倍数，当前长度: %d",
			ucs2HexCharsPerUnit, len(ucs2HexString))
	}
	return nil
}

// parseUCS2Character 解析单个 UCS2 字符（4 个 HEX 字符）
func parseUCS2Character(hexChunk string) (rune, error) {
	var codePoint uint16
	_, err := fmt.Sscanf(hexChunk, "%04X", &codePoint)
	if err != nil {
		return 0, fmt.Errorf("无效的 HEX 格式 '%s': %v", hexChunk, err)
	}
	return rune(codePoint), nil
}
