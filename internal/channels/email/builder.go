package email

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"mime"
	"net/textproto"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"push-gateway/internal/config"
	"push-gateway/internal/push"
)

//
// 常量定义
//

const (
	// MIME 相关常量
	mimeVersion           = "1.0"
	contentTransferBase64 = "base64"
	contentTransfer8Bit   = "8bit"
	charsetUTF8           = "UTF-8"
	defaultContentType    = "application/octet-stream"

	// 邮件格式常量
	base64LineLength  = 76
	defaultSenderName = "系统通知"
	defaultSubject    = "(no subject)"
	defaultBody       = "您收到了一条新的推送消息。"

	// 邮件头常量
	headerFrom        = "From"
	headerTo          = "To"
	headerCC          = "CC"
	headerSubject     = "Subject"
	headerMimeVersion = "MIME-Version"
	headerXMailer     = "X-Mailer"
	headerDate        = "Date"
	headerContentType = "Content-Type"

	// 其他常量
	mailerIdentifier = "PushGateway/1.0"
	lineBreak        = "\r\n"
	doubleLineBreak  = "\r\n\r\n"
)

//
// 数据模型
//

// BuildParams 邮件构建参数
// 包含构建完整邮件所需的所有信息
type BuildParams struct {
	Subject     string
	BodyHTML    string
	To          []string
	CC          []string
	BCC         []string
	Attachments []push.EmailAttachment
	Data        map[string]any
}

//
// 邮件构建器
//

// EmailBuilder 邮件构建器
// 负责生成符合 RFC822 标准的邮件内容
type EmailBuilder struct {
	configuration config.EmailProvider
}

// NewEmailBuilder 创建邮件构建器实例
func NewEmailBuilder(configuration config.EmailProvider) *EmailBuilder {
	return &EmailBuilder{
		configuration: configuration,
	}
}

// Build 构建完整的邮件内容
// 生成符合 RFC822 标准的原始邮件数据
func (builder *EmailBuilder) Build(params BuildParams) ([]byte, error) {
	if err := builder.validateParams(params); err != nil {
		return nil, err
	}

	boundary := builder.generateBoundary()
	headers := builder.buildHeaders(params, boundary)

	var content strings.Builder
	builder.writeHeaders(&content, headers)
	builder.writeBody(&content, params.BodyHTML, boundary)

	if err := builder.writeAttachments(&content, params.Attachments, boundary); err != nil {
		return nil, err
	}

	builder.writeEndBoundary(&content, boundary)

	return []byte(content.String()), nil
}

//
// 验证方法
//

// validateParams 验证构建参数
func (builder *EmailBuilder) validateParams(params BuildParams) error {
	if len(params.To) == 0 {
		return fmt.Errorf("收件人列表不能为空")
	}

	return nil
}

//
// 头部构建方法
//

// buildHeaders 构建邮件头部
func (builder *EmailBuilder) buildHeaders(
	params BuildParams,
	boundary string,
) textproto.MIMEHeader {
	headers := textproto.MIMEHeader{}

	builder.setFromHeader(headers)
	builder.setToHeader(headers, params.To)
	builder.setCCHeader(headers, params.CC)
	builder.setSubjectHeader(headers, params.Subject)
	builder.setStandardHeaders(headers, boundary)

	return headers
}

// setFromHeader 设置发件人头部
func (builder *EmailBuilder) setFromHeader(headers textproto.MIMEHeader) {
	senderName := builder.getSenderName()
	fromAddress := fmt.Sprintf("%s <%s>", senderName, builder.configuration.From)
	headers.Set(headerFrom, fromAddress)
}

// setToHeader 设置收件人头部
func (builder *EmailBuilder) setToHeader(headers textproto.MIMEHeader, recipients []string) {
	headers.Set(headerTo, strings.Join(recipients, ", "))
}

// setCCHeader 设置抄送头部
func (builder *EmailBuilder) setCCHeader(headers textproto.MIMEHeader, ccList []string) {
	if len(ccList) > 0 {
		headers.Set(headerCC, strings.Join(ccList, ", "))
	}
}

// setSubjectHeader 设置主题头部
func (builder *EmailBuilder) setSubjectHeader(headers textproto.MIMEHeader, subject string) {
	encodedSubject := encodeSubject(subject)
	headers.Set(headerSubject, encodedSubject)
}

// setStandardHeaders 设置标准头部
func (builder *EmailBuilder) setStandardHeaders(
	headers textproto.MIMEHeader,
	boundary string,
) {
	headers.Set(headerMimeVersion, mimeVersion)
	headers.Set(headerXMailer, mailerIdentifier)
	headers.Set(headerDate, time.Now().Format(time.RFC1123Z))
	headers.Set(
		headerContentType,
		fmt.Sprintf("multipart/mixed; boundary=\"%s\"", boundary),
	)
}

// getSenderName 获取发件人名称
func (builder *EmailBuilder) getSenderName() string {
	if builder.configuration.FromName != "" {
		return builder.configuration.FromName
	}

	return defaultSenderName
}

// generateBoundary 生成 MIME 边界字符串
func (builder *EmailBuilder) generateBoundary() string {
	return fmt.Sprintf("mixed_%d", time.Now().UnixNano())
}

//
// 内容写入方法
//

// writeHeaders 写入邮件头部
func (builder *EmailBuilder) writeHeaders(
	content *strings.Builder,
	headers textproto.MIMEHeader,
) {
	for key, values := range headers {
		for _, value := range values {
			content.WriteString(fmt.Sprintf("%s: %s%s", key, value, lineBreak))
		}
	}

	content.WriteString(lineBreak)
}

// writeBody 写入邮件正文
func (builder *EmailBuilder) writeBody(
	content *strings.Builder,
	bodyHTML string,
	boundary string,
) {
	content.WriteString(fmt.Sprintf("--%s%s", boundary, lineBreak))
	content.WriteString(fmt.Sprintf("Content-Type: text/html; charset=%s%s", charsetUTF8, lineBreak))
	content.WriteString(fmt.Sprintf("Content-Transfer-Encoding: %s%s", contentTransfer8Bit, doubleLineBreak))
	content.WriteString(bodyHTML)
	content.WriteString(lineBreak)
}

// writeAttachments 写入所有附件
func (builder *EmailBuilder) writeAttachments(
	content *strings.Builder,
	attachments []push.EmailAttachment,
	boundary string,
) error {
	for _, attachment := range attachments {
		if err := builder.writeAttachment(content, attachment, boundary); err != nil {
			return err
		}
	}

	return nil
}

// writeAttachment 写入单个附件
func (builder *EmailBuilder) writeAttachment(
	content *strings.Builder,
	attachment push.EmailAttachment,
	boundary string,
) error {
	attachmentContent, err := builder.loadAttachmentContent(attachment)
	if err != nil {
		return err
	}

	fileName := builder.getAttachmentFileName(attachment)
	mimeType := builder.getAttachmentMimeType(attachment)
	encodedContent := base64.StdEncoding.EncodeToString(attachmentContent)

	builder.writeAttachmentHeaders(content, boundary, fileName, mimeType)
	builder.writeEncodedContent(content, encodedContent)

	return nil
}

// writeEndBoundary 写入结束边界
func (builder *EmailBuilder) writeEndBoundary(content *strings.Builder, boundary string) {
	content.WriteString(fmt.Sprintf("--%s--%s", boundary, lineBreak))
}

//
// 附件处理方法
//

// loadAttachmentContent 加载附件内容
func (builder *EmailBuilder) loadAttachmentContent(
	attachment push.EmailAttachment,
) ([]byte, error) {
	// 优先使用已有内容
	if len(attachment.Content) > 0 {
		return attachment.Content, nil
	}

	// 从文件路径加载
	if attachment.FilePath != "" {
		return builder.readAttachmentFile(attachment.FilePath)
	}

	return nil, fmt.Errorf("附件内容为空: %s", attachment.FileName)
}

// readAttachmentFile 读取附件文件
func (builder *EmailBuilder) readAttachmentFile(filePath string) ([]byte, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("附件文件不存在: %s", filePath)
		}
		return nil, fmt.Errorf("读取附件失败 %s: %w", filePath, err)
	}

	return data, nil
}

// getAttachmentFileName 获取附件文件名
func (builder *EmailBuilder) getAttachmentFileName(attachment push.EmailAttachment) string {
	if attachment.FileName != "" {
		return attachment.FileName
	}

	return fmt.Sprintf("file_%d", time.Now().UnixNano())
}

// getAttachmentMimeType 获取附件 MIME 类型
func (builder *EmailBuilder) getAttachmentMimeType(attachment push.EmailAttachment) string {
	// 优先使用指定的类型
	if attachment.FileType != "" {
		return attachment.FileType
	}

	// 从文件扩展名推断
	extension := strings.ToLower(filepath.Ext(attachment.FileName))
	if mimeType := mime.TypeByExtension(extension); mimeType != "" {
		return mimeType
	}

	return defaultContentType
}

// writeAttachmentHeaders 写入附件头部
func (builder *EmailBuilder) writeAttachmentHeaders(
	content *strings.Builder,
	boundary string,
	fileName string,
	mimeType string,
) {
	content.WriteString(fmt.Sprintf("--%s%s", boundary, lineBreak))
	content.WriteString(fmt.Sprintf("Content-Type: %s; name=\"%s\"%s", mimeType, fileName, lineBreak))
	content.WriteString(fmt.Sprintf("Content-Transfer-Encoding: %s%s", contentTransferBase64, lineBreak))
	content.WriteString(fmt.Sprintf("Content-Disposition: attachment; filename=\"%s\"%s", fileName, doubleLineBreak))
}

// writeEncodedContent 写入 Base64 编码的内容
// 每 76 个字符换行以符合 MIME 标准
func (builder *EmailBuilder) writeEncodedContent(
	content *strings.Builder,
	encodedContent string,
) {
	for startIndex := 0; startIndex < len(encodedContent); startIndex += base64LineLength {
		endIndex := startIndex + base64LineLength
		if endIndex > len(encodedContent) {
			endIndex = len(encodedContent)
		}

		content.WriteString(encodedContent[startIndex:endIndex])
		content.WriteString(lineBreak)
	}
}

//
// 主题编码
//

// encodeSubject 编码邮件主题
// 如果包含非 ASCII 字符则使用 Base64 编码
func encodeSubject(subject string) string {
	if subject == "" {
		return defaultSubject
	}

	if containsNonASCII(subject) {
		return encodeToBase64Subject(subject)
	}

	return subject
}

// containsNonASCII 检查字符串是否包含非 ASCII 字符
func containsNonASCII(text string) bool {
	for _, character := range text {
		if character > 127 {
			return true
		}
	}

	return false
}

// encodeToBase64Subject 将主题编码为 Base64 格式
func encodeToBase64Subject(subject string) string {
	encoded := base64.StdEncoding.EncodeToString([]byte(subject))
	return fmt.Sprintf("=?UTF-8?B?%s?=", encoded)
}

//
// 地址处理工具
//

// MergeAddresses 合并并去重地址列表
// 移除空白地址和重复地址
func MergeAddresses(addressLists ...[]string) []string {
	addressSet := make(map[string]struct{})
	var orderedAddresses []string

	for _, addressList := range addressLists {
		for _, address := range addressList {
			cleanedAddress := strings.TrimSpace(address)

			if cleanedAddress == "" {
				continue
			}

			if _, exists := addressSet[cleanedAddress]; !exists {
				addressSet[cleanedAddress] = struct{}{}
				orderedAddresses = append(orderedAddresses, cleanedAddress)
			}
		}
	}

	return orderedAddresses
}

//
// HTML 内容生成
//

// DeriveBodyTable 从数据生成 HTML 表格
// 用于将结构化数据转换为可读的 HTML 格式
func DeriveBodyTable(data map[string]any, mainContent string) string {
	if data == nil {
		return getDefaultOrMainContent(mainContent)
	}

	var htmlBuilder strings.Builder
	htmlBuilder.WriteString(buildHTMLContainer())

	if mainContent != "" {
		htmlBuilder.WriteString(buildMainContentSection(mainContent))
	}

	htmlBuilder.WriteString(buildDataTable(data))
	htmlBuilder.WriteString(buildTimestamp())
	htmlBuilder.WriteString("</div>")

	return htmlBuilder.String()
}

// getDefaultOrMainContent 获取默认或主要内容
func getDefaultOrMainContent(mainContent string) string {
	if mainContent == "" {
		return defaultBody
	}
	return mainContent
}

// buildHTMLContainer 构建 HTML 容器
func buildHTMLContainer() string {
	return "<div style='font-family:Arial;margin:0;padding:0;'>"
}

// buildMainContentSection 构建主要内容区域
func buildMainContentSection(content string) string {
	style := "padding:12px;background:#f8f9fa;border-left:4px solid #007bff;margin-bottom:12px;"
	return fmt.Sprintf("<div style='%s'>%s</div>", style, HTMLEscape(content))
}

// buildDataTable 构建数据表格
func buildDataTable(data map[string]any) string {
	keys := extractSortedKeys(data)

	var tableBuilder strings.Builder
	tableBuilder.WriteString("<table style='border-collapse:collapse;width:100%;font-size:13px;'>")

	for _, key := range keys {
		value := data[key]
		tableBuilder.WriteString(buildTableRow(key, value))
	}

	tableBuilder.WriteString("</table>")
	return tableBuilder.String()
}

// extractSortedKeys 提取并排序数据键
// 排除特殊字段如 subject 和 body
func extractSortedKeys(data map[string]any) []string {
	var keys []string

	for key := range data {
		if key != "subject" && key != "body" {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)
	return keys
}

// buildTableRow 构建表格行
func buildTableRow(key string, value any) string {
	keyStyle := "border:1px solid #eee;padding:6px 10px;background:#fafafa;font-weight:600;width:140px;"
	valueStyle := "border:1px solid #eee;padding:6px 10px;"

	formattedValue := fmt.Sprintf("%v", value)

	return fmt.Sprintf(
		"<tr><td style='%s'>%s</td><td style='%s'>%s</td></tr>",
		keyStyle,
		HTMLEscape(key),
		valueStyle,
		HTMLEscape(formattedValue),
	)
}

// buildTimestamp 构建时间戳
func buildTimestamp() string {
	style := "margin-top:18px;color:#888;font-size:11px;text-align:center;"
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	return fmt.Sprintf("<div style='%s'>发送时间: %s</div>", style, timestamp)
}

// HTMLEscape HTML 转义
// 防止 XSS 攻击
func HTMLEscape(text string) string {
	replacer := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		"\"", "&quot;",
	)

	return replacer.Replace(text)
}
