package email

import (
	"context"
	"fmt"
	"strings"

	"push-gateway/internal/config"
	"push-gateway/internal/push"
)

//
// 常量定义
//

const (
	providerName = "email_smtp"

	// 数据字段键名
	dataKeyCC       = "cc"
	dataKeyBCC      = "bcc"
	dataKeySubject  = "subject"
	dataKeyTitle    = "title"
	dataKeyBody     = "body"
	dataKeyContent  = "content"
	dataKeyMessage  = "message"
	dataKeySeverity = "severity"

	// 默认主题
	defaultAlertSubject        = "系统告警通知"
	defaultNotificationSubject = "系统通知"
	defaultMessageSubject      = "消息通知"
	alertSubjectPrefix         = "系统告警 - "

	// 消息类型
	messageKindAlert        = "alert"
	messageKindNotification = "notification"
	messageKindMsg          = "msg"
)

//
// SMTP 邮件提供者
//

// SMTPProvider SMTP 邮件服务提供者
// 负责通过 SMTP 协议发送邮件
type SMTPProvider struct {
	providerName  string
	configuration config.EmailProvider
	transport     *SMTPTransport
	builder       *EmailBuilder
}

// NewSMTP 创建 SMTP 邮件提供者实例
func NewSMTP(configuration config.EmailProvider) *SMTPProvider {
	return &SMTPProvider{
		providerName:  providerName,
		configuration: configuration,
		transport:     NewSMTPTransport(configuration),
		builder:       NewEmailBuilder(configuration),
	}
}

// NewSES 创建 SES 邮件提供者实例 (向后兼容)
// 内部使用相同的 SMTP 实现
func NewSES(configuration config.EmailProvider) *SMTPProvider {
	return NewSMTP(configuration)
}

// Name 返回提供者名称
func (provider *SMTPProvider) Name() string {
	return provider.providerName
}

// Channels 返回支持的通道列表
func (provider *SMTPProvider) Channels() []push.Channel {
	return []push.Channel{push.ChEmail}
}

// SetGlobalCC 设置全局抄送列表
// 用于动态配置所有邮件的默认抄送地址
func (provider *SMTPProvider) SetGlobalCC(ccAddresses []string) {
	provider.configuration.CCAddresses = ccAddresses
}

//
// 邮件发送主流程
//

// Send 发送邮件
// 完整的邮件发送流程,包括地址收集、内容构建和实际发送
func (provider *SMTPProvider) Send(
	ctx context.Context,
	message push.Message,
	plan push.RoutePlan,
) error {
	recipients := provider.collectRecipients(message)
	if err := provider.validateRecipients(recipients); err != nil {
		return err
	}

	emailContent, err := provider.buildEmailContent(message, recipients)
	if err != nil {
		return fmt.Errorf("构建邮件内容失败: %w", err)
	}

	return provider.sendEmail(ctx, emailContent, recipients)
}

//
// 收件人处理
//

// EmailRecipients 邮件收件人信息
type EmailRecipients struct {
	To                 []string
	CC                 []string
	BCC                []string
	EnvelopeRecipients []string
}

// collectRecipients 收集所有收件人信息
func (provider *SMTPProvider) collectRecipients(message push.Message) EmailRecipients {
	toAddresses := provider.collectPrimaryRecipients(message)
	ccAddresses := provider.collectCCAddresses(message)
	bccAddresses := provider.collectBCCAddresses(message)
	envelopeRecipients := MergeAddresses(toAddresses, ccAddresses, bccAddresses)

	return EmailRecipients{
		To:                 toAddresses,
		CC:                 ccAddresses,
		BCC:                bccAddresses,
		EnvelopeRecipients: envelopeRecipients,
	}
}

// collectPrimaryRecipients 收集主要收件人
func (provider *SMTPProvider) collectPrimaryRecipients(message push.Message) []string {
	var addresses []string

	for _, recipient := range message.To {
		if recipient.Email != "" {
			addresses = append(addresses, strings.TrimSpace(recipient.Email))
		}
	}

	return MergeAddresses(addresses)
}

// collectCCAddresses 收集抄送地址
// 合并全局配置和消息级别的抄送地址
func (provider *SMTPProvider) collectCCAddresses(message push.Message) []string {
	messageLevelCC := extractAddressList(message.Data, dataKeyCC)
	return MergeAddresses(provider.configuration.CCAddresses, messageLevelCC)
}

// collectBCCAddresses 收集密送地址
func (provider *SMTPProvider) collectBCCAddresses(message push.Message) []string {
	messageLevelBCC := extractAddressList(message.Data, dataKeyBCC)
	return MergeAddresses(messageLevelBCC)
}

// validateRecipients 验证收件人
func (provider *SMTPProvider) validateRecipients(recipients EmailRecipients) error {
	if len(recipients.To) == 0 {
		return fmt.Errorf("收件人列表不能为空")
	}

	if len(recipients.EnvelopeRecipients) == 0 {
		return fmt.Errorf("信封收件人列表不能为空")
	}

	return nil
}

//
// 邮件内容构建
//

// buildEmailContent 构建邮件内容
func (provider *SMTPProvider) buildEmailContent(
	message push.Message,
	recipients EmailRecipients,
) ([]byte, error) {
	subject, bodyHTML := provider.deriveSubjectAndBody(message)

	params := BuildParams{
		Subject:     subject,
		BodyHTML:    bodyHTML,
		To:          recipients.To,
		CC:          recipients.CC,
		BCC:         recipients.BCC,
		Attachments: message.Attachments,
		Data:        message.Data,
	}

	return provider.builder.Build(params)
}

// deriveSubjectAndBody 提取主题和正文
// 从消息的多个可能字段中提取内容
func (provider *SMTPProvider) deriveSubjectAndBody(
	message push.Message,
) (string, string) {
	subject := provider.extractSubject(message)
	mainBody := provider.extractMainBody(message)
	bodyHTML := DeriveBodyTable(message.Data, mainBody)

	return subject, bodyHTML
}

// extractSubject 提取邮件主题
func (provider *SMTPProvider) extractSubject(message push.Message) string {
	// 优先使用消息的 Subject 字段
	if message.Subject != "" {
		return message.Subject
	}

	// 尝试从 Data 中提取
	subject := provider.extractSubjectFromData(message.Data)
	if subject != "" {
		return subject
	}

	// 使用默认主题
	return provider.getDefaultSubject(message.Kind, message.Data)
}

// extractSubjectFromData 从数据中提取主题
func (provider *SMTPProvider) extractSubjectFromData(data map[string]any) string {
	if data == nil {
		return ""
	}

	// 尝试多个可能的字段
	subjectFields := []string{dataKeySubject, dataKeyTitle}

	for _, field := range subjectFields {
		if value, ok := data[field].(string); ok && value != "" {
			return value
		}
	}

	return ""
}

// extractMainBody 提取邮件正文
func (provider *SMTPProvider) extractMainBody(message push.Message) string {
	// 优先使用消息的 Body 字段
	if message.Body != "" {
		return message.Body
	}

	// 尝试从 Data 中提取
	return provider.extractBodyFromData(message.Data)
}

// extractBodyFromData 从数据中提取正文
func (provider *SMTPProvider) extractBodyFromData(data map[string]any) string {
	if data == nil {
		return ""
	}

	// 尝试多个可能的字段
	bodyFields := []string{dataKeyBody, dataKeyContent, dataKeyMessage}

	for _, field := range bodyFields {
		if value, ok := data[field].(string); ok && value != "" {
			return value
		}
	}

	return ""
}

// getDefaultSubject 根据消息类型获取默认主题
func (provider *SMTPProvider) getDefaultSubject(
	kind push.MessageKind,
	data map[string]any,
) string {
	switch kind {
	case messageKindAlert:
		return provider.getAlertSubject(data)
	case messageKindNotification:
		return defaultNotificationSubject
	case messageKindMsg:
		return defaultMessageSubject
	default:
		return provider.getGenericSubject(kind)
	}
}

// getAlertSubject 获取告警类型的主题
func (provider *SMTPProvider) getAlertSubject(data map[string]any) string {
	if data == nil {
		return defaultAlertSubject
	}

	severity, ok := data[dataKeySeverity].(string)
	if ok && severity != "" {
		return alertSubjectPrefix + severity
	}

	return defaultAlertSubject
}

// getGenericSubject 获取通用主题
func (provider *SMTPProvider) getGenericSubject(kind push.MessageKind) string {
	return fmt.Sprintf("%s通知", strings.ToUpper(string(kind)))
}

//
// 邮件发送
//

// sendEmail 发送邮件
func (provider *SMTPProvider) sendEmail(
	ctx context.Context,
	emailContent []byte,
	recipients EmailRecipients,
) error {
	err := provider.transport.SendRaw(ctx, emailContent, recipients.EnvelopeRecipients)
	if err != nil {
		return fmt.Errorf("SMTP 发送失败: %w", err)
	}

	return nil
}

//
// 地址列表提取工具
//

// extractAddressList 从数据中提取地址列表
// 支持多种数据格式: string, []string, []interface{}
func extractAddressList(data map[string]any, key string) []string {
	if data == nil {
		return nil
	}

	value, exists := data[key]
	if !exists || value == nil {
		return nil
	}

	switch typedValue := value.(type) {
	case string:
		return parseDelimitedAddresses(typedValue)
	case []string:
		return cleanAddressList(typedValue)
	case []interface{}:
		return convertInterfaceSliceToAddresses(typedValue)
	default:
		return nil
	}
}

// parseDelimitedAddresses 解析分隔符分隔的地址
// 支持逗号和分号作为分隔符
func parseDelimitedAddresses(addresses string) []string {
	parts := strings.FieldsFunc(addresses, func(character rune) bool {
		return character == ',' || character == ';'
	})

	return cleanAddressList(parts)
}

// convertInterfaceSliceToAddresses 转换接口切片为地址列表
func convertInterfaceSliceToAddresses(items []interface{}) []string {
	var addresses []string

	for _, item := range items {
		if stringValue, ok := item.(string); ok {
			addresses = append(addresses, stringValue)
		}
	}

	return cleanAddressList(addresses)
}

// cleanAddressList 清理地址列表
// 移除空白字符和空字符串
func cleanAddressList(addresses []string) []string {
	var cleanedList []string

	for _, address := range addresses {
		trimmedAddress := strings.TrimSpace(address)
		if trimmedAddress != "" {
			cleanedList = append(cleanedList, trimmedAddress)
		}
	}

	return cleanedList
}

//
// 向后兼容的别名
//
