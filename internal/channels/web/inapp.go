package web

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"push-gateway/internal/inbox"
	"push-gateway/internal/push"
)

const (
	// Provider 常量
	ProviderNameInApp = "web_inapp"

	// 消息字段名常量
	FieldTitle       = "title"
	FieldSubject     = "subject"
	FieldName        = "name"
	FieldDescription = "description"
	FieldMessage     = "message"
	FieldBody        = "body"
	FieldContent     = "content"

	// 业务字段名常量
	FieldProductName    = "productName"
	FieldMaintenanceID  = "maintenanceId"
	FieldOrderNumber    = "orderNumber"
	FieldSeverity       = "severity"
	FieldAppID          = "appId"
	FieldType           = "type"
	FieldTimestamp      = "timestamp"
	FieldStartTime      = "startTime"
	FieldEndTime        = "endTime"
	FieldLaunchDate     = "launchDate"
	FieldVersion        = "version"
	FieldTrackingNumber = "trackingNumber"
	FieldPrice          = "price"
	FieldAmount         = "amount"
	FieldStatus         = "status"
	FieldRiskLevel      = "riskLevel"
	FieldIPAddress      = "ipAddress"
	FieldDownloadURL    = "downloadUrl"

	// 默认文本
	DefaultContentEmpty = "无内容"
)

// InApp 站内信服务提供者
// 将消息投递到用户的收件箱,支持多种消息类型和动态内容提取
type InApp struct {
	inboxStore inbox.Store
}

// NewInApp 创建站内信服务提供者实例
func NewInApp(inboxStore inbox.Store) *InApp {
	return &InApp{
		inboxStore: inboxStore,
	}
}

// Name 返回提供者名称
func (provider *InApp) Name() string {
	return ProviderNameInApp
}

// Channels 返回支持的推送通道列表
func (provider *InApp) Channels() []push.Channel {
	return []push.Channel{push.ChWeb}
}

// Send 发送站内信消息
// 将消息分发到每个收件人的个人收件箱
func (provider *InApp) Send(ctx context.Context, message push.Message, plan push.RoutePlan) error {
	log.Printf("[WEB_INAPP] 开始发送站内信,收件人数量: %d, 消息类型: %s", len(message.To), message.Kind)

	if provider.inboxStore == nil {
		log.Printf("[WEB_INAPP] 警告: 收件箱存储未配置,消息将不会被持久化")
		return nil
	}

	return provider.deliverToRecipients(ctx, message)
}

// deliverToRecipients 将消息投递到所有收件人
// 使用部分成功策略:即使部分投递失败也继续处理其他收件人
func (provider *InApp) deliverToRecipients(ctx context.Context, message push.Message) error {
	var firstError error
	successCount := 0

	for index, recipient := range message.To {
		if err := provider.deliverToSingleRecipient(ctx, message, recipient, index); err != nil {
			if firstError == nil {
				firstError = err
			}
			continue
		}
		successCount++
	}

	log.Printf("[WEB_INAPP] 站内信发送完成: 成功 %d/%d", successCount, len(message.To))

	return firstError
}

// deliverToSingleRecipient 将消息投递到单个收件人
func (provider *InApp) deliverToSingleRecipient(
	ctx context.Context,
	message push.Message,
	recipient push.Recipient,
	recipientIndex int,
) error {
	userID, err := provider.extractUserID(recipient, recipientIndex)
	if err != nil {
		return err
	}

	subject, body := provider.extractSubjectAndBody(message)

	inboxMessage := inbox.Message{
		UserID:    userID,
		Kind:      string(message.Kind),
		Subject:   subject,
		Body:      body,
		Data:      message.Data,
		CreatedAt: 0, // 由存储层自动填充
		ReadAt:    0, // 未读状态
	}

	messageID, err := provider.inboxStore.Add(ctx, inboxMessage)
	if err != nil {
		log.Printf("[WEB_INAPP] 存储失败 - 用户 %s: %v", userID, err)
		return fmt.Errorf("failed to store message for user %s: %w", userID, err)
	}

	log.Printf("[WEB_INAPP] 存储成功 - 用户 %s, 消息ID: %s", userID, messageID)
	return nil
}

// extractUserID 从收件人信息中提取用户ID
// 优先使用 UserID,其次使用 WebInbox 字段
func (provider *InApp) extractUserID(recipient push.Recipient, recipientIndex int) (string, error) {
	userID := findFirstNonEmptyString(recipient.UserID, recipient.WebInbox)

	if strings.TrimSpace(userID) == "" {
		return "", fmt.Errorf(
			"missing user_id for web inbox, recipient %d has UserID=%q WebInbox=%q",
			recipientIndex,
			recipient.UserID,
			recipient.WebInbox,
		)
	}

	return userID, nil
}

// extractSubjectAndBody 从消息中提取主题和正文
// 按优先级依次尝试: 原始字段 → Data动态提取 → 默认值
func (provider *InApp) extractSubjectAndBody(message push.Message) (string, string) {
	subject := message.Subject
	body := message.Body

	// 从 Data 字段动态提取内容(支持灵活的消息格式)
	if message.Data != nil {
		messageKind := strings.ToLower(string(message.Kind))

		if subject == "" {
			subject = provider.extractSubjectFromData(messageKind, message.Data)
		}

		if body == "" {
			body = provider.extractBodyFromData(messageKind, message.Data)
		}

		// 追加额外的详细信息(时间、状态、金额等)
		body = provider.appendAdditionalDetails(messageKind, body, message.Data)
	}

	// 确保有默认值(避免空消息)
	subject = provider.ensureDefaultSubject(subject, message.Kind)
	body = provider.ensureDefaultBody(body, message.Data)

	return subject, body
}

// extractSubjectFromData 从 Data 字段中提取主题
// 优先使用通用标题字段,其次根据业务字段智能构建
func (provider *InApp) extractSubjectFromData(messageKind string, data map[string]any) string {
	// 尝试通用标题字段
	titleFields := []string{FieldTitle, FieldSubject, FieldName}
	for _, fieldName := range titleFields {
		if value, ok := data[fieldName].(string); ok && value != "" {
			return value
		}
	}

	// 根据业务字段智能构建主题
	return provider.buildSubjectFromBusinessFields(messageKind, data)
}

// buildSubjectFromBusinessFields 根据业务字段构建主题
// 支持多种消息场景的自动识别和格式化
func (provider *InApp) buildSubjectFromBusinessFields(messageKind string, data map[string]any) string {
	// 产品发布通知
	if productName, ok := data[FieldProductName].(string); ok {
		return fmt.Sprintf("新产品发布：%s", productName)
	}

	// 系统维护通知
	if maintenanceID, ok := data[FieldMaintenanceID].(string); ok {
		return fmt.Sprintf("系统维护通知：%s", maintenanceID)
	}

	// 订单状态通知
	if orderNumber, ok := data[FieldOrderNumber].(string); ok {
		return fmt.Sprintf("订单 %s 状态更新", orderNumber)
	}

	// 安全警报通知
	if severity, ok := data[FieldSeverity].(string); ok {
		return fmt.Sprintf("[%s] 警报", strings.ToUpper(severity))
	}

	// 应用消息通知
	if appID, ok := data[FieldAppID].(string); ok {
		subject := fmt.Sprintf("来自 %s 的消息", appID)

		if messageType, ok := data[FieldType].(string); ok && messageType != "" {
			subject = fmt.Sprintf("%s [%s]", subject, messageType)
		}

		return subject
	}

	// 默认主题
	return fmt.Sprintf("%s 通知", messageKind)
}

// extractBodyFromData 从 Data 字段中提取正文
// 按优先级尝试多个常见内容字段名
func (provider *InApp) extractBodyFromData(messageKind string, data map[string]any) string {
	// 尝试标准内容字段
	bodyFields := []string{FieldDescription, FieldMessage, FieldBody, FieldContent}
	for _, fieldName := range bodyFields {
		if value, ok := data[fieldName].(string); ok && value != "" {
			return value
		}
	}

	// 处理嵌套的 content 对象(某些消息类型使用对象格式)
	if contentObject, ok := data[FieldContent].(map[string]interface{}); ok {
		if contentBytes, err := json.Marshal(contentObject); err == nil {
			return string(contentBytes)
		}
	}

	return ""
}

// appendAdditionalDetails 追加额外的详细信息到正文
// 自动从 Data 中提取常见业务字段并格式化
func (provider *InApp) appendAdditionalDetails(messageKind string, body string, data map[string]any) string {
	details := provider.extractDetailsFromData(data)

	if len(details) > 0 {
		return fmt.Sprintf("%s\n%s", body, strings.Join(details, " | "))
	}

	return body
}

// extractDetailsFromData 从 Data 中提取详细信息
// 返回格式化后的字段列表
func (provider *InApp) extractDetailsFromData(data map[string]any) []string {
	var details []string

	details = append(details, provider.extractTimeDetails(data)...)
	details = append(details, provider.extractVersionDetails(data)...)
	details = append(details, provider.extractMoneyDetails(data)...)
	details = append(details, provider.extractStatusDetails(data)...)
	details = append(details, provider.extractNetworkDetails(data)...)
	details = append(details, provider.extractURLDetails(data)...)

	return details
}

// extractTimeDetails 提取时间相关信息
func (provider *InApp) extractTimeDetails(data map[string]any) []string {
	var details []string

	if timestamp, ok := data[FieldTimestamp].(string); ok {
		details = append(details, fmt.Sprintf("时间: %s", timestamp))
	}

	if startTime, ok := data[FieldStartTime].(string); ok {
		if endTime, ok := data[FieldEndTime].(string); ok {
			details = append(details, fmt.Sprintf("时间: %s 至 %s", startTime, endTime))
		}
	}

	if launchDate, ok := data[FieldLaunchDate].(string); ok {
		details = append(details, fmt.Sprintf("发布日期: %s", launchDate))
	}

	return details
}

// extractVersionDetails 提取版本和追踪信息
func (provider *InApp) extractVersionDetails(data map[string]any) []string {
	var details []string

	if version, ok := data[FieldVersion].(string); ok {
		details = append(details, fmt.Sprintf("版本: %s", version))
	}

	if trackingNumber, ok := data[FieldTrackingNumber].(string); ok && trackingNumber != "" {
		details = append(details, fmt.Sprintf("物流单号: %s", trackingNumber))
	}

	return details
}

// extractMoneyDetails 提取价格和金额信息
func (provider *InApp) extractMoneyDetails(data map[string]any) []string {
	var details []string

	if price, ok := data[FieldPrice].(float64); ok {
		details = append(details, fmt.Sprintf("价格: ¥%.2f", price))
	}

	if amount, ok := data[FieldAmount].(float64); ok {
		details = append(details, fmt.Sprintf("金额: ¥%.2f", amount))
	}

	return details
}

// extractStatusDetails 提取状态信息
func (provider *InApp) extractStatusDetails(data map[string]any) []string {
	var details []string

	if status, ok := data[FieldStatus].(string); ok {
		details = append(details, fmt.Sprintf("状态: %s", status))
	}

	if riskLevel, ok := data[FieldRiskLevel].(string); ok {
		details = append(details, fmt.Sprintf("风险级别: %s", riskLevel))
	}

	return details
}

// extractNetworkDetails 提取网络相关信息
func (provider *InApp) extractNetworkDetails(data map[string]any) []string {
	var details []string

	if ipAddress, ok := data[FieldIPAddress].(string); ok {
		details = append(details, fmt.Sprintf("IP: %s", ipAddress))
	}

	return details
}

// extractURLDetails 提取URL信息
func (provider *InApp) extractURLDetails(data map[string]any) []string {
	var details []string

	if downloadURL, ok := data[FieldDownloadURL].(string); ok && downloadURL != "" {
		details = append(details, fmt.Sprintf("下载链接: %s", downloadURL))
	}

	return details
}

// ensureDefaultSubject 确保主题有默认值
func (provider *InApp) ensureDefaultSubject(subject string, messageKind push.MessageKind) string {
	if subject == "" {
		return fmt.Sprintf("%s 通知", string(messageKind))
	}
	return subject
}

// ensureDefaultBody 确保正文有默认值
// 尝试序列化 Data 作为备用内容
func (provider *InApp) ensureDefaultBody(body string, data map[string]any) string {
	if body != "" {
		return body
	}

	if data != nil {
		if dataBytes, err := json.Marshal(data); err == nil {
			return string(dataBytes)
		}
	}

	return DefaultContentEmpty
}

// findFirstNonEmptyString 返回第一个非空字符串
// 用于在多个字段中查找有效值
func findFirstNonEmptyString(candidates ...string) string {
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate) != "" {
			return candidate
		}
	}
	return ""
}
