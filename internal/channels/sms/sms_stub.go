package sms

import (
	"context"
	"fmt"
	"log"
	"time"

	"push-gateway/internal/config"
	"push-gateway/internal/modem"
	"push-gateway/internal/push"
	"push-gateway/internal/queue"
	"push-gateway/internal/status"
)

const (
	// SMS 相关常量
	ProviderNameModem   = "sms_4g_modem"
	ProviderNameTencent = "sms_tencent"

	ChannelNameSMS = "sms"

	StatusPending = "pending"
	StatusSuccess = "success"
	StatusFailed  = "failed"

	// 错误消息常量
	ErrorModemNotConfigured = "4G模块未配置"
)

// ModemSMSProvider 基于 4G Modem 的短信服务提供者
// 支持异步队列发送和同步直接发送两种模式
type ModemSMSProvider struct {
	providerName string
	smsConfig    config.SMSProvider

	// lazyManager 负责与物理 4G 模块交互
	lazyManager *modem.LazyModemManager

	// smsProducer 用于异步队列发送,提高并发性能
	smsProducer *queue.SMSProducer

	// statusStore 持久化消息发送状态,支持查询追踪
	statusStore status.StatusStore
}

// NewModem 创建 4G Modem 短信服务提供者实例
func NewModem(
	smsConfig config.SMSProvider,
	lazyManager *modem.LazyModemManager,
	smsProducer *queue.SMSProducer,
	statusStore status.StatusStore,
) *ModemSMSProvider {
	return &ModemSMSProvider{
		providerName: ProviderNameModem,
		smsConfig:    smsConfig,
		lazyManager:  lazyManager,
		smsProducer:  smsProducer,
		statusStore:  statusStore,
	}
}

// Name 返回提供者名称
func (provider *ModemSMSProvider) Name() string {
	return provider.providerName
}

// Channels 返回支持的推送通道列表
func (provider *ModemSMSProvider) Channels() []push.Channel {
	return []push.Channel{push.ChSMS}
}

// Send 发送短信消息
// 优先使用异步队列发送,队列未配置时降级为同步发送
func (provider *ModemSMSProvider) Send(ctx context.Context, message push.Message, plan push.RoutePlan) error {
	phone, err := provider.validateRecipient(message)
	if err != nil {
		return err
	}

	content, err := provider.extractContent(message)
	if err != nil {
		return err
	}

	messageID := provider.ensureMessageID(&message)

	// 异步队列模式:入队后立即返回,由消费者异步处理
	if provider.smsProducer != nil {
		return provider.sendAsync(ctx, messageID, phone, content, int(message.Priority))
	}

	// 同步模式:直接调用 Modem 发送(无队列时的降级方案)
	return provider.sendSync(ctx, messageID, phone, content)
}

// validateRecipient 验证收件人信息
// 确保至少有一个有效的手机号码
func (provider *ModemSMSProvider) validateRecipient(message push.Message) (string, error) {
	if len(message.To) == 0 {
		return "", fmt.Errorf("recipients list is empty")
	}

	phone := message.To[0].Phone
	if phone == "" {
		return "", fmt.Errorf("phone number cannot be empty")
	}

	return phone, nil
}

// extractContent 从消息中提取短信内容
// 按优先级依次尝试: Body > Data["content"] > Subject
func (provider *ModemSMSProvider) extractContent(message push.Message) (string, error) {
	// 优先使用 Body 字段(最常用的短信内容字段)
	if message.Body != "" {
		return message.Body, nil
	}

	// 尝试从 Data 映射中获取 content 字段
	if message.Data != nil {
		if content, exists := message.Data["content"]; exists {
			if contentString, ok := content.(string); ok && contentString != "" {
				return contentString, nil
			}
		}
	}

	// 最后尝试使用 Subject(兼容邮件类消息)
	if message.Subject != "" {
		return message.Subject, nil
	}

	return "", fmt.Errorf("no valid content found in message")
}

// ensureMessageID 确保消息拥有唯一标识符
// 优先使用 dispatcher 注入的 message_id,不存在时自动生成
func (provider *ModemSMSProvider) ensureMessageID(message *push.Message) string {
	// 尝试从 Data 中获取已存在的 message_id
	if message.Data != nil {
		if messageID, ok := message.Data["message_id"].(string); ok && messageID != "" {
			return messageID
		}
	}

	// 生成新的消息ID(使用纳秒时间戳确保唯一性)
	messageID := fmt.Sprintf("sms_%d", time.Now().UnixNano())

	// 回写到消息对象中
	if message.Data == nil {
		message.Data = make(map[string]interface{})
	}
	message.Data["message_id"] = messageID

	return messageID
}

// sendAsync 异步发送短信(入队模式)
// 将消息放入队列后立即返回,由后台消费者处理实际发送
func (provider *ModemSMSProvider) sendAsync(
	ctx context.Context,
	messageID string,
	phone string,
	content string,
	priority int,
) error {
	// 保存初始状态为 pending
	if err := provider.saveInitialStatus(ctx, messageID, phone, content); err != nil {
		log.Printf("[SMS] 警告: 保存初始状态失败 (message_id=%s): %v", messageID, err)
	}

	// 构造队列消息
	queueMessage := queue.SMSMessage{
		MessageID: messageID,
		Phone:     phone,
		Content:   content,
		Priority:  priority,
	}

	if err := provider.smsProducer.SendSMSMessage(ctx, queueMessage); err != nil {
		return fmt.Errorf("failed to enqueue sms message: %w", err)
	}

	log.Printf("[SMS] 短信已入队 -> %s, 内容: %s (message_id=%s)", phone, content, messageID)
	return nil
}

// saveInitialStatus 保存消息的初始状态
// 避免重复保存已存在的状态记录
func (provider *ModemSMSProvider) saveInitialStatus(
	ctx context.Context,
	messageID string,
	phone string,
	content string,
) error {
	if provider.statusStore == nil {
		return nil
	}

	// 检查状态是否已存在(避免覆盖)
	existingStatus, _ := provider.statusStore.GetStatus(ctx, messageID)
	if existingStatus != nil {
		return nil
	}

	now := time.Now().Unix()
	initialStatus := &status.MessageStatus{
		MessageID: messageID,
		Channel:   ChannelNameSMS,
		Phone:     phone,
		Content:   content,
		Status:    StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	return provider.statusStore.SaveStatus(ctx, initialStatus)
}

// sendSync 同步发送短信(直连模式)
// 直接调用 Modem 发送,适用于队列未配置的降级场景
func (provider *ModemSMSProvider) sendSync(
	ctx context.Context,
	messageID string,
	phone string,
	content string,
) error {
	log.Printf("[SMS] 队列未配置,使用同步模式发送 -> %s, 内容: %s (message_id=%s)", phone, content, messageID)

	// 验证 Modem 模块是否已配置
	if provider.lazyManager == nil {
		provider.updateMessageStatus(ctx, messageID, StatusFailed, ErrorModemNotConfigured)
		return fmt.Errorf(ErrorModemNotConfigured)
	}

	// 执行实际发送
	if err := provider.lazyManager.SendSMS(ctx, phone, content); err != nil {
		provider.updateMessageStatus(ctx, messageID, StatusFailed, err.Error())
		return fmt.Errorf("failed to send sms: %w", err)
	}

	provider.updateMessageStatus(ctx, messageID, StatusSuccess, "")
	return nil
}

// updateMessageStatus 更新消息状态
// 统一的状态更新入口,避免重复代码
func (provider *ModemSMSProvider) updateMessageStatus(
	ctx context.Context,
	messageID string,
	newStatus string,
	errorMessage string,
) {
	if provider.statusStore == nil {
		return
	}

	if err := provider.statusStore.UpdateStatus(ctx, messageID, newStatus, errorMessage); err != nil {
		log.Printf("[SMS] 警告: 更新状态失败 (message_id=%s, status=%s): %v", messageID, newStatus, err)
	}
}

// TencentSMSProvider 腾讯云短信服务提供者
// 作为 4G Modem 的备用方案,当物理模块不可用时使用
type TencentSMSProvider struct {
	providerName string
	smsConfig    config.SMSProvider
}

// NewTencent 创建腾讯云短信服务提供者实例
func NewTencent(smsConfig config.SMSProvider) *TencentSMSProvider {
	return &TencentSMSProvider{
		providerName: ProviderNameTencent,
		smsConfig:    smsConfig,
	}
}

// Name 返回提供者名称
func (provider *TencentSMSProvider) Name() string {
	return provider.providerName
}

// Channels 返回支持的推送通道列表
func (provider *TencentSMSProvider) Channels() []push.Channel {
	return []push.Channel{push.ChSMS}
}

// Send 发送短信消息(腾讯云 SDK)
func (provider *TencentSMSProvider) Send(
	ctx context.Context,
	message push.Message,
	plan push.RoutePlan,
) error {
	if len(message.To) == 0 {
		return fmt.Errorf("recipients list is empty")
	}

	phone := message.To[0].Phone
	if phone == "" {
		return fmt.Errorf("phone number cannot be empty")
	}

	log.Printf("[SMS] 使用腾讯云发送短信 -> %s", phone)

	// TODO: 集成腾讯云 SMS SDK 的实际发送逻辑
	return nil
}
