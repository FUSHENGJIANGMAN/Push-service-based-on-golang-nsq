package voice

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
	// Voice 相关常量
	ProviderNameModem  = "voice_4g_modem"
	ProviderNameTwilio = "voice_twilio"

	ChannelNameVoice = "voice"

	StatusPending = "pending"
	StatusSuccess = "success"
	StatusFailed  = "failed"

	// 数据键常量
	DataKeyMessageID    = "message_id"
	DataKeyAsyncPending = "__async_pending_voice"
	DataKeyContent      = "content"

	// 错误消息常量
	ErrorQueueNotConfigured = "voice queue not configured"
)

// ModemVoiceProvider 基于 4G Modem 的语音通话服务提供者
// 采用异步队列模式,支持消息状态追踪和优先级调度
type ModemVoiceProvider struct {
	providerName string
	voiceConfig  config.VoiceProvider

	// lazyManager 负责与物理 4G 模块交互进行语音通话
	lazyManager *modem.LazyModemManager

	// voiceProducer 用于异步队列发送,解耦发送请求和实际拨号
	voiceProducer *queue.VoiceProducer

	// statusStore 持久化消息状态,支持异步状态查询
	statusStore status.StatusStore
}

// NewModem 创建 4G Modem 语音服务提供者实例
func NewModem(
	voiceConfig config.VoiceProvider,
	lazyManager *modem.LazyModemManager,
	voiceProducer *queue.VoiceProducer,
	statusStore status.StatusStore,
) *ModemVoiceProvider {
	return &ModemVoiceProvider{
		providerName:  ProviderNameModem,
		voiceConfig:   voiceConfig,
		lazyManager:   lazyManager,
		voiceProducer: voiceProducer,
		statusStore:   statusStore,
	}
}

// Name 返回提供者名称
func (provider *ModemVoiceProvider) Name() string {
	return provider.providerName
}

// Channels 返回支持的推送通道列表
func (provider *ModemVoiceProvider) Channels() []push.Channel {
	return []push.Channel{push.ChVoice}
}

// Send 发送语音消息(异步模式)
// 将消息放入队列后立即返回,由后台消费者处理实际拨号
func (provider *ModemVoiceProvider) Send(ctx context.Context, message push.Message, plan push.RoutePlan) error {
	if provider.voiceProducer == nil {
		return fmt.Errorf(ErrorQueueNotConfigured)
	}

	phone, err := provider.validateRecipient(message)
	if err != nil {
		return err
	}

	content, err := provider.extractContent(message)
	if err != nil {
		return err
	}

	messageID := provider.ensureMessageID(&message)

	// 标记为异步待处理状态,防止 dispatcher 提前返回 success
	provider.markAsyncPending(&message)

	if err := provider.saveInitialStatus(ctx, messageID, phone, content); err != nil {
		log.Printf("[VOICE] 警告: 保存初始状态失败 (message_id=%s): %v", messageID, err)
	}

	return provider.enqueueVoiceMessage(ctx, messageID, phone, content, int(message.Priority))
}

// validateRecipient 验证收件人信息
// 确保至少有一个有效的手机号码
func (provider *ModemVoiceProvider) validateRecipient(message push.Message) (string, error) {
	if len(message.To) == 0 {
		return "", fmt.Errorf("recipients list is empty")
	}

	phone := message.To[0].Phone
	if phone == "" {
		return "", fmt.Errorf("phone number cannot be empty")
	}

	return phone, nil
}

// extractContent 从消息中提取语音内容
// 按优先级依次尝试: Body > Data["content"] > Subject
func (provider *ModemVoiceProvider) extractContent(message push.Message) (string, error) {
	// 优先使用 Body 字段(最常用的语音内容字段)
	if message.Body != "" {
		return message.Body, nil
	}

	// 尝试从 Data 映射中获取 content 字段
	if message.Data != nil {
		if content, exists := message.Data[DataKeyContent]; exists {
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
// 优先复用 dispatcher 注入的 message_id,避免生成多个 ID
func (provider *ModemVoiceProvider) ensureMessageID(message *push.Message) string {
	// 初始化 Data 映射(如果不存在)
	if message.Data == nil {
		message.Data = make(map[string]interface{})
	}

	// 尝试获取已存在的 message_id
	if messageID, ok := message.Data[DataKeyMessageID].(string); ok && messageID != "" {
		return messageID
	}

	// 生成新的消息ID(使用纳秒时间戳确保唯一性)
	messageID := fmt.Sprintf("voice_%d", time.Now().UnixNano())
	message.Data[DataKeyMessageID] = messageID

	return messageID
}

// markAsyncPending 标记消息为异步待处理状态
// 防止 dispatcher 在入队后就立即返回 success,需等待实际拨号完成
func (provider *ModemVoiceProvider) markAsyncPending(message *push.Message) {
	if message.Data == nil {
		message.Data = make(map[string]interface{})
	}

	message.Data[DataKeyAsyncPending] = true
}

// saveInitialStatus 保存消息的初始 pending 状态
// 仅在状态不存在时创建,避免覆盖 dispatcher 已写入的状态
func (provider *ModemVoiceProvider) saveInitialStatus(
	ctx context.Context,
	messageID string,
	phone string,
	content string,
) error {
	if provider.statusStore == nil {
		return nil
	}

	// 检查状态是否已存在(避免重复保存)
	existingStatus, _ := provider.statusStore.GetStatus(ctx, messageID)
	if existingStatus != nil {
		return nil
	}

	now := time.Now().Unix()
	initialStatus := &status.MessageStatus{
		MessageID: messageID,
		Channel:   ChannelNameVoice,
		Phone:     phone,
		Content:   content,
		Status:    StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	return provider.statusStore.SaveStatus(ctx, initialStatus)
}

// enqueueVoiceMessage 将语音消息放入队列
// 返回后实际拨号由后台消费者异步处理
func (provider *ModemVoiceProvider) enqueueVoiceMessage(
	ctx context.Context,
	messageID string,
	phone string,
	content string,
	priority int,
) error {
	queueMessage := queue.VoiceMessage{
		MessageID: messageID,
		Phone:     phone,
		Content:   content,
		Priority:  priority,
	}

	if err := provider.voiceProducer.SendVoiceMessage(ctx, queueMessage); err != nil {
		return fmt.Errorf("failed to enqueue voice message: %w", err)
	}

	log.Printf("[VOICE] 语音消息已入队 -> %s, 内容: %s (message_id=%s)", phone, content, messageID)
	return nil
}

// TwilioProvider Twilio 云语音服务提供者
// 作为 4G Modem 的备用方案,当物理模块不可用时使用
type TwilioProvider struct {
	providerName string
	voiceConfig  config.VoiceProvider
}

// NewTwilio 创建 Twilio 语音服务提供者实例
func NewTwilio(voiceConfig config.VoiceProvider) *TwilioProvider {
	return &TwilioProvider{
		providerName: ProviderNameTwilio,
		voiceConfig:  voiceConfig,
	}
}

// Name 返回提供者名称
func (provider *TwilioProvider) Name() string {
	return provider.providerName
}

// Channels 返回支持的推送通道列表
func (provider *TwilioProvider) Channels() []push.Channel {
	return []push.Channel{push.ChVoice}
}

// Send 发送语音消息(Twilio SDK)
func (provider *TwilioProvider) Send(
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

	log.Printf("[VOICE] 使用 Twilio 发起语音通话 -> %s", phone)

	// TODO: 集成 Twilio SDK 的实际拨号逻辑
	return nil
}
