package push

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"push-gateway/internal/modem"
	"push-gateway/internal/queue"
	"push-gateway/internal/status"
)

// ==================== 常量定义 ====================

const (
	// 消息类型常量
	messageTypeVoice = "voice"
	messageTypeSMS   = "sms"

	// 消息状态常量
	messageStatusPending = "pending"
	messageStatusFailed  = "failed"

	// 通道名称常量
	channelNameVoice = "voice"
	channelNameSMS   = "sms"

	// 内容长度阈值
	contentLengthThreshold = 100

	// 默认消息内容
	defaultMessageContent = "无内容"
)

// ==================== 错误定义 ====================

type DispatchError struct {
	Channel   string
	Operation string
	Reason    string
	Err       error
}

func (e *DispatchError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s失败: %s (%v)", e.Channel, e.Operation, e.Reason, e.Err)
	}
	return fmt.Sprintf("[%s] %s失败: %s", e.Channel, e.Operation, e.Reason)
}

func newDispatchError(channel, operation, reason string, err error) *DispatchError {
	return &DispatchError{
		Channel:   channel,
		Operation: operation,
		Reason:    reason,
		Err:       err,
	}
}

// ==================== 分发器结构 ====================

// ChannelSpecificDispatcher 基于通道的消息分发器
// 根据消息的目标通道,将消息路由到专用队列
type ChannelSpecificDispatcher struct {
	voiceProducer   *queue.VoiceProducer
	smsProducer     *queue.SMSProducer
	generalEnqueuer Enqueuer
	lazyManager     *modem.LazyModemManager
	statusStore     status.StatusStore
}

// NewChannelSpecificDispatcher 创建基于通道的消息分发器
func NewChannelSpecificDispatcher(
	voiceProducer *queue.VoiceProducer,
	smsProducer *queue.SMSProducer,
	generalEnqueuer Enqueuer,
	lazyManager *modem.LazyModemManager,
	statusStore status.StatusStore,
) *ChannelSpecificDispatcher {
	return &ChannelSpecificDispatcher{
		voiceProducer:   voiceProducer,
		smsProducer:     smsProducer,
		generalEnqueuer: generalEnqueuer,
		lazyManager:     lazyManager,
		statusStore:     statusStore,
	}
}

// ==================== 核心分发逻辑 ====================

// DispatchMessage 根据消息类型分发到不同的队列
func (d *ChannelSpecificDispatcher) DispatchMessage(ctx context.Context, delivery Delivery) error {
	targetChannels := d.getTargetChannels(delivery)

	dispatchResult := d.dispatchToChannels(ctx, delivery, targetChannels)

	if !dispatchResult.hasSuccess && dispatchResult.lastError != nil {
		return fmt.Errorf("所有通道消息分发失败: %w", dispatchResult.lastError)
	}

	return nil
}

// dispatchResult 分发结果
type dispatchResult struct {
	hasSuccess bool
	lastError  error
}

// dispatchToChannels 向多个通道分发消息
func (d *ChannelSpecificDispatcher) dispatchToChannels(
	ctx context.Context,
	delivery Delivery,
	targetChannels []Channel,
) dispatchResult {
	result := dispatchResult{
		hasSuccess: false,
		lastError:  nil,
	}

	for _, channel := range targetChannels {
		if err := d.dispatchToSingleChannel(ctx, delivery, channel); err != nil {
			log.Printf("[CHANNEL_DISPATCHER] %s消息分发失败: %v", channel, err)
			result.lastError = err
		} else {
			log.Printf("[CHANNEL_DISPATCHER] %s消息分发成功", channel)
			result.hasSuccess = true
		}
	}

	return result
}

// dispatchToSingleChannel 向单个通道分发消息
func (d *ChannelSpecificDispatcher) dispatchToSingleChannel(
	ctx context.Context,
	delivery Delivery,
	channel Channel,
) error {
	switch channel {
	case ChVoice:
		return d.dispatchVoiceMessage(ctx, delivery)
	case ChSMS:
		return d.dispatchSMSMessage(ctx, delivery)
	default:
		return d.dispatchGeneralMessage(ctx, delivery)
	}
}

// ==================== 通道判断逻辑 ====================

// getTargetChannels 获取消息的目标通道
func (d *ChannelSpecificDispatcher) getTargetChannels(delivery Delivery) []Channel {
	var channels []Channel

	for _, recipient := range delivery.Msg.To {
		recipientChannels := d.determineRecipientChannels(recipient, delivery.Msg)
		channels = append(channels, recipientChannels...)
	}

	return removeDuplicateChannels(channels)
}

// determineRecipientChannels 确定单个收件人的通道
func (d *ChannelSpecificDispatcher) determineRecipientChannels(
	recipient Recipient,
	message Message,
) []Channel {
	var channels []Channel

	if recipient.Phone != "" {
		phoneChannels := d.determinePhoneChannels(message)
		channels = append(channels, phoneChannels...)
	}

	if recipient.Email != "" {
		channels = append(channels, ChEmail)
	}

	if recipient.UserID != "" {
		channels = append(channels, ChWeb)
	}

	return channels
}

// determinePhoneChannels 确定电话号码对应的通道(语音或短信)
func (d *ChannelSpecificDispatcher) determinePhoneChannels(message Message) []Channel {
	// 优先从消息数据中获取指定的类型
	if messageType := extractMessageType(message.Data); messageType != "" {
		return getChannelsByMessageType(messageType)
	}

	// 根据内容长度自动判断
	return getChannelsByContentLength(message.Body)
}

// extractMessageType 从消息数据中提取消息类型
func extractMessageType(data map[string]interface{}) string {
	if data == nil {
		return ""
	}

	messageType, exists := data["type"]
	if !exists {
		return ""
	}

	typeString, ok := messageType.(string)
	if !ok {
		return ""
	}

	return typeString
}

// getChannelsByMessageType 根据消息类型获取通道列表
func getChannelsByMessageType(messageType string) []Channel {
	switch messageType {
	case messageTypeVoice:
		return []Channel{ChVoice}
	case messageTypeSMS:
		return []Channel{ChSMS}
	default:
		// 未知类型,同时使用语音和短信
		return []Channel{ChVoice, ChSMS}
	}
}

// getChannelsByContentLength 根据内容长度判断通道
func getChannelsByContentLength(content string) []Channel {
	if len(content) > contentLengthThreshold {
		// 内容较长,优先使用语音
		return []Channel{ChVoice}
	}
	// 内容较短,优先使用短信
	return []Channel{ChSMS}
}

// removeDuplicateChannels 移除重复的通道
func removeDuplicateChannels(channels []Channel) []Channel {
	seen := make(map[Channel]bool)
	uniqueChannels := make([]Channel, 0, len(channels))

	for _, channel := range channels {
		if !seen[channel] {
			uniqueChannels = append(uniqueChannels, channel)
			seen[channel] = true
		}
	}

	return uniqueChannels
}

// ==================== 语音消息分发 ====================

// dispatchVoiceMessage 分发语音消息到语音专用队列
func (d *ChannelSpecificDispatcher) dispatchVoiceMessage(ctx context.Context, delivery Delivery) error {
	if d.voiceProducer == nil {
		return newDispatchError(channelNameVoice, "分发", "语音消息生产者未配置", nil)
	}

	for _, recipient := range delivery.Msg.To {
		if recipient.Phone == "" {
			continue
		}

		if err := d.sendVoiceMessageToQueue(ctx, recipient, delivery.Msg); err != nil {
			return err
		}
	}

	return nil
}

// sendVoiceMessageToQueue 将单条语音消息发送到队列
func (d *ChannelSpecificDispatcher) sendVoiceMessageToQueue(
	ctx context.Context,
	recipient Recipient,
	message Message,
) error {
	messageID := status.GenerateMessageID()

	voiceMessage := d.buildVoiceMessage(messageID, recipient, message)

	if err := d.saveInitialMessageStatus(ctx, messageID, channelNameVoice, recipient.Phone, voiceMessage.Content); err != nil {
		log.Printf("[CHANNEL_DISPATCHER] 保存语音消息初始状态失败: %v", err)
	}

	if err := d.voiceProducer.SendVoiceMessage(ctx, voiceMessage); err != nil {
		return newDispatchError(channelNameVoice, "入队", "发送到队列失败", err)
	}

	log.Printf("[CHANNEL_DISPATCHER] 语音消息已发送到专用队列: %s", recipient.Phone)
	return nil
}

// buildVoiceMessage 构建语音消息对象
func (d *ChannelSpecificDispatcher) buildVoiceMessage(
	messageID string,
	recipient Recipient,
	message Message,
) queue.VoiceMessage {
	return queue.VoiceMessage{
		MessageID: messageID,
		Phone:     recipient.Phone,
		Content:   d.extractMessageContent(message),
		Priority:  message.Priority,
		Metadata: map[string]string{
			"user_id": recipient.UserID,
			"subject": message.Subject,
		},
	}
}

// ==================== 短信消息分发 ====================

// dispatchSMSMessage 分发短信消息到短信专用队列
func (d *ChannelSpecificDispatcher) dispatchSMSMessage(ctx context.Context, delivery Delivery) error {
	if d.smsProducer == nil {
		return newDispatchError(channelNameSMS, "分发", "短信消息生产者未配置", nil)
	}

	for _, recipient := range delivery.Msg.To {
		if recipient.Phone == "" {
			continue
		}

		if err := d.sendSMSMessageToQueue(ctx, recipient, delivery.Msg); err != nil {
			return err
		}
	}

	return nil
}

// sendSMSMessageToQueue 将单条短信消息发送到队列
func (d *ChannelSpecificDispatcher) sendSMSMessageToQueue(
	ctx context.Context,
	recipient Recipient,
	message Message,
) error {
	messageID := status.GenerateMessageID()

	smsMessage := d.buildSMSMessage(messageID, recipient, message)

	if err := d.saveInitialMessageStatus(ctx, messageID, channelNameSMS, recipient.Phone, smsMessage.Content); err != nil {
		log.Printf("[CHANNEL_DISPATCHER] 保存短信消息初始状态失败: %v", err)
	}

	// 检查4G模块可用性
	if !d.isModemAvailable() {
		return d.handleModemUnavailable(ctx, messageID, recipient.Phone)
	}

	if err := d.smsProducer.SendSMSMessage(ctx, smsMessage); err != nil {
		d.updateMessageStatusAsFailed(ctx, messageID, "入队失败: "+err.Error())
		return newDispatchError(channelNameSMS, "入队", "发送到队列失败", err)
	}

	log.Printf("[CHANNEL_DISPATCHER] 短信消息已发送到专用队列: %s", recipient.Phone)
	return nil
}

// buildSMSMessage 构建短信消息对象
func (d *ChannelSpecificDispatcher) buildSMSMessage(
	messageID string,
	recipient Recipient,
	message Message,
) queue.SMSMessage {
	return queue.SMSMessage{
		MessageID: messageID,
		Phone:     recipient.Phone,
		Content:   d.extractMessageContent(message),
		Priority:  message.Priority,
		Metadata: map[string]string{
			"user_id": recipient.UserID,
			"subject": message.Subject,
		},
	}
}

// isModemAvailable 检查4G模块是否可用
func (d *ChannelSpecificDispatcher) isModemAvailable() bool {
	if d.lazyManager == nil {
		return false
	}

	_, _, _, _, available := d.lazyManager.GetQueueStatus()
	return available
}

// handleModemUnavailable 处理4G模块不可用的情况
func (d *ChannelSpecificDispatcher) handleModemUnavailable(
	ctx context.Context,
	messageID string,
	phone string,
) error {
	d.updateMessageStatusAsFailed(ctx, messageID, "4G模块不可用或初始化失败")
	log.Printf("[CHANNEL_DISPATCHER] 短信消息因4G模块不可用立即标记为失败: %s", phone)
	return newDispatchError(channelNameSMS, "发送", "4G模块不可用", nil)
}

// ==================== 通用消息分发 ====================

// dispatchGeneralMessage 分发其他消息到通用队列
func (d *ChannelSpecificDispatcher) dispatchGeneralMessage(ctx context.Context, delivery Delivery) error {
	if d.generalEnqueuer == nil {
		return newDispatchError("通用", "分发", "通用消息队列未配置", nil)
	}

	payload, err := json.Marshal(delivery)
	if err != nil {
		return newDispatchError("通用", "序列化", "消息序列化失败", err)
	}

	// 尝试使用优先级队列
	if priorityEnqueuer, ok := d.generalEnqueuer.(PriorityEnqueuer); ok {
		return priorityEnqueuer.EnqueueWithPriority(ctx, payload, delivery.Msg.Priority)
	}

	// 使用普通队列
	return d.generalEnqueuer.Enqueue(ctx, payload)
}

// ==================== 状态管理辅助函数 ====================

// saveInitialMessageStatus 保存消息的初始状态
func (d *ChannelSpecificDispatcher) saveInitialMessageStatus(
	ctx context.Context,
	messageID string,
	channel string,
	phone string,
	content string,
) error {
	if d.statusStore == nil {
		return nil
	}

	initialStatus := &status.MessageStatus{
		MessageID: messageID,
		Channel:   channel,
		Phone:     phone,
		Content:   content,
		Status:    messageStatusPending,
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
	}

	return d.statusStore.SaveStatus(ctx, initialStatus)
}

// updateMessageStatusAsFailed 更新消息状态为失败
func (d *ChannelSpecificDispatcher) updateMessageStatusAsFailed(
	ctx context.Context,
	messageID string,
	reason string,
) {
	if d.statusStore == nil {
		return
	}

	if err := d.statusStore.UpdateStatus(ctx, messageID, messageStatusFailed, reason); err != nil {
		log.Printf("[CHANNEL_DISPATCHER] 更新消息状态为失败时出错: %v", err)
	}
}

// ==================== 内容提取辅助函数 ====================

// extractMessageContent 从消息中提取内容
func (d *ChannelSpecificDispatcher) extractMessageContent(message Message) string {
	// 优先使用 Body
	if message.Body != "" {
		return message.Body
	}

	// 尝试从 Data 中提取 content
	if contentFromData := extractContentFromData(message.Data); contentFromData != "" {
		return contentFromData
	}

	// 使用 Subject
	if message.Subject != "" {
		return message.Subject
	}

	return defaultMessageContent
}

// extractContentFromData 从消息数据中提取内容
func extractContentFromData(data map[string]interface{}) string {
	if data == nil {
		return ""
	}

	content, exists := data["content"]
	if !exists {
		return ""
	}

	contentString, ok := content.(string)
	if !ok {
		return ""
	}

	return contentString
}

// ==================== 公共接口 ====================

// GetQueueStatus 获取4G模块队列状态
func (d *ChannelSpecificDispatcher) GetQueueStatus() (
	voiceQueueLength int,
	smsQueueLength int,
	isVoiceBusy bool,
	isSMSBusy bool,
	available bool,
) {
	if d.lazyManager == nil {
		return 0, 0, false, false, false
	}

	return d.lazyManager.GetQueueStatus()
}
