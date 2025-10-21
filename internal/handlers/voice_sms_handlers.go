package handlers

import (
	"context"
	"fmt"
	"log"
	"time"

	"push-gateway/internal/modem"
	"push-gateway/internal/queue"
	"push-gateway/internal/status"
)

// 消息处理常量
const (
	// 消息状态
	StatusSuccess = "success"
	StatusFailed  = "failed"
	StatusPending = "pending"

	// 通道类型
	ChannelVoice = "voice"
	ChannelSMS   = "sms"

	// 重试配置
	MaxRetryAttempts = 3

	// 错误消息
	ErrorModemNotConfigured  = "4G模块未配置"
	ErrorMaxAttemptsExceeded = "exceed max attempts"
)

// VoiceMessageHandler 语音消息处理器
// 负责处理语音通话队列消息,支持幂等和重试控制
type VoiceMessageHandler struct {
	modemManager *modem.LazyModemManager
	statusStore  status.StatusStore
}

// NewVoiceMessageHandler 创建语音消息处理器实例
func NewVoiceMessageHandler(
	modemManager *modem.LazyModemManager,
	statusStore status.StatusStore,
) *VoiceMessageHandler {
	return &VoiceMessageHandler{
		modemManager: modemManager,
		statusStore:  statusStore,
	}
}

// HandleVoiceMessage 处理语音消息
// 实现幂等性和最大重试次数控制
func (handler *VoiceMessageHandler) HandleVoiceMessage(
	ctx context.Context,
	message queue.VoiceMessage,
	attempts uint16,
) error {
	log.Printf(
		"[VOICE_HANDLER] 处理语音消息 -> %s, 内容: %s (尝试次数: %d)",
		message.Phone,
		message.Content,
		attempts,
	)

	messageID := message.GetMessageID()

	// 检查消息是否已成功处理(幂等性保证)
	if shouldSkip, err := handler.checkMessageStatus(ctx, messageID, attempts); shouldSkip {
		return err
	}

	// 超过最大重试次数,不再处理
	if attempts > MaxRetryAttempts {
		return nil
	}

	// 执行实际的语音发送
	return handler.sendVoiceCall(ctx, messageID, message.Phone, message.Content)
}

// checkMessageStatus 检查消息状态
// 返回是否应该跳过处理和可能的错误
func (handler *VoiceMessageHandler) checkMessageStatus(
	ctx context.Context,
	messageID string,
	attempts uint16,
) (bool, error) {
	if handler.statusStore == nil || messageID == "" {
		return false, nil
	}

	messageStatus, err := handler.statusStore.GetStatus(ctx, messageID)

	// 状态不存在,创建初始状态记录(兼容历史数据)
	if err != nil || messageStatus == nil {
		handler.createInitialStatus(ctx, messageID, "", "", ChannelVoice)
		return false, nil
	}

	// 消息已成功处理,跳过(幂等性)
	if messageStatus.Status == StatusSuccess {
		log.Printf("[VOICE_HANDLER] 消息 %s 已成功处理,跳过重复执行", messageID)
		return true, nil
	}

	// 超过最大重试次数,标记为失败
	if attempts > MaxRetryAttempts {
		log.Printf("[VOICE_HANDLER] 消息 %s 超过最大重试次数,标记为失败", messageID)
		handler.updateMessageStatus(ctx, messageID, StatusFailed, ErrorMaxAttemptsExceeded)
		return true, nil
	}

	return false, nil
}

// createInitialStatus 创建初始消息状态记录
// 用于兼容未通过正常流程创建状态的历史消息
func (handler *VoiceMessageHandler) createInitialStatus(
	ctx context.Context,
	messageID string,
	phone string,
	content string,
	channelType string,
) {
	if handler.statusStore == nil || messageID == "" {
		return
	}

	now := time.Now().Unix()
	initialStatus := &status.MessageStatus{
		MessageID: messageID,
		Channel:   channelType,
		Phone:     phone,
		Content:   content,
		Status:    StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := handler.statusStore.SaveStatus(ctx, initialStatus); err != nil {
		log.Printf("[VOICE_HANDLER] 警告: 创建初始状态失败 (message_id=%s): %v", messageID, err)
	}
}

// sendVoiceCall 执行实际的语音拨号
func (handler *VoiceMessageHandler) sendVoiceCall(
	ctx context.Context,
	messageID string,
	phone string,
	content string,
) error {
	// 验证 Modem 模块是否已配置
	if handler.modemManager == nil {
		handler.updateMessageStatus(ctx, messageID, StatusFailed, ErrorModemNotConfigured)
		return fmt.Errorf(ErrorModemNotConfigured)
	}

	// 执行语音拨号
	if err := handler.modemManager.SendVoice(ctx, phone, content); err != nil {
		handler.updateMessageStatus(ctx, messageID, StatusFailed, err.Error())
		return fmt.Errorf("语音发送失败: %w", err)
	}

	// 更新为成功状态
	handler.updateMessageStatus(ctx, messageID, StatusSuccess, "")
	return nil
}

// updateMessageStatus 更新消息状态
// 统一的状态更新入口,包含错误处理
func (handler *VoiceMessageHandler) updateMessageStatus(
	ctx context.Context,
	messageID string,
	newStatus string,
	errorMessage string,
) {
	if handler.statusStore == nil || messageID == "" {
		return
	}

	if err := handler.statusStore.UpdateStatus(ctx, messageID, newStatus, errorMessage); err != nil {
		log.Printf(
			"[VOICE_HANDLER] 警告: 更新状态失败 (message_id=%s, status=%s): %v",
			messageID,
			newStatus,
			err,
		)
	}
}

// SMSMessageHandler 短信消息处理器
// 负责处理短信队列消息,支持幂等和重试控制
type SMSMessageHandler struct {
	modemManager *modem.LazyModemManager
	statusStore  status.StatusStore
}

// NewSMSMessageHandler 创建短信消息处理器实例
func NewSMSMessageHandler(
	modemManager *modem.LazyModemManager,
	statusStore status.StatusStore,
) *SMSMessageHandler {
	return &SMSMessageHandler{
		modemManager: modemManager,
		statusStore:  statusStore,
	}
}

// HandleSMSMessage 处理短信消息
// 实现幂等性和最大重试次数控制
func (handler *SMSMessageHandler) HandleSMSMessage(
	ctx context.Context,
	message queue.SMSMessage,
	attempts uint16,
) error {
	log.Printf(
		"[SMS_HANDLER] 处理短信消息 -> %s, 内容: %s (尝试次数: %d)",
		message.Phone,
		message.Content,
		attempts,
	)

	messageID := message.GetMessageID()

	// 检查消息是否已成功处理(幂等性保证)
	if shouldSkip, err := handler.checkMessageStatus(ctx, messageID, attempts); shouldSkip {
		return err
	}

	// 超过最大重试次数,不再处理
	if attempts > MaxRetryAttempts {
		return nil
	}

	// 执行实际的短信发送
	return handler.sendSMSMessage(ctx, messageID, message.Phone, message.Content)
}

// checkMessageStatus 检查消息状态
// 返回是否应该跳过处理和可能的错误
func (handler *SMSMessageHandler) checkMessageStatus(
	ctx context.Context,
	messageID string,
	attempts uint16,
) (bool, error) {
	if handler.statusStore == nil || messageID == "" {
		return false, nil
	}

	messageStatus, err := handler.statusStore.GetStatus(ctx, messageID)

	// 状态不存在,创建初始状态记录(兼容历史数据)
	if err != nil || messageStatus == nil {
		handler.createInitialStatus(ctx, messageID, "", "", ChannelSMS)
		return false, nil
	}

	// 消息已成功处理,跳过(幂等性)
	if messageStatus.Status == StatusSuccess {
		log.Printf("[SMS_HANDLER] 消息 %s 已成功处理,跳过重复执行", messageID)
		return true, nil
	}

	// 超过最大重试次数,标记为失败
	if attempts > MaxRetryAttempts {
		log.Printf("[SMS_HANDLER] 消息 %s 超过最大重试次数,标记为失败", messageID)
		handler.updateMessageStatus(ctx, messageID, StatusFailed, ErrorMaxAttemptsExceeded)
		return true, nil
	}

	return false, nil
}

// createInitialStatus 创建初始消息状态记录
// 用于兼容未通过正常流程创建状态的历史消息
func (handler *SMSMessageHandler) createInitialStatus(
	ctx context.Context,
	messageID string,
	phone string,
	content string,
	channelType string,
) {
	if handler.statusStore == nil || messageID == "" {
		return
	}

	now := time.Now().Unix()
	initialStatus := &status.MessageStatus{
		MessageID: messageID,
		Channel:   channelType,
		Phone:     phone,
		Content:   content,
		Status:    StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := handler.statusStore.SaveStatus(ctx, initialStatus); err != nil {
		log.Printf("[SMS_HANDLER] 警告: 创建初始状态失败 (message_id=%s): %v", messageID, err)
	}
}

// sendSMSMessage 执行实际的短信发送
func (handler *SMSMessageHandler) sendSMSMessage(
	ctx context.Context,
	messageID string,
	phone string,
	content string,
) error {
	// 验证 Modem 模块是否已配置
	if handler.modemManager == nil {
		handler.updateMessageStatus(ctx, messageID, StatusFailed, ErrorModemNotConfigured)
		return fmt.Errorf(ErrorModemNotConfigured)
	}

	// 执行短信发送
	if err := handler.modemManager.SendSMS(ctx, phone, content); err != nil {
		handler.updateMessageStatus(ctx, messageID, StatusFailed, err.Error())
		return fmt.Errorf("短信发送失败: %w", err)
	}

	// 更新为成功状态
	handler.updateMessageStatus(ctx, messageID, StatusSuccess, "")
	return nil
}

// updateMessageStatus 更新消息状态
// 统一的状态更新入口,包含错误处理
func (handler *SMSMessageHandler) updateMessageStatus(
	ctx context.Context,
	messageID string,
	newStatus string,
	errorMessage string,
) {
	if handler.statusStore == nil || messageID == "" {
		return
	}

	if err := handler.statusStore.UpdateStatus(ctx, messageID, newStatus, errorMessage); err != nil {
		log.Printf(
			"[SMS_HANDLER] 警告: 更新状态失败 (message_id=%s, status=%s): %v",
			messageID,
			newStatus,
			err,
		)
	}
}
