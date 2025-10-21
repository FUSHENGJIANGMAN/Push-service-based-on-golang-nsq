package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"push-gateway/internal/push"
)

// ==================== 常量定义 ====================

const (
	simpleMessageTimeout  = 30 * time.Second
	defaultSimplePriority = 3
	maxContentLogLength   = 50
	maxContentRespLength  = 100
	partialSuccessStatus  = http.StatusPartialContent
)

// ==================== Simple Message Handler ====================

// SimpleMessageHandler 处理简单消息（语音和短信）
// 只包含内容的简化消息类型
type SimpleMessageHandler struct {
	service     Service
	channel     push.Channel
	messageType string
}

// NewVoiceMessageHandler 创建语音消息处理器
func NewVoiceMessageHandler(service Service) *SimpleMessageHandler {
	return &SimpleMessageHandler{
		service:     service,
		channel:     push.ChVoice,
		messageType: "VoiceMessage",
	}
}

// NewSMSMessageHandler 创建短信消息处理器
func NewSMSMessageHandler(service Service) *SimpleMessageHandler {
	return &SimpleMessageHandler{
		service:     service,
		channel:     push.ChSMS,
		messageType: "SMSMessage",
	}
}

// ==================== 数据模型 ====================

// SimpleMessageRequest 简单消息请求结构
type SimpleMessageRequest struct {
	To       []push.Recipient `json:"to"`
	Content  string           `json:"content"`
	Priority int              `json:"priority,omitempty"`
	Sender   uint64           `json:"sender,omitempty"`
}

// ==================== HTTP 处理方法 ====================

// ServeHTTP 实现 http.Handler 接口
func (handler *SimpleMessageHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	log.Printf("[%s] 收到请求: %s", handler.messageType, request.URL.String())

	if err := handler.handleSimpleMessage(writer, request); err != nil {
		log.Printf("[%s] 处理失败: %v", handler.messageType, err)
	}
}

// ==================== 核心处理逻辑 ====================

// handleSimpleMessage 处理简单消息的核心逻辑
func (handler *SimpleMessageHandler) handleSimpleMessage(writer http.ResponseWriter, request *http.Request) error {
	messageRequest, err := handler.parseRequest(request)
	if err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return err
	}

	if err := handler.validateRequest(messageRequest); err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return err
	}

	delivery := handler.buildDelivery(messageRequest)

	log.Printf("[%s] 处理投递: To=%v, Channel=%s, Content=%s",
		handler.messageType,
		delivery.Msg.To,
		handler.channel,
		truncateString(messageRequest.Content, maxContentLogLength))

	result, err := handler.sendMessage(request.Context(), delivery)
	if err != nil {
		handler.writeFailureResponse(writer, messageRequest, err)
		return err
	}

	handler.writeSuccessResponse(writer, messageRequest, result)
	log.Printf("[%s] 处理完成，状态: %s", handler.messageType, result.OverallStatus)
	return nil
}

// ==================== 请求解析和验证 ====================

// parseRequest 解析请求
func (handler *SimpleMessageHandler) parseRequest(request *http.Request) (*SimpleMessageRequest, error) {
	var messageRequest SimpleMessageRequest
	if err := json.NewDecoder(request.Body).Decode(&messageRequest); err != nil {
		return nil, fmt.Errorf("JSON解析失败: %w", err)
	}
	return &messageRequest, nil
}

// validateRequest 验证请求
func (handler *SimpleMessageHandler) validateRequest(request *SimpleMessageRequest) error {
	if len(request.To) == 0 {
		return fmt.Errorf("缺少必填字段: to")
	}

	if request.Content == "" {
		return fmt.Errorf("缺少必填字段: content")
	}

	return handler.validateRecipients(request.To)
}

// validateRecipients 验证接收者信息
func (handler *SimpleMessageHandler) validateRecipients(recipients []push.Recipient) error {
	for i, recipient := range recipients {
		if recipient.Phone == "" {
			return fmt.Errorf("接收者 %d 缺少电话号码", i)
		}
	}
	return nil
}

// ==================== 消息构建 ====================

// buildDelivery 构建投递对象
func (handler *SimpleMessageHandler) buildDelivery(request *SimpleMessageRequest) push.Delivery {
	message := handler.buildMessage(request)
	routePlan := handler.buildRoutePlan()

	return push.Delivery{
		Msg:   message,
		Plan:  routePlan,
		Topic: handler.messageType,
	}
}

// buildMessage 构建消息
func (handler *SimpleMessageHandler) buildMessage(request *SimpleMessageRequest) push.Message {
	priority := request.Priority
	if priority == 0 {
		priority = defaultSimplePriority
	}

	return push.Message{
		Kind:      push.KindInApp,
		Subject:   fmt.Sprintf("%s消息", handler.messageType),
		Body:      request.Content,
		To:        request.To,
		Priority:  priority,
		CreatedAt: time.Now(),
		Data: map[string]any{
			"content": request.Content,
		},
	}
}

// buildRoutePlan 构建路由计划
// 只使用对应的通道
func (handler *SimpleMessageHandler) buildRoutePlan() push.RoutePlan {
	return push.RoutePlan{
		Primary: []push.Channel{handler.channel},
	}
}

// ==================== 消息发送 ====================

// sendMessage 发送消息
// SMS和语音消息总是使用同步发送以获取真实结果
func (handler *SimpleMessageHandler) sendMessage(ctx context.Context, delivery push.Delivery) (*push.SendResult, error) {
	sendContext, cancel := context.WithTimeout(ctx, simpleMessageTimeout)
	defer cancel()

	return handler.service.SendSyncWithResult(sendContext, delivery)
}

// ==================== 响应写入 ====================

// writeFailureResponse 写入失败响应
func (handler *SimpleMessageHandler) writeFailureResponse(
	writer http.ResponseWriter,
	request *SimpleMessageRequest,
	err error,
) {
	log.Printf("[%s] 发送失败: %v", handler.messageType, err)

	response := map[string]interface{}{
		"success":        false,
		"message":        fmt.Sprintf("%s发送失败", handler.messageType),
		"error":          err.Error(),
		"overall_status": "failed",
		"timestamp":      time.Now().Unix(),
		"to":             request.To,
		"content":        truncateString(request.Content, maxContentRespLength),
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusInternalServerError)
	_ = json.NewEncoder(writer).Encode(response)
}

// writeSuccessResponse 写入成功响应
func (handler *SimpleMessageHandler) writeSuccessResponse(
	writer http.ResponseWriter,
	request *SimpleMessageRequest,
	result *push.SendResult,
) {
	channelMessages := buildChannelMessages(result.ChannelResults)
	messageIDs := extractMessageIDs(result.ChannelResults)

	response := handler.buildResponseData(request, result, channelMessages, messageIDs)

	statusCode := http.StatusOK
	if result.OverallStatus != "success" {
		statusCode = partialSuccessStatus
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)
	_ = json.NewEncoder(writer).Encode(response)
}

// buildResponseData 构建响应数据
func (handler *SimpleMessageHandler) buildResponseData(
	request *SimpleMessageRequest,
	result *push.SendResult,
	channelMessages []string,
	messageIDs []string,
) map[string]interface{} {
	response := map[string]interface{}{
		"success":          result.OverallStatus == "success",
		"message":          fmt.Sprintf("%s处理完成", handler.messageType),
		"overall_status":   result.OverallStatus,
		"channel_results":  result.ChannelResults,
		"channel_messages": channelMessages,
		"summary": map[string]interface{}{
			"total_channels": result.TotalChannels,
			"success_count":  result.SuccessCount,
			"failed_count":   result.FailedCount,
			"pending_count":  result.PendingCount,
		},
		"timestamp": time.Now().Unix(),
		"to":        request.To,
		"content":   truncateString(request.Content, maxContentRespLength),
	}

	handler.addMessageIDsToResponse(response, messageIDs)

	return response
}

// addMessageIDsToResponse 添加消息ID到响应
func (handler *SimpleMessageHandler) addMessageIDsToResponse(
	response map[string]interface{},
	messageIDs []string,
) {
	if len(messageIDs) == 1 {
		response["message_id"] = messageIDs[0]
	} else if len(messageIDs) > 1 {
		response["message_ids"] = messageIDs
	}
}

// ==================== 工具函数 ====================

// truncateString 截断字符串用于日志和响应
func truncateString(content string, maxLength int) string {
	if len(content) <= maxLength {
		return content
	}
	return content[:maxLength] + "..."
}
