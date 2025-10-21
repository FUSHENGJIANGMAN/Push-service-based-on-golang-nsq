package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"push-gateway/internal/modem"
	"push-gateway/internal/push"
	"push-gateway/internal/status"
)

// ==================== 常量定义 ====================

const (
	maxRequestBodySize      = 1 << 20 // 1MB
	defaultSendTimeout      = 10 * time.Second
	defaultRoutePlanTimeout = 5 * time.Second
	defaultRetryBackoff     = 300 * time.Millisecond
	defaultTopic            = "Msg"
	defaultPriority         = 1
	defaultMaxRetryAttempts = 1
)

// ==================== 服务接口 ====================

// Service 推送服务统一接口
// 解耦 HTTP 层与业务实现
type Service interface {
	Send(ctx context.Context, delivery push.Delivery) (string, error)
	SendAsync(ctx context.Context, delivery push.Delivery) (string, error)
	SendSync(ctx context.Context, delivery push.Delivery) error
	SendSyncWithResult(ctx context.Context, delivery push.Delivery) (*push.SendResult, error)
}

// ==================== Handler 处理器 ====================

// Handler 通用推送接口处理器
// 处理 /v1/push 请求
type Handler struct {
	service       Service
	allowedTopics map[string]struct{}
	formats       map[string]push.MessageFormat
	registry      *push.MessageRegistry
	statusStore   status.StatusStore
	modemManager  *modem.LazyModemManager
}

// NewHandler 创建基于静态格式的处理器
func NewHandler(
	service Service,
	allowedTopics []string,
	formats map[string]push.MessageFormat,
	statusStore status.StatusStore,
) *Handler {
	return &Handler{
		service:       service,
		allowedTopics: buildTopicMap(allowedTopics),
		formats:       formats,
		statusStore:   statusStore,
	}
}

// NewHandlerWithRegistry 创建支持动态消息格式的处理器
func NewHandlerWithRegistry(
	service Service,
	allowedTopics []string,
	formats map[string]push.MessageFormat,
	registry *push.MessageRegistry,
	statusStore status.StatusStore,
	modemManager *modem.LazyModemManager,
) *Handler {
	return &Handler{
		service:       service,
		allowedTopics: buildTopicMap(allowedTopics),
		formats:       formats,
		registry:      registry,
		statusStore:   statusStore,
		modemManager:  modemManager,
	}
}

// ServeHTTP 实现 http.Handler 接口
// POST /v1/push?mode=sync|async
func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := handler.handlePushRequest(writer, request); err != nil {
		log.Printf("[PUSH_HANDLER] 处理失败: %v", err)
	}
}

// ==================== 核心处理逻辑 ====================

// handlePushRequest 处理推送请求的核心逻辑
func (handler *Handler) handlePushRequest(writer http.ResponseWriter, request *http.Request) error {
	requestData, err := handler.parseRequestBody(request)
	if err != nil {
		writeError(writer, "解析请求失败: "+err.Error(), http.StatusBadRequest)
		return err
	}

	topic := handler.extractTopic(requestData)
	if !handler.isTopicAllowed(topic) {
		writeError(writer, "topic 不允许访问", http.StatusBadRequest)
		return fmt.Errorf("topic not allowed: %s", topic)
	}

	messageData, err := handler.extractMessageData(requestData)
	if err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return err
	}

	handler.mergeTopLevelFields(requestData, messageData)

	if err := handler.validateMessageFields(topic, messageData); err != nil {
		writeError(writer, "消息验证失败: "+err.Error(), http.StatusBadRequest)
		return err
	}

	channels := handler.extractChannels(requestData)
	recipients := handler.extractRecipientsByChannel(messageData, channels)

	log.Printf("[PUSH_HANDLER] 处理消息: topic=%s, channels=%v, recipients=%d",
		topic, channels, len(recipients))

	delivery, err := handler.buildDelivery(topic, requestData, messageData, channels, recipients)
	if err != nil {
		writeError(writer, "构建投递失败: "+err.Error(), http.StatusBadRequest)
		return err
	}

	messageID, err := handler.sendMessage(request.Context(), delivery)
	if err != nil {
		writeError(writer, "发送失败: "+err.Error(), http.StatusInternalServerError)
		return err
	}

	handler.writeSuccessResponse(writer, messageID)
	return nil
}

// ==================== 请求解析 ====================

// parseRequestBody 解析请求体为 map
func (handler *Handler) parseRequestBody(request *http.Request) (map[string]interface{}, error) {
	request.Body = http.MaxBytesReader(nil, request.Body, maxRequestBodySize)
	defer request.Body.Close()

	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()

	var requestData map[string]interface{}
	if err := decoder.Decode(&requestData); err != nil {
		return nil, fmt.Errorf("JSON 解析失败: %w", err)
	}

	// 合并 URL 查询参数中的 mode
	if queryMode := request.URL.Query().Get("mode"); queryMode != "" {
		requestData["mode"] = queryMode
	}

	return requestData, nil
}

// extractTopic 提取并规范化 topic
func (handler *Handler) extractTopic(requestData map[string]interface{}) string {
	topic, _ := requestData["topic"].(string)
	if topic == "" {
		topic = defaultTopic
		requestData["topic"] = topic
	}
	return topic
}

// extractMessageData 提取消息数据
func (handler *Handler) extractMessageData(requestData map[string]interface{}) (map[string]interface{}, error) {
	messageData, ok := requestData["msg"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("msg 字段必须是对象")
	}
	return messageData, nil
}

// mergeTopLevelFields 合并顶层字段到消息数据
// 兼容不同的调用风格
func (handler *Handler) mergeTopLevelFields(requestData, messageData map[string]interface{}) {
	topLevelFields := []string{"phones", "emails", "userIds"}

	for _, field := range topLevelFields {
		if value, exists := requestData[field]; exists {
			if _, hasField := messageData[field]; !hasField || messageData[field] == nil {
				messageData[field] = value
			}
		}
	}
}

// extractChannels 提取通道列表
func (handler *Handler) extractChannels(requestData map[string]interface{}) []push.Channel {
	var channels []push.Channel

	channelsRaw, ok := requestData["channels"].([]interface{})
	if !ok {
		return []push.Channel{push.ChWeb} // 默认 Web 通道
	}

	for _, channelValue := range channelsRaw {
		channelString, ok := channelValue.(string)
		if !ok {
			continue
		}

		if channel := parseChannel(channelString); channel != "" {
			channels = append(channels, channel)
		}
	}

	if len(channels) == 0 {
		return []push.Channel{push.ChWeb}
	}

	return channels
}

// extractRecipientsByChannel 根据通道类型提取收件人
func (handler *Handler) extractRecipientsByChannel(
	messageData map[string]interface{},
	channels []push.Channel,
) []push.Recipient {
	var recipients []push.Recipient

	log.Printf("[PUSH_HANDLER] 提取收件人: channels=%v, messageKeys=%v",
		channels, getMapKeys(messageData))

	for _, channel := range channels {
		switch channel {
		case push.ChWeb:
			recipients = append(recipients, handler.extractWebRecipients(messageData)...)
		case push.ChEmail:
			recipients = append(recipients, handler.extractEmailRecipients(messageData)...)
		case push.ChSMS, push.ChVoice:
			recipients = append(recipients, handler.extractPhoneRecipients(messageData)...)
		}
	}

	// 回退：如果没有提取到收件人，尝试从 userIds 提取
	if len(recipients) == 0 {
		recipients = handler.extractWebRecipients(messageData)
	}

	return recipients
}

// extractWebRecipients 提取 Web 通道收件人
func (handler *Handler) extractWebRecipients(messageData map[string]interface{}) []push.Recipient {
	userIDs, ok := messageData["userIds"].([]interface{})
	if !ok {
		return nil
	}

	recipients := make([]push.Recipient, 0, len(userIDs))
	for _, userID := range userIDs {
		userIDString := convertToString(userID)
		if userIDString != "" {
			recipients = append(recipients, push.Recipient{
				UserID:   userIDString,
				WebInbox: userIDString,
			})
		}
	}

	return recipients
}

// extractEmailRecipients 提取邮件通道收件人
func (handler *Handler) extractEmailRecipients(messageData map[string]interface{}) []push.Recipient {
	emails, ok := messageData["emails"].([]interface{})
	if !ok {
		return nil
	}

	recipients := make([]push.Recipient, 0, len(emails))
	for _, email := range emails {
		emailString, ok := email.(string)
		if ok && emailString != "" {
			recipients = append(recipients, push.Recipient{Email: emailString})
		}
	}

	return recipients
}

// extractPhoneRecipients 提取电话通道收件人
func (handler *Handler) extractPhoneRecipients(messageData map[string]interface{}) []push.Recipient {
	phones, ok := messageData["phones"].([]interface{})
	if !ok {
		return nil
	}

	recipients := make([]push.Recipient, 0, len(phones))
	for _, phone := range phones {
		phoneString, ok := phone.(string)
		if ok && phoneString != "" {
			recipients = append(recipients, push.Recipient{Phone: phoneString})
		}
	}

	return recipients
}

// ==================== 消息构建 ====================

// buildDelivery 构建推送投递对象
func (handler *Handler) buildDelivery(
	topic string,
	requestData map[string]interface{},
	messageData map[string]interface{},
	channels []push.Channel,
	recipients []push.Recipient,
) (push.Delivery, error) {
	if handler.registry != nil {
		return handler.buildDynamicDelivery(topic, requestData, messageData, channels, recipients)
	}
	return handler.buildStaticDelivery(topic, requestData, messageData, channels, recipients)
}

// buildDynamicDelivery 使用动态格式构建投递
func (handler *Handler) buildDynamicDelivery(
	topic string,
	requestData map[string]interface{},
	messageData map[string]interface{},
	channels []push.Channel,
	recipients []push.Recipient,
) (push.Delivery, error) {
	dynamicMessage, err := handler.registry.ParseMessage(topic, messageData)
	if err != nil {
		return push.Delivery{}, fmt.Errorf("动态消息解析失败: %w", err)
	}

	standardMessage := dynamicMessage.ToMessage()
	if len(recipients) > 0 {
		standardMessage.To = recipients
	}

	mode, _ := requestData["mode"].(string)

	routePlan := handler.extractRoutePlan(requestData, channels)

	return push.Delivery{
		Topic: topic,
		Mode:  mode,
		Msg:   standardMessage,
		Plan:  routePlan,
	}, nil
}

// buildStaticDelivery 使用静态格式构建投递
func (handler *Handler) buildStaticDelivery(
	topic string,
	requestData map[string]interface{},
	messageData map[string]interface{},
	channels []push.Channel,
	recipients []push.Recipient,
) (push.Delivery, error) {
	deliveryBytes, _ := json.Marshal(requestData)

	var delivery push.Delivery
	if err := json.Unmarshal(deliveryBytes, &delivery); err != nil {
		return push.Delivery{}, fmt.Errorf("投递格式错误: %w", err)
	}

	if delivery.Msg.Data == nil {
		delivery.Msg.Data = make(map[string]any)
	}

	handler.populateMessageData(&delivery.Msg, topic, messageData)
	handler.mapUserIDsToRecipients(&delivery.Msg, topic, messageData)

	if len(recipients) > 0 {
		delivery.Msg.To = recipients
	}

	if len(delivery.Plan.Primary) == 0 {
		delivery.Plan.Primary = channels
	}

	delivery.Msg.Kind = handler.topicToKind(topic)

	return delivery, nil
}

// populateMessageData 填充消息数据字段
func (handler *Handler) populateMessageData(message *push.Message, topic string, messageData map[string]interface{}) {
	format, exists := handler.formats[topic]
	if !exists {
		return
	}

	for _, field := range format.Fields {
		if value, ok := messageData[field.Name]; ok {
			message.Data[field.Name] = value
		}
	}
}

// mapUserIDsToRecipients 从 userIds 字段映射收件人
// 仅当消息还没有收件人时生效
func (handler *Handler) mapUserIDsToRecipients(message *push.Message, topic string, messageData map[string]interface{}) {
	if len(message.To) > 0 {
		return
	}

	userIDField := handler.extractUserIDField(topic)
	userIDs, ok := messageData[userIDField].([]interface{})
	if !ok {
		return
	}

	recipients := make([]push.Recipient, 0, len(userIDs))
	for _, userID := range userIDs {
		userIDString := convertToString(userID)
		if userIDString != "" {
			recipients = append(recipients, push.Recipient{UserID: userIDString})
		}
	}

	if len(recipients) > 0 {
		message.To = recipients
	}
}

// extractRoutePlan 提取或构建路由计划
func (handler *Handler) extractRoutePlan(requestData map[string]interface{}, channels []push.Channel) push.RoutePlan {
	planRaw, ok := requestData["plan"].(map[string]interface{})
	if ok {
		planBytes, _ := json.Marshal(planRaw)
		var plan push.RoutePlan
		json.Unmarshal(planBytes, &plan)
		return plan
	}

	return push.RoutePlan{
		Primary:  channels,
		Fallback: []push.Channel{},
		Timeout:  defaultRoutePlanTimeout,
		Retry: push.RetryPolicy{
			MaxAttempts: defaultMaxRetryAttempts,
			Backoff:     defaultRetryBackoff,
		},
	}
}

// ==================== 消息发送 ====================

// sendMessage 发送消息
func (handler *Handler) sendMessage(ctx context.Context, delivery push.Delivery) (string, error) {
	sendContext, cancel := context.WithTimeout(ctx, defaultSendTimeout)
	defer cancel()

	switch delivery.Mode {
	case "async":
		return handler.service.SendAsync(sendContext, delivery)
	default:
		return handler.service.SendAsync(sendContext, delivery)
	}
}

// writeSuccessResponse 写入成功响应
func (handler *Handler) writeSuccessResponse(writer http.ResponseWriter, messageID string) {
	response := map[string]any{
		"code":       200,
		"msg":        "success",
		"message":    "消息已加入异步发送队列",
		"message_id": messageID,
		"timestamp":  time.Now().Unix(),
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(response)
}

// ==================== 验证方法 ====================

// isTopicAllowed 检查 topic 是否允许
func (handler *Handler) isTopicAllowed(topic string) bool {
	_, allowed := handler.allowedTopics[topic]
	return allowed
}

// validateMessageFields 验证消息字段
func (handler *Handler) validateMessageFields(topic string, messageData map[string]interface{}) error {
	if handler.registry != nil {
		return handler.registry.ValidateMessage(topic, messageData)
	}

	format, ok := handler.formats[topic]
	if !ok {
		return fmt.Errorf("未找到 topic 格式: %s", topic)
	}

	for _, field := range format.Fields {
		if field.Required {
			if _, exists := messageData[field.Name]; !exists {
				return fmt.Errorf("缺少必填字段: %s", field.Name)
			}
		}
	}

	return nil
}

// ==================== 辅助方法 ====================

// topicToKind 将 topic 转换为消息类型
// 影响 message_id 前缀生成
func (handler *Handler) topicToKind(topic string) push.MessageKind {
	// 优先处理特殊类型
	switch topic {
	case "VoiceMessage":
		return push.KindVoice
	case "SMSMessage":
		return push.KindSMS
	}

	// 使用动态注册表
	if handler.registry != nil {
		if _, exists := handler.registry.GetFormat(topic); exists {
			return push.MessageKind(strings.ToLower(topic))
		}
	}

	// 静态映射
	staticKindMap := map[string]push.MessageKind{
		"Notification":      "notification",
		"Alert":             "alert",
		"Msg":               "msg",
		"SystemMaintenance": "systemmaintenance",
		"OrderUpdate":       "orderupdate",
		"AccountSecurity":   "accountsecurity",
		"ProductLaunch":     "productlaunch",
	}

	if kind, exists := staticKindMap[topic]; exists {
		return kind
	}

	return push.MessageKind(strings.ToLower(topic))
}

// extractUserIDField 提取用户 ID 字段名
func (handler *Handler) extractUserIDField(topic string) string {
	if handler.registry == nil {
		return "userIds"
	}

	format, exists := handler.registry.GetFormat(topic)
	if !exists {
		return "userIds"
	}

	for _, field := range format.Fields {
		fieldNameLower := strings.ToLower(field.Name)
		if strings.Contains(fieldNameLower, "userid") {
			return field.Name
		}
	}

	return "userIds"
}

// ==================== DynamicMessageHandler ====================

// DynamicMessageHandler 动态消息专用处理器
// 处理 /v1/push/<Topic> 风格的请求
type DynamicMessageHandler struct {
	service  Service
	registry *push.MessageRegistry
	topic    string
}

// NewDynamicMessageHandler 创建动态消息处理器
func NewDynamicMessageHandler(service Service, registry *push.MessageRegistry, topic string) *DynamicMessageHandler {
	return &DynamicMessageHandler{
		service:  service,
		registry: registry,
		topic:    topic,
	}
}

// ServeHTTP 实现 http.Handler 接口
func (handler *DynamicMessageHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := handler.handleDynamicMessage(writer, request); err != nil {
		log.Printf("[DYNAMIC_HANDLER] 处理失败: %v", err)
	}
}

// handleDynamicMessage 处理动态消息的核心逻辑
func (handler *DynamicMessageHandler) handleDynamicMessage(writer http.ResponseWriter, request *http.Request) error {
	messageData, err := handler.parseMessageData(request)
	if err != nil {
		writeError(writer, "解析消息失败: "+err.Error(), http.StatusBadRequest)
		return err
	}

	log.Printf("[DYNAMIC_HANDLER] 收到%s消息, 字段: %v", handler.topic, getMapKeys(messageData))

	if err := handler.registry.ValidateMessage(handler.topic, messageData); err != nil {
		writeError(writer, "验证失败: "+err.Error(), http.StatusBadRequest)
		return err
	}

	dynamicMessage, err := handler.registry.ParseMessage(handler.topic, messageData)
	if err != nil {
		writeError(writer, "解析失败: "+err.Error(), http.StatusBadRequest)
		return err
	}

	delivery := handler.buildDynamicDelivery(dynamicMessage, messageData)

	log.Printf("[DYNAMIC_HANDLER] 投递信息: Topic=%s, Kind=%s, Recipients=%d, Channels=%v",
		delivery.Topic, delivery.Msg.Kind, len(delivery.Msg.To), delivery.Plan.Primary)

	shouldUseDetailedResult := handler.shouldUseDetailedResult(delivery.Plan.Primary, delivery.Mode)

	if shouldUseDetailedResult {
		return handler.sendWithDetailedResult(writer, request, delivery)
	}

	return handler.sendAsync(writer, request, delivery)
}

// parseMessageData 解析消息数据
func (handler *DynamicMessageHandler) parseMessageData(request *http.Request) (map[string]interface{}, error) {
	request.Body = http.MaxBytesReader(nil, request.Body, maxRequestBodySize)
	defer request.Body.Close()

	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()

	var messageData map[string]interface{}
	if err := decoder.Decode(&messageData); err != nil {
		return nil, fmt.Errorf("JSON解析失败: %w", err)
	}

	return messageData, nil
}

// buildDynamicDelivery 构建动态消息投递
func (handler *DynamicMessageHandler) buildDynamicDelivery(
	dynamicMessage *push.DynamicMessage,
	messageData map[string]interface{},
) push.Delivery {
	recipients := extractAllRecipients(messageData)
	priority := extractPriority(messageData)
	channels := extractChannelsFromData(messageData)
	attachments := extractAttachments(handler.topic, messageData)

	message := push.Message{
		Kind:        push.MessageKind(handler.topic),
		Subject:     dynamicMessage.Subject,
		Body:        dynamicMessage.Body,
		To:          recipients,
		CreatedAt:   time.Now(),
		Data:        messageData,
		Priority:    priority,
		Attachments: attachments,
	}

	routePlan := push.RoutePlan{
		Primary:  channels,
		Fallback: []push.Channel{},
		Timeout:  defaultRoutePlanTimeout,
		Retry: push.RetryPolicy{
			MaxAttempts: defaultMaxRetryAttempts,
			Backoff:     defaultRetryBackoff,
		},
	}

	return push.Delivery{
		Topic: handler.topic,
		Mode:  "async",
		Msg:   message,
		Plan:  routePlan,
	}
}

// sendWithDetailedResult 发送并返回详细结果
func (handler *DynamicMessageHandler) sendWithDetailedResult(
	writer http.ResponseWriter,
	request *http.Request,
	delivery push.Delivery,
) error {
	log.Printf("[DYNAMIC_HANDLER] 同步发送...")

	result, err := handler.service.SendSyncWithResult(request.Context(), delivery)
	if err != nil {
		writeError(writer, "发送失败: "+err.Error(), http.StatusInternalServerError)
		return err
	}

	handler.writeDetailedResultResponse(writer, result)
	return nil
}

// sendAsync 异步发送
func (handler *DynamicMessageHandler) sendAsync(
	writer http.ResponseWriter,
	request *http.Request,
	delivery push.Delivery,
) error {
	log.Printf("[DYNAMIC_HANDLER] 异步发送...")

	messageID, err := handler.service.SendAsync(request.Context(), delivery)
	if err != nil {
		writeError(writer, "发送失败: "+err.Error(), http.StatusInternalServerError)
		return err
	}

	handler.writeAsyncResponse(writer, messageID)
	return nil
}

// writeDetailedResultResponse 写入详细结果响应
func (handler *DynamicMessageHandler) writeDetailedResultResponse(writer http.ResponseWriter, result *push.SendResult) {
	channelMessages := buildChannelMessages(result.ChannelResults)
	messageIDs := extractMessageIDs(result.ChannelResults)

	response := map[string]interface{}{
		"code":             200,
		"msg":              "success",
		"overall_status":   result.OverallStatus,
		"channel_results":  result.ChannelResults,
		"channel_messages": channelMessages,
		"summary": map[string]interface{}{
			"total_channels": result.TotalChannels,
			"success_count":  result.SuccessCount,
			"failed_count":   result.FailedCount,
			"pending_count":  result.PendingCount,
		},
		"message_ids": messageIDs,
		"timestamp":   time.Now().Unix(),
	}

	if len(messageIDs) == 1 {
		response["message_id"] = messageIDs[0]
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(response)
}

// writeAsyncResponse 写入异步响应
func (handler *DynamicMessageHandler) writeAsyncResponse(writer http.ResponseWriter, messageID string) {
	response := map[string]interface{}{
		"code":       200,
		"msg":        "success",
		"message":    "消息已加入异步发送队列",
		"message_id": messageID,
		"timestamp":  time.Now().Unix(),
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(response)
}

// shouldUseDetailedResult 判断是否需要返回详细结果
func (handler *DynamicMessageHandler) shouldUseDetailedResult(channels []push.Channel, mode string) bool {
	if mode == "sync" {
		return true
	}

	for _, channel := range channels {
		if channel == push.ChSMS || channel == push.ChVoice {
			return true
		}
	}

	return false
}

// ==================== 工具函数 ====================

// buildTopicMap 构建 topic 白名单 map
func buildTopicMap(topics []string) map[string]struct{} {
	topicMap := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		topicMap[topic] = struct{}{}
	}
	return topicMap
}

// parseChannel 解析通道字符串
func parseChannel(channelString string) push.Channel {
	channelMap := map[string]push.Channel{
		"web":   push.ChWeb,
		"email": push.ChEmail,
		"sms":   push.ChSMS,
		"voice": push.ChVoice,
	}

	if channel, exists := channelMap[channelString]; exists {
		return channel
	}

	return ""
}

// convertToString 将 interface{} 转换为字符串
func convertToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return fmt.Sprintf("%.0f", v)
	case int:
		return fmt.Sprintf("%d", v)
	default:
		return ""
	}
}

// getMapKeys 获取 map 的所有键
func getMapKeys(data map[string]interface{}) []string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	return keys
}

// extractAllRecipients 从消息数据中提取所有收件人
func extractAllRecipients(messageData map[string]interface{}) []push.Recipient {
	var recipients []push.Recipient

	// 提取 userIds
	if userIDs, ok := messageData["userIds"].([]interface{}); ok {
		for _, userID := range userIDs {
			userIDString := convertToString(userID)
			if userIDString != "" {
				recipients = append(recipients, push.Recipient{
					UserID:   userIDString,
					WebInbox: userIDString,
				})
			}
		}
	}

	// 提取 emails
	if emails, ok := messageData["emails"].([]interface{}); ok {
		for _, email := range emails {
			if emailString, ok := email.(string); ok && emailString != "" {
				recipients = appendUniqueEmailRecipient(recipients, emailString)
			}
		}
	}

	// 提取 phones
	if phones, ok := messageData["phones"].([]interface{}); ok {
		for _, phone := range phones {
			if phoneString, ok := phone.(string); ok && phoneString != "" {
				recipients = appendUniquePhoneRecipient(recipients, phoneString)
			}
		}
	}

	return recipients
}

// appendUniqueEmailRecipient 添加唯一的邮件收件人
func appendUniqueEmailRecipient(recipients []push.Recipient, email string) []push.Recipient {
	for _, recipient := range recipients {
		if recipient.Email == email {
			return recipients
		}
	}

	return append(recipients, push.Recipient{
		UserID: email,
		Email:  email,
	})
}

// appendUniquePhoneRecipient 添加唯一的电话收件人
func appendUniquePhoneRecipient(recipients []push.Recipient, phone string) []push.Recipient {
	for _, recipient := range recipients {
		if recipient.Phone == phone {
			return recipients
		}
	}

	return append(recipients, push.Recipient{
		UserID: phone,
		Phone:  phone,
	})
}

// extractPriority 提取优先级
func extractPriority(messageData map[string]interface{}) int {
	priorityValue, exists := messageData["priority"]
	if !exists {
		return defaultPriority
	}

	switch p := priorityValue.(type) {
	case int:
		return p
	case float64:
		return int(p)
	case string:
		if priorityInt, err := strconv.Atoi(p); err == nil {
			return priorityInt
		}
	}

	return defaultPriority
}

// extractChannelsFromData 从消息数据中提取通道
func extractChannelsFromData(messageData map[string]interface{}) []push.Channel {
	channelsValue, exists := messageData["channels"]
	if !exists {
		return []push.Channel{push.ChWeb}
	}

	channelsList, ok := channelsValue.([]interface{})
	if !ok {
		return []push.Channel{push.ChWeb}
	}

	var channels []push.Channel
	for _, channelValue := range channelsList {
		channelString, ok := channelValue.(string)
		if !ok {
			continue
		}

		if channel := parseChannel(channelString); channel != "" {
			channels = append(channels, channel)
		}
	}

	if len(channels) == 0 {
		return []push.Channel{push.ChWeb}
	}

	return channels
}

// extractAttachments 提取附件
func extractAttachments(topic string, messageData map[string]interface{}) []push.EmailAttachment {
	if topic != "EmailWithAttachments" {
		return nil
	}

	attachmentsValue, exists := messageData["attachments"]
	if !exists {
		return nil
	}

	attachmentsList, ok := attachmentsValue.([]interface{})
	if !ok {
		return nil
	}

	attachments := make([]push.EmailAttachment, 0, len(attachmentsList))
	for _, attachmentValue := range attachmentsList {
		attachmentMap, ok := attachmentValue.(map[string]interface{})
		if !ok {
			continue
		}

		attachment := push.EmailAttachment{}

		if fileName, ok := attachmentMap["fileName"].(string); ok {
			attachment.FileName = fileName
		}
		if fileType, ok := attachmentMap["fileType"].(string); ok {
			attachment.FileType = fileType
		}
		if filePath, ok := attachmentMap["filePath"].(string); ok {
			attachment.FilePath = filePath
		}

		attachments = append(attachments, attachment)
	}

	log.Printf("[DYNAMIC_HANDLER] 提取附件数量: %d", len(attachments))
	return attachments
}

// buildChannelMessages 构建通道消息列表
func buildChannelMessages(channelResults []*push.ChannelResult) []string { // 改为指针切片
	messages := make([]string, 0, len(channelResults))

	for _, result := range channelResults {
		channelName := getChannelDisplayName(result.Channel)

		var message string
		switch result.Status {
		case "success":
			message = fmt.Sprintf("%s发送成功", channelName)
		case "failed":
			errorMessage := result.Error
			if errorMessage == "" {
				errorMessage = "未知错误"
			}
			message = fmt.Sprintf("%s发送失败: %s", channelName, errorMessage)
		default:
			message = fmt.Sprintf("%s发送中...", channelName)
		}

		messages = append(messages, message)
	}

	return messages
}

// extractMessageIDs 提取所有消息 ID
func extractMessageIDs(channelResults []*push.ChannelResult) []string { // 改为指针切片
	messageIDs := make([]string, 0, len(channelResults))

	for _, result := range channelResults {
		if result.MessageID != "" {
			messageIDs = append(messageIDs, result.MessageID)
		}
	}

	return messageIDs
}

// getChannelDisplayName 获取通道的中文显示名称
func getChannelDisplayName(channel push.Channel) string {
	displayNames := map[push.Channel]string{
		push.ChWeb:   "站内信",
		push.ChEmail: "邮件",
		push.ChSMS:   "短信",
		push.ChVoice: "语音",
	}

	if name, exists := displayNames[channel]; exists {
		return name
	}

	return string(channel)
}
