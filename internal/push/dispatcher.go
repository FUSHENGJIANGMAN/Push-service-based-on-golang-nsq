package push

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"push-gateway/internal/status"
)

// ==================== 常量定义 ====================

const (
	// 消息状态常量
	statusPending         = "pending"
	statusSuccess         = "success"
	statusFailed          = "failed"
	statusPartial         = "partial"
	statusFallbackSuccess = "fallback_success"
	statusFallbackFailed  = "fallback_failed"

	// HTTP 状态码常量
	httpStatusOK          = "200"
	httpStatusServerError = "500"

	// 消息内容常量
	messageEnqueuePending = "入队等待处理"
	messageEnqueueFailed  = "入队失败"
	messageSyncSending    = "同步发送"
	messageNoRecipients   = "无收件人，自动成功"

	// 重试策略默认值
	defaultRetryBackoff = 300 * time.Millisecond

	// 内部标记
	asyncVoicePendingKey = "__async_pending_voice"
)

// ==================== 核心数据结构 ====================

// ChannelResult 表示单个通道的发送结果
type ChannelResult struct {
	Channel   Channel `json:"channel"`
	Status    string  `json:"status"` // "success", "failed", "pending"
	Error     string  `json:"error,omitempty"`
	SentAt    int64   `json:"sent_at,omitempty"`
	MessageID string  `json:"message_id,omitempty"`
}

// SendResult 表示整体发送结果
type SendResult struct {
	OverallStatus  string           `json:"overall_status"` // "success", "partial", "failed", "pending"
	ChannelResults []*ChannelResult `json:"channel_results"`
	TotalChannels  int              `json:"total_channels"`
	SuccessCount   int              `json:"success_count"`
	FailedCount    int              `json:"failed_count"`
	PendingCount   int              `json:"pending_count"`
}

// deliveryContext 消息投递上下文
type deliveryContext struct {
	delivery       Delivery
	messageID      string
	primarySuccess bool
	firstError     error
	asyncVoice     bool
}

// ==================== 接口定义 ====================

type Enqueuer interface {
	Enqueue(ctx context.Context, payload []byte) error
	Close()
}

// PriorityEnqueuer 支持优先级的入队器接口
type PriorityEnqueuer interface {
	Enqueuer
	EnqueueWithPriority(ctx context.Context, payload []byte, priority int) error
}

// Idempotency 幂等检查器接口
type Idempotency interface {
	CheckAndSet(ctx context.Context, delivery Delivery, ttl time.Duration) (bool, string, error)
}

// ==================== Dispatcher 结构 ====================

type Dispatcher struct {
	registry              Registry
	store                 Store
	enqueuer              Enqueuer
	currentTime           func() time.Time
	namespace             string
	syncPriorityThreshold int
	idempotency           Idempotency
	idempotencyTTL        time.Duration
	statusStore           status.StatusStore
}

func NewDispatcher(
	registry Registry,
	store Store,
	enqueuer Enqueuer,
	namespace string,
	syncThreshold int,
) *Dispatcher {
	return &Dispatcher{
		registry:              registry,
		store:                 store,
		enqueuer:              enqueuer,
		currentTime:           time.Now,
		namespace:             namespace,
		syncPriorityThreshold: syncThreshold,
	}
}

// SetIdempotency 注入幂等器(可选)
func (d *Dispatcher) SetIdempotency(idempotency Idempotency, ttl time.Duration) {
	d.idempotency = idempotency
	d.idempotencyTTL = ttl
}

// SetStatusStore 注入状态存储(可选)
func (d *Dispatcher) SetStatusStore(statusStore status.StatusStore) {
	d.statusStore = statusStore
}

// ==================== 公共发送接口 ====================

// Send 异步发送消息,返回唯一 message_id
func (d *Dispatcher) Send(ctx context.Context, delivery Delivery) (string, error) {
	return d.SendAsync(ctx, delivery)
}

// SendAsync 异步发送,返回唯一 message_id
func (d *Dispatcher) SendAsync(ctx context.Context, delivery Delivery) (string, error) {
	messageID := generateMessageID(delivery)
	delivery = d.attachMessageIDToDelivery(delivery, messageID)

	d.saveRecordAndUpdateStatus(ctx, delivery, statusPending, messageEnqueuePending, "", messageID)

	if err := d.enqueueDelivery(ctx, delivery); err != nil {
		d.saveRecordAndUpdateStatus(ctx, delivery, statusFailed, messageEnqueueFailed, err.Error(), messageID)
		return messageID, err
	}

	return messageID, nil
}

// SendSync 同步发送,返回唯一 message_id
func (d *Dispatcher) SendSync(ctx context.Context, delivery Delivery) error {
	messageID := generateMessageID(delivery)
	d.saveRecordAndUpdateStatus(ctx, delivery, statusPending, messageSyncSending, "", messageID)
	return d.deliverSync(ctx, delivery, messageID)
}

// DeliverSync 直接投递,不进行幂等性检查,供消费者端使用
func (d *Dispatcher) DeliverSync(ctx context.Context, delivery Delivery, messageID string) error {
	d.saveRecordAndUpdateStatus(ctx, delivery, statusPending, messageSyncSending, "", messageID)
	return d.deliverSync(ctx, delivery, messageID)
}

// SendSyncWithResult 同步发送并返回详细的通道级结果
func (d *Dispatcher) SendSyncWithResult(ctx context.Context, delivery Delivery) (*SendResult, error) {
	if err := d.checkIdempotency(ctx, delivery); err != nil {
		return nil, err
	}

	messageID := generateMessageID(delivery)
	d.saveRecordAndUpdateStatus(ctx, delivery, statusPending, messageSyncSending, "", messageID)
	return d.deliverSyncWithResult(ctx, delivery, messageID)
}

// ==================== 同步投递核心逻辑 ====================

// deliverSync 同步投递消息到各个通道
func (d *Dispatcher) deliverSync(ctx context.Context, delivery Delivery, messageID string) error {
	deliveryCtx := &deliveryContext{
		delivery:       delivery,
		messageID:      messageID,
		primarySuccess: false,
		firstError:     nil,
		asyncVoice:     false,
	}

	// 1. 发送到主通道
	d.sendToPrimaryChannels(ctx, deliveryCtx)

	// 2. 主通道全部失败时,尝试降级通道
	if !deliveryCtx.primarySuccess {
		d.sendToFallbackChannels(ctx, deliveryCtx)
	}

	// 3. 更新最终状态
	d.updateFinalDeliveryStatus(ctx, deliveryCtx)

	// 4. 返回错误(如果有)
	if !deliveryCtx.primarySuccess && deliveryCtx.firstError != nil {
		return deliveryCtx.firstError
	}

	return nil
}

// deliverSyncWithResult 同步投递并返回详细结果
func (d *Dispatcher) deliverSyncWithResult(ctx context.Context, delivery Delivery, messageID string) (*SendResult, error) {
	result := d.initializeSendResult(delivery, messageID)
	deliveryCtx := &deliveryContext{
		delivery:       delivery,
		messageID:      messageID,
		primarySuccess: false,
		firstError:     nil,
	}

	// 1. 发送到主通道
	d.sendToPrimaryChannelsWithResult(ctx, deliveryCtx, result)

	// 2. 主通道全部失败时,尝试降级通道
	if !deliveryCtx.primarySuccess {
		d.sendToFallbackChannelsWithResult(ctx, deliveryCtx, result)
	}

	// 3. 计算结果统计
	d.calculateResultStatistics(result)

	// 4. 保存最终记录
	d.saveFinalResultRecord(ctx, deliveryCtx, result)

	// 5. 返回结果
	if !deliveryCtx.primarySuccess && deliveryCtx.firstError != nil {
		return result, deliveryCtx.firstError
	}

	return result, nil
}

// ==================== 通道发送逻辑 ====================

// sendToPrimaryChannels 发送消息到主通道
func (d *Dispatcher) sendToPrimaryChannels(ctx context.Context, deliveryCtx *deliveryContext) {
	for index, channel := range deliveryCtx.delivery.Plan.Primary {
		log.Printf("[DISPATCHER] ===== 处理第 %d 个通道: %s =====", index+1, channel)

		if err := d.sendToSingleChannel(ctx, deliveryCtx, channel); err != nil {
			log.Printf("[DISPATCHER] 第 %d 个通道 %s 发送失败: %v", index+1, channel, err)
			d.handleChannelFailure(ctx, deliveryCtx, channel, err)
			continue
		}

		log.Printf("[DISPATCHER] 第 %d 个通道 %s 发送成功", index+1, channel)
		d.handleChannelSuccess(ctx, deliveryCtx, channel)
	}
}

// sendToFallbackChannels 发送消息到降级通道
func (d *Dispatcher) sendToFallbackChannels(ctx context.Context, deliveryCtx *deliveryContext) {
	if len(deliveryCtx.delivery.Plan.Fallback) == 0 {
		return
	}

	for _, channel := range deliveryCtx.delivery.Plan.Fallback {
		if err := d.sendToSingleChannel(ctx, deliveryCtx, channel); err != nil {
			d.updateStatusIfAvailable(ctx, deliveryCtx.messageID, statusFallbackFailed, err.Error())
			continue
		}

		// 降级成功
		d.saveFallbackSuccessRecord(ctx, deliveryCtx, channel)
		deliveryCtx.primarySuccess = true
		return
	}
}

// sendToPrimaryChannelsWithResult 发送到主通道并记录结果
func (d *Dispatcher) sendToPrimaryChannelsWithResult(
	ctx context.Context,
	deliveryCtx *deliveryContext,
	result *SendResult,
) {
	for _, channel := range deliveryCtx.delivery.Plan.Primary {
		channelResult := d.createChannelResult(channel, deliveryCtx.messageID)
		result.ChannelResults = append(result.ChannelResults, channelResult)

		if err := d.sendToSingleChannel(ctx, deliveryCtx, channel); err != nil {
			d.updateChannelResultAsFailed(channelResult, err)
			d.updateStatusIfAvailable(ctx, deliveryCtx.messageID, statusFailed, err.Error())
			continue
		}

		d.updateChannelResultAsSuccess(channelResult, d.currentTime().Unix())
		d.updateStatusIfAvailable(ctx, deliveryCtx.messageID, statusSuccess, "")
		deliveryCtx.primarySuccess = true
	}
}

// sendToFallbackChannelsWithResult 发送到降级通道并记录结果
func (d *Dispatcher) sendToFallbackChannelsWithResult(
	ctx context.Context,
	deliveryCtx *deliveryContext,
	result *SendResult,
) {
	if len(deliveryCtx.delivery.Plan.Fallback) == 0 {
		return
	}

	for _, channel := range deliveryCtx.delivery.Plan.Fallback {
		channelResult := d.createChannelResult(channel, deliveryCtx.messageID)
		result.ChannelResults = append(result.ChannelResults, channelResult)

		if err := d.sendToSingleChannel(ctx, deliveryCtx, channel); err != nil {
			d.updateChannelResultAsFailed(channelResult, err)
			continue
		}

		d.updateChannelResultAsSuccess(channelResult, d.currentTime().Unix())
		d.saveFallbackSuccessRecord(ctx, deliveryCtx, channel)
		deliveryCtx.primarySuccess = true
		return
	}
}

// sendToSingleChannel 向单个通道发送消息
func (d *Dispatcher) sendToSingleChannel(
	ctx context.Context,
	deliveryCtx *deliveryContext,
	channel Channel,
) error {
	log.Printf("[DISPATCHER] 尝试通过渠道 %s 发送消息", channel)

	provider, err := d.getProviderForChannel(channel)
	if err != nil {
		return err
	}

	filteredMessage, err := d.prepareMessageForChannel(deliveryCtx.delivery.Msg, channel)
	if err != nil {
		return err
	}

	// 检查是否为异步语音通道
	if d.isAsyncVoiceChannel(channel, filteredMessage) {
		deliveryCtx.asyncVoice = true
		log.Printf("[DISPATCHER] 语音异步 pending，跳过 success 状态写入: %s", deliveryCtx.messageID)
	}

	return d.sendMessageWithRetry(ctx, provider, filteredMessage, deliveryCtx.delivery.Plan)
}

// ==================== 辅助函数 ====================

// getProviderForChannel 获取通道对应的提供者
func (d *Dispatcher) getProviderForChannel(channel Channel) (Provider, error) {
	providers := d.registry.GetByChannel(channel)
	if len(providers) == 0 {
		return nil, fmt.Errorf("no provider for channel=%s", channel)
	}

	log.Printf("[DISPATCHER] 使用提供者: %s", providers[0].Name())
	return providers[0], nil
}

// prepareMessageForChannel 为特定通道准备消息
func (d *Dispatcher) prepareMessageForChannel(message Message, channel Channel) (Message, error) {
	filteredRecipients := d.filterRecipientsForChannel(message.To, channel)

	log.Printf("[DISPATCHER] 原始收件人数量: %d, 过滤后 %s 通道收件人数量: %d",
		len(message.To), channel, len(filteredRecipients))

	if len(filteredRecipients) == 0 {
		return message, fmt.Errorf(messageNoRecipients)
	}

	filteredMessage := message
	filteredMessage.To = filteredRecipients
	return filteredMessage, nil
}

// isAsyncVoiceChannel 判断是否为异步语音通道
func (d *Dispatcher) isAsyncVoiceChannel(channel Channel, message Message) bool {
	if channel != ChVoice || message.Data == nil {
		return false
	}

	asyncPending, exists := message.Data[asyncVoicePendingKey].(bool)
	return exists && asyncPending
}

// sendMessageWithRetry 带重试机制地发送消息
func (d *Dispatcher) sendMessageWithRetry(
	ctx context.Context,
	provider Provider,
	message Message,
	plan RoutePlan,
) error {
	timeoutContext, cancel := context.WithTimeout(ctx, plan.Timeout)
	defer cancel()

	return withRetryJitter(timeoutContext, func(retryContext context.Context) error {
		log.Printf("[DISPATCHER] 调用提供者发送方法")
		return provider.Send(retryContext, message, plan)
	}, plan.Retry)
}

// ==================== 状态处理函数 ====================

// handleChannelSuccess 处理通道发送成功
func (d *Dispatcher) handleChannelSuccess(ctx context.Context, deliveryCtx *deliveryContext, channel Channel) {
	deliveryCtx.primarySuccess = true
	d.saveChannelRecord(ctx, deliveryCtx, []Channel{channel}, statusSuccess, httpStatusOK, "OK", "")
}

// handleChannelFailure 处理通道发送失败
func (d *Dispatcher) handleChannelFailure(
	ctx context.Context,
	deliveryCtx *deliveryContext,
	channel Channel,
	err error,
) {
	if deliveryCtx.firstError == nil {
		deliveryCtx.firstError = err
	}
	d.saveChannelRecord(ctx, deliveryCtx, []Channel{channel}, statusFailed, httpStatusServerError, err.Error(), err.Error())
}

// updateFinalDeliveryStatus 更新最终投递状态
func (d *Dispatcher) updateFinalDeliveryStatus(ctx context.Context, deliveryCtx *deliveryContext) {
	// 如果是异步语音,不更新状态
	if deliveryCtx.asyncVoice {
		return
	}

	// 主通道全部成功
	if deliveryCtx.primarySuccess && deliveryCtx.firstError == nil {
		d.updateStatusIfAvailable(ctx, deliveryCtx.messageID, statusSuccess, "")
		return
	}

	// 全部失败
	if !deliveryCtx.primarySuccess && deliveryCtx.firstError != nil {
		d.updateStatusIfAvailable(ctx, deliveryCtx.messageID, statusFailed, deliveryCtx.firstError.Error())
	}
}

// saveFallbackSuccessRecord 保存降级成功记录
func (d *Dispatcher) saveFallbackSuccessRecord(
	ctx context.Context,
	deliveryCtx *deliveryContext,
	channel Channel,
) {
	d.saveChannelRecord(ctx, deliveryCtx, []Channel{channel}, statusFallbackSuccess, httpStatusOK, "OK", "")
	d.updateStatusIfAvailable(ctx, deliveryCtx.messageID, statusFallbackSuccess, "")
}

// ==================== 结果处理函数 ====================

// initializeSendResult 初始化发送结果
func (d *Dispatcher) initializeSendResult(delivery Delivery, messageID string) *SendResult {
	return &SendResult{
		ChannelResults: []*ChannelResult{},
		TotalChannels:  len(delivery.Plan.Primary),
	}
}

// createChannelResult 创建通道结果对象
func (d *Dispatcher) createChannelResult(channel Channel, messageID string) *ChannelResult {
	return &ChannelResult{
		Channel:   channel,
		Status:    statusPending,
		MessageID: messageID,
	}
}

// updateChannelResultAsSuccess 更新通道结果为成功
func (d *Dispatcher) updateChannelResultAsSuccess(result *ChannelResult, sentAt int64) {
	result.Status = statusSuccess
	result.SentAt = sentAt
}

// updateChannelResultAsFailed 更新通道结果为失败
func (d *Dispatcher) updateChannelResultAsFailed(result *ChannelResult, err error) {
	result.Status = statusFailed
	result.Error = err.Error()
}

// calculateResultStatistics 计算结果统计
func (d *Dispatcher) calculateResultStatistics(result *SendResult) {
	for _, channelResult := range result.ChannelResults {
		switch channelResult.Status {
		case statusSuccess:
			result.SuccessCount++
		case statusFailed:
			result.FailedCount++
		case statusPending:
			result.PendingCount++
		}
	}

	// 确定整体状态
	if result.SuccessCount == result.TotalChannels {
		result.OverallStatus = statusSuccess
	} else if result.SuccessCount > 0 {
		result.OverallStatus = statusPartial
	} else {
		result.OverallStatus = statusFailed
	}
}

// saveFinalResultRecord 保存最终结果记录
func (d *Dispatcher) saveFinalResultRecord(ctx context.Context, deliveryCtx *deliveryContext, result *SendResult) {
	if result.OverallStatus == statusFailed && deliveryCtx.firstError != nil {
		d.saveChannelRecord(ctx, deliveryCtx, deliveryCtx.delivery.Plan.Primary, statusFailed, "",
			deliveryCtx.firstError.Error(), deliveryCtx.firstError.Error())
		d.updateStatusIfAvailable(ctx, deliveryCtx.messageID, statusFailed, deliveryCtx.firstError.Error())
	} else if result.OverallStatus == statusPartial {
		d.saveChannelRecord(ctx, deliveryCtx, deliveryCtx.delivery.Plan.Primary, statusPartial, "",
			deliveryCtx.firstError.Error(), deliveryCtx.firstError.Error())
		d.updateStatusIfAvailable(ctx, deliveryCtx.messageID, statusPartial, deliveryCtx.firstError.Error())
	}
}

// ==================== 存储操作函数 ====================

// saveRecordAndUpdateStatus 保存记录并更新状态
func (d *Dispatcher) saveRecordAndUpdateStatus(
	ctx context.Context,
	delivery Delivery,
	recordStatus string,
	content string,
	errorDetail string,
	messageID string,
) {
	if d.store != nil {
		record := d.createPushRecord(delivery, delivery.Plan.Primary, recordStatus, "", content, errorDetail, messageID)
		_ = d.store.SaveRecord(ctx, record)
		_, _ = d.store.Trim(ctx)
	}

	if d.statusStore != nil {
		log.Printf("[DISPATCHER] UpdateStatus: messageID=%s, status=%s", messageID, recordStatus)
		_ = d.statusStore.UpdateStatus(ctx, messageID, recordStatus, content)
	}
}

// saveChannelRecord 保存通道记录
func (d *Dispatcher) saveChannelRecord(
	ctx context.Context,
	deliveryCtx *deliveryContext,
	channels []Channel,
	recordStatus string,
	code string,
	content string,
	errorDetail string,
) {
	if d.store == nil {
		return
	}

	record := d.createPushRecord(deliveryCtx.delivery, channels, recordStatus, code, content, errorDetail, deliveryCtx.messageID)
	_ = d.store.SaveRecord(ctx, record)
	_, _ = d.store.Trim(ctx)
}

// updateStatusIfAvailable 如果状态存储可用则更新状态
func (d *Dispatcher) updateStatusIfAvailable(ctx context.Context, messageID string, recordStatus string, message string) {
	if d.statusStore == nil {
		return
	}

	log.Printf("[DISPATCHER] UpdateStatus: messageID=%s, status=%s", messageID, recordStatus)
	_ = d.statusStore.UpdateStatus(ctx, messageID, recordStatus, message)
}

// createPushRecord 创建完整的推送记录
func (d *Dispatcher) createPushRecord(
	delivery Delivery,
	channels []Channel,
	recordStatus string,
	code string,
	content string,
	errorDetail string,
	messageID string,
) Record {
	currentTime := d.currentTime().Unix()
	return Record{
		Key:         messageID,
		MessageID:   messageID,
		Namespace:   d.namespace,
		Kind:        delivery.Msg.Kind,
		Subject:     delivery.Msg.Subject,
		Body:        delivery.Msg.Body,
		MessageData: delivery.Msg.Data,
		Channels:    channels,
		Recipients:  delivery.Msg.To,
		Status:      recordStatus,
		Code:        code,
		Content:     content,
		ErrorDetail: errorDetail,
		Priority:    delivery.Msg.Priority,
		CreatedAt:   currentTime,
		SentAt:      currentTime,
	}
}

// ==================== 入队操作 ====================

// enqueueDelivery 将消息入队
func (d *Dispatcher) enqueueDelivery(ctx context.Context, delivery Delivery) error {
	payload, err := json.Marshal(delivery)
	if err != nil {
		return fmt.Errorf("序列化消息失败: %w", err)
	}

	// 尝试使用优先级队列
	if priorityEnqueuer, ok := d.enqueuer.(PriorityEnqueuer); ok {
		return priorityEnqueuer.EnqueueWithPriority(ctx, payload, delivery.Msg.Priority)
	}

	// 使用普通队列
	return d.enqueuer.Enqueue(ctx, payload)
}

// ==================== 幂等性检查 ====================

// checkIdempotency 检查幂等性
func (d *Dispatcher) checkIdempotency(ctx context.Context, delivery Delivery) error {
	if d.idempotency == nil || d.idempotencyTTL <= 0 {
		return nil
	}

	isProcessed, _, err := d.idempotency.CheckAndSet(ctx, delivery, d.idempotencyTTL)
	if err != nil {
		return fmt.Errorf("idempotency: %w", err)
	}

	if !isProcessed {
		return errors.New("消息已处理，忽略重复请求")
	}

	return nil
}

// ==================== 重试机制 ====================

// withRetryJitter 带抖动的重试机制
func withRetryJitter(ctx context.Context, executeFunc func(context.Context) error, retryPolicy RetryPolicy) error {
	if retryPolicy.MaxAttempts <= 0 {
		return executeFunc(ctx)
	}

	backoffDelay := retryPolicy.Backoff
	if backoffDelay <= 0 {
		backoffDelay = defaultRetryBackoff
	}

	var lastError error
	for attempt := 0; attempt < retryPolicy.MaxAttempts; attempt++ {
		lastError = executeFunc(ctx)
		if lastError == nil {
			return nil
		}

		// 最后一次尝试失败后不再等待
		if attempt == retryPolicy.MaxAttempts-1 {
			break
		}

		// 计算带抖动的延迟时间
		sleepDuration := calculateJitteredDelay(backoffDelay)

		// 等待或取消
		select {
		case <-time.After(sleepDuration):
		case <-ctx.Done():
			return ctx.Err()
		}

		// 指数退避
		backoffDelay *= 2
	}

	return lastError
}

// calculateJitteredDelay 计算带抖动的延迟时间
func calculateJitteredDelay(baseDelay time.Duration) time.Duration {
	jitter := time.Duration(generateRandomUint32() % uint32(baseDelay/2+1))
	return baseDelay + jitter
}

// ==================== 收件人过滤 ====================

// filterRecipientsForChannel 为特定通道过滤收件人
func (d *Dispatcher) filterRecipientsForChannel(recipients []Recipient, channel Channel) []Recipient {
	var filteredRecipients []Recipient

	for _, recipient := range recipients {
		if d.isRecipientValidForChannel(recipient, channel) {
			filteredRecipients = append(filteredRecipients, recipient)
		}
	}

	return filteredRecipients
}

// isRecipientValidForChannel 判断收件人是否适用于指定通道
func (d *Dispatcher) isRecipientValidForChannel(recipient Recipient, channel Channel) bool {
	switch channel {
	case ChWeb:
		return recipient.UserID != "" || recipient.WebInbox != ""
	case ChEmail:
		return recipient.Email != ""
	case ChSMS, ChVoice:
		return recipient.Phone != ""
	default:
		return false
	}
}

// ==================== 工具函数 ====================

// attachMessageIDToDelivery 将 messageID 附加到 delivery
func (d *Dispatcher) attachMessageIDToDelivery(delivery Delivery, messageID string) Delivery {
	if delivery.Msg.Data == nil {
		delivery.Msg.Data = make(map[string]interface{})
	}
	delivery.Msg.Data["message_id"] = messageID
	return delivery
}

// generateMessageID 生成唯一的消息ID
func generateMessageID(delivery Delivery) string {
	timestamp := time.Now().UnixNano()
	randomSuffix := generateRandomUint32()
	return fmt.Sprintf("%s_%d_%d", delivery.Msg.Kind, timestamp, randomSuffix)
}

// generateRandomUint32 生成随机的 uint32 数字
func generateRandomUint32() uint32 {
	var randomBytes [4]byte
	_, _ = rand.Read(randomBytes[:])
	return binary.LittleEndian.Uint32(randomBytes[:])
}
