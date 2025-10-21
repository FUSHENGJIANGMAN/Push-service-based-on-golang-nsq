package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"push-gateway/internal/handlers"
	"push-gateway/internal/modem"
	"push-gateway/internal/push"
	"push-gateway/internal/queue"
	"push-gateway/internal/status"
)

//
// 常量定义
//

const (
	messageProcessingTimeout = 60 * time.Second
	unknownMessageKind       = "unknown"
	processingStatus         = "processing"
	failedStatus             = "failed"
	retryFailedStatus        = "retry_failed"
	consumedSuccessStatus    = "consumed_success"
)

//
// 消费者接口定义
//

// QueueConsumer 队列消费者接口
// 统一普通消费者和优先级消费者的行为
type QueueConsumer interface {
	Run() error
	Stop()
	AttachDLQProducer(address string) error
}

//
// 主推送队列消费者
//

// MainConsumerManager 主推送队列消费者管理器
type MainConsumerManager struct {
	config         *AppContext
	dispatcher     *push.Dispatcher
	recordStore    push.Store
	consumerConfig ConsumerConfig
}

// ConsumerConfig 消费者配置
type ConsumerConfig struct {
	topic                string
	channel              string
	maxInFlight          int
	concurrency          int
	nsqdAddresses        []string
	lookupdAddresses     []string
	dlqTopic             string
	maxAttemptsBeforeDLQ uint16
	priorityTopics       map[string]string
	priorityThresholds   map[string]int
	storageNamespace     string
}

// NewMainConsumerManager 创建主消费者管理器实例
func NewMainConsumerManager(appContext *AppContext) *MainConsumerManager {
	return &MainConsumerManager{
		config:      appContext,
		dispatcher:  appContext.Dispatcher,
		recordStore: appContext.RecordStore,
		consumerConfig: ConsumerConfig{
			topic:                appContext.Config.NSQ.Topic,
			channel:              appContext.Config.NSQ.Channel,
			maxInFlight:          appContext.Config.NSQ.MaxInFlight,
			concurrency:          appContext.Config.NSQ.Concurrency,
			nsqdAddresses:        appContext.Config.NSQ.NsqdTCPAddrs,
			lookupdAddresses:     appContext.Config.NSQ.LookupdHTTPAddrs,
			dlqTopic:             appContext.Config.NSQ.DLQTopic,
			maxAttemptsBeforeDLQ: uint16(appContext.Config.NSQ.MaxConsumeAttemptsBeforeDLQ),
			priorityTopics:       appContext.Config.NSQ.PriorityTopics,
			priorityThresholds:   appContext.Config.NSQ.PriorityThresholds,
			storageNamespace:     appContext.Config.Storage.Namespace,
		},
	}
}

// Start 启动主推送队列消费者
// 从 NSQ 读取消息并分发到各个通道
func (manager *MainConsumerManager) Start() {
	if !manager.isConsumerEnabled() {
		log.Println("[MainConsumer] 消费者未启用,跳过启动")
		return
	}

	consumer := manager.createConsumer()
	manager.attachDeadLetterQueue(consumer)
	manager.runConsumerInBackground(consumer)

	log.Println("[MainConsumer] 主推送队列消费者启动成功")
}

// isConsumerEnabled 检查消费者是否启用
func (manager *MainConsumerManager) isConsumerEnabled() bool {
	return manager.config.Config.NSQ.ConsumerEnabled
}

// createConsumer 创建消费者实例
// 根据配置决定使用普通消费者还是优先级消费者
func (manager *MainConsumerManager) createConsumer() QueueConsumer {
	consumeFunction := manager.buildConsumeFunction()

	if manager.isPriorityModeEnabled() {
		return manager.createPriorityConsumer(consumeFunction)
	}

	return manager.createNormalConsumer(consumeFunction)
}

// isPriorityModeEnabled 检查是否启用优先级模式
func (manager *MainConsumerManager) isPriorityModeEnabled() bool {
	return len(manager.consumerConfig.priorityTopics) > 0 &&
		len(manager.consumerConfig.priorityThresholds) > 0
}

// createPriorityConsumer 创建优先级消费者
func (manager *MainConsumerManager) createPriorityConsumer(
	consumeFunction func(context.Context, []byte, uint16) error,
) QueueConsumer {
	consumer, err := queue.NewPriorityConsumer(
		manager.consumerConfig.topic,
		manager.consumerConfig.channel,
		manager.consumerConfig.maxInFlight,
		manager.consumerConfig.concurrency,
		manager.consumerConfig.nsqdAddresses,
		manager.consumerConfig.lookupdAddresses,
		manager.consumerConfig.dlqTopic,
		manager.consumerConfig.maxAttemptsBeforeDLQ,
		consumeFunction,
		manager.consumerConfig.priorityTopics,
		manager.consumerConfig.priorityThresholds,
	)

	if err != nil {
		log.Fatalf("[MainConsumer] 创建优先级消费者失败: %v", err)
	}

	return consumer
}

// createNormalConsumer 创建普通消费者
func (manager *MainConsumerManager) createNormalConsumer(
	consumeFunction func(context.Context, []byte, uint16) error,
) QueueConsumer {
	consumer, err := queue.NewNSQConsumer(
		manager.consumerConfig.topic,
		manager.consumerConfig.channel,
		manager.consumerConfig.maxInFlight,
		manager.consumerConfig.concurrency,
		manager.consumerConfig.nsqdAddresses,
		manager.consumerConfig.lookupdAddresses,
		manager.consumerConfig.dlqTopic,
		manager.consumerConfig.maxAttemptsBeforeDLQ,
		consumeFunction,
	)

	if err != nil {
		log.Fatalf("[MainConsumer] 创建普通消费者失败: %v", err)
	}

	return consumer
}

// buildConsumeFunction 构建消息消费函数
// 这是消费者的核心处理逻辑
func (manager *MainConsumerManager) buildConsumeFunction() func(context.Context, []byte, uint16) error {
	return func(ctx context.Context, payload []byte, attemptCount uint16) error {
		delivery, err := manager.parseDelivery(payload, attemptCount)
		if err != nil {
			return err
		}

		log.Printf("[MainConsumer] 处理消息: To=%v, Channels=%v, Topic=%s (尝试:%d)",
			delivery.Msg.To, delivery.Plan.Primary, delivery.Topic, attemptCount)

		return manager.processDelivery(ctx, delivery, attemptCount)
	}
}

// parseDelivery 解析投递消息
func (manager *MainConsumerManager) parseDelivery(
	payload []byte,
	attemptCount uint16,
) (*push.Delivery, error) {
	var delivery push.Delivery

	if err := json.Unmarshal(payload, &delivery); err != nil {
		log.Printf("[MainConsumer] 反序列化失败(尝试:%d): %v", attemptCount, err)
		manager.saveDeserializationFailure(payload, err)
		return nil, fmt.Errorf("解析消息失败: %w", err)
	}

	return &delivery, nil
}

// saveDeserializationFailure 保存反序列化失败记录
// 即使消息格式错误也要记录,便于排查问题
func (manager *MainConsumerManager) saveDeserializationFailure(payload []byte, err error) {
	if manager.recordStore == nil {
		return
	}

	record := push.Record{
		Namespace:   manager.consumerConfig.storageNamespace,
		Kind:        unknownMessageKind,
		Subject:     "反序列化失败",
		Body:        string(payload),
		MessageData: map[string]any{"raw_payload": string(payload)},
		Status:      failedStatus,
		Code:        "400",
		Content:     "消息格式错误",
		ErrorDetail: err.Error(),
		CreatedAt:   time.Now().Unix(),
		SentAt:      time.Now().Unix(),
	}

	_ = manager.recordStore.SaveRecord(context.Background(), record)
}

// processDelivery 处理投递消息
func (manager *MainConsumerManager) processDelivery(
	ctx context.Context,
	delivery *push.Delivery,
	attemptCount uint16,
) error {
	processingContext, cancel := context.WithTimeout(ctx, messageProcessingTimeout)
	defer cancel()

	messageID := manager.generateMessageID(delivery)

	// 首次尝试时记录 processing 状态
	if attemptCount == 1 {
		manager.saveProcessingRecord(delivery, messageID)
	}

	// 执行实际的消息分发
	if err := manager.dispatcher.DeliverSync(processingContext, *delivery, messageID); err != nil {
		manager.saveFailureRecord(delivery, messageID, attemptCount, err)
		return err
	}

	// 仅首次成功时记录,避免重复记录
	if attemptCount == 1 {
		manager.saveSuccessRecord(delivery, messageID)
	}

	return nil
}

// generateMessageID 生成消息ID
// 优先使用业务提供的ID,否则生成临时ID
func (manager *MainConsumerManager) generateMessageID(delivery *push.Delivery) string {
	if messageID, ok := delivery.Msg.Data["message_id"].(string); ok && messageID != "" {
		return messageID
	}

	return fmt.Sprintf("%s_%d_%d",
		delivery.Msg.Kind,
		time.Now().UnixNano(),
		time.Now().Nanosecond(),
	)
}

// saveProcessingRecord 保存处理中记录
func (manager *MainConsumerManager) saveProcessingRecord(
	delivery *push.Delivery,
	messageID string,
) {
	if manager.recordStore == nil {
		return
	}

	now := time.Now().Unix()
	record := push.Record{
		Key:         messageID + "_processing",
		MessageID:   messageID,
		Namespace:   manager.consumerConfig.storageNamespace,
		Kind:        delivery.Msg.Kind,
		Subject:     delivery.Msg.Subject,
		Body:        delivery.Msg.Body,
		MessageData: delivery.Msg.Data,
		Channels:    delivery.Plan.Primary,
		Recipients:  delivery.Msg.To,
		Status:      processingStatus,
		Priority:    delivery.Msg.Priority,
		CreatedAt:   now,
		SentAt:      now,
	}

	_ = manager.recordStore.SaveRecord(context.Background(), record)
}

// saveFailureRecord 保存失败记录
// 根据尝试次数区分首次失败和重试失败
func (manager *MainConsumerManager) saveFailureRecord(
	delivery *push.Delivery,
	messageID string,
	attemptCount uint16,
	err error,
) {
	if manager.recordStore == nil {
		return
	}

	status, errorDetail := manager.determineFailureStatus(attemptCount, err)
	now := time.Now().Unix()

	record := push.Record{
		Key: fmt.Sprintf("%s_%s_%d_%d",
			delivery.Msg.Kind,
			status,
			now,
			time.Now().Nanosecond(),
		),
		MessageID:   messageID,
		Namespace:   manager.consumerConfig.storageNamespace,
		Kind:        delivery.Msg.Kind,
		Subject:     delivery.Msg.Subject,
		Body:        delivery.Msg.Body,
		MessageData: delivery.Msg.Data,
		Channels:    delivery.Plan.Primary,
		Recipients:  delivery.Msg.To,
		Status:      status,
		Content:     "队列消费失败",
		ErrorDetail: errorDetail,
		Priority:    delivery.Msg.Priority,
		CreatedAt:   now,
		SentAt:      now,
	}

	_ = manager.recordStore.SaveRecord(context.Background(), record)
}

// determineFailureStatus 确定失败状态
func (manager *MainConsumerManager) determineFailureStatus(
	attemptCount uint16,
	err error,
) (string, string) {
	if attemptCount > 1 {
		return retryFailedStatus, fmt.Sprintf("尝试次数 %d: %s", attemptCount, err.Error())
	}

	return failedStatus, err.Error()
}

// saveSuccessRecord 保存成功记录
func (manager *MainConsumerManager) saveSuccessRecord(
	delivery *push.Delivery,
	messageID string,
) {
	if manager.recordStore == nil {
		return
	}

	now := time.Now().Unix()
	record := push.Record{
		Key: fmt.Sprintf("%s_consumed_success_%d_%d",
			delivery.Msg.Kind,
			now,
			time.Now().Nanosecond(),
		),
		MessageID:   messageID,
		Namespace:   manager.consumerConfig.storageNamespace,
		Kind:        delivery.Msg.Kind,
		Subject:     delivery.Msg.Subject,
		Body:        delivery.Msg.Body,
		MessageData: delivery.Msg.Data,
		Channels:    delivery.Plan.Primary,
		Recipients:  delivery.Msg.To,
		Status:      consumedSuccessStatus,
		Code:        "200",
		Content:     "队列消费成功",
		Priority:    delivery.Msg.Priority,
		CreatedAt:   now,
		SentAt:      now,
	}

	_ = manager.recordStore.SaveRecord(context.Background(), record)
}

// attachDeadLetterQueue 附加死信队列
// 用于处理多次失败的消息
func (manager *MainConsumerManager) attachDeadLetterQueue(consumer QueueConsumer) {
	if !manager.shouldAttachDLQ() {
		return
	}

	address := manager.consumerConfig.nsqdAddresses[0]
	if err := consumer.AttachDLQProducer(address); err != nil {
		log.Fatalf("[MainConsumer] 附加死信队列失败: %v", err)
	}

	log.Printf("[MainConsumer] 死信队列附加成功: %s", manager.consumerConfig.dlqTopic)
}

// shouldAttachDLQ 判断是否需要附加死信队列
func (manager *MainConsumerManager) shouldAttachDLQ() bool {
	return len(manager.consumerConfig.nsqdAddresses) > 0 &&
		manager.consumerConfig.dlqTopic != ""
}

// runConsumerInBackground 在后台运行消费者
func (manager *MainConsumerManager) runConsumerInBackground(consumer QueueConsumer) {
	go func() {
		if err := consumer.Run(); err != nil {
			log.Fatalf("[MainConsumer] 消费者运行失败: %v", err)
		}
	}()
}

//
// 语音和短信队列消费者
//

// VoiceSMSConsumerManager 语音和短信队列消费者管理器
type VoiceSMSConsumerManager struct {
	config       *AppContext
	modemManager *modem.LazyModemManager
	statusStore  status.StatusStore
}

// NewVoiceSMSConsumerManager 创建语音短信消费者管理器实例
func NewVoiceSMSConsumerManager(appContext *AppContext) *VoiceSMSConsumerManager {
	return &VoiceSMSConsumerManager{
		config:       appContext,
		modemManager: appContext.ModemManager,
		statusStore:  appContext.StatusStore,
	}
}

// Start 启动语音和短信队列消费者
// 仅在启用 Modem 时生效
func (manager *VoiceSMSConsumerManager) Start() {
	if !manager.isModemEnabled() {
		log.Println("[VoiceSMSConsumer] Modem 未启用,跳过启动")
		return
	}

	manager.startVoiceConsumer()
	manager.startSMSConsumer()

	log.Println("[VoiceSMSConsumer] 语音和短信队列消费者启动完成")
}

// isModemEnabled 检查 Modem 是否启用
func (manager *VoiceSMSConsumerManager) isModemEnabled() bool {
	return manager.config.Config.Providers.Modem.Enabled &&
		manager.modemManager != nil
}

// startVoiceConsumer 启动语音消费者
func (manager *VoiceSMSConsumerManager) startVoiceConsumer() {
	config := manager.config.Config.VoiceQueue

	if !config.ConsumerEnabled {
		return
	}

	voiceHandler := handlers.NewVoiceMessageHandler(
		manager.modemManager,
		manager.statusStore,
	)

	consumer, err := queue.NewVoiceConsumer(
		config.Topic,
		config.Channel,
		config.MaxInFlight,
		config.Concurrency,
		config.NsqdTCPAddrs,
		config.LookupdHTTPAddrs,
		config.DLQTopic,
		uint16(config.MaxConsumeAttemptsBeforeDLQ),
		voiceHandler.HandleVoiceMessage,
	)

	if err != nil {
		log.Printf("[VoiceConsumer] 创建消费者失败: %v", err)
		return
	}

	manager.attachDLQAndRun(consumer, config.NsqdTCPAddrs, config.DLQTopic, "VoiceConsumer")
}

// startSMSConsumer 启动短信消费者
func (manager *VoiceSMSConsumerManager) startSMSConsumer() {
	config := manager.config.Config.SMSQueue

	if !config.ConsumerEnabled {
		return
	}

	smsHandler := handlers.NewSMSMessageHandler(
		manager.modemManager,
		manager.statusStore,
	)

	consumer, err := queue.NewSMSConsumer(
		config.Topic,
		config.Channel,
		config.MaxInFlight,
		config.Concurrency,
		config.NsqdTCPAddrs,
		config.LookupdHTTPAddrs,
		config.DLQTopic,
		uint16(config.MaxConsumeAttemptsBeforeDLQ),
		smsHandler.HandleSMSMessage,
	)

	if err != nil {
		log.Printf("[SMSConsumer] 创建消费者失败: %v", err)
		return
	}

	manager.attachDLQAndRun(consumer, config.NsqdTCPAddrs, config.DLQTopic, "SMSConsumer")
}

// attachDLQAndRun 附加死信队列并运行消费者
// 提取公共逻辑,避免在语音和短信中重复
func (manager *VoiceSMSConsumerManager) attachDLQAndRun(
	consumer QueueConsumer,
	addresses []string,
	dlqTopic string,
	consumerName string,
) {
	if len(addresses) > 0 && dlqTopic != "" {
		if err := consumer.AttachDLQProducer(addresses[0]); err != nil {
			log.Printf("[%s] 附加死信队列失败: %v", consumerName, err)
		}
	}

	go func() {
		log.Printf("[%s] 消费者启动", consumerName)
		if err := consumer.Run(); err != nil {
			log.Printf("[%s] 运行失败: %v", consumerName, err)
		}
	}()
}

//
// 外部调用接口 - 保持向后兼容
//

// startMainConsumer 启动主推送队列消费者
// 保持原有的函数签名,便于现有代码调用
func startMainConsumer(app *AppContext) {
	manager := NewMainConsumerManager(app)
	manager.Start()
}

// startVoiceSMSConsumers 启动语音和短信队列消费者
// 保持原有的函数签名,便于现有代码调用
func startVoiceSMSConsumers(app *AppContext) {
	manager := NewVoiceSMSConsumerManager(app)
	manager.Start()
}
