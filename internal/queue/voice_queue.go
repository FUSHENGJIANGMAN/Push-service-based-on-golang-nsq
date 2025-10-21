package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nsqio/go-nsq"
)

// ==================== 常量定义 ====================

const (
	// 语音消息处理超时时间
	voiceMessageHandleTimeout = 30 * time.Second

	// 用户代理标识
	voiceConsumerUserAgent = "voice-consumer"

	// 日志前缀
	voiceLogPrefix = "[voice-nsq] "
	voiceQueueTag  = "[VOICE_QUEUE] "

	// 最大尝试次数（硬性限制）
	maxVoiceAttempts = 3

	// 错误消息常量
	errorMessageVoicePhoneRequired           = "phone number is required"
	errorMessageVoiceContentRequired         = "content is required"
	errorMessageVoiceHandlerRequired         = "voice handler is required"
	errorMessageVoiceProducerCreateFailed    = "failed to create voice producer: %w"
	errorMessageVoiceConsumerCreateFailed    = "failed to create voice consumer: %w"
	errorMessageVoiceDLQProducerCreateFailed = "failed to create voice DLQ producer: %w"
	errorMessageVoiceMarshalFailed           = "failed to marshal voice message: %w"
	errorMessageVoicePublishFailed           = "failed to publish voice message: %w"
	errorMessageVoiceUnmarshalFailed         = "failed to unmarshal voice message: %w"
)

// ==================== 语音消息结构 ====================

// VoiceMessage 语音消息结构
type VoiceMessage struct {
	MessageID string            `json:"message_id"`
	Phone     string            `json:"phone"`
	Content   string            `json:"content"`
	Priority  int               `json:"priority"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// GetMessageID 获取消息ID
func (message *VoiceMessage) GetMessageID() string {
	return message.MessageID
}

// Validate 验证语音消息
func (message *VoiceMessage) Validate() error {
	if message.Phone == "" {
		return fmt.Errorf(errorMessageVoicePhoneRequired)
	}

	if message.Content == "" {
		return fmt.Errorf(errorMessageVoiceContentRequired)
	}

	return nil
}

// ==================== 语音生产者 ====================

// VoiceProducer 语音消息专用生产者
type VoiceProducer struct {
	producer *nsq.Producer
	topic    string
}

// VoiceProducerConfig 语音生产者配置
type VoiceProducerConfig struct {
	NsqdAddress string
	Topic       string
}

// NewVoiceProducer 创建语音消息生产者
func NewVoiceProducer(nsqdAddress string, topic string) (*VoiceProducer, error) {
	config := VoiceProducerConfig{
		NsqdAddress: nsqdAddress,
		Topic:       topic,
	}
	return NewVoiceProducerFromConfig(config)
}

// NewVoiceProducerFromConfig 从配置创建语音生产者
func NewVoiceProducerFromConfig(config VoiceProducerConfig) (*VoiceProducer, error) {
	if err := validateVoiceProducerConfig(config); err != nil {
		return nil, err
	}

	nsqConfig := nsq.NewConfig()
	producer, err := nsq.NewProducer(config.NsqdAddress, nsqConfig)
	if err != nil {
		return nil, fmt.Errorf(errorMessageVoiceProducerCreateFailed, err)
	}

	return &VoiceProducer{
		producer: producer,
		topic:    config.Topic,
	}, nil
}

// validateVoiceProducerConfig 验证语音生产者配置
func validateVoiceProducerConfig(config VoiceProducerConfig) error {
	if config.NsqdAddress == "" {
		return fmt.Errorf("nsqd address is required")
	}

	if config.Topic == "" {
		return fmt.Errorf("topic is required")
	}

	return nil
}

// SendVoiceMessage 发送语音消息到专用队列
func (producer *VoiceProducer) SendVoiceMessage(ctx context.Context, message VoiceMessage) error {
	if err := message.Validate(); err != nil {
		return err
	}

	payload, err := marshalVoiceMessage(message)
	if err != nil {
		return err
	}

	if err := producer.publishMessage(payload); err != nil {
		return err
	}

	log.Printf("%s发送语音消息到队列: %s -> %s", voiceQueueTag, message.Phone, message.Content)
	return nil
}

// marshalVoiceMessage 序列化语音消息
func marshalVoiceMessage(message VoiceMessage) ([]byte, error) {
	payload, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf(errorMessageVoiceMarshalFailed, err)
	}
	return payload, nil
}

// publishMessage 发布消息到队列
func (producer *VoiceProducer) publishMessage(payload []byte) error {
	err := producer.producer.Publish(producer.topic, payload)
	if err != nil {
		return fmt.Errorf(errorMessageVoicePublishFailed, err)
	}
	return nil
}

// GetTopic 获取 topic 名称
func (producer *VoiceProducer) GetTopic() string {
	return producer.topic
}

// Close 关闭生产者
func (producer *VoiceProducer) Close() {
	if producer.producer != nil {
		producer.producer.Stop()
	}
}

// ==================== 语音消费者 ====================

// VoiceHandlerFunc 语音消息处理函数类型
type VoiceHandlerFunc func(ctx context.Context, message VoiceMessage, attempts uint16) error

// VoiceConsumer 语音消息专用消费者
type VoiceConsumer struct {
	consumer             *nsq.Consumer
	topic                string
	channel              string
	nsqdAddresses        []string
	lookupdAddresses     []string
	handler              VoiceHandlerFunc
	maxInFlight          int
	concurrency          int
	dlqTopic             string
	maxAttemptsBeforeDLQ uint16
	dlqProducer          *nsq.Producer
	messageHandleTimeout time.Duration
}

// VoiceConsumerConfig 语音消费者配置
type VoiceConsumerConfig struct {
	Topic                string
	Channel              string
	MaxInFlight          int
	Concurrency          int
	NsqdAddresses        []string
	LookupdAddresses     []string
	DLQTopic             string
	MaxAttemptsBeforeDLQ uint16
	MessageHandleTimeout time.Duration
	Handler              VoiceHandlerFunc
}

// NewVoiceConsumer 创建语音消息消费者
func NewVoiceConsumer(
	topic string,
	channel string,
	maxInFlight int,
	concurrency int,
	nsqdAddresses []string,
	lookupdAddresses []string,
	dlqTopic string,
	maxAttemptsBeforeDLQ uint16,
	handler VoiceHandlerFunc,
) (*VoiceConsumer, error) {
	config := VoiceConsumerConfig{
		Topic:                topic,
		Channel:              channel,
		MaxInFlight:          maxInFlight,
		Concurrency:          concurrency,
		NsqdAddresses:        nsqdAddresses,
		LookupdAddresses:     lookupdAddresses,
		DLQTopic:             dlqTopic,
		MaxAttemptsBeforeDLQ: maxAttemptsBeforeDLQ,
		MessageHandleTimeout: voiceMessageHandleTimeout,
		Handler:              handler,
	}
	return NewVoiceConsumerFromConfig(config)
}

// NewVoiceConsumerFromConfig 从配置创建语音消费者
func NewVoiceConsumerFromConfig(config VoiceConsumerConfig) (*VoiceConsumer, error) {
	if err := validateVoiceConsumerConfig(config); err != nil {
		return nil, err
	}

	nsqConfig := createVoiceNSQConfig(config.MaxInFlight)
	consumer, err := createVoiceNSQConsumer(config.Topic, config.Channel, nsqConfig)
	if err != nil {
		return nil, err
	}

	timeout := config.MessageHandleTimeout
	if timeout == 0 {
		timeout = voiceMessageHandleTimeout
	}

	voiceConsumer := &VoiceConsumer{
		consumer:             consumer,
		topic:                config.Topic,
		channel:              config.Channel,
		nsqdAddresses:        config.NsqdAddresses,
		lookupdAddresses:     config.LookupdAddresses,
		handler:              config.Handler,
		maxInFlight:          config.MaxInFlight,
		concurrency:          config.Concurrency,
		dlqTopic:             config.DLQTopic,
		maxAttemptsBeforeDLQ: config.MaxAttemptsBeforeDLQ,
		messageHandleTimeout: timeout,
	}

	return voiceConsumer, nil
}

// validateVoiceConsumerConfig 验证语音消费者配置
func validateVoiceConsumerConfig(config VoiceConsumerConfig) error {
	if config.Topic == "" {
		return fmt.Errorf("topic is required")
	}

	if config.Channel == "" {
		return fmt.Errorf("channel is required")
	}

	if config.Handler == nil {
		return fmt.Errorf(errorMessageVoiceHandlerRequired)
	}

	if len(config.NsqdAddresses) == 0 && len(config.LookupdAddresses) == 0 {
		return fmt.Errorf("no nsqd address or lookupd configured")
	}

	return nil
}

// createVoiceNSQConfig 创建 NSQ 配置
func createVoiceNSQConfig(maxInFlight int) *nsq.Config {
	config := nsq.NewConfig()

	if maxInFlight > 0 {
		config.MaxInFlight = maxInFlight
	}

	config.UserAgent = voiceConsumerUserAgent
	return config
}

// createVoiceNSQConsumer 创建 NSQ 消费者实例
func createVoiceNSQConsumer(topic string, channel string, config *nsq.Config) (*nsq.Consumer, error) {
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		return nil, fmt.Errorf(errorMessageVoiceConsumerCreateFailed, err)
	}

	setupVoiceConsumerLogger(consumer)
	return consumer, nil
}

// setupVoiceConsumerLogger 设置消费者日志
func setupVoiceConsumerLogger(consumer *nsq.Consumer) {
	logger := log.New(os.Stdout, voiceLogPrefix, log.LstdFlags)
	consumer.SetLogger(logger, nsq.LogLevelInfo)
}

// ==================== DLQ 配置 ====================

// AttachDLQProducer 附加死信队列生产者
func (consumer *VoiceConsumer) AttachDLQProducer(nsqdAddress string) error {
	if !consumer.isDLQConfigured() || nsqdAddress == "" {
		return nil
	}

	producer, err := createVoiceDLQProducer(nsqdAddress)
	if err != nil {
		return err
	}

	consumer.dlqProducer = producer
	return nil
}

// createVoiceDLQProducer 创建 DLQ 生产者
func createVoiceDLQProducer(nsqdAddress string) (*nsq.Producer, error) {
	producer, err := nsq.NewProducer(nsqdAddress, nsq.NewConfig())
	if err != nil {
		return nil, fmt.Errorf(errorMessageVoiceDLQProducerCreateFailed, err)
	}
	return producer, nil
}

// isDLQConfigured 检查是否配置了 DLQ
func (consumer *VoiceConsumer) isDLQConfigured() bool {
	return consumer.dlqTopic != ""
}

// ==================== 消息处理 ====================

// Run 启动语音消息消费者
func (consumer *VoiceConsumer) Run() error {
	if err := consumer.registerMessageHandler(); err != nil {
		return err
	}

	if err := consumer.connectToNSQ(); err != nil {
		return err
	}

	log.Printf("%s语音消息消费者已启动，监听主题: %s", voiceQueueTag, consumer.topic)
	consumer.waitForShutdown()
	return nil
}

// registerMessageHandler 注册消息处理器
func (consumer *VoiceConsumer) registerMessageHandler() error {
	messageHandler := consumer.createMessageHandler()
	consumer.consumer.AddConcurrentHandlers(messageHandler, consumer.concurrency)
	return nil
}

// createMessageHandler 创建消息处理器
func (consumer *VoiceConsumer) createMessageHandler() nsq.Handler {
	return nsq.HandlerFunc(func(message *nsq.Message) error {
		return consumer.handleMessage(message)
	})
}

// handleMessage 处理单条消息
func (consumer *VoiceConsumer) handleMessage(message *nsq.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), consumer.messageHandleTimeout)
	defer cancel()

	voiceMessage, err := consumer.parseVoiceMessage(message.Body)
	if err != nil {
		return err
	}

	consumer.logMessageProcessing(voiceMessage, message.Attempts)

	// 语音消息特殊处理：超过硬性最大尝试次数直接放弃
	if consumer.shouldAbandonMessage(message) {
		consumer.logMessageAbandoned(voiceMessage)
		return nil
	}

	err = consumer.handler(ctx, voiceMessage, message.Attempts)
	if err == nil {
		consumer.logMessageSuccess(voiceMessage)
		return nil
	}

	consumer.logMessageFailure(err)
	return consumer.handleFailedMessage(message, voiceMessage, err)
}

// parseVoiceMessage 解析语音消息
func (consumer *VoiceConsumer) parseVoiceMessage(data []byte) (VoiceMessage, error) {
	var voiceMessage VoiceMessage
	if err := json.Unmarshal(data, &voiceMessage); err != nil {
		log.Printf("%s解析语音消息失败: %v", voiceQueueTag, err)
		return voiceMessage, fmt.Errorf(errorMessageVoiceUnmarshalFailed, err)
	}
	return voiceMessage, nil
}

// shouldAbandonMessage 判断是否应该放弃消息（语音消息特有逻辑）
func (consumer *VoiceConsumer) shouldAbandonMessage(message *nsq.Message) bool {
	return message.Attempts > maxVoiceAttempts
}

// logMessageProcessing 记录消息处理日志
func (consumer *VoiceConsumer) logMessageProcessing(message VoiceMessage, attempts uint16) {
	log.Printf("%s处理语音消息: %s -> %s (尝试次数: %d)",
		voiceQueueTag, message.Phone, message.Content, attempts)
}

// logMessageSuccess 记录消息处理成功日志
func (consumer *VoiceConsumer) logMessageSuccess(message VoiceMessage) {
	log.Printf("%s语音消息处理成功: %s", voiceQueueTag, message.Phone)
}

// logMessageFailure 记录消息处理失败日志
func (consumer *VoiceConsumer) logMessageFailure(err error) {
	log.Printf("%s语音消息处理失败: %v", voiceQueueTag, err)
}

// logMessageAbandoned 记录消息放弃日志
func (consumer *VoiceConsumer) logMessageAbandoned(message VoiceMessage) {
	log.Printf("%s超过最大尝试次数，直接放弃: %s", voiceQueueTag, message.Phone)
}

// handleFailedMessage 处理失败的消息
func (consumer *VoiceConsumer) handleFailedMessage(
	message *nsq.Message,
	voiceMessage VoiceMessage,
	originalError error,
) error {
	if !consumer.shouldSendToDLQ(message) {
		return originalError
	}

	log.Printf("%s语音消息达到最大重试次数，发送到死信队列: %s", voiceQueueTag, voiceMessage.Phone)

	if err := consumer.sendMessageToDLQ(message); err != nil {
		log.Printf("%s发送到死信队列失败: %v", voiceQueueTag, err)
		return originalError
	}

	log.Printf("%s语音消息已发送到死信队列: %s", voiceQueueTag, voiceMessage.Phone)
	return nil
}

// shouldSendToDLQ 判断是否应该发送到 DLQ
func (consumer *VoiceConsumer) shouldSendToDLQ(message *nsq.Message) bool {
	if !consumer.isDLQConfigured() {
		return false
	}

	if consumer.dlqProducer == nil {
		return false
	}

	if message.Attempts < consumer.maxAttemptsBeforeDLQ {
		return false
	}

	return true
}

// sendMessageToDLQ 发送消息到 DLQ
func (consumer *VoiceConsumer) sendMessageToDLQ(message *nsq.Message) error {
	return consumer.dlqProducer.Publish(consumer.dlqTopic, message.Body)
}

// ==================== 连接管理 ====================

// connectToNSQ 连接到 NSQ
func (consumer *VoiceConsumer) connectToNSQ() error {
	if err := consumer.connectToNSQD(); err != nil {
		return err
	}

	if err := consumer.connectToLookupd(); err != nil {
		return err
	}

	return nil
}

// connectToNSQD 连接到 NSQD 节点
func (consumer *VoiceConsumer) connectToNSQD() error {
	for _, address := range consumer.nsqdAddresses {
		if err := consumer.consumer.ConnectToNSQD(address); err != nil {
			return fmt.Errorf("failed to connect to nsqd %s: %w", address, err)
		}
		log.Printf("%sConnected to nsqd: %s", voiceQueueTag, address)
	}
	return nil
}

// connectToLookupd 连接到 Lookupd 节点
func (consumer *VoiceConsumer) connectToLookupd() error {
	for _, address := range consumer.lookupdAddresses {
		if err := consumer.consumer.ConnectToNSQLookupd(address); err != nil {
			return fmt.Errorf("failed to connect to lookupd %s: %w", address, err)
		}
		log.Printf("%sConnected to lookupd: %s", voiceQueueTag, address)
	}
	return nil
}

// waitForShutdown 等待关闭信号
func (consumer *VoiceConsumer) waitForShutdown() {
	<-consumer.consumer.StopChan
}

// ==================== 生命周期管理 ====================

// Stop 停止语音消息消费者
func (consumer *VoiceConsumer) Stop() {
	consumer.stopConsumer()
	consumer.stopDLQProducer()
}

// stopConsumer 停止主消费者
func (consumer *VoiceConsumer) stopConsumer() {
	if consumer.consumer != nil {
		log.Printf("%sStopping Voice consumer for topic: %s", voiceQueueTag, consumer.topic)
		consumer.consumer.Stop()
	}
}

// stopDLQProducer 停止 DLQ 生产者
func (consumer *VoiceConsumer) stopDLQProducer() {
	if consumer.dlqProducer != nil {
		log.Printf("%sStopping Voice DLQ producer", voiceQueueTag)
		consumer.dlqProducer.Stop()
	}
}

// ==================== 状态查询 ====================

// GetStats 获取消费者统计信息
func (consumer *VoiceConsumer) GetStats() *nsq.ConsumerStats {
	return consumer.consumer.Stats()
}

// IsConnected 检查是否已连接
func (consumer *VoiceConsumer) IsConnected() bool {
	stats := consumer.consumer.Stats()
	return stats.Connections > 0
}

// GetTopic 获取 Topic 名称
func (consumer *VoiceConsumer) GetTopic() string {
	return consumer.topic
}

// GetChannel 获取 Channel 名称
func (consumer *VoiceConsumer) GetChannel() string {
	return consumer.channel
}

// IsDLQEnabled 检查是否启用了 DLQ
func (consumer *VoiceConsumer) IsDLQEnabled() bool {
	return consumer.isDLQConfigured() && consumer.dlqProducer != nil
}
