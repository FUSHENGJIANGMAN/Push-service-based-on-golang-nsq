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
	// 短信消息处理超时时间
	smsMessageHandleTimeout = 30 * time.Second

	// 用户代理标识
	smsConsumerUserAgent = "sms-consumer"

	// 日志前缀
	smsLogPrefix = "[sms-nsq] "
	smsQueueTag  = "[SMS_QUEUE] "

	// 错误消息常量
	errorMessageSMSPhoneRequired           = "phone number is required"
	errorMessageSMSContentRequired         = "content is required"
	errorMessageSMSHandlerRequired         = "sms handler is required"
	errorMessageSMSProducerCreateFailed    = "failed to create sms producer: %w"
	errorMessageSMSConsumerCreateFailed    = "failed to create sms consumer: %w"
	errorMessageSMSDLQProducerCreateFailed = "failed to create sms DLQ producer: %w"
	errorMessageSMSMarshalFailed           = "failed to marshal sms message: %w"
	errorMessageSMSPublishFailed           = "failed to publish sms message: %w"
	errorMessageSMSUnmarshalFailed         = "failed to unmarshal sms message: %w"
)

// ==================== 短信消息结构 ====================

// SMSMessage 短信消息结构
type SMSMessage struct {
	MessageID string            `json:"message_id"`
	Phone     string            `json:"phone"`
	Content   string            `json:"content"`
	Priority  int               `json:"priority"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// GetMessageID 获取消息ID
func (message *SMSMessage) GetMessageID() string {
	return message.MessageID
}

// Validate 验证短信消息
func (message *SMSMessage) Validate() error {
	if message.Phone == "" {
		return fmt.Errorf(errorMessageSMSPhoneRequired)
	}

	if message.Content == "" {
		return fmt.Errorf(errorMessageSMSContentRequired)
	}

	return nil
}

// ==================== 短信生产者 ====================

// SMSProducer 短信消息专用生产者
type SMSProducer struct {
	producer *nsq.Producer
	topic    string
}

// SMSProducerConfig 短信生产者配置
type SMSProducerConfig struct {
	NsqdAddress string
	Topic       string
}

// NewSMSProducer 创建短信消息生产者
func NewSMSProducer(nsqdAddress string, topic string) (*SMSProducer, error) {
	config := SMSProducerConfig{
		NsqdAddress: nsqdAddress,
		Topic:       topic,
	}
	return NewSMSProducerFromConfig(config)
}

// NewSMSProducerFromConfig 从配置创建短信生产者
func NewSMSProducerFromConfig(config SMSProducerConfig) (*SMSProducer, error) {
	if err := validateSMSProducerConfig(config); err != nil {
		return nil, err
	}

	nsqConfig := nsq.NewConfig()
	producer, err := nsq.NewProducer(config.NsqdAddress, nsqConfig)
	if err != nil {
		return nil, fmt.Errorf(errorMessageSMSProducerCreateFailed, err)
	}

	return &SMSProducer{
		producer: producer,
		topic:    config.Topic,
	}, nil
}

// validateSMSProducerConfig 验证短信生产者配置
func validateSMSProducerConfig(config SMSProducerConfig) error {
	if config.NsqdAddress == "" {
		return fmt.Errorf("nsqd address is required")
	}

	if config.Topic == "" {
		return fmt.Errorf("topic is required")
	}

	return nil
}

// SendSMSMessage 发送短信消息到专用队列
func (producer *SMSProducer) SendSMSMessage(ctx context.Context, message SMSMessage) error {
	if err := message.Validate(); err != nil {
		return err
	}

	payload, err := marshalSMSMessage(message)
	if err != nil {
		return err
	}

	if err := producer.publishMessage(payload); err != nil {
		return err
	}

	log.Printf("%s发送短信消息到队列: %s -> %s", smsQueueTag, message.Phone, message.Content)
	return nil
}

// marshalSMSMessage 序列化短信消息
func marshalSMSMessage(message SMSMessage) ([]byte, error) {
	payload, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf(errorMessageSMSMarshalFailed, err)
	}
	return payload, nil
}

// publishMessage 发布消息到队列
func (producer *SMSProducer) publishMessage(payload []byte) error {
	err := producer.producer.Publish(producer.topic, payload)
	if err != nil {
		return fmt.Errorf(errorMessageSMSPublishFailed, err)
	}
	return nil
}

// GetTopic 获取 topic 名称
func (producer *SMSProducer) GetTopic() string {
	return producer.topic
}

// Close 关闭生产者
func (producer *SMSProducer) Close() {
	if producer.producer != nil {
		producer.producer.Stop()
	}
}

// ==================== 短信消费者 ====================

// SMSHandlerFunc 短信消息处理函数类型
type SMSHandlerFunc func(ctx context.Context, message SMSMessage, attempts uint16) error

// SMSConsumer 短信消息专用消费者
type SMSConsumer struct {
	consumer             *nsq.Consumer
	topic                string
	channel              string
	nsqdAddresses        []string
	lookupdAddresses     []string
	handler              SMSHandlerFunc
	maxInFlight          int
	concurrency          int
	dlqTopic             string
	maxAttemptsBeforeDLQ uint16
	dlqProducer          *nsq.Producer
	messageHandleTimeout time.Duration
}

// SMSConsumerConfig 短信消费者配置
type SMSConsumerConfig struct {
	Topic                string
	Channel              string
	MaxInFlight          int
	Concurrency          int
	NsqdAddresses        []string
	LookupdAddresses     []string
	DLQTopic             string
	MaxAttemptsBeforeDLQ uint16
	MessageHandleTimeout time.Duration
	Handler              SMSHandlerFunc
}

// NewSMSConsumer 创建短信消息消费者
func NewSMSConsumer(
	topic string,
	channel string,
	maxInFlight int,
	concurrency int,
	nsqdAddresses []string,
	lookupdAddresses []string,
	dlqTopic string,
	maxAttemptsBeforeDLQ uint16,
	handler SMSHandlerFunc,
) (*SMSConsumer, error) {
	config := SMSConsumerConfig{
		Topic:                topic,
		Channel:              channel,
		MaxInFlight:          maxInFlight,
		Concurrency:          concurrency,
		NsqdAddresses:        nsqdAddresses,
		LookupdAddresses:     lookupdAddresses,
		DLQTopic:             dlqTopic,
		MaxAttemptsBeforeDLQ: maxAttemptsBeforeDLQ,
		MessageHandleTimeout: smsMessageHandleTimeout,
		Handler:              handler,
	}
	return NewSMSConsumerFromConfig(config)
}

// NewSMSConsumerFromConfig 从配置创建短信消费者
func NewSMSConsumerFromConfig(config SMSConsumerConfig) (*SMSConsumer, error) {
	if err := validateSMSConsumerConfig(config); err != nil {
		return nil, err
	}

	nsqConfig := createSMSNSQConfig(config.MaxInFlight)
	consumer, err := createSMSNSQConsumer(config.Topic, config.Channel, nsqConfig)
	if err != nil {
		return nil, err
	}

	timeout := config.MessageHandleTimeout
	if timeout == 0 {
		timeout = smsMessageHandleTimeout
	}

	smsConsumer := &SMSConsumer{
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

	return smsConsumer, nil
}

// validateSMSConsumerConfig 验证短信消费者配置
func validateSMSConsumerConfig(config SMSConsumerConfig) error {
	if config.Topic == "" {
		return fmt.Errorf("topic is required")
	}

	if config.Channel == "" {
		return fmt.Errorf("channel is required")
	}

	if config.Handler == nil {
		return fmt.Errorf(errorMessageSMSHandlerRequired)
	}

	if len(config.NsqdAddresses) == 0 && len(config.LookupdAddresses) == 0 {
		return fmt.Errorf("no nsqd address or lookupd configured")
	}

	return nil
}

// createSMSNSQConfig 创建 NSQ 配置
func createSMSNSQConfig(maxInFlight int) *nsq.Config {
	config := nsq.NewConfig()

	if maxInFlight > 0 {
		config.MaxInFlight = maxInFlight
	}

	config.UserAgent = smsConsumerUserAgent
	return config
}

// createSMSNSQConsumer 创建 NSQ 消费者实例
func createSMSNSQConsumer(topic string, channel string, config *nsq.Config) (*nsq.Consumer, error) {
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		return nil, fmt.Errorf(errorMessageSMSConsumerCreateFailed, err)
	}

	setupSMSConsumerLogger(consumer)
	return consumer, nil
}

// setupSMSConsumerLogger 设置消费者日志
func setupSMSConsumerLogger(consumer *nsq.Consumer) {
	logger := log.New(os.Stdout, smsLogPrefix, log.LstdFlags)
	consumer.SetLogger(logger, nsq.LogLevelInfo)
}

// ==================== DLQ 配置 ====================

// AttachDLQProducer 附加死信队列生产者
func (consumer *SMSConsumer) AttachDLQProducer(nsqdAddress string) error {
	if !consumer.isDLQConfigured() || nsqdAddress == "" {
		return nil
	}

	producer, err := createSMSDLQProducer(nsqdAddress)
	if err != nil {
		return err
	}

	consumer.dlqProducer = producer
	return nil
}

// createSMSDLQProducer 创建 DLQ 生产者
func createSMSDLQProducer(nsqdAddress string) (*nsq.Producer, error) {
	producer, err := nsq.NewProducer(nsqdAddress, nsq.NewConfig())
	if err != nil {
		return nil, fmt.Errorf(errorMessageSMSDLQProducerCreateFailed, err)
	}
	return producer, nil
}

// isDLQConfigured 检查是否配置了 DLQ
func (consumer *SMSConsumer) isDLQConfigured() bool {
	return consumer.dlqTopic != ""
}

// ==================== 消息处理 ====================

// Run 启动短信消息消费者
func (consumer *SMSConsumer) Run() error {
	if err := consumer.registerMessageHandler(); err != nil {
		return err
	}

	if err := consumer.connectToNSQ(); err != nil {
		return err
	}

	log.Printf("%s短信消息消费者已启动，监听主题: %s", smsQueueTag, consumer.topic)
	consumer.waitForShutdown()
	return nil
}

// registerMessageHandler 注册消息处理器
func (consumer *SMSConsumer) registerMessageHandler() error {
	messageHandler := consumer.createMessageHandler()
	consumer.consumer.AddConcurrentHandlers(messageHandler, consumer.concurrency)
	return nil
}

// createMessageHandler 创建消息处理器
func (consumer *SMSConsumer) createMessageHandler() nsq.Handler {
	return nsq.HandlerFunc(func(message *nsq.Message) error {
		return consumer.handleMessage(message)
	})
}

// handleMessage 处理单条消息
func (consumer *SMSConsumer) handleMessage(message *nsq.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), consumer.messageHandleTimeout)
	defer cancel()

	smsMessage, err := consumer.parseSMSMessage(message.Body)
	if err != nil {
		return err
	}

	consumer.logMessageProcessing(smsMessage, message.Attempts)

	err = consumer.handler(ctx, smsMessage, message.Attempts)
	if err == nil {
		consumer.logMessageSuccess(smsMessage)
		return nil
	}

	consumer.logMessageFailure(err)
	return consumer.handleFailedMessage(message, smsMessage, err)
}

// parseSMSMessage 解析短信消息
func (consumer *SMSConsumer) parseSMSMessage(data []byte) (SMSMessage, error) {
	var smsMessage SMSMessage
	if err := json.Unmarshal(data, &smsMessage); err != nil {
		log.Printf("%s解析短信消息失败: %v", smsQueueTag, err)
		return smsMessage, fmt.Errorf(errorMessageSMSUnmarshalFailed, err)
	}
	return smsMessage, nil
}

// logMessageProcessing 记录消息处理日志
func (consumer *SMSConsumer) logMessageProcessing(message SMSMessage, attempts uint16) {
	log.Printf("%s处理短信消息: %s -> %s (尝试次数: %d)",
		smsQueueTag, message.Phone, message.Content, attempts)
}

// logMessageSuccess 记录消息处理成功日志
func (consumer *SMSConsumer) logMessageSuccess(message SMSMessage) {
	log.Printf("%s短信消息处理成功: %s", smsQueueTag, message.Phone)
}

// logMessageFailure 记录消息处理失败日志
func (consumer *SMSConsumer) logMessageFailure(err error) {
	log.Printf("%s短信消息处理失败: %v", smsQueueTag, err)
}

// handleFailedMessage 处理失败的消息
func (consumer *SMSConsumer) handleFailedMessage(
	message *nsq.Message,
	smsMessage SMSMessage,
	originalError error,
) error {
	if !consumer.shouldSendToDLQ(message) {
		return originalError
	}

	log.Printf("%s短信消息达到最大重试次数，发送到死信队列: %s", smsQueueTag, smsMessage.Phone)

	if err := consumer.sendMessageToDLQ(message); err != nil {
		log.Printf("%s发送到死信队列失败: %v", smsQueueTag, err)
		return originalError
	}

	log.Printf("%s短信消息已发送到死信队列: %s", smsQueueTag, smsMessage.Phone)
	return nil
}

// shouldSendToDLQ 判断是否应该发送到 DLQ
func (consumer *SMSConsumer) shouldSendToDLQ(message *nsq.Message) bool {
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
func (consumer *SMSConsumer) sendMessageToDLQ(message *nsq.Message) error {
	return consumer.dlqProducer.Publish(consumer.dlqTopic, message.Body)
}

// ==================== 连接管理 ====================

// connectToNSQ 连接到 NSQ
func (consumer *SMSConsumer) connectToNSQ() error {
	if err := consumer.connectToNSQD(); err != nil {
		return err
	}

	if err := consumer.connectToLookupd(); err != nil {
		return err
	}

	return nil
}

// connectToNSQD 连接到 NSQD 节点
func (consumer *SMSConsumer) connectToNSQD() error {
	for _, address := range consumer.nsqdAddresses {
		if err := consumer.consumer.ConnectToNSQD(address); err != nil {
			return fmt.Errorf("failed to connect to nsqd %s: %w", address, err)
		}
		log.Printf("%sConnected to nsqd: %s", smsQueueTag, address)
	}
	return nil
}

// connectToLookupd 连接到 Lookupd 节点
func (consumer *SMSConsumer) connectToLookupd() error {
	for _, address := range consumer.lookupdAddresses {
		if err := consumer.consumer.ConnectToNSQLookupd(address); err != nil {
			return fmt.Errorf("failed to connect to lookupd %s: %w", address, err)
		}
		log.Printf("%sConnected to lookupd: %s", smsQueueTag, address)
	}
	return nil
}

// waitForShutdown 等待关闭信号
func (consumer *SMSConsumer) waitForShutdown() {
	<-consumer.consumer.StopChan
}

// ==================== 生命周期管理 ====================

// Stop 停止短信消息消费者
func (consumer *SMSConsumer) Stop() {
	consumer.stopConsumer()
	consumer.stopDLQProducer()
}

// stopConsumer 停止主消费者
func (consumer *SMSConsumer) stopConsumer() {
	if consumer.consumer != nil {
		log.Printf("%sStopping SMS consumer for topic: %s", smsQueueTag, consumer.topic)
		consumer.consumer.Stop()
	}
}

// stopDLQProducer 停止 DLQ 生产者
func (consumer *SMSConsumer) stopDLQProducer() {
	if consumer.dlqProducer != nil {
		log.Printf("%sStopping SMS DLQ producer", smsQueueTag)
		consumer.dlqProducer.Stop()
	}
}

// ==================== 状态查询 ====================

// GetStats 获取消费者统计信息
func (consumer *SMSConsumer) GetStats() *nsq.ConsumerStats {
	return consumer.consumer.Stats()
}

// IsConnected 检查是否已连接
func (consumer *SMSConsumer) IsConnected() bool {
	stats := consumer.consumer.Stats()
	return stats.Connections > 0
}

// GetTopic 获取 Topic 名称
func (consumer *SMSConsumer) GetTopic() string {
	return consumer.topic
}

// GetChannel 获取 Channel 名称
func (consumer *SMSConsumer) GetChannel() string {
	return consumer.channel
}

// IsDLQEnabled 检查是否启用了 DLQ
func (consumer *SMSConsumer) IsDLQEnabled() bool {
	return consumer.isDLQConfigured() && consumer.dlqProducer != nil
}
