package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nsqio/go-nsq"
)

// ==================== 常量定义 ====================

const (
	// 默认超时时间
	defaultMessageHandleTimeout = 30 * time.Second

	// 用户代理标识
	defaultUserAgent = "push-gateway"

	// 日志前缀
	logPrefix = "[nsq] "

	// 错误消息常量

	errorMessageTopicRequired        = "topic is required"
	errorMessageChannelRequired      = "channel is required"
	errorMessageNoAddressConfigured  = "no nsqd address or lookupd configured"
	errorMessageDLQPublishFailed     = "failed to publish message to DLQ"
	errorMessageConsumerCreationFail = "failed to create NSQ consumer"
)

// ==================== 类型定义 ====================

// HandlerFunc 消息处理函数类型
type HandlerFunc func(ctx context.Context, payload []byte, attempts uint16) error

// NSQConsumer NSQ 消费者
type NSQConsumer struct {
	// 基础配置
	config  *nsq.Config
	topic   string
	channel string

	// 连接地址
	nsqdAddresses    []string // nsqd TCP 地址
	lookupdAddresses []string // lookupd HTTP 地址

	// 核心组件
	consumer *nsq.Consumer
	handler  HandlerFunc

	// 并发控制
	maxInFlight int
	concurrency int

	// DLQ (死信队列) 配置
	dlqTopic             string
	maxAttemptsBeforeDLQ uint16
	dlqProducer          *nsq.Producer

	// 消息处理超时
	messageHandleTimeout time.Duration
}

// ConsumerConfig 消费者配置
type ConsumerConfig struct {
	Topic                string
	Channel              string
	MaxInFlight          int
	Concurrency          int
	NsqdAddresses        []string
	LookupdAddresses     []string
	DLQTopic             string
	MaxAttemptsBeforeDLQ uint16
	MessageHandleTimeout time.Duration
	Handler              HandlerFunc
}

// ==================== 构造函数 ====================

// NewNSQConsumer 创建 NSQ 消费者
func NewNSQConsumer(
	topic string,
	channel string,
	maxInFlight int,
	concurrency int,
	nsqdAddresses []string,
	lookupdAddresses []string,
	dlqTopic string,
	maxAttemptsBeforeDLQ uint16,
	handler HandlerFunc,
) (*NSQConsumer, error) {
	config := ConsumerConfig{
		Topic:                topic,
		Channel:              channel,
		MaxInFlight:          maxInFlight,
		Concurrency:          concurrency,
		NsqdAddresses:        nsqdAddresses,
		LookupdAddresses:     lookupdAddresses,
		DLQTopic:             dlqTopic,
		MaxAttemptsBeforeDLQ: maxAttemptsBeforeDLQ,
		MessageHandleTimeout: defaultMessageHandleTimeout,
		Handler:              handler,
	}

	return NewNSQConsumerFromConfig(config)
}

// NewNSQConsumerFromConfig 从配置创建 NSQ 消费者
func NewNSQConsumerFromConfig(config ConsumerConfig) (*NSQConsumer, error) {
	if err := validateConsumerConfig(config); err != nil {
		return nil, err
	}

	nsqConfig := createNSQConfig(config.MaxInFlight)
	consumer, err := createNSQConsumer(config.Topic, config.Channel, nsqConfig)
	if err != nil {
		return nil, err
	}

	timeout := config.MessageHandleTimeout
	if timeout == 0 {
		timeout = defaultMessageHandleTimeout
	}

	nsqConsumer := &NSQConsumer{
		config:               nsqConfig,
		topic:                config.Topic,
		channel:              config.Channel,
		nsqdAddresses:        config.NsqdAddresses,
		lookupdAddresses:     config.LookupdAddresses,
		consumer:             consumer,
		handler:              config.Handler,
		maxInFlight:          config.MaxInFlight,
		concurrency:          config.Concurrency,
		dlqTopic:             config.DLQTopic,
		maxAttemptsBeforeDLQ: config.MaxAttemptsBeforeDLQ,
		messageHandleTimeout: timeout,
	}

	return nsqConsumer, nil
}

// ==================== 配置验证 ====================

// validateConsumerConfig 验证消费者配置
func validateConsumerConfig(config ConsumerConfig) error {
	if config.Topic == "" {
		return errors.New(errorMessageTopicRequired)
	}

	if config.Channel == "" {
		return errors.New(errorMessageChannelRequired)
	}

	if config.Handler == nil {
		return errors.New(errorMessageHandlerRequired)
	}

	if len(config.NsqdAddresses) == 0 && len(config.LookupdAddresses) == 0 {
		return errors.New(errorMessageNoAddressConfigured)
	}

	return nil
}

// ==================== NSQ 配置创建 ====================

// createNSQConfig 创建 NSQ 配置
func createNSQConfig(maxInFlight int) *nsq.Config {
	config := nsq.NewConfig()

	if maxInFlight > 0 {
		config.MaxInFlight = maxInFlight
	}

	config.UserAgent = defaultUserAgent

	return config
}

// createNSQConsumer 创建 NSQ 消费者实例
func createNSQConsumer(topic string, channel string, config *nsq.Config) (*nsq.Consumer, error) {
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errorMessageConsumerCreationFail, err)
	}

	setupConsumerLogger(consumer)

	return consumer, nil
}

// setupConsumerLogger 设置消费者日志
func setupConsumerLogger(consumer *nsq.Consumer) {
	logger := log.New(os.Stdout, logPrefix, log.LstdFlags)
	consumer.SetLogger(logger, nsq.LogLevelInfo)
}

// ==================== DLQ 配置 ====================

// AttachDLQProducer 附加 DLQ 生产者
func (consumer *NSQConsumer) AttachDLQProducer(nsqdAddress string) error {
	if !consumer.isDLQConfigured() {
		return nil
	}

	if nsqdAddress == "" {
		return nil
	}

	producer, err := createDLQProducer(nsqdAddress)
	if err != nil {
		return err
	}

	consumer.dlqProducer = producer
	return nil
}

// createDLQProducer 创建 DLQ 生产者
func createDLQProducer(nsqdAddress string) (*nsq.Producer, error) {
	producer, err := nsq.NewProducer(nsqdAddress, nsq.NewConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create DLQ producer: %w", err)
	}
	return producer, nil
}

// isDLQConfigured 检查是否配置了 DLQ
func (consumer *NSQConsumer) isDLQConfigured() bool {
	return consumer.dlqTopic != ""
}

// ==================== 消息处理 ====================

// Run 启动消费者
func (consumer *NSQConsumer) Run() error {
	if err := consumer.registerMessageHandler(); err != nil {
		return err
	}

	if err := consumer.connectToNSQ(); err != nil {
		return err
	}

	consumer.waitForShutdown()
	return nil
}

// registerMessageHandler 注册消息处理器
func (consumer *NSQConsumer) registerMessageHandler() error {
	messageHandler := consumer.createMessageHandler()
	consumer.consumer.AddConcurrentHandlers(messageHandler, consumer.concurrency)
	return nil
}

// createMessageHandler 创建消息处理器
func (consumer *NSQConsumer) createMessageHandler() nsq.Handler {
	return nsq.HandlerFunc(func(message *nsq.Message) error {
		return consumer.handleMessage(message)
	})
}

// handleMessage 处理单条消息
func (consumer *NSQConsumer) handleMessage(message *nsq.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), consumer.messageHandleTimeout)
	defer cancel()

	err := consumer.handler(ctx, message.Body, message.Attempts)
	if err == nil {
		return nil
	}

	// 处理失败的消息
	return consumer.handleFailedMessage(message, err)
}

// handleFailedMessage 处理失败的消息
func (consumer *NSQConsumer) handleFailedMessage(message *nsq.Message, originalError error) error {
	// 检查是否应该发送到 DLQ
	if !consumer.shouldSendToDLQ(message) {
		return originalError
	}

	// 尝试发送到 DLQ
	if err := consumer.sendMessageToDLQ(message); err != nil {
		log.Printf("%s: %v, original error: %v", errorMessageDLQPublishFailed, err, originalError)
		return originalError
	}

	// 成功发送到 DLQ，返回 nil 告诉 NSQ 不再重试
	log.Printf("Message sent to DLQ after %d attempts", message.Attempts)
	return nil
}

// shouldSendToDLQ 判断是否应该发送到 DLQ
func (consumer *NSQConsumer) shouldSendToDLQ(message *nsq.Message) bool {
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
func (consumer *NSQConsumer) sendMessageToDLQ(message *nsq.Message) error {
	return consumer.dlqProducer.Publish(consumer.dlqTopic, message.Body)
}

// ==================== 连接管理 ====================

// connectToNSQ 连接到 NSQ
func (consumer *NSQConsumer) connectToNSQ() error {
	if err := consumer.connectToNSQD(); err != nil {
		return err
	}

	if err := consumer.connectToLookupd(); err != nil {
		return err
	}

	return nil
}

// connectToNSQD 连接到 NSQD 节点
func (consumer *NSQConsumer) connectToNSQD() error {
	for _, address := range consumer.nsqdAddresses {
		if err := consumer.consumer.ConnectToNSQD(address); err != nil {
			return fmt.Errorf("failed to connect to nsqd %s: %w", address, err)
		}
		log.Printf("Connected to nsqd: %s", address)
	}

	return nil
}

// connectToLookupd 连接到 Lookupd 节点
func (consumer *NSQConsumer) connectToLookupd() error {
	for _, address := range consumer.lookupdAddresses {
		if err := consumer.consumer.ConnectToNSQLookupd(address); err != nil {
			return fmt.Errorf("failed to connect to lookupd %s: %w", address, err)
		}
		log.Printf("Connected to lookupd: %s", address)
	}

	return nil
}

// waitForShutdown 等待关闭信号
func (consumer *NSQConsumer) waitForShutdown() {
	<-consumer.consumer.StopChan
}

// ==================== 生命周期管理 ====================

// Stop 停止消费者
func (consumer *NSQConsumer) Stop() {
	consumer.stopConsumer()
	consumer.stopDLQProducer()
}

// stopConsumer 停止主消费者
func (consumer *NSQConsumer) stopConsumer() {
	if consumer.consumer != nil {
		log.Printf("Stopping NSQ consumer for topic: %s", consumer.topic)
		consumer.consumer.Stop()
	}
}

// stopDLQProducer 停止 DLQ 生产者
func (consumer *NSQConsumer) stopDLQProducer() {
	if consumer.dlqProducer != nil {
		log.Printf("Stopping DLQ producer for topic: %s", consumer.dlqTopic)
		consumer.dlqProducer.Stop()
	}
}

// ==================== 状态查询 ====================

// GetStats 获取消费者统计信息
func (consumer *NSQConsumer) GetStats() *nsq.ConsumerStats {
	return consumer.consumer.Stats()
}

// IsConnected 检查是否已连接
func (consumer *NSQConsumer) IsConnected() bool {
	stats := consumer.consumer.Stats()
	return stats.Connections > 0
}

// GetTopic 获取 Topic 名称
func (consumer *NSQConsumer) GetTopic() string {
	return consumer.topic
}

// GetChannel 获取 Channel 名称
func (consumer *NSQConsumer) GetChannel() string {
	return consumer.channel
}

// IsDLQEnabled 检查是否启用了 DLQ
func (consumer *NSQConsumer) IsDLQEnabled() bool {
	return consumer.isDLQConfigured() && consumer.dlqProducer != nil
}
