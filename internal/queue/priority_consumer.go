package queue

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// ==================== 常量定义 ====================

const (
	// 消费者启动延迟
	consumerStartupDelay = 100 * time.Millisecond

	// 默认优先级名称
	defaultPriorityName = "default"

	// 默认优先级阈值
	defaultPriorityThreshold = 0

	// 错误消息常量
	errorMessageHandlerRequired         = "handler is required"
	errorMessageConsumerAlreadyStopped  = "consumer already stopped"
	errorMessageConsumerCreationFailed  = "failed to create consumer for level %s: %w"
	errorMessageDLQProducerAttachFailed = "failed to attach DLQ producer: %w"
)

// ==================== 数据结构定义 ====================

// PriorityConsumer 支持优先级的 NSQ 消费者
type PriorityConsumer struct {
	// 消费者列表
	consumers []*NSQConsumer // 按优先级排序的消费者列表

	// 优先级配置
	priorityTopics     map[string]string // 优先级名称到 topic 的映射
	priorityThresholds map[string]int    // 优先级阈值
	defaultTopic       string            // 默认 topic

	// 消费配置
	channel              string      // 消费通道
	maxInFlight          int         // 最大并行处理数
	concurrency          int         // 消费并发数
	nsqdAddresses        []string    // nsqd TCP 地址
	lookupdAddresses     []string    // lookupd HTTP 地址
	dlqTopic             string      // 死信队列主题
	maxAttemptsBeforeDLQ uint16      // 进入死信队列前的最大尝试次数
	handler              HandlerFunc // 消息处理函数

	// 状态管理
	waitGroup sync.WaitGroup
	stopChan  chan struct{}
	stopped   bool
	mutex     sync.Mutex
}

// PriorityLevel 优先级级别信息
type PriorityLevel struct {
	Name      string
	Topic     string
	Threshold int
}

// PriorityConsumerConfig 优先级消费者配置
type PriorityConsumerConfig struct {
	DefaultTopic         string
	Channel              string
	MaxInFlight          int
	Concurrency          int
	NsqdAddresses        []string
	LookupdAddresses     []string
	DLQTopic             string
	MaxAttemptsBeforeDLQ uint16
	Handler              HandlerFunc
	PriorityTopics       map[string]string
	PriorityThresholds   map[string]int
}

// ==================== 构造函数 ====================

// NewPriorityConsumer 创建支持优先级的 NSQ 消费者
func NewPriorityConsumer(
	defaultTopic string,
	channel string,
	maxInFlight int,
	concurrency int,
	nsqdAddresses []string,
	lookupdAddresses []string,
	dlqTopic string,
	maxAttemptsBeforeDLQ uint16,
	handler HandlerFunc,
	priorityTopics map[string]string,
	priorityThresholds map[string]int,
) (*PriorityConsumer, error) {
	config := PriorityConsumerConfig{
		DefaultTopic:         defaultTopic,
		Channel:              channel,
		MaxInFlight:          maxInFlight,
		Concurrency:          concurrency,
		NsqdAddresses:        nsqdAddresses,
		LookupdAddresses:     lookupdAddresses,
		DLQTopic:             dlqTopic,
		MaxAttemptsBeforeDLQ: maxAttemptsBeforeDLQ,
		Handler:              handler,
		PriorityTopics:       priorityTopics,
		PriorityThresholds:   priorityThresholds,
	}

	return NewPriorityConsumerFromConfig(config)
}

// NewPriorityConsumerFromConfig 从配置创建优先级消费者
func NewPriorityConsumerFromConfig(config PriorityConsumerConfig) (*PriorityConsumer, error) {
	if err := validatePriorityConsumerConfig(config); err != nil {
		return nil, err
	}

	consumer := &PriorityConsumer{
		priorityTopics:       config.PriorityTopics,
		priorityThresholds:   config.PriorityThresholds,
		defaultTopic:         config.DefaultTopic,
		channel:              config.Channel,
		maxInFlight:          config.MaxInFlight,
		concurrency:          config.Concurrency,
		nsqdAddresses:        config.NsqdAddresses,
		lookupdAddresses:     config.LookupdAddresses,
		dlqTopic:             config.DLQTopic,
		maxAttemptsBeforeDLQ: config.MaxAttemptsBeforeDLQ,
		handler:              config.Handler,
		stopChan:             make(chan struct{}),
	}

	if err := consumer.initializeConsumers(); err != nil {
		return nil, err
	}

	return consumer, nil
}

// ==================== 配置验证 ====================

// validatePriorityConsumerConfig 验证优先级消费者配置
func validatePriorityConsumerConfig(config PriorityConsumerConfig) error {
	if config.Handler == nil {
		return errors.New(errorMessageHandlerRequired)
	}

	if config.DefaultTopic == "" {
		return errors.New("default topic is required")
	}

	if config.Channel == "" {
		return errors.New("channel is required")
	}

	return nil
}

// ==================== 消费者初始化 ====================

// initializeConsumers 初始化所有优先级消费者
func (consumer *PriorityConsumer) initializeConsumers() error {
	priorityLevels := consumer.buildPriorityLevels()
	sortedLevels := consumer.sortPriorityLevels(priorityLevels)

	for _, level := range sortedLevels {
		if err := consumer.createConsumerForLevel(level); err != nil {
			consumer.cleanup()
			return err
		}
	}

	return nil
}

// buildPriorityLevels 构建优先级级别列表
func (consumer *PriorityConsumer) buildPriorityLevels() []PriorityLevel {
	var levels []PriorityLevel

	// 添加配置的优先级队列
	for name, threshold := range consumer.priorityThresholds {
		topic, exists := consumer.priorityTopics[name]
		if !exists {
			continue
		}

		levels = append(levels, PriorityLevel{
			Name:      name,
			Topic:     topic,
			Threshold: threshold,
		})
	}

	// 添加默认队列（最低优先级）
	levels = append(levels, PriorityLevel{
		Name:      defaultPriorityName,
		Topic:     consumer.defaultTopic,
		Threshold: defaultPriorityThreshold,
	})

	return levels
}

// sortPriorityLevels 按优先级从高到低排序
func (consumer *PriorityConsumer) sortPriorityLevels(levels []PriorityLevel) []PriorityLevel {
	sort.Slice(levels, func(i, j int) bool {
		return levels[i].Threshold > levels[j].Threshold
	})
	return levels
}

// createConsumerForLevel 为指定优先级级别创建消费者
func (consumer *PriorityConsumer) createConsumerForLevel(level PriorityLevel) error {
	nsqConsumer, err := NewNSQConsumer(
		level.Topic,
		consumer.channel,
		consumer.maxInFlight,
		consumer.concurrency,
		consumer.nsqdAddresses,
		consumer.lookupdAddresses,
		consumer.dlqTopic,
		consumer.maxAttemptsBeforeDLQ,
		consumer.handler,
	)

	if err != nil {
		return fmt.Errorf(errorMessageConsumerCreationFailed, level.Name, err)
	}

	consumer.consumers = append(consumer.consumers, nsqConsumer)
	log.Printf("Created priority consumer for level %s (topic: %s, threshold: %d)",
		level.Name, level.Topic, level.Threshold)

	return nil
}

// ==================== DLQ 配置 ====================

// AttachDLQProducer 为所有消费者附加死信队列生产者
func (consumer *PriorityConsumer) AttachDLQProducer(nsqdAddress string) error {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	for _, nsqConsumer := range consumer.consumers {
		if err := nsqConsumer.AttachDLQProducer(nsqdAddress); err != nil {
			return fmt.Errorf(errorMessageDLQProducerAttachFailed, err)
		}
	}

	return nil
}

// ==================== 生命周期管理 ====================

// Run 启动所有优先级消费者
func (consumer *PriorityConsumer) Run() error {
	if err := consumer.checkNotStopped(); err != nil {
		return err
	}

	log.Printf("Starting priority consumer with %d levels", len(consumer.consumers))

	consumer.startAllConsumers()
	consumer.waitForShutdown()

	log.Printf("Priority consumer stopped")
	return nil
}

// checkNotStopped 检查消费者是否已停止
func (consumer *PriorityConsumer) checkNotStopped() error {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	if consumer.stopped {
		return errors.New(errorMessageConsumerAlreadyStopped)
	}

	return nil
}

// startAllConsumers 启动所有消费者
func (consumer *PriorityConsumer) startAllConsumers() {
	for index, nsqConsumer := range consumer.consumers {
		consumer.startSingleConsumer(index, nsqConsumer)
		consumer.delayBeforeNextConsumer(index)
	}
}

// startSingleConsumer 启动单个消费者
func (consumer *PriorityConsumer) startSingleConsumer(index int, nsqConsumer *NSQConsumer) {
	consumer.waitGroup.Add(1)

	go func(consumerIndex int, c *NSQConsumer) {
		defer consumer.waitGroup.Done()

		log.Printf("Starting consumer for priority level %d", consumerIndex)

		if err := c.Run(); err != nil {
			log.Printf("Consumer %d error: %v", consumerIndex, err)
		}
	}(index, nsqConsumer)
}

// delayBeforeNextConsumer 在启动下一个消费者前延迟
func (consumer *PriorityConsumer) delayBeforeNextConsumer(currentIndex int) {
	// 最后一个消费者不需要延迟
	if currentIndex >= len(consumer.consumers)-1 {
		return
	}

	// 稍微延迟启动下一个消费者，让高优先级的消费者优先连接
	time.Sleep(consumerStartupDelay)
}

// waitForShutdown 等待关闭信号
func (consumer *PriorityConsumer) waitForShutdown() {
	<-consumer.stopChan
}

// Stop 停止所有优先级消费者
func (consumer *PriorityConsumer) Stop() {
	if !consumer.markAsStopped() {
		return
	}

	log.Printf("Stopping priority consumer...")

	consumer.stopAllConsumers()
	consumer.signalShutdown()
	consumer.waitForAllConsumersToStop()

	log.Printf("All priority consumers stopped")
}

// markAsStopped 标记消费者为已停止状态
func (consumer *PriorityConsumer) markAsStopped() bool {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	if consumer.stopped {
		return false
	}

	consumer.stopped = true
	return true
}

// stopAllConsumers 停止所有消费者
func (consumer *PriorityConsumer) stopAllConsumers() {
	for index, nsqConsumer := range consumer.consumers {
		log.Printf("Stopping consumer %d", index)
		nsqConsumer.Stop()
	}
}

// signalShutdown 发送关闭信号
func (consumer *PriorityConsumer) signalShutdown() {
	close(consumer.stopChan)
}

// waitForAllConsumersToStop 等待所有消费者停止
func (consumer *PriorityConsumer) waitForAllConsumersToStop() {
	consumer.waitGroup.Wait()
}

// ==================== 清理资源 ====================

// cleanup 清理资源
func (consumer *PriorityConsumer) cleanup() {
	for _, nsqConsumer := range consumer.consumers {
		nsqConsumer.Stop()
	}
}

// ==================== 状态查询 ====================

// GetConsumerCount 获取消费者数量
func (consumer *PriorityConsumer) GetConsumerCount() int {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return len(consumer.consumers)
}

// IsStopped 检查消费者是否已停止
func (consumer *PriorityConsumer) IsStopped() bool {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return consumer.stopped
}

// GetPriorityLevels 获取所有优先级级别信息
func (consumer *PriorityConsumer) GetPriorityLevels() []PriorityLevel {
	levels := consumer.buildPriorityLevels()
	return consumer.sortPriorityLevels(levels)
}

// GetConsumers 获取所有消费者（只读访问）
func (consumer *PriorityConsumer) GetConsumers() []*NSQConsumer {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	// 返回副本，避免外部修改
	consumers := make([]*NSQConsumer, len(consumer.consumers))
	copy(consumers, consumer.consumers)
	return consumers
}
