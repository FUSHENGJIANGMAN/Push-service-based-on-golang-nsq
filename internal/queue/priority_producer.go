package queue

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/nsqio/go-nsq"
)

// ==================== 常量定义 ====================

const (
	// 默认优先级
	defaultPriority = 1

	// 错误消息常量
	errorMessageEmptyPayload           = "empty payload"
	errorMessageDefaultProducerCreate  = "failed to create default producer: %w"
	errorMessagePriorityProducerCreate = "failed to create producer for priority %s: %w"
	errorMessagePublishFailed          = "failed to publish to topic %s: %w"
	errorMessageInvalidAddress         = "nsqd address is required"
	errorMessageInvalidDefaultTopic    = "default topic is required"
)

// ==================== 数据结构定义 ====================

// PriorityProducer 支持优先级的 NSQ 生产者
type PriorityProducer struct {
	// 生产者映射
	producers       map[string]*nsq.Producer // 按优先级名称存储生产者
	defaultProducer *nsq.Producer            // 默认生产者

	// 优先级配置
	priorityTopics     map[string]string // 优先级名称到 topic 的映射
	priorityThresholds map[string]int    // 优先级阈值
	defaultTopic       string            // 默认 topic

	// 并发控制
	mutex sync.RWMutex
}

// PriorityProducerConfig 优先级生产者配置
type PriorityProducerConfig struct {
	NsqdAddress        string
	DefaultTopic       string
	PriorityTopics     map[string]string
	PriorityThresholds map[string]int
}

// priorityMapping 优先级映射关系
type priorityMapping struct {
	name      string
	threshold int
}

// ==================== 构造函数 ====================

// NewPriorityProducer 创建支持优先级的 NSQ 生产者
func NewPriorityProducer(
	nsqdAddress string,
	defaultTopic string,
	priorityTopics map[string]string,
	priorityThresholds map[string]int,
) (*PriorityProducer, error) {
	config := PriorityProducerConfig{
		NsqdAddress:        nsqdAddress,
		DefaultTopic:       defaultTopic,
		PriorityTopics:     priorityTopics,
		PriorityThresholds: priorityThresholds,
	}

	return NewPriorityProducerFromConfig(config)
}

// NewPriorityProducerFromConfig 从配置创建优先级生产者
func NewPriorityProducerFromConfig(config PriorityProducerConfig) (*PriorityProducer, error) {
	if err := validateProducerConfig(config); err != nil {
		return nil, err
	}

	producer := &PriorityProducer{
		producers:          make(map[string]*nsq.Producer),
		priorityTopics:     config.PriorityTopics,
		priorityThresholds: config.PriorityThresholds,
		defaultTopic:       config.DefaultTopic,
	}

	if err := producer.initializeProducers(config.NsqdAddress); err != nil {
		return nil, err
	}

	return producer, nil
}

// ==================== 配置验证 ====================

// validateProducerConfig 验证生产者配置
func validateProducerConfig(config PriorityProducerConfig) error {
	if config.NsqdAddress == "" {
		return fmt.Errorf(errorMessageInvalidAddress)
	}

	if config.DefaultTopic == "" {
		return fmt.Errorf(errorMessageInvalidDefaultTopic)
	}

	return nil
}

// ==================== 生产者初始化 ====================

// initializeProducers 初始化所有生产者
func (producer *PriorityProducer) initializeProducers(nsqdAddress string) error {
	nsqConfig := nsq.NewConfig()

	// 创建默认生产者
	if err := producer.createDefaultProducer(nsqdAddress, nsqConfig); err != nil {
		return err
	}

	// 创建优先级生产者
	if err := producer.createPriorityProducers(nsqdAddress, nsqConfig); err != nil {
		producer.Close()
		return err
	}

	return nil
}

// createDefaultProducer 创建默认生产者
func (producer *PriorityProducer) createDefaultProducer(nsqdAddress string, config *nsq.Config) error {
	defaultProducer, err := nsq.NewProducer(nsqdAddress, config)
	if err != nil {
		return fmt.Errorf(errorMessageDefaultProducerCreate, err)
	}

	producer.defaultProducer = defaultProducer
	return nil
}

// createPriorityProducers 创建所有优先级生产者
func (producer *PriorityProducer) createPriorityProducers(nsqdAddress string, config *nsq.Config) error {
	for priorityName := range producer.priorityTopics {
		if err := producer.createSinglePriorityProducer(nsqdAddress, config, priorityName); err != nil {
			return err
		}
	}

	return nil
}

// createSinglePriorityProducer 创建单个优先级生产者
func (producer *PriorityProducer) createSinglePriorityProducer(
	nsqdAddress string,
	config *nsq.Config,
	priorityName string,
) error {
	priorityProducer, err := nsq.NewProducer(nsqdAddress, config)
	if err != nil {
		return fmt.Errorf(errorMessagePriorityProducerCreate, priorityName, err)
	}

	producer.producers[priorityName] = priorityProducer
	return nil
}

// ==================== 消息发送 ====================

// Enqueue 使用默认优先级将消息发送到队列
func (producer *PriorityProducer) Enqueue(ctx context.Context, payload []byte) error {
	return producer.EnqueueWithPriority(ctx, payload, defaultPriority)
}

// EnqueueWithPriority 根据指定优先级将消息发送到队列
func (producer *PriorityProducer) EnqueueWithPriority(ctx context.Context, payload []byte, priority int) error {
	if err := validatePayload(payload); err != nil {
		return err
	}

	producer.mutex.RLock()
	defer producer.mutex.RUnlock()

	topic, nsqProducer := producer.selectTopicAndProducer(priority)

	return producer.publishMessage(nsqProducer, topic, payload)
}

// validatePayload 验证消息载荷
func validatePayload(payload []byte) error {
	if len(payload) == 0 {
		return fmt.Errorf(errorMessageEmptyPayload)
	}
	return nil
}

// selectTopicAndProducer 根据优先级选择 topic 和生产者
func (producer *PriorityProducer) selectTopicAndProducer(priority int) (string, *nsq.Producer) {
	priorityName := producer.findMatchingPriorityName(priority)

	if priorityName == "" {
		return producer.defaultTopic, producer.defaultProducer
	}

	topic := producer.priorityTopics[priorityName]
	nsqProducer := producer.producers[priorityName]

	return topic, nsqProducer
}

// findMatchingPriorityName 查找匹配的优先级名称
func (producer *PriorityProducer) findMatchingPriorityName(priority int) string {
	mappings := producer.buildSortedPriorityMappings()

	for _, mapping := range mappings {
		if priority >= mapping.threshold {
			if _, exists := producer.priorityTopics[mapping.name]; exists {
				return mapping.name
			}
		}
	}

	return ""
}

// buildSortedPriorityMappings 构建并排序优先级映射列表
func (producer *PriorityProducer) buildSortedPriorityMappings() []priorityMapping {
	mappings := make([]priorityMapping, 0, len(producer.priorityThresholds))

	for name, threshold := range producer.priorityThresholds {
		mappings = append(mappings, priorityMapping{
			name:      name,
			threshold: threshold,
		})
	}

	// 按阈值降序排序(高优先级先匹配)
	sort.Slice(mappings, func(i, j int) bool {
		return mappings[i].threshold > mappings[j].threshold
	})

	return mappings
}

// publishMessage 发布消息到指定 topic
func (producer *PriorityProducer) publishMessage(nsqProducer *nsq.Producer, topic string, payload []byte) error {
	err := nsqProducer.Publish(topic, payload)
	if err != nil {
		return fmt.Errorf(errorMessagePublishFailed, topic, err)
	}

	return nil
}

// ==================== 查询方法 ====================

// GetTopicForPriority 获取指定优先级对应的 topic 名称
func (producer *PriorityProducer) GetTopicForPriority(priority int) string {
	producer.mutex.RLock()
	defer producer.mutex.RUnlock()

	priorityName := producer.findMatchingPriorityName(priority)

	if priorityName == "" {
		return producer.defaultTopic
	}

	if topic, exists := producer.priorityTopics[priorityName]; exists {
		return topic
	}

	return producer.defaultTopic
}

// GetDefaultTopic 获取默认 topic
func (producer *PriorityProducer) GetDefaultTopic() string {
	producer.mutex.RLock()
	defer producer.mutex.RUnlock()
	return producer.defaultTopic
}

// GetPriorityTopics 获取所有优先级 topic 映射
func (producer *PriorityProducer) GetPriorityTopics() map[string]string {
	producer.mutex.RLock()
	defer producer.mutex.RUnlock()

	// 返回副本，避免外部修改
	topics := make(map[string]string, len(producer.priorityTopics))
	for key, value := range producer.priorityTopics {
		topics[key] = value
	}

	return topics
}

// GetPriorityThresholds 获取所有优先级阈值
func (producer *PriorityProducer) GetPriorityThresholds() map[string]int {
	producer.mutex.RLock()
	defer producer.mutex.RUnlock()

	// 返回副本，避免外部修改
	thresholds := make(map[string]int, len(producer.priorityThresholds))
	for key, value := range producer.priorityThresholds {
		thresholds[key] = value
	}

	return thresholds
}

// GetProducerCount 获取生产者数量(包括默认生产者)
func (producer *PriorityProducer) GetProducerCount() int {
	producer.mutex.RLock()
	defer producer.mutex.RUnlock()
	return len(producer.producers) + 1 // +1 为默认生产者
}

// ==================== 生命周期管理 ====================

// Close 关闭所有生产者连接
func (producer *PriorityProducer) Close() {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()

	producer.closeDefaultProducer()
	producer.closePriorityProducers()
}

// closeDefaultProducer 关闭默认生产者
func (producer *PriorityProducer) closeDefaultProducer() {
	if producer.defaultProducer != nil {
		producer.defaultProducer.Stop()
		producer.defaultProducer = nil
	}
}

// closePriorityProducers 关闭所有优先级生产者
func (producer *PriorityProducer) closePriorityProducers() {
	for name, nsqProducer := range producer.producers {
		if nsqProducer != nil {
			nsqProducer.Stop()
			delete(producer.producers, name)
		}
	}
}
