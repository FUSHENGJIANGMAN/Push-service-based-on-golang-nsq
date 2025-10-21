package push

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"

	"gopkg.in/yaml.v3"
)

// ==================== 常量定义 ====================

const (
	// 字段类型常量(与 dynamic_message.go 保持一致)
	fieldTypeString      = "string"
	fieldTypeInt         = "int"
	fieldTypeInt64       = "int64"
	fieldTypeUint64      = "uint64"
	fieldTypeFloat64     = "float64"
	fieldTypeBool        = "bool"
	fieldTypeObject      = "object"
	fieldTypeStringArray = "[]string"
	fieldTypeUint64Array = "[]uint64"
	fieldTypeObjectArray = "[]object"

	// 默认值常量
	defaultFieldDescription = "无描述"

	// 错误消息常量
	errorMessageDecodeFailure   = "消息解码失败"
	errorMessageParseFailure    = "动态消息解析失败"
	errorMessageHandlerNotFound = "没有找到处理器"
	errorMessageInvalidType     = "invalid type '%s' for field %s"
)

// validFieldTypes 有效的字段类型集合
var validFieldTypes = map[string]bool{
	fieldTypeString:      true,
	fieldTypeInt:         true,
	fieldTypeInt64:       true,
	fieldTypeUint64:      true,
	fieldTypeFloat64:     true,
	fieldTypeBool:        true,
	fieldTypeObject:      true,
	fieldTypeStringArray: true,
	fieldTypeUint64Array: true,
	fieldTypeObjectArray: true,
}

// ==================== 基础消息结构 ====================

// BaseMessage 基础消息结构
type BaseMessage struct {
	ID        string `json:"id"`
	UserID    string `json:"user_id"`
	Kind      string `json:"kind"`
	CreatedAt int64  `json:"created_at"`
	ReadAt    int64  `json:"read_at"`
	Subject   string `json:"subject"`
	Body      string `json:"body"`
	Sender    uint64 `json:"sender"`
}

// Msg 通用消息类型
type Msg struct {
	BaseMessage
	AppID     string   `json:"app_id"`
	Content   string   `json:"content"`
	Receivers []uint64 `json:"receivers"`
	Type      string   `json:"type"`
}

// Notification 通知消息类型
type Notification struct {
	BaseMessage
	Title   string   `json:"title"`
	Body    string   `json:"body"`
	UserIDs []uint64 `json:"user_ids"`
}

// Alert 告警消息类型
type Alert struct {
	BaseMessage
	Severity  string   `json:"severity"`
	Message   string   `json:"message"`
	Timestamp string   `json:"timestamp"`
	UserIDs   []uint64 `json:"user_ids"`
}

// ==================== 消息处理接口 ====================

// MessageHandler 消息处理器接口
type MessageHandler interface {
	HandleMessage(ctx context.Context, messageBytes []byte) error
}

// ==================== 动态消息处理器 ====================

// DynamicMessageHandler 动态消息处理器
type DynamicMessageHandler struct {
	registry *MessageRegistry
	topic    string
}

// NewDynamicMessageHandler 创建动态消息处理器
func NewDynamicMessageHandler(registry *MessageRegistry, topic string) *DynamicMessageHandler {
	return &DynamicMessageHandler{
		registry: registry,
		topic:    topic,
	}
}

// HandleMessage 处理动态消息
func (handler *DynamicMessageHandler) HandleMessage(ctx context.Context, messageBytes []byte) error {
	rawData, err := handler.decodeMessageBytes(messageBytes)
	if err != nil {
		return err
	}

	if err := handler.parseAndValidateMessage(rawData); err != nil {
		return err
	}

	// TODO: 在这里添加实际的消息处理逻辑
	return nil
}

// decodeMessageBytes 解码消息字节为 map
func (handler *DynamicMessageHandler) decodeMessageBytes(messageBytes []byte) (map[string]interface{}, error) {
	var rawData map[string]interface{}
	if err := json.Unmarshal(messageBytes, &rawData); err != nil {
		return nil, fmt.Errorf("%s: %w", errorMessageDecodeFailure, err)
	}
	return rawData, nil
}

// parseAndValidateMessage 解析并验证消息
func (handler *DynamicMessageHandler) parseAndValidateMessage(rawData map[string]interface{}) error {
	_, err := handler.registry.ParseMessage(handler.topic, rawData)
	if err != nil {
		return fmt.Errorf("%s: %w", errorMessageParseFailure, err)
	}
	return nil
}

// ==================== 消息处理器注册和路由 ====================

// MsgProcessor 消息处理器管理器
type MsgProcessor struct {
	handlers map[string]MessageHandler
}

// NewMsgProcessor 创建消息处理器管理器
func NewMsgProcessor() *MsgProcessor {
	return &MsgProcessor{
		handlers: make(map[string]MessageHandler),
	}
}

// RegisterHandler 注册消息处理器
func (processor *MsgProcessor) RegisterHandler(topic string, handler MessageHandler) {
	if processor.handlers == nil {
		processor.handlers = make(map[string]MessageHandler)
	}
	processor.handlers[topic] = handler
}

// ProcessMessage 根据 topic 路由并处理消息
func (processor *MsgProcessor) ProcessMessage(ctx context.Context, topic string, messageBytes []byte) error {
	handler, found := processor.handlers[topic]
	if !found {
		return fmt.Errorf("%s, topic: %s", errorMessageHandlerNotFound, topic)
	}

	return handler.HandleMessage(ctx, messageBytes)
}

// GetHandler 获取指定 topic 的处理器
func (processor *MsgProcessor) GetHandler(topic string) (MessageHandler, bool) {
	handler, exists := processor.handlers[topic]
	return handler, exists
}

// HasHandler 检查是否存在指定 topic 的处理器
func (processor *MsgProcessor) HasHandler(topic string) bool {
	_, exists := processor.handlers[topic]
	return exists
}

// GetAllTopics 获取所有已注册的 topic
func (processor *MsgProcessor) GetAllTopics() []string {
	topics := make([]string, 0, len(processor.handlers))
	for topic := range processor.handlers {
		topics = append(topics, topic)
	}
	return topics
}

// ==================== 通用消息处理器 ====================

// GenericHandler 通用消息处理器,支持动态消息类型解析
type GenericHandler struct {
	messageType reflect.Type
}

// NewGenericHandler 创建通用消息处理器
func NewGenericHandler(messageType reflect.Type) MessageHandler {
	return &GenericHandler{
		messageType: messageType,
	}
}

// HandleMessage 处理消息,解析并映射到相应的结构体
func (handler *GenericHandler) HandleMessage(ctx context.Context, messageBytes []byte) error {
	messageInstance, err := handler.createMessageInstance()
	if err != nil {
		return err
	}

	if err := handler.decodeMessage(messageBytes, messageInstance); err != nil {
		return err
	}

	// TODO: 在这里添加实际的消息处理逻辑
	return nil
}

// createMessageInstance 动态创建消息结构体实例
func (handler *GenericHandler) createMessageInstance() (interface{}, error) {
	if handler.messageType == nil {
		return nil, fmt.Errorf("message type is nil")
	}
	return reflect.New(handler.messageType).Interface(), nil
}

// decodeMessage 解码消息到目标结构体
func (handler *GenericHandler) decodeMessage(messageBytes []byte, target interface{}) error {
	if err := json.Unmarshal(messageBytes, target); err != nil {
		return fmt.Errorf("%s: %w", errorMessageDecodeFailure, err)
	}
	return nil
}

// ==================== 消息格式配置 ====================

// MessageFormat 消息格式定义
type MessageFormat struct {
	Type     string         `yaml:"type"`     // 消息类型(如 push.Msg、push.Notification)
	Channels []string       `yaml:"channels"` // 支持的通道列表(如 web、email)
	Fields   []MessageField `yaml:"fields"`   // 字段列表
}

// MessageField 消息字段定义
type MessageField struct {
	Name        string `yaml:"name"`
	Type        string `yaml:"type"`
	Required    bool   `yaml:"required"`
	Description string `yaml:"description"`
}

// Validate 验证消息格式是否合法
func (format *MessageFormat) Validate() error {
	for _, field := range format.Fields {
		if err := validateFieldType(field); err != nil {
			return err
		}
	}
	return nil
}

// validateFieldType 验证字段类型是否有效
func validateFieldType(field MessageField) error {
	if !validFieldTypes[field.Type] {
		return fmt.Errorf(errorMessageInvalidType, field.Type, field.Name)
	}
	return nil
}

// SetDefaults 为所有字段设置默认值
func (format *MessageFormat) SetDefaults() {
	for index := range format.Fields {
		format.Fields[index].SetDefaults()
	}
}

// SetDefaults 设置字段的默认值
func (field *MessageField) SetDefaults() {
	// Required 字段默认为 false (Go 的零值)
	// 只需设置 Description 的默认值
	if field.Description == "" {
		field.Description = defaultFieldDescription
	}
}

// ==================== 配置加载 ====================

// Config 配置结构体
type Config struct {
	MessageFormats map[string]MessageFormat `yaml:"MessageFormats"`
}

// LoadConfig 从 YAML 文件加载配置
func LoadConfig(filePath string) (*Config, error) {
	file, err := openConfigFile(filePath)
	if err != nil {
		return nil, err
	}
	defer closeConfigFile(file, filePath)

	config, err := decodeYAMLConfig(file)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// openConfigFile 打开配置文件
func openConfigFile(filePath string) (*os.File, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file %s: %w", filePath, err)
	}
	return file, nil
}

// closeConfigFile 关闭配置文件
func closeConfigFile(file *os.File, filePath string) {
	if err := file.Close(); err != nil {
		// 记录关闭文件时的错误,但不中断程序
		fmt.Printf("warning: failed to close config file %s: %v\n", filePath, err)
	}
}

// decodeYAMLConfig 解码 YAML 配置
func decodeYAMLConfig(file *os.File) (*Config, error) {
	var config Config
	decoder := yaml.NewDecoder(file)

	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode YAML config: %w", err)
	}

	return &config, nil
}

// LoadAndValidateConfig 加载并验证配置
func LoadAndValidateConfig(filePath string) (*Config, error) {
	config, err := LoadConfig(filePath)
	if err != nil {
		return nil, err
	}

	if err := ValidateConfig(config); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return config, nil
}

// ValidateConfig 验证配置的有效性
func ValidateConfig(config *Config) error {
	if config == nil {
		return fmt.Errorf("config is nil")
	}

	if len(config.MessageFormats) == 0 {
		return fmt.Errorf("no message formats defined in config")
	}

	for topic, format := range config.MessageFormats {
		if err := format.Validate(); err != nil {
			return fmt.Errorf("invalid format for topic %s: %w", topic, err)
		}
	}

	return nil
}

// ApplyDefaultsToConfig 为配置中的所有格式应用默认值
func ApplyDefaultsToConfig(config *Config) {
	if config == nil {
		return
	}

	for topic := range config.MessageFormats {
		format := config.MessageFormats[topic]
		format.SetDefaults()
		config.MessageFormats[topic] = format
	}
}

// ==================== 辅助函数 ====================

// IsValidFieldType 检查字段类型是否有效
func IsValidFieldType(fieldType string) bool {
	return validFieldTypes[fieldType]
}

// GetSupportedFieldTypes 获取所有支持的字段类型
func GetSupportedFieldTypes() []string {
	types := make([]string, 0, len(validFieldTypes))
	for fieldType := range validFieldTypes {
		types = append(types, fieldType)
	}
	return types
}
