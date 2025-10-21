package push

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// ==================== 常量定义 ====================

const (
	// 字段类型常量

	// 默认优先级
	defaultMessagePriority = 5
	minMessagePriority     = 1
	maxMessagePriority     = 5

	// 字段名常量
	fieldNameID            = "id"
	fieldNameUserID        = "user_id"
	fieldNameSubject       = "subject"
	fieldNameBody          = "body"
	fieldNameSender        = "sender"
	fieldNamePriority      = "priority"
	fieldNameTitle         = "title"
	fieldNameDescription   = "description"
	fieldNameMessage       = "message"
	fieldNameProductName   = "productName"
	fieldNameProductID     = "productId"
	fieldNameMaintenanceID = "maintenanceId"
)

// 接收者字段候选列表
var recipientFieldCandidates = []string{"userIds", "user_ids", "receivers", "to"}

// ==================== 核心数据结构 ====================

// DynamicMessage 动态消息结构,可以根据配置动态处理任意字段
type DynamicMessage struct {
	BaseMessage
	Fields map[string]interface{} `json:"fields"` // 动态字段存储
	Topic  string                 `json:"topic"`  // 消息主题/类型
}

// MessageRegistry 消息注册表,管理所有消息类型的配置
type MessageRegistry struct {
	formats        map[string]MessageFormat
	typeValidators map[string]TypeValidator
	typeNormalizer map[string]TypeNormalizer
}

// TypeValidator 类型验证函数
type TypeValidator func(value interface{}) error

// TypeNormalizer 类型标准化函数
type TypeNormalizer func(value interface{}) interface{}

// ==================== 构造函数 ====================

// NewMessageRegistry 创建新的消息注册表
func NewMessageRegistry() *MessageRegistry {
	registry := &MessageRegistry{
		formats:        make(map[string]MessageFormat),
		typeValidators: make(map[string]TypeValidator),
		typeNormalizer: make(map[string]TypeNormalizer),
	}

	registry.initializeTypeValidators()
	registry.initializeTypeNormalizers()

	return registry
}

// ==================== 格式管理 ====================

// RegisterFormat 注册消息格式
func (registry *MessageRegistry) RegisterFormat(topic string, format MessageFormat) error {
	if err := format.Validate(); err != nil {
		return fmt.Errorf("invalid format for topic %s: %w", topic, err)
	}

	registry.formats[topic] = format
	return nil
}

// GetFormat 获取消息格式
func (registry *MessageRegistry) GetFormat(topic string) (MessageFormat, bool) {
	format, exists := registry.formats[topic]
	return format, exists
}

// GetAllFormats 获取所有消息格式配置
func (registry *MessageRegistry) GetAllFormats() map[string]MessageFormat {
	result := make(map[string]MessageFormat, len(registry.formats))
	for key, value := range registry.formats {
		result[key] = value
	}
	return result
}

// ==================== 消息验证 ====================

// ValidateMessage 根据配置验证消息
func (registry *MessageRegistry) ValidateMessage(topic string, data map[string]interface{}) error {
	format, exists := registry.formats[topic]
	if !exists {
		return fmt.Errorf("unknown message topic: %s", topic)
	}

	if err := registry.validateRequiredFields(format, data); err != nil {
		return err
	}

	if err := registry.validateFieldTypes(format, data); err != nil {
		return err
	}

	return nil
}

// validateRequiredFields 验证必填字段
func (registry *MessageRegistry) validateRequiredFields(format MessageFormat, data map[string]interface{}) error {
	for _, field := range format.Fields {
		if !field.Required {
			continue
		}

		if _, exists := data[field.Name]; !exists {
			return fmt.Errorf("missing required field: %s", field.Name)
		}
	}

	return nil
}

// validateFieldTypes 验证字段类型
func (registry *MessageRegistry) validateFieldTypes(format MessageFormat, data map[string]interface{}) error {
	for _, field := range format.Fields {
		value, exists := data[field.Name]
		if !exists {
			continue
		}

		if err := registry.validateFieldType(field, value); err != nil {
			return fmt.Errorf("field %s: %w", field.Name, err)
		}
	}

	return nil
}

// validateFieldType 验证单个字段类型
func (registry *MessageRegistry) validateFieldType(field MessageField, value interface{}) error {
	validator, exists := registry.typeValidators[field.Type]
	if !exists {
		return fmt.Errorf("unsupported field type: %s", field.Type)
	}

	return validator(value)
}

// ==================== 类型验证器初始化 ====================

// initializeTypeValidators 初始化类型验证器
func (registry *MessageRegistry) initializeTypeValidators() {
	registry.typeValidators[fieldTypeString] = validateStringType
	registry.typeValidators[fieldTypeInt] = validateIntType
	registry.typeValidators[fieldTypeUint64] = validateUint64Type
	registry.typeValidators[fieldTypeBool] = validateBoolType
	registry.typeValidators[fieldTypeFloat64] = validateFloat64Type
	registry.typeValidators[fieldTypeObject] = validateObjectType
	registry.typeValidators[fieldTypeStringArray] = validateStringArrayType
	registry.typeValidators[fieldTypeUint64Array] = validateUint64ArrayType
	registry.typeValidators[fieldTypeObjectArray] = validateObjectArrayType
}

// ==================== 具体类型验证器 ====================

// validateStringType 验证字符串类型
func validateStringType(value interface{}) error {
	if _, ok := value.(string); !ok {
		return fmt.Errorf("expected string, got %T", value)
	}
	return nil
}

// validateIntType 验证整数类型
func validateIntType(value interface{}) error {
	switch typedValue := value.(type) {
	case int, int32, int64:
		return nil
	case float64:
		if !isFloatAnInteger(typedValue) {
			return fmt.Errorf("expected integer, got float with decimals")
		}
		return nil
	default:
		return fmt.Errorf("expected integer, got %T", value)
	}
}

// validateUint64Type 验证 uint64 类型
func validateUint64Type(value interface{}) error {
	switch typedValue := value.(type) {
	case uint64:
		return nil
	case float64:
		if !isValidUint64Float(typedValue) {
			return fmt.Errorf("expected uint64, got %v", typedValue)
		}
		return nil
	default:
		return fmt.Errorf("expected uint64, got %T", value)
	}
}

// validateBoolType 验证布尔类型
func validateBoolType(value interface{}) error {
	if _, ok := value.(bool); !ok {
		return fmt.Errorf("expected bool, got %T", value)
	}
	return nil
}

// validateFloat64Type 验证浮点数类型
func validateFloat64Type(value interface{}) error {
	if _, ok := value.(float64); !ok {
		return fmt.Errorf("expected float64, got %T", value)
	}
	return nil
}

// validateObjectType 验证对象类型
func validateObjectType(value interface{}) error {
	if _, ok := value.(map[string]interface{}); !ok {
		return fmt.Errorf("expected object, got %T", value)
	}
	return nil
}

// validateStringArrayType 验证字符串数组类型
func validateStringArrayType(value interface{}) error {
	array, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("expected array, got %T", value)
	}

	for index, item := range array {
		if _, ok := item.(string); !ok {
			return fmt.Errorf("array[%d]: expected string, got %T", index, item)
		}
	}

	return nil
}

// validateUint64ArrayType 验证 uint64 数组类型
func validateUint64ArrayType(value interface{}) error {
	array, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("expected array, got %T", value)
	}

	for index, item := range array {
		switch typedItem := item.(type) {
		case uint64:
			continue
		case float64:
			if !isValidUint64Float(typedItem) {
				return fmt.Errorf("array[%d]: expected uint64, got %v", index, typedItem)
			}
		default:
			return fmt.Errorf("array[%d]: expected uint64, got %T", index, item)
		}
	}

	return nil
}

// validateObjectArrayType 验证对象数组类型
func validateObjectArrayType(value interface{}) error {
	array, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("expected array, got %T", value)
	}

	for index, item := range array {
		if _, ok := item.(map[string]interface{}); !ok {
			return fmt.Errorf("array[%d]: expected object, got %T", index, item)
		}
	}

	return nil
}

// ==================== 类型检查辅助函数 ====================

// isFloatAnInteger 检查浮点数是否为整数
func isFloatAnInteger(floatValue float64) bool {
	return floatValue == float64(int64(floatValue))
}

// isValidUint64Float 检查浮点数是否可以安全转换为 uint64
func isValidUint64Float(floatValue float64) bool {
	return floatValue >= 0 && floatValue == float64(uint64(floatValue))
}

// ==================== 消息解析 ====================

// ParseMessage 解析动态消息
func (registry *MessageRegistry) ParseMessage(topic string, data map[string]interface{}) (*DynamicMessage, error) {
	if err := registry.ValidateMessage(topic, data); err != nil {
		return nil, err
	}

	message := &DynamicMessage{
		Topic:  topic,
		Fields: make(map[string]interface{}),
	}

	registry.populateBaseFields(message, data)
	registry.populateDynamicFields(message, topic, data)

	return message, nil
}

// populateBaseFields 填充基础字段
func (registry *MessageRegistry) populateBaseFields(message *DynamicMessage, data map[string]interface{}) {
	if id, ok := data[fieldNameID].(string); ok {
		message.BaseMessage.ID = id
	}

	if userID, ok := data[fieldNameUserID].(string); ok {
		message.BaseMessage.UserID = userID
	}

	if subject, ok := data[fieldNameSubject].(string); ok {
		message.BaseMessage.Subject = subject
	}

	if body, ok := data[fieldNameBody].(string); ok {
		message.BaseMessage.Body = body
	}

	if sender, ok := data[fieldNameSender].(float64); ok {
		message.BaseMessage.Sender = uint64(sender)
	}

	message.BaseMessage.Kind = message.Topic
	message.BaseMessage.CreatedAt = time.Now().Unix()
}

// populateDynamicFields 填充动态字段
func (registry *MessageRegistry) populateDynamicFields(message *DynamicMessage, topic string, data map[string]interface{}) {
	format := registry.formats[topic]

	for _, field := range format.Fields {
		value, exists := data[field.Name]
		if !exists {
			continue
		}

		message.Fields[field.Name] = registry.normalizeFieldValue(field, value)
	}
}

// ==================== 类型标准化器初始化 ====================

// initializeTypeNormalizers 初始化类型标准化器
func (registry *MessageRegistry) initializeTypeNormalizers() {
	registry.typeNormalizer[fieldTypeInt] = normalizeIntValue
	registry.typeNormalizer[fieldTypeUint64] = normalizeUint64Value
	registry.typeNormalizer[fieldTypeUint64Array] = normalizeUint64ArrayValue
	registry.typeNormalizer[fieldTypeStringArray] = normalizeStringArrayValue
}

// normalizeFieldValue 标准化字段值
func (registry *MessageRegistry) normalizeFieldValue(field MessageField, value interface{}) interface{} {
	normalizer, exists := registry.typeNormalizer[field.Type]
	if !exists {
		return value
	}

	return normalizer(value)
}

// ==================== 具体类型标准化器 ====================

// normalizeIntValue 标准化整数值
func normalizeIntValue(value interface{}) interface{} {
	if floatValue, ok := value.(float64); ok {
		return int64(floatValue)
	}
	return value
}

// normalizeUint64Value 标准化 uint64 值
func normalizeUint64Value(value interface{}) interface{} {
	if floatValue, ok := value.(float64); ok {
		return uint64(floatValue)
	}
	return value
}

// normalizeUint64ArrayValue 标准化 uint64 数组值
func normalizeUint64ArrayValue(value interface{}) interface{} {
	array, ok := value.([]interface{})
	if !ok {
		return value
	}

	result := make([]uint64, len(array))
	for index, item := range array {
		if floatValue, ok := item.(float64); ok {
			result[index] = uint64(floatValue)
		}
	}

	return result
}

// normalizeStringArrayValue 标准化字符串数组值
func normalizeStringArrayValue(value interface{}) interface{} {
	array, ok := value.([]interface{})
	if !ok {
		return value
	}

	result := make([]string, len(array))
	for index, item := range array {
		if stringValue, ok := item.(string); ok {
			result[index] = stringValue
		}
	}

	return result
}

// ==================== 消息转换 ====================

// ToMessage 转换为标准 Message 结构
func (message *DynamicMessage) ToMessage() Message {
	standardMessage := message.createBaseMessage()

	message.copyDynamicFieldsToData(&standardMessage)
	message.extractAndSetPriority(&standardMessage)
	message.populateSubjectIfEmpty(&standardMessage)
	message.populateBodyIfEmpty(&standardMessage)
	message.extractAndSetRecipients(&standardMessage)

	return standardMessage
}

// createBaseMessage 创建基础消息
func (message *DynamicMessage) createBaseMessage() Message {
	return Message{
		Kind:      MessageKind(message.Topic),
		Subject:   message.BaseMessage.Subject,
		Body:      message.BaseMessage.Body,
		Data:      make(map[string]any),
		Priority:  defaultMessagePriority,
		CreatedAt: time.Unix(message.BaseMessage.CreatedAt, 0),
	}
}

// copyDynamicFieldsToData 复制动态字段到 Data
func (message *DynamicMessage) copyDynamicFieldsToData(standardMessage *Message) {
	for key, value := range message.Fields {
		standardMessage.Data[key] = value
	}
}

// extractAndSetPriority 提取并设置优先级
func (message *DynamicMessage) extractAndSetPriority(standardMessage *Message) {
	priorityValue, exists := message.Fields[fieldNamePriority]
	if !exists {
		return
	}

	priority := convertToPriority(priorityValue)
	standardMessage.Priority = clampPriority(priority)
}

// populateSubjectIfEmpty 如果 Subject 为空则填充
func (message *DynamicMessage) populateSubjectIfEmpty(standardMessage *Message) {
	if standardMessage.Subject != "" {
		return
	}

	standardMessage.Subject = message.generateSubject()
}

// populateBodyIfEmpty 如果 Body 为空则填充
func (message *DynamicMessage) populateBodyIfEmpty(standardMessage *Message) {
	if standardMessage.Body != "" {
		return
	}

	standardMessage.Body = message.generateBody()
}

// extractAndSetRecipients 提取并设置接收者
func (message *DynamicMessage) extractAndSetRecipients(standardMessage *Message) {
	standardMessage.To = message.extractRecipients()
}

// ==================== 内容生成辅助函数 ====================

// generateSubject 生成消息主题
func (message *DynamicMessage) generateSubject() string {
	// 尝试从 title 字段生成
	if title := message.getStringField(fieldNameTitle); title != "" {
		return title
	}

	// 尝试从产品名称生成
	if productName := message.getStringField(fieldNameProductName); productName != "" {
		return fmt.Sprintf("新产品发布：%s", productName)
	}

	// 尝试从维护ID生成
	if maintenanceID := message.getStringField(fieldNameMaintenanceID); maintenanceID != "" {
		return fmt.Sprintf("系统维护通知：%s", maintenanceID)
	}

	return ""
}

// generateBody 生成消息正文
func (message *DynamicMessage) generateBody() string {
	// 尝试从 description 字段生成
	if description := message.getStringField(fieldNameDescription); description != "" {
		return description
	}

	// 尝试从 message 字段生成
	if messageText := message.getStringField(fieldNameMessage); messageText != "" {
		return messageText
	}

	// 尝试从产品ID生成
	if productID := message.getStringField(fieldNameProductID); productID != "" {
		return fmt.Sprintf("产品发布通知 - ID: %s", productID)
	}

	return ""
}

// getStringField 获取字符串字段值
func (message *DynamicMessage) getStringField(fieldName string) string {
	if value, exists := message.Fields[fieldName]; exists {
		if stringValue, ok := value.(string); ok {
			return stringValue
		}
	}
	return ""
}

// ==================== 接收者提取 ====================

// extractRecipients 从动态字段中提取接收者信息
func (message *DynamicMessage) extractRecipients() []Recipient {
	for _, fieldName := range recipientFieldCandidates {
		if recipients := message.extractRecipientsFromField(fieldName); len(recipients) > 0 {
			return recipients
		}
	}

	return []Recipient{}
}

// extractRecipientsFromField 从指定字段提取接收者
func (message *DynamicMessage) extractRecipientsFromField(fieldName string) []Recipient {
	idList, exists := message.Fields[fieldName]
	if !exists {
		return nil
	}

	switch typedList := idList.(type) {
	case []uint64:
		return convertUint64ArrayToRecipients(typedList)
	case []interface{}:
		return convertInterfaceArrayToRecipients(typedList)
	default:
		return nil
	}
}

// convertUint64ArrayToRecipients 将 uint64 数组转换为接收者列表
func convertUint64ArrayToRecipients(idList []uint64) []Recipient {
	recipients := make([]Recipient, len(idList))
	for index, id := range idList {
		recipients[index] = Recipient{
			UserID: strconv.FormatUint(id, 10),
		}
	}
	return recipients
}

// convertInterfaceArrayToRecipients 将 interface 数组转换为接收者列表
func convertInterfaceArrayToRecipients(idList []interface{}) []Recipient {
	recipients := make([]Recipient, 0, len(idList))

	for _, id := range idList {
		if userID, ok := id.(float64); ok {
			recipients = append(recipients, Recipient{
				UserID: strconv.FormatUint(uint64(userID), 10),
			})
		}
	}

	return recipients
}

// ==================== 优先级处理 ====================

// convertToPriority 将各种类型转换为优先级整数
func convertToPriority(value interface{}) int {
	switch typedValue := value.(type) {
	case int:
		return typedValue
	case int64:
		return int(typedValue)
	case float64:
		return int(typedValue)
	case string:
		if priority, err := strconv.Atoi(typedValue); err == nil {
			return priority
		}
	}

	return defaultMessagePriority
}

// clampPriority 限制优先级在合法范围内
func clampPriority(priority int) int {
	if priority < minMessagePriority {
		return minMessagePriority
	}
	if priority > maxMessagePriority {
		return maxMessagePriority
	}
	return priority
}

// ==================== 字段操作 ====================

// GetFieldValue 获取动态字段值
func (message *DynamicMessage) GetFieldValue(fieldName string) (interface{}, bool) {
	value, exists := message.Fields[fieldName]
	return value, exists
}

// SetFieldValue 设置动态字段值
func (message *DynamicMessage) SetFieldValue(fieldName string, value interface{}) {
	if message.Fields == nil {
		message.Fields = make(map[string]interface{})
	}
	message.Fields[fieldName] = value
}

// ==================== JSON 序列化 ====================

// ToJSON 转换为 JSON
func (message *DynamicMessage) ToJSON() ([]byte, error) {
	return json.Marshal(message)
}

// FromJSON 从 JSON 解析
func (message *DynamicMessage) FromJSON(data []byte) error {
	return json.Unmarshal(data, message)
}
