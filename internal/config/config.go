package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// 默认配置常量
const (
	// NSQ 队列默认配置
	DefaultNSQTopic       = "push-messages"
	DefaultNSQChannel     = "push-workers"
	DefaultNSQMaxInFlight = 128
	DefaultNSQConcurrency = 8
	DefaultNSQMaxAttempts = 5
	DefaultDLQTopicSuffix = ".DLQ"

	// 应用默认配置
	DefaultHTTPAddress           = ":8080"
	DefaultSyncPriorityThreshold = 3
	DefaultRequestTimeout        = 5 * time.Second
	DefaultIdempotencyTTL        = 5 * time.Minute

	// 存储默认配置
	DefaultRedisNamespace  = "push"
	DefaultMaxKeepMessages = 10_000_000
	DefaultMessageTTL      = 365 * 24 * time.Hour
	DefaultInboxMaxPerUser = 5000
	DefaultInboxTTL        = 365 * 24 * time.Hour

	// 字段类型常量
	FieldTypeString      = "string"
	FieldTypeInt         = "int"
	FieldTypeBool        = "bool"
	FieldTypeObject      = "object"
	FieldTypeUInt64      = "uint64"
	FieldTypeInt64       = "int64"
	FieldTypeFloat64     = "float64"
	FieldTypeUInt64Array = "[]uint64"
	FieldTypeStringArray = "[]string"
	FieldTypeObjectArray = "[]object"

	// YAML 字段名常量
	YAMLFieldCommonFields = "CommonFields"
	YAMLFieldName         = "name"
	YAMLFieldType         = "type"
	YAMLFieldRequired     = "required"
	YAMLFieldDescription  = "description"
	YAMLFieldChannels     = "channels"
	YAMLFieldFields       = "fields"

	// 默认描述
	DefaultFieldDescription = "无描述"
)

// Retry 重试配置
// 定义消息发送失败时的重试策略
type Retry struct {
	MaxAttempts int           `yaml:"MaxAttempts"` // 最大重试次数
	Backoff     time.Duration `yaml:"Backoff"`     // 重试间隔时间
}

// ProviderCommon 通道提供者公共配置
// 所有消息通道共享的基础配置
type ProviderCommon struct {
	Timeout time.Duration `yaml:"Timeout"` // 调用超时时间
	Retry   Retry         `yaml:"Retry"`   // 重试策略
}

// EmailProvider 邮件服务配置
// 包含 SMTP 连接、认证和发送参数
type EmailProvider struct {
	From           string           `yaml:"From"`        // 发件人邮箱地址
	SMTPHost       string           `yaml:"SMTPHost"`    // SMTP 服务器主机名
	SMTPPort       int              `yaml:"SMTPPort"`    // SMTP 服务器端口
	Username       string           `yaml:"Username"`    // SMTP 认证用户名
	Password       string           `yaml:"Password"`    // SMTP 认证密码
	UseTLS         bool             `yaml:"UseTLS"`      // 是否启用 STARTTLS
	UseSSL         bool             `yaml:"UseSSL"`      // 是否使用 SSL 直连
	FromName       string           `yaml:"FromName"`    // 发件人显示名称
	CCAddresses    []string         `yaml:"CCAddresses"` // 全局默认抄送地址
	ProviderCommon `yaml:",inline"` // 公共配置
}

// SMSProvider 短信服务配置
type SMSProvider struct {
	Sign           string `yaml:"Sign"` // 短信签名
	ProviderCommon `yaml:",inline"`
}

// VoiceProvider 语音服务配置
type VoiceProvider struct {
	ProviderCommon `yaml:",inline"`
}

// ModemProvider 4G 模块配置
// 用于通过物理 4G 模块发送短信和拨打语音电话
type ModemProvider struct {
	PortName           string        `yaml:"PortName"`           // 串口名称
	BaudRate           int           `yaml:"BaudRate"`           // 波特率
	ReadTimeout        time.Duration `yaml:"ReadTimeout"`        // 串口读取超时
	CommandTimeout     time.Duration `yaml:"CommandTimeout"`     // AT 命令超时
	DialTimeout        time.Duration `yaml:"DialTimeout"`        // 拨号超时
	CallDuration       time.Duration `yaml:"CallDuration"`       // 通话持续时间
	SMSEncoding        string        `yaml:"SMSEncoding"`        // 短信编码格式
	VoiceQueueSize     int           `yaml:"VoiceQueueSize"`     // 语音队列大小
	SMSQueueSize       int           `yaml:"SMSQueueSize"`       // 短信队列大小
	MaxConcurrentVoice int           `yaml:"MaxConcurrentVoice"` // 最大并发语音数
	MaxConcurrentSMS   int           `yaml:"MaxConcurrentSMS"`   // 最大并发短信数
	Enabled            bool          `yaml:"Enabled"`            // 是否启用模块
	ProviderCommon     `yaml:",inline"`
}

// Providers 所有消息通道配置集合
type Providers struct {
	Web   struct{}      `yaml:"Web"`   // 站内信通道
	Email EmailProvider `yaml:"Email"` // 邮件通道
	SMS   SMSProvider   `yaml:"SMS"`   // 短信通道
	Voice VoiceProvider `yaml:"Voice"` // 语音通道
	Modem ModemProvider `yaml:"Modem"` // 4G 模块
}

// NSQ 消息队列配置
// 用于异步消息处理和优先级调度
type NSQ struct {
	Topic                       string            `yaml:"Topic"`                        // 消息主题
	PriorityTopics              map[string]string `yaml:"PriorityTopics,omitempty"`     // 优先级主题映射
	Channel                     string            `yaml:"Channel"`                      // 消费者通道
	NsqdTCPAddrs                []string          `yaml:"NsqdTCPAddrs"`                 // NSQD TCP 地址列表
	LookupdHTTPAddrs            []string          `yaml:"LookupdHTTPAddrs"`             // Lookupd HTTP 地址列表
	MaxInFlight                 int               `yaml:"MaxInFlight"`                  // 最大并发消息数
	Concurrency                 int               `yaml:"Concurrency"`                  // 处理并发数
	ProducerAddr                string            `yaml:"ProducerAddr"`                 // 生产者地址
	ConsumerEnabled             bool              `yaml:"ConsumerEnabled"`              // 是否启用消费
	DLQTopic                    string            `yaml:"DLQTopic"`                     // 死信队列主题
	MaxConsumeAttemptsBeforeDLQ int               `yaml:"MaxConsumeAttemptsBeforeDLQ"`  // 进入死信队列前最大尝试次数
	PriorityThresholds          map[string]int    `yaml:"PriorityThresholds,omitempty"` // 优先级阈值
}

// VoiceQueue 语音消息专用队列配置
type VoiceQueue struct {
	Topic                       string   `yaml:"Topic"`
	Channel                     string   `yaml:"Channel"`
	NsqdTCPAddrs                []string `yaml:"NsqdTCPAddrs"`
	LookupdHTTPAddrs            []string `yaml:"LookupdHTTPAddrs"`
	MaxInFlight                 int      `yaml:"MaxInFlight"`
	Concurrency                 int      `yaml:"Concurrency"`
	ProducerAddr                string   `yaml:"ProducerAddr"`
	ConsumerEnabled             bool     `yaml:"ConsumerEnabled"`
	DLQTopic                    string   `yaml:"DLQTopic"`
	MaxConsumeAttemptsBeforeDLQ int      `yaml:"MaxConsumeAttemptsBeforeDLQ"`
}

// SMSQueue 短信消息专用队列配置
type SMSQueue struct {
	Topic                       string   `yaml:"Topic"`
	Channel                     string   `yaml:"Channel"`
	NsqdTCPAddrs                []string `yaml:"NsqdTCPAddrs"`
	LookupdHTTPAddrs            []string `yaml:"LookupdHTTPAddrs"`
	MaxInFlight                 int      `yaml:"MaxInFlight"`
	Concurrency                 int      `yaml:"Concurrency"`
	ProducerAddr                string   `yaml:"ProducerAddr"`
	ConsumerEnabled             bool     `yaml:"ConsumerEnabled"`
	DLQTopic                    string   `yaml:"DLQTopic"`
	MaxConsumeAttemptsBeforeDLQ int      `yaml:"MaxConsumeAttemptsBeforeDLQ"`
}

// App 应用全局配置
type App struct {
	Addr                  string        `yaml:"Addr"`                  // HTTP 监听地址
	SyncPriorityThreshold int           `yaml:"SyncPriorityThreshold"` // 同步模式优先级阈值
	RequestTimeout        time.Duration `yaml:"RequestTimeout"`        // HTTP 请求超时
	IdempotencyTTL        time.Duration `yaml:"IdempotencyTTL"`        // 幂等键过期时间
}

// Storage 存储配置
// 包含 Redis 缓存和 MySQL 持久化配置
type Storage struct {
	RedisAddr       string        `yaml:"RedisAddr"`       // Redis 地址
	Namespace       string        `yaml:"Namespace"`       // Redis 键前缀
	MaxKeep         int64         `yaml:"MaxKeep"`         // 最大保留消息数
	TTL             time.Duration `yaml:"TTL"`             // 消息过期时间
	InboxMaxPerUser int64         `yaml:"InboxMaxPerUser"` // 单用户收件箱最大消息数
	InboxTTL        time.Duration `yaml:"InboxTTL"`        // 收件箱消息过期时间
	MySQL           MySQLConfig   `yaml:"MySQL"`           // MySQL 配置
	AsyncSync       AsyncConfig   `yaml:"AsyncSync"`       // 异步同步配置
}

// MySQLConfig MySQL 数据库连接配置
type MySQLConfig struct {
	DSN             string        `yaml:"DSN"`             // 数据源配置
	MaxOpenConns    int           `yaml:"MaxOpenConns"`    // 最大打开连接数
	MaxIdleConns    int           `yaml:"MaxIdleConns"`    // 最大空闲连接数
	ConnMaxLifetime time.Duration `yaml:"ConnMaxLifetime"` // 连接最大生命周期
}

// AsyncConfig 异步落库配置
// 控制批量写入和后台同步行为
type AsyncConfig struct {
	Enabled       bool          `yaml:"Enabled"`       // 是否启用异步写入
	BatchSize     int           `yaml:"BatchSize"`     // 批量写入大小
	FlushInterval time.Duration `yaml:"FlushInterval"` // 刷新间隔
	RetryAttempts int           `yaml:"RetryAttempts"` // 重试次数
	WorkerCount   int           `yaml:"WorkerCount"`   // 工作协程数
}

// Field 消息字段元信息
// 用于动态消息验证和文档生成
type Field struct {
	Name        string `yaml:"name"`        // 字段名称
	Type        string `yaml:"type"`        // 字段类型
	Required    bool   `yaml:"required"`    // 是否必填
	Description string `yaml:"description"` // 字段描述
}

// MessageFormat 消息格式定义
// 支持动态消息格式配置和验证
type MessageFormat struct {
	Type     string   `yaml:"type"`     // 消息类型标识
	Channels []string `yaml:"channels"` // 支持的通道列表
	Fields   []Field  `yaml:"fields"`   // 字段列表
}

// Config 应用完整配置
type Config struct {
	App            App                  `yaml:"App"`
	Storage        Storage              `yaml:"Storage"`
	NSQ            NSQ                  `yaml:"NSQ"`
	VoiceQueue     VoiceQueue           `yaml:"VoiceQueue"`
	SMSQueue       SMSQueue             `yaml:"SMSQueue"`
	Providers      Providers            `yaml:"Providers"`
	MessageFormats MessageFormatsConfig `yaml:"MessageFormats"`
	Notification   MessageFormat        `yaml:"Notification"`
}

// MessageFormatsConfig 消息格式配置集合
// 支持公共字段继承和动态格式定义
type MessageFormatsConfig struct {
	CommonFields []Field                  `yaml:"CommonFields,omitempty"` // 公共字段
	Formats      map[string]MessageFormat `yaml:"-"`                      // 格式映射
}

// UnmarshalYAML 自定义 YAML 解析
// 将配置文件中的动态键解析为消息格式
func (config *MessageFormatsConfig) UnmarshalYAML(node *yaml.Node) error {
	rawData, err := parseYAMLNode(node)
	if err != nil {
		return err
	}

	config.Formats = make(map[string]MessageFormat)

	for key, value := range rawData {
		if key == YAMLFieldCommonFields {
			config.CommonFields = parseCommonFields(value)
			continue
		}

		config.Formats[key] = parseMessageFormat(value)
	}

	return nil
}

// parseYAMLNode 解析 YAML 节点为 map
func parseYAMLNode(node *yaml.Node) (map[string]interface{}, error) {
	var rawData map[string]interface{}
	if err := node.Decode(&rawData); err != nil {
		return nil, fmt.Errorf("failed to decode yaml node: %w", err)
	}
	return rawData, nil
}

// parseCommonFields 解析公共字段列表
func parseCommonFields(value interface{}) []Field {
	fieldsList, ok := value.([]interface{})
	if !ok {
		return nil
	}

	commonFields := make([]Field, 0, len(fieldsList))

	for _, fieldData := range fieldsList {
		if field := parseField(fieldData); field != nil {
			commonFields = append(commonFields, *field)
		}
	}

	return commonFields
}

// parseField 解析单个字段配置
func parseField(fieldData interface{}) *Field {
	fieldMap, ok := fieldData.(map[string]interface{})
	if !ok {
		return nil
	}

	field := &Field{}

	if name, ok := fieldMap[YAMLFieldName].(string); ok {
		field.Name = name
	}

	if fieldType, ok := fieldMap[YAMLFieldType].(string); ok {
		field.Type = fieldType
	}

	if required, ok := fieldMap[YAMLFieldRequired].(bool); ok {
		field.Required = required
	}

	if description, ok := fieldMap[YAMLFieldDescription].(string); ok {
		field.Description = description
	}

	return field
}

// parseMessageFormat 解析消息格式配置
func parseMessageFormat(value interface{}) MessageFormat {
	formatData, ok := value.(map[string]interface{})
	if !ok {
		return MessageFormat{}
	}

	format := MessageFormat{}

	if messageType, ok := formatData[YAMLFieldType].(string); ok {
		format.Type = messageType
	}

	format.Channels = parseChannels(formatData[YAMLFieldChannels])
	format.Fields = parseFields(formatData[YAMLFieldFields])

	return format
}

// parseChannels 解析通道列表
func parseChannels(value interface{}) []string {
	channelsList, ok := value.([]interface{})
	if !ok {
		return nil
	}

	channels := make([]string, 0, len(channelsList))

	for _, channel := range channelsList {
		if channelString, ok := channel.(string); ok {
			channels = append(channels, channelString)
		}
	}

	return channels
}

// parseFields 解析字段列表
func parseFields(value interface{}) []Field {
	fieldsList, ok := value.([]interface{})
	if !ok {
		return nil
	}

	fields := make([]Field, 0, len(fieldsList))

	for _, fieldData := range fieldsList {
		if field := parseField(fieldData); field != nil {
			fields = append(fields, *field)
		}
	}

	return fields
}

// MustLoad 加载 YAML 配置文件
// 加载失败时直接 panic(用于应用启动阶段)
func MustLoad(configPath string) Config {
	fileContent, err := os.ReadFile(configPath)
	if err != nil {
		panic(fmt.Sprintf("failed to read config file: %v", err))
	}

	var config Config
	if err := yaml.Unmarshal(fileContent, &config); err != nil {
		panic(fmt.Sprintf("failed to unmarshal config: %v", err))
	}

	if err := config.validate(); err != nil {
		panic(fmt.Sprintf("config validation failed: %v", err))
	}

	return config
}

// validate 校验配置并设置默认值
func (config *Config) validate() error {
	if err := config.validateNSQConfig(); err != nil {
		return err
	}

	if err := config.validateAppConfig(); err != nil {
		return err
	}

	if err := config.validateStorageConfig(); err != nil {
		return err
	}

	if err := config.validateMessageFormats(); err != nil {
		return err
	}

	return nil
}

// validateNSQConfig 校验 NSQ 配置并设置默认值
func (config *Config) validateNSQConfig() error {
	if config.NSQ.Topic == "" {
		config.NSQ.Topic = DefaultNSQTopic
	}

	if config.NSQ.Channel == "" {
		config.NSQ.Channel = DefaultNSQChannel
	}

	if config.NSQ.MaxInFlight <= 0 {
		config.NSQ.MaxInFlight = DefaultNSQMaxInFlight
	}

	if config.NSQ.Concurrency <= 0 {
		config.NSQ.Concurrency = DefaultNSQConcurrency
	}

	if config.NSQ.MaxConsumeAttemptsBeforeDLQ <= 0 {
		config.NSQ.MaxConsumeAttemptsBeforeDLQ = DefaultNSQMaxAttempts
	}

	if config.NSQ.DLQTopic == "" {
		config.NSQ.DLQTopic = config.NSQ.Topic + DefaultDLQTopicSuffix
	}

	return nil
}

// validateAppConfig 校验应用配置并设置默认值
func (config *Config) validateAppConfig() error {
	if config.App.Addr == "" {
		config.App.Addr = DefaultHTTPAddress
	}

	if config.App.SyncPriorityThreshold <= 0 {
		config.App.SyncPriorityThreshold = DefaultSyncPriorityThreshold
	}

	if config.App.RequestTimeout <= 0 {
		config.App.RequestTimeout = DefaultRequestTimeout
	}

	if config.App.IdempotencyTTL <= 0 {
		config.App.IdempotencyTTL = DefaultIdempotencyTTL
	}

	return nil
}

// validateStorageConfig 校验存储配置并设置默认值
func (config *Config) validateStorageConfig() error {
	if config.Storage.Namespace == "" {
		config.Storage.Namespace = DefaultRedisNamespace
	}

	if config.Storage.MaxKeep <= 0 {
		config.Storage.MaxKeep = DefaultMaxKeepMessages
	}

	if config.Storage.TTL <= 0 {
		config.Storage.TTL = DefaultMessageTTL
	}

	if config.Storage.InboxMaxPerUser <= 0 {
		config.Storage.InboxMaxPerUser = DefaultInboxMaxPerUser
	}

	if config.Storage.InboxTTL <= 0 {
		config.Storage.InboxTTL = DefaultInboxTTL
	}

	return nil
}

// validateMessageFormats 校验消息格式配置
func (config *Config) validateMessageFormats() error {
	formats := config.MessageFormats.getAllFormatsMap()

	for formatName, format := range formats {
		if err := format.Validate(); err != nil {
			return fmt.Errorf("message format '%s' validation failed: %w", formatName, err)
		}

		format.SetDefaults()
	}

	return nil
}

// Validate 校验消息格式字段类型
func (format *MessageFormat) Validate() error {
	validTypes := getValidFieldTypes()

	for _, field := range format.Fields {
		if !validTypes[field.Type] {
			return fmt.Errorf("invalid type '%s' for field '%s'", field.Type, field.Name)
		}
	}

	return nil
}

// getValidFieldTypes 返回支持的字段类型集合
func getValidFieldTypes() map[string]bool {
	return map[string]bool{
		FieldTypeString:      true,
		FieldTypeInt:         true,
		FieldTypeBool:        true,
		FieldTypeObject:      true,
		FieldTypeUInt64Array: true,
		FieldTypeStringArray: true,
		FieldTypeObjectArray: true,
		FieldTypeFloat64:     true,
		FieldTypeUInt64:      true,
		FieldTypeInt64:       true,
	}
}

// SetDefaults 为消息格式字段设置默认值
func (format *MessageFormat) SetDefaults() {
	for index := range format.Fields {
		if format.Fields[index].Description == "" {
			format.Fields[index].Description = DefaultFieldDescription
		}
	}
}

// getAllFormatsMap 获取所有消息格式映射
// 懒初始化模式,确保 Formats 不为 nil
func (config *MessageFormatsConfig) getAllFormatsMap() map[string]MessageFormat {
	if config.Formats == nil {
		config.Formats = make(map[string]MessageFormat)
	}
	return config.Formats
}

// GetAllFormats 获取合并公共字段后的完整格式映射
// 返回只读副本,避免外部修改原始配置
func (config *MessageFormatsConfig) GetAllFormats() map[string]MessageFormat {
	result := make(map[string]MessageFormat)
	formats := config.getAllFormatsMap()

	for formatName, format := range formats {
		mergedFields := make([]Field, 0, len(config.CommonFields)+len(format.Fields))
		mergedFields = append(mergedFields, config.CommonFields...)
		mergedFields = append(mergedFields, format.Fields...)

		result[formatName] = MessageFormat{
			Type:     format.Type,
			Channels: format.Channels,
			Fields:   mergedFields,
		}
	}

	return result
}
