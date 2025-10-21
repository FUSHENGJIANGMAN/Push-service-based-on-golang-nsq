package push

import (
	"fmt"
	"reflect"

	"push-gateway/internal/config"
)

// ServiceBuilder 服务构建器，根据配置动态构建服务
type ServiceBuilder struct {
	registry     *MessageRegistry
	msgProcessor *MsgProcessor
}

// NewServiceBuilder 创建新的服务构建器
func NewServiceBuilder() *ServiceBuilder {
	return &ServiceBuilder{
		registry:     NewMessageRegistry(),
		msgProcessor: &MsgProcessor{},
	}
}

// BuildFromConfig 根据配置构建服务组件
func (sb *ServiceBuilder) BuildFromConfig(cfg config.Config) error {
	// 获取包含通用字段的完整消息格式
	allFormats := cfg.MessageFormats.GetAllFormats()

	// 注册所有消息格式到动态注册表
	for topic, format := range allFormats {
		pushFormat := sb.convertConfigFormat(format)
		if err := sb.registry.RegisterFormat(topic, pushFormat); err != nil {
			return fmt.Errorf("failed to register format for topic %s: %w", topic, err)
		}

		// 根据配置类型选择处理器
		var handler MessageHandler
		if sb.shouldUseDynamicHandler(format) {
			// 使用动态处理器
			handler = NewDynamicMessageHandler(sb.registry, topic)
		} else {
			// 使用传统的静态处理器（向后兼容）
			handler = sb.createLegacyHandler(format.Type)
			if handler == nil {
				handler = NewDynamicMessageHandler(sb.registry, topic)
			}
		}

		sb.msgProcessor.RegisterHandler(topic, handler)
	}

	return nil
}

// convertConfigFormat 将配置格式转换为推送格式
func (sb *ServiceBuilder) convertConfigFormat(cfg config.MessageFormat) MessageFormat {
	fields := make([]MessageField, len(cfg.Fields))
	for i, field := range cfg.Fields {
		fields[i] = MessageField{
			Name:        field.Name,
			Type:        field.Type,
			Required:    field.Required,
			Description: field.Description,
		}
	}

	format := MessageFormat{
		Type:     cfg.Type,
		Channels: cfg.Channels,
		Fields:   fields,
	}
	format.SetDefaults()

	return format
}

// shouldUseDynamicHandler 判断是否应该使用动态处理器
func (sb *ServiceBuilder) shouldUseDynamicHandler(format config.MessageFormat) bool {
	// 如果配置中有自定义字段定义，使用动态处理器
	if len(format.Fields) > 0 {
		return true
	}

	// 如果是未知的消息类型，使用动态处理器
	knownTypes := map[string]bool{
		"push.Msg":          true,
		"push.Notification": true,
		"push.Alert":        true,
		"push.Delivery":     true,
	}

	return !knownTypes[format.Type]
}

// createLegacyHandler 创建传统的静态处理器（向后兼容）
func (sb *ServiceBuilder) createLegacyHandler(msgType string) MessageHandler {
	switch msgType {
	case "push.Msg":
		return NewGenericHandler(reflect.TypeOf(Msg{}))
	case "push.Notification":
		return NewGenericHandler(reflect.TypeOf(Notification{}))
	case "push.Alert":
		return NewGenericHandler(reflect.TypeOf(Alert{}))
	default:
		return nil
	}
}

// GetMessageRegistry 获取消息注册表
func (sb *ServiceBuilder) GetMessageRegistry() *MessageRegistry {
	return sb.registry
}

// GetMsgProcessor 获取消息处理器
func (sb *ServiceBuilder) GetMsgProcessor() *MsgProcessor {
	return sb.msgProcessor
}

// ValidateMessageData 验证消息数据
func (sb *ServiceBuilder) ValidateMessageData(topic string, data map[string]interface{}) error {
	return sb.registry.ValidateMessage(topic, data)
}

// ParseDynamicMessage 解析动态消息
func (sb *ServiceBuilder) ParseDynamicMessage(topic string, data map[string]interface{}) (*DynamicMessage, error) {
	return sb.registry.ParseMessage(topic, data)
}
