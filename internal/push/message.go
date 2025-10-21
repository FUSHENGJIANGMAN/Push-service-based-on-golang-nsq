package push

import "time"

type MessageKind string

const (
	KindAlarm  MessageKind = "alarm"
	KindInApp  MessageKind = "inapp"  // 站内信/页面
	KindTicket MessageKind = "ticket" // 流程单
	KindEmail  MessageKind = "email"  // 邮件
	KindSMS    MessageKind = "sms"    // 短信
	KindVoice  MessageKind = "voice"  // 语音
)

type Channel string

const (
	ChWeb   Channel = "web" // 页面/站内
	ChEmail Channel = "email"
	ChSMS   Channel = "sms"
	ChVoice Channel = "voice"
)

type Recipient struct {
	UserID   string `json:"user_id,omitempty"`
	Email    string `json:"email,omitempty"`
	Phone    string `json:"phone,omitempty"`
	WebInbox string `json:"web_inbox,omitempty"` // 站内信箱ID/用户ID
}

// EmailAttachment 邮件附件结构
type EmailAttachment struct {
	FileName string `json:"file_name"`         // 文件名称
	FileType string `json:"file_type"`         // 文件类型 (MIME type)
	FilePath string `json:"file_path"`         // 文件位置 (本地路径或URL)
	Content  []byte `json:"content,omitempty"` // 文件内容 (可选，用于小文件)
}

type Message struct {
	Kind        MessageKind       `json:"kind"`
	Subject     string            `json:"subject,omitempty"`
	Body        string            `json:"body,omitempty"`
	Data        map[string]any    `json:"data,omitempty"`
	To          []Recipient       `json:"to"`
	Meta        map[string]string `json:"meta,omitempty"`
	Priority    int               `json:"priority"` // 1~5，越大越紧急
	CreatedAt   time.Time         `json:"created_at"`
	Attachments []EmailAttachment `json:"attachments,omitempty"` // 邮件附件
}

type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
	Jitter      bool
}

// RoutePlan 代表发送计划（包括通道、超时等）
type RoutePlan struct {
	Primary  []Channel     `json:"primary"`  // 主要通道
	Fallback []Channel     `json:"fallback"` // 备用通道
	Template string        `json:"template"` // 模板（如果需要）
	Timeout  time.Duration `json:"timeout"`  // 超时时间
	Retry    RetryPolicy   `json:"retry"`    // 重试策略
}

type Delivery struct {
	Msg   Message   `json:"msg"`   // 消息内容
	Plan  RoutePlan `json:"plan"`  // 发送计划（包括通道、超时等）
	Mode  string    `json:"mode"`  // 消息发送模式（同步/异步）
	Topic string    `json:"topic"` // 消息的主题（topic）
}
