package push

import "context"

type Record struct {
	Namespace   string
	Key         string
	MessageID   string // 新增，唯一消息ID，便于查状态
	Kind        MessageKind
	Subject     string                 // 消息主题
	Body        string                 // 消息内容
	MessageData map[string]interface{} // 原始消息数据
	Channels    []Channel
	Recipients  []Recipient
	Status      string // success/partial/failed/pending
	Code        string
	Content     string
	ErrorDetail string // 详细错误信息
	Priority    int    // 消息优先级
	CreatedAt   int64
	SentAt      int64 // 实际发送时间
}

type Store interface {
	SaveRecord(ctx context.Context, rec Record) error
	Trim(ctx context.Context) (int, error) // 触发清理（超过 MaxKeep/TTL）
}
