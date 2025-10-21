package inbox

import (
	"context"
	"time"
)

// Message 表示站内信的存储结构
type Message struct {
	ID        string                 `json:"id"`         // 由存储生成
	UserID    string                 `json:"user_id"`    // 用户ID
	Kind      string                 `json:"kind"`       // 消息类型，如 "msg", "notification", "alert"
	Subject   string                 `json:"subject"`    // 消息主题
	Body      string                 `json:"body"`       // 消息内容
	Data      map[string]interface{} `json:"data"`       // 原始消息数据
	CreatedAt int64                  `json:"created_at"` // 消息创建时间，Unix 时间戳
	ReadAt    int64                  `json:"read_at"`    // 消息阅读时间，0 表示未读
}

// Store 定义站内信的存储能力
type Store interface {
	// Add 保存一条消息，返回生成的消息ID
	Add(ctx context.Context, m Message) (string, error)
	// List 返回按时间逆序的消息列表（offset/limit 基于逆序），并返回总数（可用于分页）
	// status: "all" | "unread" | "read"
	List(ctx context.Context, userID string, status string, offset, limit int64) (items []Message, total int64, err error)
	// MarkRead 将指定消息设为已读（忽略不存在或不属于该用户的ID），返回成功数量
	MarkRead(ctx context.Context, userID string, ids []string) (int, error)
	// Delete 删除指定消息（忽略不存在或不属于该用户的ID），返回成功数量
	Delete(ctx context.Context, userID string, ids []string) (int, error)
	// TrimUser 对某个用户按上限裁剪旧消息，返回删除数量
	TrimUser(ctx context.Context, userID string) (int, error)
}

// Options 存储配置
type Options struct {
	Namespace  string
	MaxPerUser int64
	TTL        time.Duration // 针对每条消息hash的过期时间；ZSET不自动过期
}
