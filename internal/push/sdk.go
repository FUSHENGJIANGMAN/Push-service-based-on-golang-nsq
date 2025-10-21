package push

import "context"

// Service 接口 - 支持异步和同步模式，以及详细结果返回
type Service interface {
	Send(ctx context.Context, del Delivery) (string, error)
	SendAsync(ctx context.Context, del Delivery) (string, error)
	SendSync(ctx context.Context, del Delivery) error
	SendSyncWithResult(ctx context.Context, del Delivery) (*SendResult, error)
}

// 具体实现：增加可选的 processor 字段
type service struct {
	d         *Dispatcher
	processor *MsgProcessor // ← 新增：可选的消息处理器
}

// 构造函数保持不变，返回接口类型
func NewService(d *Dispatcher) Service { return &service{d: d} }

// 可选：提供处理器注入方法（main 里用类型断言调用）
// 这样不需要改 Service 接口签名，也不会影响现有使用方
func (s *service) SetMsgProcessor(p *MsgProcessor) { s.processor = p }

// 如需在 httpapi 或内部代码里读取 processor，可再加一个 Getter（可选）
func (s *service) Processor() *MsgProcessor { return s.processor }

func (s *service) Send(ctx context.Context, del Delivery) (string, error) { return s.d.Send(ctx, del) } // 默认异步
func (s *service) SendAsync(ctx context.Context, del Delivery) (string, error) {
	return s.d.SendAsync(ctx, del)
}
func (s *service) SendSync(ctx context.Context, del Delivery) error { return s.d.SendSync(ctx, del) }
func (s *service) SendSyncWithResult(ctx context.Context, del Delivery) (*SendResult, error) {
	return s.d.SendSyncWithResult(ctx, del)
}
