package push

import "context"

type Provider interface {
	Name() string
	Channels() []Channel
	// Send 不负责记录落库，由上层 Dispatcher 控制是否记账（页面通道默认不记）
	Send(ctx context.Context, msg Message, plan RoutePlan) error
}

type Registry interface {
	Register(p Provider)
	GetByChannel(ch Channel) []Provider
}

type registry struct {
	m map[Channel][]Provider
}

func NewRegistry() Registry { return &registry{m: map[Channel][]Provider{}} }

func (r *registry) Register(p Provider) {
	for _, ch := range p.Channels() {
		r.m[ch] = append(r.m[ch], p)
	}
}

func (r *registry) GetByChannel(ch Channel) []Provider {
	return r.m[ch]
}
