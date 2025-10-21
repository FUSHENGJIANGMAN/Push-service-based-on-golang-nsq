package queue

import (
	"context"
	"fmt"

	"github.com/nsqio/go-nsq"
)

type NSQProducer struct {
	p     *nsq.Producer
	topic string
}

// 创建一个新的 NSQ 生产者
func NewNSQProducer(addr, topic string) (*NSQProducer, error) {
	cfg := nsq.NewConfig()
	p, err := nsq.NewProducer(addr, cfg)
	if err != nil {
		return nil, err
	}
	return &NSQProducer{p: p, topic: topic}, nil
}

func (n *NSQProducer) Enqueue(ctx context.Context, payload []byte) error {
	if len(payload) == 0 {
		return fmt.Errorf("empty payload")
	}
	// nsqio/go-nsq 的 Publish 不接收 context，但这里仍保持 ctx 以满足接口规范
	return n.p.Publish(n.topic, payload)
}

// 为了兼容上层，给一个 Close
func (n *NSQProducer) Close() {
	if n.p != nil {
		n.p.Stop()
	}
}
