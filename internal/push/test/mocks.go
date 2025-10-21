package test

import (
	"context"
	"encoding/json"
	"sync"

	"push-gateway/internal/push"
)

// ---- Provider Mock ----
type MockProvider struct {
	NameVal string
	Chs     []push.Channel
	Err     error

	mu        sync.Mutex
	SendCalls int
	LastMsg   push.Message
	LastPlan  push.RoutePlan
}

func (m *MockProvider) Name() string             { return m.NameVal }
func (m *MockProvider) Channels() []push.Channel { return m.Chs }
func (m *MockProvider) Send(ctx context.Context, msg push.Message, plan push.RoutePlan) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SendCalls++
	m.LastMsg = msg
	m.LastPlan = plan
	return m.Err
}

// ---- Registry Stub（最小即可：按通道返回固定 Provider 列表） ----
type StubRegistry struct {
	ByCh map[push.Channel][]push.Provider
}

func (s *StubRegistry) Register(p push.Provider)                     {}
func (s *StubRegistry) GetByChannel(ch push.Channel) []push.Provider { return s.ByCh[ch] }

// ---- Store Mock ----
type MockStore struct {
	mu      sync.Mutex
	Records []push.Record
	Trimmed int
	Err     error
}

func (s *MockStore) SaveRecord(ctx context.Context, rec push.Record) error {
	if s.Err != nil {
		return s.Err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Records = append(s.Records, rec)
	return nil
}
func (s *MockStore) Trim(ctx context.Context) (int, error) {
	if s.Err != nil {
		return 0, s.Err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Trimmed++
	return 0, nil
}

// ---- Enqueuer Mock ----
type MockEnqueuer struct {
	mu       sync.Mutex
	Payloads [][]byte
	Err      error
}

func (q *MockEnqueuer) Enqueue(ctx context.Context, payload []byte) error {
	if q.Err != nil {
		return q.Err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.Payloads = append(q.Payloads, payload)
	return nil
}
func (q *MockEnqueuer) Close() {}

func DecodeDelivery(b []byte) (push.Delivery, error) {
	var d push.Delivery
	err := json.Unmarshal(b, &d)
	return d, err
}

// ---- Helper: 最小可用消息 ----
func NewMsg(priority int) push.Message {
	return push.Message{
		Kind:     push.KindAlarm,
		Subject:  "subj",
		Body:     "body",
		To:       []push.Recipient{{Email: "u@x.com", Phone: "13800000000", UserID: "u1"}},
		Priority: priority,
	}
}
