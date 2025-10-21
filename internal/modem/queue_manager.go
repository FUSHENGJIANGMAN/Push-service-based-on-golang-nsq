package modem

import (
	"context"
	"errors"
	"sync"
	"time"
)

/*
重构要点（无感替换）：
- 消除重复：统一入队与等待逻辑 enqueueAndWait。
- 简化并发模型：单工作协程串行处理两类任务，天然避免并发；删除无效的“忙碌时重排队+Sleep”逻辑。
- 卫语句 + 单一职责：处理流程更扁平；任务处理与状态更新抽成独立函数。
- 健壮关闭：引入 isClosed 标志，避免 Close 后发送到已关闭通道的风险；不强制关闭通道，消除竞态。
- 错误规范：定义哨兵错误，便于上层识别；所有分支均返回明确错误。
- 命名语义化与驼峰：字段、函数、变量遵循 camelCase。
*/

// --------------------------- 错误定义 ---------------------------

var (
	ErrVoiceQueueFull = errors.New("voice queue is full")
	ErrSMSQueueFull   = errors.New("sms queue is full")
	ErrManagerClosed  = errors.New("queue manager is closed")
)

// --------------------------- 数据结构 ---------------------------

// QueueManager 管理语音和短信队列，确保它们不会并发执行（通过单工作协程串行保证）
type QueueManager struct {
	modem *Modem

	voiceQueue chan *VoiceTask
	smsQueue   chan *SMSTask

	isVoiceBusy bool
	isSMSBusy   bool
	isClosed    bool

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// VoiceTask 语音任务
type VoiceTask struct {
	Phone   string
	Content string
	Result  chan error
	Ctx     context.Context
}

// SMSTask 短信任务
type SMSTask struct {
	Phone   string
	Content string
	Result  chan error
	Ctx     context.Context
}

// --------------------------- 构造与关闭 ---------------------------

// NewQueueManager 创建新的队列管理器
func NewQueueManager(modem *Modem, config ModemConfig) *QueueManager {
	ctx, cancel := context.WithCancel(context.Background())

	m := &QueueManager{
		modem:      modem,
		voiceQueue: make(chan *VoiceTask, config.VoiceQueueSize),
		smsQueue:   make(chan *SMSTask, config.SMSQueueSize),
		ctx:        ctx,
		cancel:     cancel,
	}

	m.wg.Add(1)
	go m.processQueues()

	return m
}

// Close 关闭队列管理器（等待工作协程退出）
// 不主动关闭通道，避免外部并发入队时发生 send on closed channel。
func (m *QueueManager) Close() error {
	m.mu.Lock()
	if m.isClosed {
		m.mu.Unlock()
		return nil
	}
	m.isClosed = true
	m.mu.Unlock()

	m.cancel()
	m.wg.Wait()
	return nil
}

// --------------------------- 对外能力（入队+同步等待结果） ---------------------------

// SendVoice 入队语音任务并同步等待结果
func (m *QueueManager) SendVoice(ctx context.Context, phone, content string) error {
	task := &VoiceTask{
		Phone:   phone,
		Content: content,
		Result:  make(chan error, 1),
		Ctx:     ctx,
	}
	send := func() bool {
		select {
		case m.voiceQueue <- task:
			return true
		default:
			return false
		}
	}
	if err := m.enqueueAndWait(ctx, task.Result, send, ErrVoiceQueueFull); err != nil {
		return err
	}
	return nil
}

// SendSMS 入队短信任务并同步等待结果
func (m *QueueManager) SendSMS(ctx context.Context, phone, content string) error {
	task := &SMSTask{
		Phone:   phone,
		Content: content,
		Result:  make(chan error, 1),
		Ctx:     ctx,
	}
	send := func() bool {
		select {
		case m.smsQueue <- task:
			return true
		default:
			return false
		}
	}
	if err := m.enqueueAndWait(ctx, task.Result, send, ErrSMSQueueFull); err != nil {
		return err
	}
	return nil
}

// GetQueueStatus 获取队列状态
func (m *QueueManager) GetQueueStatus() (voiceQueueLen, smsQueueLen int, isVoiceBusy, isSMSBusy bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.voiceQueue), len(m.smsQueue), m.isVoiceBusy, m.isSMSBusy
}

// --------------------------- 内部：统一入队与等待 ---------------------------

func (m *QueueManager) enqueueAndWait(
	ctx context.Context,
	result <-chan error,
	trySend func() bool,
	fullErr error,
) error {
	// 管理器关闭快速失败
	if m.isManagerClosed() {
		return ErrManagerClosed
	}

	// 非阻塞入队，与原逻辑保持一致：队列满立即返回错误
	switch {
	case trySend():
		// 已入队
	case m.isManagerClosed():
		return ErrManagerClosed
	default:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-m.ctx.Done():
			return ErrManagerClosed
		default:
			return fullErr
		}
	}

	// 同步等待执行结果
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-m.ctx.Done():
		return ErrManagerClosed
	}
}

func (m *QueueManager) isManagerClosed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isClosed
}

// --------------------------- 内部：工作协程与任务执行 ---------------------------

func (m *QueueManager) processQueues() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return

		// 注意：单协程串行处理，自然保证“语音”和“短信”不并发。
		case task := <-m.voiceQueue:
			m.executeVoiceTask(task)

		case task := <-m.smsQueue:
			m.executeSMSTask(task)
		}
	}
}

func (m *QueueManager) executeVoiceTask(task *VoiceTask) {
	m.setBusy(true, false)
	err := m.modem.MakeVoiceCall(task.Ctx, task.Phone, task.Content)
	m.setBusy(false, false)
	m.deliverResult(task.Result, err, task.Ctx)
}

func (m *QueueManager) executeSMSTask(task *SMSTask) {
	m.setBusy(false, true)
	err := m.modem.SendSMS(task.Ctx, task.Phone, task.Content)
	m.setBusy(false, false)
	m.deliverResult(task.Result, err, task.Ctx)
}

func (m *QueueManager) setBusy(voiceBusy, smsBusy bool) {
	m.mu.Lock()
	m.isVoiceBusy = voiceBusy
	m.isSMSBusy = smsBusy
	m.mu.Unlock()
}

// deliverResult 将结果安全地回传到任务 Result 中
func (m *QueueManager) deliverResult(result chan<- error, err error, taskCtx context.Context) {
	select {
	case result <- err:
		// delivered
	case <-taskCtx.Done():
		// 任务上下文已取消，放弃投递
	case <-m.ctx.Done():
		// 管理器关闭，放弃投递
	default:
		// 非预期阻塞，避免泄漏：尝试带超时投递
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case result <- err:
		case <-taskCtx.Done():
		case <-m.ctx.Done():
		case <-timer.C:
		}
		timer.Stop()
	}
}
