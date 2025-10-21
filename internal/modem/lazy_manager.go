package modem

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

/*
重构说明（可无感替换）：
- 保留对外 API 与行为：NewLazyModemManager / SendVoice / SendSMS / GetQueueStatus / IsAvailable / GetLastError / Close。
- 内部命名语义化、驼峰命名；提取公共逻辑（获取队列、可用性检查、初始化流程拆分）；使用卫语句减少嵌套。
- 增加自定义错误类型，统一错误前缀，避免忽略 error。
- 常量化超时与重试参数，便于后续配置与测试。
*/

// --------------------------- 常量与错误定义 ---------------------------

const (
	initTimeout          = 15 * time.Second
	defaultRetryDelay    = 2 * time.Second
	defaultMaxRetryCount = 3
	logPrefix            = "[MODEM]"
)

var (
	// 自定义错误（哨兵错误），便于上层识别与分类处理
	ErrModemUnavailable          = errors.New("modem unavailable")
	ErrInitializationInProgress  = errors.New("modem initialization in progress")
	ErrInitializationMaxExceeded = errors.New("modem initialization max retries exceeded")
	ErrQueueManagerUninitialized = errors.New("queue manager is uninitialized")
)

// --------------------------- 结构体定义 ---------------------------

// LazyModemManager 懒加载的4G模块管理器
type LazyModemManager struct {
	config       ModemConfig
	modem        *Modem
	queueManager *QueueManager

	mu             sync.RWMutex
	initialized    bool
	initError      error
	lastTryTime    time.Time
	retryDelay     time.Duration
	initInProgress bool
	retryCount     int
	maxRetries     int
}

// --------------------------- 构造与生命周期 ---------------------------

// NewLazyModemManager 创建懒加载的4G模块管理器（启动即异步初始化）
func NewLazyModemManager(config ModemConfig) *LazyModemManager {
	m := &LazyModemManager{
		config:     config,
		retryDelay: defaultRetryDelay,
		maxRetries: defaultMaxRetryCount,
	}
	go m.backgroundInitialize()
	return m
}

// Close 关闭4G模块与队列
func (m *LazyModemManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error

	if m.queueManager != nil {
		if err := m.queueManager.Close(); err != nil {
			firstErr = err
		}
		m.queueManager = nil
	}

	if m.modem != nil {
		m.modem.Close() // Close 无返回值
		m.modem = nil
	}

	m.initialized = false
	m.initError = nil
	m.initInProgress = false
	m.retryCount = 0

	log.Printf("%s 4G模块已关闭", logPrefix)
	return firstErr
}

// --------------------------- 公共方法（发送/状态） ---------------------------

// SendVoice 发送语音消息
func (m *LazyModemManager) SendVoice(ctx context.Context, phone, content string) error {
	if err := m.checkAvailability(); err != nil {
		return fmt.Errorf("%w: %v", ErrModemUnavailable, err)
	}

	qm, err := m.getQueueManager()
	if err != nil {
		return err
	}
	return qm.SendVoice(ctx, phone, content)
}

// SendSMS 发送短信
func (m *LazyModemManager) SendSMS(ctx context.Context, phone, content string) error {
	log.Printf("%s 尝试发送短信到 %s", logPrefix, phone)

	if err := m.checkAvailability(); err != nil {
		log.Printf("%s 4G模块可用性检查失败: %v", logPrefix, err)
		return fmt.Errorf("%w: %v", ErrModemUnavailable, err)
	}

	qm, err := m.getQueueManager()
	if err != nil {
		log.Printf("%s %v", logPrefix, err)
		return err
	}

	if err := qm.SendSMS(ctx, phone, content); err != nil {
		log.Printf("%s 队列管理器发送短信失败: %v", logPrefix, err)
		return err
	}

	log.Printf("%s 队列管理器发送短信成功", logPrefix)
	return nil
}

// GetQueueStatus 获取队列状态
func (m *LazyModemManager) GetQueueStatus() (voiceQueueLen, smsQueueLen int, isVoiceBusy, isSMSBusy bool, available bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.initialized || m.initError != nil || m.queueManager == nil {
		return 0, 0, false, false, false
	}

	voiceQueueLen, smsQueueLen, isVoiceBusy, isSMSBusy = m.queueManager.GetQueueStatus()
	return voiceQueueLen, smsQueueLen, isVoiceBusy, isSMSBusy, true
}

// IsAvailable 检查4G模块是否可用
func (m *LazyModemManager) IsAvailable() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.initialized && m.initError == nil && m.queueManager != nil
}

// GetLastError 获取最后的错误信息
func (m *LazyModemManager) GetLastError() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.initError
}

// --------------------------- 内部：可用性与队列 ---------------------------

func (m *LazyModemManager) checkAvailability() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.initialized && m.initError == nil && m.queueManager != nil {
		return nil
	}

	if m.retryCount >= m.maxRetries {
		return fmt.Errorf("%w: 已重试%d次: %v", ErrInitializationMaxExceeded, m.retryCount, m.initError)
	}

	if m.initInProgress {
		return fmt.Errorf("%w: 第%d次尝试", ErrInitializationInProgress, m.retryCount+1)
	}

	if m.initError != nil {
		return fmt.Errorf("暂时不可用(重试进度 %d/%d): %w", m.retryCount, m.maxRetries, m.initError)
	}

	return errors.New("4G模块尚未初始化")
}

func (m *LazyModemManager) getQueueManager() (*QueueManager, error) {
	m.mu.RLock()
	qm := m.queueManager
	m.mu.RUnlock()

	if qm == nil {
		return nil, ErrQueueManagerUninitialized
	}
	return qm, nil
}

// --------------------------- 内部：初始化流程 ---------------------------

func (m *LazyModemManager) backgroundInitialize() {
	log.Printf("%s 开始后台初始化4G模块: %s@%d", logPrefix, m.config.PortName, m.config.BaudRate)

	m.markInitStart()

	modem, err := m.createModemWithTimeout(initTimeout)
	if err != nil {
		m.handleInitFailure(err)
		return
	}

	if err := m.finalizeInitSuccess(modem); err != nil {
		m.handleInitFailure(err)
		return
	}

	m.postInitProbing(modem)
}

func (m *LazyModemManager) markInitStart() {
	m.mu.Lock()
	m.initInProgress = true
	m.lastTryTime = time.Now()
	m.mu.Unlock()
}

func (m *LazyModemManager) createModemWithTimeout(timeout time.Duration) (*Modem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	type initResult struct {
		modem *Modem
		err   error
	}

	resultChan := make(chan initResult, 1)
	go func() {
		modem, err := NewModem(m.config)
		resultChan <- initResult{modem: modem, err: err}
	}()

	select {
	case result := <-resultChan:
		if result.err != nil {
			return nil, fmt.Errorf("创建Modem失败: %w", result.err)
		}
		return result.modem, nil
	case <-ctx.Done():
		log.Printf("%s 4G模块后台初始化超时(%s)", logPrefix, timeout)
		return nil, fmt.Errorf("4G模块后台初始化超时(%s)", timeout)
	}
}

func (m *LazyModemManager) handleInitFailure(err error) {
	m.mu.Lock()
	m.initInProgress = false
	m.initError = fmt.Errorf("4G模块后台初始化失败: %w", err)
	m.retryCount++
	currentRetry := m.retryCount
	maxRetry := m.maxRetries
	retryDelay := m.retryDelay
	shouldRetry := currentRetry < maxRetry
	m.mu.Unlock()

	log.Printf("%s 4G模块后台初始化失败(第%d次): %v", logPrefix, currentRetry, err)

	if !shouldRetry {
		log.Printf("%s 已达到最大重试次数(%d)，停止重试。请检查4G模块连接和配置", logPrefix, maxRetry)
		return
	}

	log.Printf("%s 将在%v后进行第%d次重试", logPrefix, retryDelay, currentRetry+1)
	time.AfterFunc(retryDelay, func() {
		m.mu.RLock()
		canRetry := !m.initialized && m.initError != nil && m.retryCount < m.maxRetries
		m.mu.RUnlock()
		if canRetry {
			log.Printf("%s 开始第%d次重试4G模块初始化", logPrefix, currentRetry+1)
			m.backgroundInitialize()
		}
	})
}

func (m *LazyModemManager) finalizeInitSuccess(modem *Modem) error {
	queueManager := NewQueueManager(modem, m.config)

	m.mu.Lock()
	m.modem = modem
	m.queueManager = queueManager
	m.initialized = true
	m.initError = nil
	m.initInProgress = false
	m.retryCount = 0
	m.mu.Unlock()

	return nil
}

// postInitProbing 执行非致命的初始化探测（失败仅记录日志）
func (m *LazyModemManager) postInitProbing(modem *Modem) {
	// 基础 AT 初始化（失败仅记录）
	if err := modem.Initialize(); err != nil {
		log.Printf("%s 基础初始化指令失败: %v", logPrefix, err)
	}

	// 运营商检测（带重试，失败仅记录）
	if op, err := detectOperatorWithRetry(modem, 5, time.Second); err != nil {
		log.Printf("%s 运营商检测失败: %v", logPrefix, err)
	} else {
		log.Printf("%s 运营商确认: %s", logPrefix, op.String())
	}

	log.Printf("%s 4G模块后台初始化成功，运营商: %s", logPrefix, modem.GetOperator().String())
}
