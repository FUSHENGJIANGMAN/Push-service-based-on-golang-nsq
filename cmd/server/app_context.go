package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"push-gateway/internal/asyncsync"
	"push-gateway/internal/channels/email"
	"push-gateway/internal/channels/sms"
	"push-gateway/internal/channels/voice"
	"push-gateway/internal/channels/web"
	"push-gateway/internal/config"
	"push-gateway/internal/database"
	"push-gateway/internal/idempotency"
	"push-gateway/internal/inbox"
	"push-gateway/internal/modem"
	"push-gateway/internal/push"
	"push-gateway/internal/queue"
	"push-gateway/internal/recorder"
	"push-gateway/internal/status"

	redis "github.com/redis/go-redis/v9"
)

const (
	defaultSyncPriorityThreshold = 3
	defaultStatusStoreTTL        = 24 * time.Hour
)

// RecordStore 推送记录存储接口
// 与 recorder.RedisStore 和 recorder.HybridStore 的实际方法签名匹配
type RecordStore interface {
	SaveRecord(ctx context.Context, record push.Record) error
	QueryRecords(ctx context.Context, namespace string, limit int64) ([]push.Record, error)
	Trim(ctx context.Context) (int, error)
}

// AppContext 应用运行时上下文
// 聚合所有运行期依赖,统一管理生命周期
type AppContext struct {
	Config           config.Config
	RedisClient      *redis.Client
	MySQL            *database.MySQLDB
	AsyncSyncManager *asyncsync.Manager
	InboxStore       inbox.Store
	RecordStore      push.Store  // 给 Dispatcher 使用
	RecordStoreImpl  interface{} // 给 HTTP Handler 使用，存储实际实现
	Idempotency      idempotency.Checker
	StatusStore      status.StatusStore
	ChannelRegistry  push.Registry
	Dispatcher       *push.Dispatcher
	Service          push.Service
	MessageRegistry  *push.MessageRegistry
	MessageProcessor *push.MsgProcessor
	Enqueuer         push.Enqueuer
	VoiceProducer    *queue.VoiceProducer
	SMSProducer      *queue.SMSProducer
	ModemManager     *modem.LazyModemManager
}

// Close 释放应用上下文持有的所有资源
// 按照依赖关系倒序释放,避免资源泄漏
func (context *AppContext) Close() {
	context.closeAsyncSyncManager()
	context.closeMySQLConnection()
	context.closeModemManager()
	context.closeQueueProducers()
}

// closeAsyncSyncManager 关闭异步同步管理器
func (context *AppContext) closeAsyncSyncManager() {
	if context.AsyncSyncManager != nil {
		context.AsyncSyncManager.Stop()
	}
}

// closeMySQLConnection 关闭 MySQL 连接
func (context *AppContext) closeMySQLConnection() {
	if context.MySQL != nil {
		context.MySQL.Close()
	}
}

// closeModemManager 关闭 Modem 管理器
func (context *AppContext) closeModemManager() {
	if context.ModemManager != nil {
		context.ModemManager.Close()
	}
}

// closeQueueProducers 关闭所有队列生产者
func (context *AppContext) closeQueueProducers() {
	if context.VoiceProducer != nil {
		context.VoiceProducer.Close()
	}

	if context.SMSProducer != nil {
		context.SMSProducer.Close()
	}

	if context.Enqueuer != nil {
		context.Enqueuer.Close()
	}
}

// LoggingEnqueuer 带日志功能的队列生产者包装器
// 用于在消息入队时添加监控和日志记录
type LoggingEnqueuer struct {
	wrapped push.Enqueuer
}

// NewLoggingEnqueuer 创建带日志的队列生产者
func NewLoggingEnqueuer(enqueuer push.Enqueuer) *LoggingEnqueuer {
	return &LoggingEnqueuer{wrapped: enqueuer}
}

// Enqueue 普通入队操作
func (enqueuer *LoggingEnqueuer) Enqueue(ctx context.Context, payload []byte) error {
	return enqueuer.wrapped.Enqueue(ctx, payload)
}

// EnqueueWithPriority 带优先级的入队操作
func (enqueuer *LoggingEnqueuer) EnqueueWithPriority(
	ctx context.Context,
	payload []byte,
	priority int,
) error {
	priorityEnqueuer, supportsPriority := enqueuer.wrapped.(push.PriorityEnqueuer)
	if supportsPriority {
		return priorityEnqueuer.EnqueueWithPriority(ctx, payload, priority)
	}

	return enqueuer.Enqueue(ctx, payload)
}

// Close 关闭队列生产者
func (enqueuer *LoggingEnqueuer) Close() {
	enqueuer.wrapped.Close()
}

//
// 应用初始化器
//

// ApplicationInitializer 应用初始化器
// 负责构建完整的应用运行上下文
type ApplicationInitializer struct {
	configuration    config.Config
	redisClient      *redis.Client
	mysqlDatabase    *database.MySQLDB
	asyncSyncManager *asyncsync.Manager
	modemManager     *modem.LazyModemManager
}

// NewApplicationInitializer 创建应用初始化器实例
func NewApplicationInitializer(configuration config.Config) *ApplicationInitializer {
	return &ApplicationInitializer{
		configuration: configuration,
	}
}

// Initialize 初始化应用上下文
// 按照依赖关系依次初始化各个组件
func (initializer *ApplicationInitializer) Initialize() *AppContext {
	initializer.initializeRedis()
	initializer.initializeMySQLAndAsyncSync()
	initializer.initializeModemManager()

	inboxStore := initializer.createInboxStore()
	recordStore := initializer.createRecordStore()
	idempotencyChecker := initializer.createIdempotencyChecker()
	statusStore := initializer.createStatusStore()

	voiceProducer := initializer.createVoiceProducer()
	smsProducer := initializer.createSMSProducer()

	channelRegistry := initializer.createChannelRegistry(
		inboxStore,
		statusStore,
		voiceProducer,
		smsProducer,
	)

	mainEnqueuer := initializer.createMainEnqueuer()

	dispatcher := initializer.createDispatcher(
		channelRegistry,
		recordStore,
		mainEnqueuer,
		statusStore,
		idempotencyChecker,
	)

	service := push.NewService(dispatcher)
	messageRegistry, messageProcessor := initializer.createMessageFormats()
	initializer.attachMessageProcessor(service, messageProcessor)

	return initializer.buildAppContext(
		inboxStore,
		recordStore,
		idempotencyChecker,
		statusStore,
		channelRegistry,
		dispatcher,
		service,
		messageRegistry,
		messageProcessor,
		mainEnqueuer,
		voiceProducer,
		smsProducer,
	)
}

// initializeRedis 初始化 Redis 客户端
func (initializer *ApplicationInitializer) initializeRedis() {
	initializer.redisClient = redis.NewClient(&redis.Options{
		Addr: initializer.configuration.Storage.RedisAddr,
	})

	log.Println("[Initializer] Redis 客户端初始化完成")
}

// initializeMySQLAndAsyncSync 初始化 MySQL 和异步同步管理器
// 仅在配置了 DSN 时才初始化
func (initializer *ApplicationInitializer) initializeMySQLAndAsyncSync() {
	dsn := initializer.configuration.Storage.MySQL.DSN
	if dsn == "" {
		log.Println("[Initializer] 未配置 MySQL,跳过初始化")
		return
	}

	if err := initializer.connectMySQL(); err != nil {
		log.Printf("[Initializer] MySQL 连接失败: %v", err)
		return
	}

	if err := initializer.initializeAsyncSync(); err != nil {
		log.Printf("[Initializer] 异步同步管理器启动失败: %v", err)
		initializer.asyncSyncManager = nil
	}
}

// connectMySQL 连接 MySQL 数据库
func (initializer *ApplicationInitializer) connectMySQL() error {
	database, err := database.NewMySQLDB(initializer.configuration.Storage.MySQL)
	if err != nil {
		return fmt.Errorf("创建连接失败: %w", err)
	}

	if err := database.InitTables(); err != nil {
		return fmt.Errorf("初始化表结构失败: %w", err)
	}

	initializer.mysqlDatabase = database
	log.Println("[Initializer] MySQL 连接成功")
	return nil
}

// initializeAsyncSync 初始化异步同步管理器
func (initializer *ApplicationInitializer) initializeAsyncSync() error {
	manager := asyncsync.NewManager(
		initializer.mysqlDatabase,
		initializer.configuration.Storage.AsyncSync,
	)

	if err := manager.Start(); err != nil {
		return fmt.Errorf("启动管理器失败: %w", err)
	}

	initializer.asyncSyncManager = manager
	log.Println("[Initializer] 异步同步管理器启动成功")
	return nil
}

// initializeModemManager 初始化 Modem 管理器
func (initializer *ApplicationInitializer) initializeModemManager() {
	if !initializer.configuration.Providers.Modem.Enabled {
		return
	}

	modemConfig := modem.ModemConfig{
		PortName:       initializer.configuration.Providers.Modem.PortName,
		BaudRate:       initializer.configuration.Providers.Modem.BaudRate,
		VoiceQueueSize: initializer.configuration.Providers.Modem.VoiceQueueSize,
		SMSQueueSize:   initializer.configuration.Providers.Modem.SMSQueueSize,
	}

	initializer.modemManager = modem.NewLazyModemManager(modemConfig)
	log.Println("[Initializer] Modem 管理器(懒加载)创建完成")
}

// isHybridModeEnabled 判断是否启用混合存储模式
// 混合模式需要同时具备 MySQL 和 AsyncSync
func (initializer *ApplicationInitializer) isHybridModeEnabled() bool {
	return initializer.mysqlDatabase != nil && initializer.asyncSyncManager != nil
}

// createInboxStore 创建收件箱存储
func (initializer *ApplicationInitializer) createInboxStore() inbox.Store {
	// 构建 Redis 存储配置
	redisStoreOptions := inbox.Options{
		Namespace:  initializer.configuration.Storage.Namespace,
		MaxPerUser: initializer.configuration.Storage.InboxMaxPerUser,
		TTL:        initializer.configuration.Storage.InboxTTL,
	}

	// 创建 Redis 存储实例
	redisInboxStore := inbox.NewRedisStore(
		initializer.redisClient,
		redisStoreOptions,
	)

	// 如果未启用混合模式，直接返回 Redis 存储
	if !initializer.isHybridModeEnabled() {
		log.Println("[INITIALIZER] Using Redis-only storage for inbox")
		return redisInboxStore
	}

	// 启用混合模式，返回混合存储
	log.Println("[INITIALIZER] Using hybrid storage (Redis + MySQL) for inbox")
	return inbox.NewHybridStore(
		redisInboxStore,
		initializer.mysqlDatabase,
		initializer.asyncSyncManager,
	)
}

// createRecordStore 创建推送记录存储
func (initializer *ApplicationInitializer) createRecordStore() push.Store {
	redisRecorder := recorder.NewRedisStore(
		initializer.redisClient,
		initializer.configuration.Storage.Namespace,
		initializer.configuration.Storage.MaxKeep,
		initializer.configuration.Storage.TTL,
	)

	if !initializer.isHybridModeEnabled() {
		log.Println("[Initializer] 推送记录使用 Redis 存储")
		return redisRecorder
	}

	log.Println("[Initializer] 推送记录使用混合存储")
	return recorder.NewHybridStore(redisRecorder, initializer.mysqlDatabase, initializer.asyncSyncManager)
}

// createIdempotencyChecker 创建幂等检查器
func (initializer *ApplicationInitializer) createIdempotencyChecker() idempotency.Checker {
	redisChecker := idempotency.NewRedisChecker(
		initializer.redisClient,
		initializer.configuration.Storage.Namespace,
	)

	if !initializer.isHybridModeEnabled() {
		log.Println("[Initializer] 幂等检查使用 Redis")
		return redisChecker
	}

	log.Println("[Initializer] 幂等检查使用混合模式")
	return idempotency.NewHybridChecker(
		redisChecker,
		initializer.mysqlDatabase,
		initializer.asyncSyncManager,
	)
}

// createStatusStore 创建状态存储
func (initializer *ApplicationInitializer) createStatusStore() status.StatusStore {
	redisStatus := status.NewRedisStatusStore(
		initializer.redisClient,
		defaultStatusStoreTTL,
	)

	if !initializer.isHybridModeEnabled() {
		log.Println("[Initializer] 状态存储使用 Redis")
		return redisStatus
	}

	log.Println("[Initializer] 状态存储使用混合模式")
	return status.NewHybridStatusStore(
		redisStatus,
		initializer.mysqlDatabase,
		initializer.asyncSyncManager,
	)
}

// createChannelRegistry 创建消息通道注册表
func (initializer *ApplicationInitializer) createChannelRegistry(
	inboxStore inbox.Store,
	statusStore status.StatusStore,
	voiceProducer *queue.VoiceProducer,
	smsProducer *queue.SMSProducer,
) push.Registry {
	registry := push.NewRegistry()

	initializer.registerBasicChannels(registry, inboxStore)
	initializer.registerSMSAndVoiceChannels(registry, statusStore, voiceProducer, smsProducer)

	log.Println("[Initializer] 消息通道注册完成")
	return registry
}

// registerBasicChannels 注册基础通道(Web Inbox 和 Email)
func (initializer *ApplicationInitializer) registerBasicChannels(
	registry push.Registry,
	inboxStore inbox.Store,
) {
	registry.Register(web.NewInApp(inboxStore))
	registry.Register(email.NewSMTP(initializer.configuration.Providers.Email))
}

// registerSMSAndVoiceChannels 注册短信和语音通道
// 根据配置选择使用 Modem 或第三方服务
func (initializer *ApplicationInitializer) registerSMSAndVoiceChannels(
	registry push.Registry,
	statusStore status.StatusStore,
	voiceProducer *queue.VoiceProducer,
	smsProducer *queue.SMSProducer,
) {
	if initializer.shouldUseModem() {
		initializer.registerModemChannels(registry, statusStore, voiceProducer, smsProducer)
		return
	}

	initializer.registerThirdPartyChannels(registry)
}

// shouldUseModem 判断是否使用本地 Modem
func (initializer *ApplicationInitializer) shouldUseModem() bool {
	return initializer.configuration.Providers.Modem.Enabled &&
		initializer.modemManager != nil
}

// registerModemChannels 注册基于 Modem 的短信和语音通道
func (initializer *ApplicationInitializer) registerModemChannels(
	registry push.Registry,
	statusStore status.StatusStore,
	voiceProducer *queue.VoiceProducer,
	smsProducer *queue.SMSProducer,
) {
	registry.Register(sms.NewModem(
		initializer.configuration.Providers.SMS,
		initializer.modemManager,
		smsProducer,
		statusStore,
	))

	registry.Register(voice.NewModem(
		initializer.configuration.Providers.Voice,
		initializer.modemManager,
		voiceProducer,
		statusStore,
	))

	log.Println("[Initializer] 使用本地 Modem 通道")
}

// registerThirdPartyChannels 注册第三方服务通道
func (initializer *ApplicationInitializer) registerThirdPartyChannels(registry push.Registry) {
	registry.Register(sms.NewTencent(initializer.configuration.Providers.SMS))
	registry.Register(voice.NewTwilio(initializer.configuration.Providers.Voice))
	log.Println("[Initializer] 使用第三方服务通道")
}

// createMainEnqueuer 创建主推送队列生产者
func (initializer *ApplicationInitializer) createMainEnqueuer() push.Enqueuer {
	var baseEnqueuer push.Enqueuer

	if initializer.shouldUsePriorityQueue() {
		baseEnqueuer = initializer.createPriorityProducer()
	} else {
		baseEnqueuer = initializer.createNormalProducer()
	}

	return NewLoggingEnqueuer(baseEnqueuer)
}

// shouldUsePriorityQueue 判断是否使用优先级队列
func (initializer *ApplicationInitializer) shouldUsePriorityQueue() bool {
	return len(initializer.configuration.NSQ.PriorityTopics) > 0 &&
		len(initializer.configuration.NSQ.PriorityThresholds) > 0
}

// createPriorityProducer 创建优先级队列生产者
func (initializer *ApplicationInitializer) createPriorityProducer() push.Enqueuer {
	producer, err := queue.NewPriorityProducer(
		initializer.configuration.NSQ.ProducerAddr,
		initializer.configuration.NSQ.Topic,
		initializer.configuration.NSQ.PriorityTopics,
		initializer.configuration.NSQ.PriorityThresholds,
	)

	if err != nil {
		log.Fatalf("[Initializer] 创建优先级生产者失败: %v", err)
	}

	log.Println("[Initializer] 使用优先级队列生产者")
	return producer
}

// createNormalProducer 创建普通队列生产者
func (initializer *ApplicationInitializer) createNormalProducer() push.Enqueuer {
	producer, err := queue.NewNSQProducer(
		initializer.configuration.NSQ.ProducerAddr,
		initializer.configuration.NSQ.Topic,
	)

	if err != nil {
		log.Fatalf("[Initializer] 创建普通生产者失败: %v", err)
	}

	log.Println("[Initializer] 使用普通队列生产者")
	return producer
}

// createVoiceProducer 创建语音队列生产者
func (initializer *ApplicationInitializer) createVoiceProducer() *queue.VoiceProducer {
	address := initializer.configuration.VoiceQueue.ProducerAddr
	topic := initializer.configuration.VoiceQueue.Topic

	if address == "" || topic == "" {
		return nil
	}

	producer, err := queue.NewVoiceProducer(address, topic)
	if err != nil {
		log.Printf("[Initializer] 创建语音生产者失败: %v", err)
		return nil
	}

	log.Println("[Initializer] 语音队列生产者创建成功")
	return producer
}

// createSMSProducer 创建短信队列生产者
func (initializer *ApplicationInitializer) createSMSProducer() *queue.SMSProducer {
	address := initializer.configuration.SMSQueue.ProducerAddr
	topic := initializer.configuration.SMSQueue.Topic

	if address == "" || topic == "" {
		return nil
	}

	producer, err := queue.NewSMSProducer(address, topic)
	if err != nil {
		log.Printf("[Initializer] 创建短信生产者失败: %v", err)
		return nil
	}

	log.Println("[Initializer] 短信队列生产者创建成功")
	return producer
}

// createDispatcher 创建消息分发器
func (initializer *ApplicationInitializer) createDispatcher(
	channelRegistry push.Registry,
	recordStore push.Store,
	enqueuer push.Enqueuer,
	statusStore status.StatusStore,
	idempotencyChecker idempotency.Checker,
) *push.Dispatcher {
	threshold := initializer.getSyncPriorityThreshold()

	dispatcher := push.NewDispatcher(
		channelRegistry,
		recordStore,
		enqueuer,
		initializer.configuration.Storage.Namespace,
		threshold,
	)

	dispatcher.SetStatusStore(statusStore)
	dispatcher.SetIdempotency(idempotencyChecker, initializer.configuration.App.IdempotencyTTL)

	log.Println("[Initializer] 消息分发器创建完成")
	return dispatcher
}

// getSyncPriorityThreshold 获取同步优先级阈值
// 低于此值的消息同步投递,高于则异步入队
func (initializer *ApplicationInitializer) getSyncPriorityThreshold() int {
	threshold := initializer.configuration.App.SyncPriorityThreshold
	if threshold <= 0 {
		return defaultSyncPriorityThreshold
	}
	return threshold
}

// createMessageFormats 创建动态消息格式
func (initializer *ApplicationInitializer) createMessageFormats() (
	*push.MessageRegistry,
	*push.MsgProcessor,
) {
	builder := push.NewServiceBuilder()

	if err := builder.BuildFromConfig(initializer.configuration); err != nil {
		log.Fatalf("[Initializer] 构建动态消息格式失败: %v", err)
	}

	log.Println("[Initializer] 动态消息格式加载完成")
	return builder.GetMessageRegistry(), builder.GetMsgProcessor()
}

// attachMessageProcessor 为服务附加消息处理器
func (initializer *ApplicationInitializer) attachMessageProcessor(
	service push.Service,
	processor *push.MsgProcessor,
) {
	type ProcessorSetter interface {
		SetMsgProcessor(*push.MsgProcessor)
	}

	if setter, ok := any(service).(ProcessorSetter); ok {
		setter.SetMsgProcessor(processor)
	}
}

// buildAppContext 构建最终的应用上下文
func (initializer *ApplicationInitializer) buildAppContext(
	inboxStore inbox.Store,
	recordStore push.Store,
	idempotencyChecker idempotency.Checker,
	statusStore status.StatusStore,
	channelRegistry push.Registry,
	dispatcher *push.Dispatcher,
	service push.Service,
	messageRegistry *push.MessageRegistry,
	messageProcessor *push.MsgProcessor,
	mainEnqueuer push.Enqueuer,
	voiceProducer *queue.VoiceProducer,
	smsProducer *queue.SMSProducer,
) *AppContext {
	return &AppContext{
		Config:           initializer.configuration,
		RedisClient:      initializer.redisClient,
		MySQL:            initializer.mysqlDatabase,
		AsyncSyncManager: initializer.asyncSyncManager,
		InboxStore:       inboxStore,
		RecordStore:      recordStore,
		RecordStoreImpl:  recordStore,
		Idempotency:      idempotencyChecker,
		StatusStore:      statusStore,
		ChannelRegistry:  channelRegistry,
		Dispatcher:       dispatcher,
		Service:          service,
		MessageRegistry:  messageRegistry,
		MessageProcessor: messageProcessor,
		Enqueuer:         mainEnqueuer,
		VoiceProducer:    voiceProducer,
		SMSProducer:      smsProducer,
		ModemManager:     initializer.modemManager,
	}
}

//
// 外部调用接口
//

// InitAppContext 初始化应用上下文
// 这是主入口函数,保持向后兼容
func InitAppContext(configuration config.Config) *AppContext {
	initializer := NewApplicationInitializer(configuration)
	return initializer.Initialize()
}
