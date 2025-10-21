package asyncsync

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"push-gateway/internal/config"
	"push-gateway/internal/database"
)

//
// 常量定义
//

const (
	recordStatusPending = "pending"
	recordStatusFailed  = "failed"

	defaultQueueBufferMultiplier = 2
	workerIdleInterval           = 100 * time.Millisecond
	retryWorkerInterval          = time.Minute
	retryRecordBatchSize         = 100

	tableInboxMessages      = "inbox_messages"
	tablePushRecords        = "push_records"
	tableMessageStatus      = "message_status"
	tableIdempotencyRecords = "idempotency_records"
)

//
// 数据模型
//

// SyncRecord 异步待同步的单条记录
// 用于在内存队列和持久化存储之间传递
type SyncRecord struct {
	ID          int64           `json:"id"`
	TableName   string          `json:"table_name"`
	Operation   string          `json:"operation"`
	RecordID    string          `json:"record_id"`
	Data        json.RawMessage `json:"data"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	CreatedAt   int64           `json:"created_at"`
	NextRetryAt int64           `json:"next_retry_at"`
	Status      string          `json:"status"`
	Error       string          `json:"error"`
}

//
// 异步同步管理器
//

// Manager 异步同步管理器
// 负责高频写操作的异步批处理和失败重试
type Manager struct {
	database      *database.MySQLDB
	configuration config.AsyncConfig
	queues        map[string]chan SyncRecord
	workers       sync.WaitGroup
	context       context.Context
	cancelFunc    context.CancelFunc
	isRunning     bool
	mutex         sync.RWMutex
}

// NewManager 创建异步同步管理器实例
func NewManager(database *database.MySQLDB, configuration config.AsyncConfig) *Manager {
	context, cancelFunc := context.WithCancel(context.Background())

	return &Manager{
		database:      database,
		configuration: configuration,
		queues:        make(map[string]chan SyncRecord),
		context:       context,
		cancelFunc:    cancelFunc,
	}
}

// Start 启动异步同步管理器
// 初始化队列并启动所有后台工作协程
func (manager *Manager) Start() error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if manager.isRunning {
		return fmt.Errorf("manager already running")
	}

	if !manager.configuration.Enabled {
		log.Println("[AsyncSync] 异步同步已禁用")
		return nil
	}

	manager.initializeQueues()
	manager.startWorkers()
	manager.startBackgroundTasks()

	manager.isRunning = true
	log.Printf("[AsyncSync] 管理器已启动,工作协程数: %d", manager.configuration.WorkerCount)
	return nil
}

// Stop 停止异步同步管理器
// 等待所有后台协程安全退出
func (manager *Manager) Stop() {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if !manager.isRunning {
		return
	}

	manager.cancelFunc()
	manager.closeAllQueues()
	manager.workers.Wait()

	manager.isRunning = false
	log.Println("[AsyncSync] 管理器已停止")
}

// AddRecord 将记录加入异步同步队列
// 队列已满时会持久化到数据库
func (manager *Manager) AddRecord(
	tableName string,
	operation string,
	recordID string,
	data interface{},
) error {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	if !manager.isEnabledAndRunning() {
		return nil
	}

	queue, exists := manager.queues[tableName]
	if !exists {
		return fmt.Errorf("unknown table: %s", tableName)
	}

	record, err := manager.createSyncRecord(tableName, operation, recordID, data)
	if err != nil {
		return err
	}

	return manager.enqueueOrPersist(queue, record)
}

//
// 初始化方法
//

// initializeQueues 初始化所有支持的表队列
func (manager *Manager) initializeQueues() {
	supportedTables := []string{
		tableInboxMessages,
		tablePushRecords,
		tableMessageStatus,
		tableIdempotencyRecords,
	}

	bufferSize := manager.configuration.BatchSize * defaultQueueBufferMultiplier

	for _, tableName := range supportedTables {
		manager.queues[tableName] = make(chan SyncRecord, bufferSize)
	}
}

// startWorkers 启动所有工作协程
func (manager *Manager) startWorkers() {
	for workerID := 0; workerID < manager.configuration.WorkerCount; workerID++ {
		manager.workers.Add(1)
		go manager.runWorker(workerID)
	}
}

// startBackgroundTasks 启动后台任务协程
func (manager *Manager) startBackgroundTasks() {
	manager.workers.Add(1)
	go manager.runPeriodicFlush()

	manager.workers.Add(1)
	go manager.runRetryWorker()
}

// closeAllQueues 关闭所有队列通道
func (manager *Manager) closeAllQueues() {
	for _, queue := range manager.queues {
		close(queue)
	}
}

//
// 辅助方法
//

// isEnabledAndRunning 检查管理器是否已启用且正在运行
func (manager *Manager) isEnabledAndRunning() bool {
	return manager.configuration.Enabled && manager.isRunning
}

// createSyncRecord 创建同步记录
func (manager *Manager) createSyncRecord(
	tableName string,
	operation string,
	recordID string,
	data interface{},
) (SyncRecord, error) {
	serializedData, err := json.Marshal(data)
	if err != nil {
		return SyncRecord{}, fmt.Errorf("序列化数据失败: %w", err)
	}

	now := time.Now().Unix()

	return SyncRecord{
		TableName:   tableName,
		Operation:   operation,
		RecordID:    recordID,
		Data:        serializedData,
		MaxAttempts: manager.configuration.RetryAttempts,
		CreatedAt:   now,
		NextRetryAt: now,
		Status:      recordStatusPending,
	}, nil
}

// enqueueOrPersist 尝试入队,失败则持久化
func (manager *Manager) enqueueOrPersist(
	queue chan SyncRecord,
	record SyncRecord,
) error {
	select {
	case queue <- record:
		return nil
	default:
		return manager.persistRecord(record)
	}
}

//
// 工作协程 - 批处理逻辑
//

// runWorker 运行工作协程
// 从队列收集记录并批量处理
func (manager *Manager) runWorker(workerID int) {
	defer manager.workers.Done()

	batch := make([]SyncRecord, 0, manager.configuration.BatchSize)
	ticker := time.NewTicker(manager.configuration.FlushInterval)
	defer ticker.Stop()

	log.Printf("[AsyncSync] Worker %d 已启动", workerID)
	defer log.Printf("[AsyncSync] Worker %d 已停止", workerID)

	for {
		select {
		case <-manager.context.Done():
			manager.flushRemainingBatch(batch)
			return

		case <-ticker.C:
			batch = manager.flushBatchIfNotEmpty(batch)

		default:
			batch = manager.collectRecordsFromQueues(batch)
		}
	}
}

// collectRecordsFromQueues 从所有队列收集记录
func (manager *Manager) collectRecordsFromQueues(batch []SyncRecord) []SyncRecord {
	recordCollected := false

	for _, queue := range manager.queues {
		select {
		case record, ok := <-queue:
			if !ok {
				continue
			}

			batch = append(batch, record)
			recordCollected = true

			if len(batch) >= manager.configuration.BatchSize {
				manager.processBatch(batch)
				return make([]SyncRecord, 0, manager.configuration.BatchSize)
			}

		default:
			continue
		}
	}

	if !recordCollected {
		time.Sleep(workerIdleInterval)
	}

	return batch
}

// flushRemainingBatch 刷新剩余批次
func (manager *Manager) flushRemainingBatch(batch []SyncRecord) {
	if len(batch) > 0 {
		manager.processBatch(batch)
	}
}

// flushBatchIfNotEmpty 如果批次不为空则刷新
func (manager *Manager) flushBatchIfNotEmpty(batch []SyncRecord) []SyncRecord {
	if len(batch) > 0 {
		manager.processBatch(batch)
		return make([]SyncRecord, 0, manager.configuration.BatchSize)
	}
	return batch
}

// processBatch 处理一个批次的记录
func (manager *Manager) processBatch(batch []SyncRecord) {
	if len(batch) == 0 {
		return
	}

	groupedRecords := manager.groupRecordsByTable(batch)
	manager.syncGroupedRecords(groupedRecords)
}

// groupRecordsByTable 按表名分组记录
func (manager *Manager) groupRecordsByTable(batch []SyncRecord) map[string][]SyncRecord {
	groups := make(map[string][]SyncRecord)

	for _, record := range batch {
		groups[record.TableName] = append(groups[record.TableName], record)
	}

	return groups
}

// syncGroupedRecords 同步分组后的记录
func (manager *Manager) syncGroupedRecords(groups map[string][]SyncRecord) {
	currentTime := time.Now()

	for tableName, records := range groups {
		if err := manager.syncRecordsToMySQL(tableName, records); err != nil {
			log.Printf("[AsyncSync] 同步失败 table=%s, count=%d, error=%v",
				tableName, len(records), err)
			manager.handleSyncFailure(records, currentTime, err)
		}
	}
}

// handleSyncFailure 处理同步失败的记录
func (manager *Manager) handleSyncFailure(
	records []SyncRecord,
	failureTime time.Time,
	syncError error,
) {
	for _, record := range records {
		record.Attempts++
		record.NextRetryAt = manager.calculateNextRetryTime(failureTime, record.Attempts)
		record.Status = recordStatusPending
		record.Error = syncError.Error()

		_ = manager.persistRecord(record)
	}
}

// calculateNextRetryTime 计算下次重试时间
// 使用线性退避策略
func (manager *Manager) calculateNextRetryTime(baseTime time.Time, attempts int) int64 {
	retryDelay := time.Duration(attempts) * time.Minute
	return baseTime.Add(retryDelay).Unix()
}

//
// 数据库同步路由
//

// syncRecordsToMySQL 根据表名路由到具体同步函数
func (manager *Manager) syncRecordsToMySQL(
	tableName string,
	records []SyncRecord,
) error {
	switch tableName {
	case tableInboxMessages:
		return manager.syncInboxMessages(records)
	case tablePushRecords:
		return manager.syncPushRecords(records)
	case tableMessageStatus:
		return manager.syncMessageStatus(records)
	case tableIdempotencyRecords:
		return manager.syncIdempotencyRecords(records)
	default:
		return fmt.Errorf("unknown table: %s", tableName)
	}
}

// persistRecord 将记录持久化到重试队列表
func (manager *Manager) persistRecord(record SyncRecord) error {
	query := `INSERT INTO async_sync_queue 
		(table_name, operation, record_id, data, attempts, max_attempts, 
		 created_at, next_retry_at, status, error) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := manager.database.Exec(
		query,
		record.TableName,
		record.Operation,
		record.RecordID,
		record.Data,
		record.Attempts,
		record.MaxAttempts,
		record.CreatedAt,
		record.NextRetryAt,
		record.Status,
		record.Error,
	)

	return err
}

//
// 周期性任务
//

// runPeriodicFlush 运行周期性刷新任务
// 保留用于未来扩展(如统计、监控等)
func (manager *Manager) runPeriodicFlush() {
	defer manager.workers.Done()

	ticker := time.NewTicker(manager.configuration.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-manager.context.Done():
			return
		case <-ticker.C:
			// 预留:可在此添加统计、指标上报等逻辑
		}
	}
}

// runRetryWorker 运行重试工作协程
// 周期性扫描并处理失败的记录
func (manager *Manager) runRetryWorker() {
	defer manager.workers.Done()

	ticker := time.NewTicker(retryWorkerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-manager.context.Done():
			return
		case <-ticker.C:
			manager.processRetryRecords()
		}
	}
}

// processRetryRecords 处理需要重试的记录
func (manager *Manager) processRetryRecords() {
	retryRecords, err := manager.fetchRetryRecords()
	if err != nil {
		log.Printf("[AsyncSync] 获取重试记录失败: %v", err)
		return
	}

	if len(retryRecords) == 0 {
		return
	}

	successIDs := manager.retryRecords(retryRecords)
	manager.deleteSuccessfulRecords(successIDs)
}

// fetchRetryRecords 从数据库获取需要重试的记录
func (manager *Manager) fetchRetryRecords() ([]SyncRecord, error) {
	query := `SELECT id, table_name, operation, record_id, data, attempts, 
		max_attempts, created_at, next_retry_at, status, error 
		FROM async_sync_queue 
		WHERE status=? AND next_retry_at<=? AND attempts<max_attempts 
		LIMIT ?`

	rows, err := manager.database.Query(
		query,
		recordStatusPending,
		time.Now().Unix(),
		retryRecordBatchSize,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []SyncRecord
	for rows.Next() {
		record, err := manager.scanRetryRecord(rows)
		if err != nil {
			log.Printf("[AsyncSync] 扫描记录失败: %v", err)
			continue
		}
		records = append(records, record)
	}

	return records, nil
}

// scanRetryRecord 扫描单条重试记录
func (manager *Manager) scanRetryRecord(rows interface {
	Scan(dest ...interface{}) error
}) (SyncRecord, error) {
	var record SyncRecord

	err := rows.Scan(
		&record.ID,
		&record.TableName,
		&record.Operation,
		&record.RecordID,
		&record.Data,
		&record.Attempts,
		&record.MaxAttempts,
		&record.CreatedAt,
		&record.NextRetryAt,
		&record.Status,
		&record.Error,
	)

	return record, err
}

// retryRecords 重试记录并返回成功的ID列表
func (manager *Manager) retryRecords(records []SyncRecord) []int64 {
	var successIDs []int64
	currentTime := time.Now()

	for _, record := range records {
		if manager.retrySingleRecord(record, currentTime) {
			successIDs = append(successIDs, record.ID)
		}
	}

	return successIDs
}

// retrySingleRecord 重试单条记录
func (manager *Manager) retrySingleRecord(
	record SyncRecord,
	currentTime time.Time,
) bool {
	if err := manager.syncRecordsToMySQL(record.TableName, []SyncRecord{record}); err != nil {
		manager.updateFailedRecord(record, currentTime, err)
		return false
	}

	return true
}

// updateFailedRecord 更新失败的记录
func (manager *Manager) updateFailedRecord(
	record SyncRecord,
	failureTime time.Time,
	syncError error,
) {
	record.Attempts++
	record.NextRetryAt = manager.calculateNextRetryTime(failureTime, record.Attempts)
	record.Error = syncError.Error()

	if record.Attempts >= record.MaxAttempts {
		record.Status = recordStatusFailed
	}

	query := `UPDATE async_sync_queue 
		SET attempts=?, next_retry_at=?, error=?, status=? 
		WHERE id=?`

	_, _ = manager.database.Exec(
		query,
		record.Attempts,
		record.NextRetryAt,
		record.Error,
		record.Status,
		record.ID,
	)
}

// deleteSuccessfulRecords 批量删除成功的记录
func (manager *Manager) deleteSuccessfulRecords(ids []int64) {
	if len(ids) == 0 {
		return
	}

	placeholders := strings.Repeat("?,", len(ids))
	placeholders = placeholders[:len(placeholders)-1]

	args := make([]interface{}, len(ids))
	for index, id := range ids {
		args[index] = id
	}

	query := fmt.Sprintf("DELETE FROM async_sync_queue WHERE id IN (%s)", placeholders)

	if _, err := manager.database.Exec(query, args...); err != nil {
		log.Printf("[AsyncSync] 删除成功记录失败: %v", err)
	}
}

//
// 具体表同步实现
//

// syncInboxMessages 同步收件箱消息到 MySQL
func (manager *Manager) syncInboxMessages(records []SyncRecord) error {
	if len(records) == 0 {
		return nil
	}

	transaction, err := manager.database.Begin()
	if err != nil {
		return err
	}
	defer transaction.Rollback()

	statement, err := transaction.Prepare(`
		INSERT INTO inbox_messages 
		(id, user_id, kind, subject, body, data, created_at, read_at, redis_synced) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) 
		ON DUPLICATE KEY UPDATE 
		subject=VALUES(subject), body=VALUES(body), data=VALUES(data), 
		read_at=VALUES(read_at), redis_synced=VALUES(redis_synced)
	`)
	if err != nil {
		return err
	}
	defer statement.Close()

	for _, record := range records {
		if err := manager.executeInboxMessageInsert(statement, record); err != nil {
			return err
		}
	}

	return transaction.Commit()
}

// executeInboxMessageInsert 执行收件箱消息插入
func (manager *Manager) executeInboxMessageInsert(
	statement *sql.Stmt, // 直接使用 *sql.Stmt
	record SyncRecord,
) error {
	var message struct {
		ID        string
		UserID    string
		Kind      string
		Subject   string
		Body      string
		Data      map[string]interface{}
		CreatedAt int64
		ReadAt    int64
	}

	if err := json.Unmarshal(record.Data, &message); err != nil {
		log.Printf("[AsyncSync] 解析收件箱消息失败: %v", err)
		return nil
	}

	serializedData, _ := json.Marshal(message.Data)

	_, err := statement.Exec(
		message.ID,
		message.UserID,
		message.Kind,
		message.Subject,
		message.Body,
		string(serializedData),
		message.CreatedAt,
		message.ReadAt,
		true,
	)

	if err != nil {
		log.Printf("[AsyncSync] 插入收件箱消息失败: %v", err)
	}

	return err
}

// syncPushRecords 同步推送记录到 MySQL
func (manager *Manager) syncPushRecords(records []SyncRecord) error {
	if len(records) == 0 {
		return nil
	}

	transaction, err := manager.database.Begin()
	if err != nil {
		return err
	}
	defer transaction.Rollback()

	statement, err := transaction.Prepare(`
		INSERT INTO push_records 
		(id, namespace, kind, channels, recipients, status, code, content, created_at, redis_synced) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
		ON DUPLICATE KEY UPDATE 
		channels=VALUES(channels), recipients=VALUES(recipients),
		status=VALUES(status), code=VALUES(code), content=VALUES(content), 
		created_at=VALUES(created_at), redis_synced=VALUES(redis_synced)
	`)
	if err != nil {
		return err
	}
	defer statement.Close()

	for _, record := range records {
		if err := manager.executePushRecordInsert(statement, record); err != nil {
			return err
		}
	}

	return transaction.Commit()
}

// executePushRecordInsert 执行推送记录插入
func (manager *Manager) executePushRecordInsert(
	statement *sql.Stmt, // 直接使用 *sql.Stmt
	record SyncRecord,
) error {
	var pushRecord struct {
		ID         string
		Namespace  string
		Kind       string
		Status     string
		Code       string
		Content    string
		Channels   interface{}
		Recipients interface{}
		CreatedAt  int64
	}

	if err := json.Unmarshal(record.Data, &pushRecord); err != nil {
		log.Printf("[AsyncSync] 解析推送记录失败: %v", err)
		return nil
	}

	channels, _ := json.Marshal(pushRecord.Channels)
	recipients, _ := json.Marshal(pushRecord.Recipients)

	_, err := statement.Exec(
		pushRecord.ID,
		pushRecord.Namespace,
		pushRecord.Kind,
		string(channels),
		string(recipients),
		pushRecord.Status,
		pushRecord.Code,
		pushRecord.Content,
		pushRecord.CreatedAt,
		true,
	)

	if err != nil {
		log.Printf("[AsyncSync] 插入推送记录失败: %v", err)
	}

	return err
}

// syncMessageStatus 同步消息状态到 MySQL
func (manager *Manager) syncMessageStatus(records []SyncRecord) error {
	if len(records) == 0 {
		return nil
	}

	transaction, err := manager.database.Begin()
	if err != nil {
		return err
	}
	defer transaction.Rollback()

	statement, err := transaction.Prepare(`
		INSERT INTO message_status 
		(message_id, channel, phone, content, status, error, created_at, updated_at, redis_synced) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) 
		ON DUPLICATE KEY UPDATE 
		status=VALUES(status), error=VALUES(error), 
		updated_at=VALUES(updated_at), redis_synced=VALUES(redis_synced)
	`)
	if err != nil {
		return err
	}
	defer statement.Close()

	for _, record := range records {
		if err := manager.executeMessageStatusInsert(statement, record); err != nil {
			return err
		}
	}

	return transaction.Commit()
}

// executeMessageStatusInsert 执行消息状态插入
func (manager *Manager) executeMessageStatusInsert(
	statement *sql.Stmt, // 直接使用 *sql.Stmt
	record SyncRecord,
) error {
	var status struct {
		MessageID string
		Channel   string
		Phone     string
		Content   string
		Status    string
		Error     string
		CreatedAt int64
		UpdatedAt int64
	}

	if err := json.Unmarshal(record.Data, &status); err != nil {
		log.Printf("[AsyncSync] 解析消息状态失败: %v", err)
		return nil
	}

	_, err := statement.Exec(
		status.MessageID,
		status.Channel,
		status.Phone,
		status.Content,
		status.Status,
		status.Error,
		status.CreatedAt,
		status.UpdatedAt,
		true,
	)

	if err != nil {
		log.Printf("[AsyncSync] 插入消息状态失败: %v", err)
	}

	return err
}

// syncIdempotencyRecords 同步幂等记录到 MySQL
func (manager *Manager) syncIdempotencyRecords(records []SyncRecord) error {
	if len(records) == 0 {
		return nil
	}

	transaction, err := manager.database.Begin()
	if err != nil {
		return err
	}
	defer transaction.Rollback()

	statement, err := transaction.Prepare(`
		INSERT INTO idempotency_records 
		(key_hash, namespace, delivery_data, created_at, expires_at) 
		VALUES (?, ?, ?, ?, ?) 
		ON DUPLICATE KEY UPDATE 
		delivery_data=VALUES(delivery_data), expires_at=VALUES(expires_at)
	`)
	if err != nil {
		return err
	}
	defer statement.Close()

	for _, record := range records {
		if err := manager.executeIdempotencyRecordInsert(statement, record); err != nil {
			return err
		}
	}

	return transaction.Commit()
}

// executeIdempotencyRecordInsert 执行幂等记录插入
func (manager *Manager) executeIdempotencyRecordInsert(
	statement *sql.Stmt, // 直接使用 *sql.Stmt
	record SyncRecord,
) error {
	var idempotency struct {
		KeyHash      string
		Namespace    string
		DeliveryData interface{}
		CreatedAt    int64
		ExpiresAt    int64
	}

	if err := json.Unmarshal(record.Data, &idempotency); err != nil {
		log.Printf("[AsyncSync] 解析幂等记录失败: %v", err)
		return nil
	}

	deliveryData, _ := json.Marshal(idempotency.DeliveryData)

	_, err := statement.Exec(
		idempotency.KeyHash,
		idempotency.Namespace,
		string(deliveryData),
		idempotency.CreatedAt,
		idempotency.ExpiresAt,
	)

	if err != nil {
		log.Printf("[AsyncSync] 插入幂等记录失败: %v", err)
	}

	return err
}
