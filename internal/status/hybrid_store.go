package status

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"push-gateway/internal/asyncsync"
	"push-gateway/internal/database"
)

// ==================== 常量定义 ====================

const (
	statusPending       = "pending"
	syncOperationInsert = "insert"
	syncOperationUpdate = "update"
	syncTableName       = "message_status"
)

// ==================== 数据结构 ====================

// HybridStatusStore Redis + MySQL 混合状态存储
type HybridStatusStore struct {
	redisStore       *RedisStatusStore
	mysqlDatabase    *database.MySQLDB
	asyncSyncManager *asyncsync.Manager
}

// statusUpdateData 状态更新数据
type statusUpdateData map[string]interface{}

// ==================== 构造函数 ====================

// NewHybridStatusStore 创建混合状态存储实例
func NewHybridStatusStore(
	redisStore *RedisStatusStore,
	mysqlDB *database.MySQLDB,
	syncManager *asyncsync.Manager,
) *HybridStatusStore {
	return &HybridStatusStore{
		redisStore:       redisStore,
		mysqlDatabase:    mysqlDB,
		asyncSyncManager: syncManager,
	}
}

// ==================== 核心方法 ====================

// SaveStatus 保存状态（先写 Redis，再同步和异步写入 MySQL）
func (store *HybridStatusStore) SaveStatus(ctx context.Context, status *MessageStatus) error {
	if err := store.saveToRedis(ctx, status); err != nil {
		return err
	}

	store.syncSaveToMySQL(status)
	store.asyncSaveToMySQL(status)

	return nil
}

// GetStatus 获取状态（仅从 Redis 查询）
func (store *HybridStatusStore) GetStatus(ctx context.Context, messageID string) (*MessageStatus, error) {
	status, err := store.redisStore.GetStatus(ctx, messageID)
	if err != nil {
		log.Printf("[HybridStatus] Redis query failed: %v", err)
		return nil, err
	}

	return status, nil
}

// UpdateStatus 更新状态（先更新 Redis，再同步和异步更新 MySQL）
func (store *HybridStatusStore) UpdateStatus(
	ctx context.Context,
	messageID string,
	newStatus string,
	errorMessage string,
) error {
	if err := store.updateInRedis(ctx, messageID, newStatus, errorMessage); err != nil {
		log.Printf("[HybridStatus] Redis update failed, fallback to MySQL: %v", err)
		return store.updateDirectlyInMySQL(ctx, messageID, newStatus, errorMessage)
	}

	store.syncUpdateToMySQL(messageID, newStatus, errorMessage)
	store.asyncUpdateToMySQL(messageID, newStatus, errorMessage)

	return nil
}

// GetPendingStatuses 获取待处理状态（Redis 优先，失败则查 MySQL）
func (store *HybridStatusStore) GetPendingStatuses(ctx context.Context, channels []string) ([]*MessageStatus, error) {
	statuses, err := store.redisStore.GetPendingStatuses(ctx, channels)
	if err != nil {
		log.Printf("[HybridStatus] Redis query pending statuses failed, try MySQL: %v", err)
		return store.queryPendingFromMySQL(ctx, channels)
	}

	if len(statuses) > 0 || store.mysqlDatabase == nil {
		return statuses, nil
	}

	return store.queryPendingFromMySQL(ctx, channels)
}

// CleanupOldStatuses 清理过期状态（同时清理 Redis 和 MySQL）
func (store *HybridStatusStore) CleanupOldStatuses(ctx context.Context, olderThan time.Duration) error {
	store.cleanupRedisStatuses(ctx, olderThan)
	return store.cleanupMySQLStatuses(olderThan)
}

// ==================== 私有方法 - Redis 操作 ====================

func (store *HybridStatusStore) saveToRedis(ctx context.Context, status *MessageStatus) error {
	if err := store.redisStore.SaveStatus(ctx, status); err != nil {
		return fmt.Errorf("redis save failed: %w", err)
	}
	return nil
}

func (store *HybridStatusStore) updateInRedis(
	ctx context.Context,
	messageID string,
	newStatus string,
	errorMessage string,
) error {
	return store.redisStore.UpdateStatus(ctx, messageID, newStatus, errorMessage)
}

func (store *HybridStatusStore) cleanupRedisStatuses(ctx context.Context, olderThan time.Duration) {
	if err := store.redisStore.CleanupOldStatuses(ctx, olderThan); err != nil {
		log.Printf("[HybridStatus] Redis cleanup failed: %v", err)
	}
}

// ==================== 私有方法 - MySQL 同步操作 ====================

func (store *HybridStatusStore) syncSaveToMySQL(status *MessageStatus) {
	if store.mysqlDatabase == nil {
		return
	}

	query := `INSERT INTO message_status 
		(message_id, channel, phone, content, status, error, created_at, updated_at, redis_synced)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE 
			status=VALUES(status), 
			error=VALUES(error), 
			updated_at=VALUES(updated_at), 
			redis_synced=VALUES(redis_synced)`

	_, err := store.mysqlDatabase.Exec(
		query,
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
		log.Printf("[HybridStatus] MySQL sync save failed: %v", err)
	}
}

func (store *HybridStatusStore) syncUpdateToMySQL(messageID, newStatus, errorMessage string) {
	if store.mysqlDatabase == nil {
		return
	}

	query := "UPDATE message_status SET status = ?, error = ?, updated_at = ? WHERE message_id = ?"
	_, err := store.mysqlDatabase.Exec(query, newStatus, errorMessage, time.Now().Unix(), messageID)

	if err != nil {
		log.Printf("[HybridStatus] MySQL sync update failed: %v", err)
	}
}

func (store *HybridStatusStore) updateDirectlyInMySQL(
	ctx context.Context,
	messageID string,
	newStatus string,
	errorMessage string,
) error {
	if store.mysqlDatabase == nil {
		return fmt.Errorf("mysql not available")
	}

	query := "UPDATE message_status SET status = ?, error = ?, updated_at = ? WHERE message_id = ?"
	_, err := store.mysqlDatabase.Exec(query, newStatus, errorMessage, time.Now().Unix(), messageID)

	if err != nil {
		return fmt.Errorf("mysql update failed: %w", err)
	}

	return nil
}

// ==================== 私有方法 - MySQL 异步操作 ====================

func (store *HybridStatusStore) asyncSaveToMySQL(status *MessageStatus) {
	if store.asyncSyncManager == nil {
		return
	}

	err := store.asyncSyncManager.AddRecord(syncTableName, syncOperationInsert, status.MessageID, status)
	if err != nil {
		log.Printf("[HybridStatus] Async sync add failed: %v", err)
	}
}

func (store *HybridStatusStore) asyncUpdateToMySQL(messageID, newStatus, errorMessage string) {
	if store.asyncSyncManager == nil {
		return
	}

	updateData := statusUpdateData{
		"message_id": messageID,
		"status":     newStatus,
		"error":      errorMessage,
		"updated_at": time.Now().Unix(),
	}

	err := store.asyncSyncManager.AddRecord(syncTableName, syncOperationUpdate, messageID, updateData)
	if err != nil {
		log.Printf("[HybridStatus] Async sync update failed: %v", err)
	}
}

// ==================== 私有方法 - MySQL 查询 ====================

func (store *HybridStatusStore) queryPendingFromMySQL(
	ctx context.Context,
	channels []string,
) ([]*MessageStatus, error) {
	if store.mysqlDatabase == nil || len(channels) == 0 {
		return []*MessageStatus{}, nil
	}

	query, args := store.buildPendingQuery(channels)
	return store.executePendingQuery(query, args)
}

func (store *HybridStatusStore) buildPendingQuery(channels []string) (string, []interface{}) {
	placeholders := store.buildPlaceholders(len(channels))
	query := fmt.Sprintf(
		"SELECT message_id, channel, phone, content, status, error, created_at, updated_at "+
			"FROM message_status WHERE status = '%s' AND channel IN (%s) ORDER BY created_at",
		statusPending,
		placeholders,
	)

	args := make([]interface{}, len(channels))
	for i, channel := range channels {
		args[i] = channel
	}

	return query, args
}

func (store *HybridStatusStore) buildPlaceholders(count int) string {
	if count == 0 {
		return ""
	}

	placeholders := make([]string, count)
	for i := range placeholders {
		placeholders[i] = "?"
	}

	return strings.Join(placeholders, ",")
}

func (store *HybridStatusStore) executePendingQuery(query string, args []interface{}) ([]*MessageStatus, error) {
	rows, err := store.mysqlDatabase.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("mysql query failed: %w", err)
	}
	defer rows.Close()

	var statuses []*MessageStatus
	for rows.Next() {
		status := &MessageStatus{}
		err := rows.Scan(
			&status.MessageID,
			&status.Channel,
			&status.Phone,
			&status.Content,
			&status.Status,
			&status.Error,
			&status.CreatedAt,
			&status.UpdatedAt,
		)

		if err != nil {
			log.Printf("[HybridStatus] Row scan failed: %v", err)
			continue
		}

		statuses = append(statuses, status)
	}

	return statuses, nil
}

// ==================== 私有方法 - MySQL 清理 ====================

func (store *HybridStatusStore) cleanupMySQLStatuses(olderThan time.Duration) error {
	if store.mysqlDatabase == nil {
		return nil
	}

	cutoffTimestamp := time.Now().Add(-olderThan).Unix()
	query := "DELETE FROM message_status WHERE created_at < ?"

	result, err := store.mysqlDatabase.Exec(query, cutoffTimestamp)
	if err != nil {
		return fmt.Errorf("mysql cleanup failed: %w", err)
	}

	affectedRows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get affected rows failed: %w", err)
	}

	log.Printf("[HybridStatus] MySQL cleaned %d expired statuses", affectedRows)
	return nil
}
