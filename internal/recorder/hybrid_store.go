package recorder

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"push-gateway/internal/asyncsync"
	"push-gateway/internal/database"
	"push-gateway/internal/push"
)

// ==================== 常量定义 ====================

const (
	// 数据库表名
	tablePushRecords = "push_records"

	// 操作类型
	operationInsert = "insert"

	// 日志标签
	logTagHybridRecorder = "[HYBRID_RECORDER] "

	// SQL 查询语句
	sqlQueryRecords = `SELECT id, namespace, kind, subject, body, message_data, channels, recipients, 
		status, code, content, error_detail, priority, created_at, sent_at 
		FROM push_records WHERE namespace = ? ORDER BY created_at DESC LIMIT ?`

	sqlDeleteOldRecords = "DELETE FROM push_records WHERE created_at < ?"

	// 错误消息常量
	errorMessageRedisStoreFailed  = "redis save failed: %w"
	errorMessageMySQLNotAvailable = "mysql not available"
	errorMessageQueryFailed       = "query failed: %w"
	errorMessageCleanupFailed     = "cleanup failed: %w"
	errorMessageGetAffectedFailed = "get affected rows failed: %w"
)

// ==================== 混合存储结构 ====================

// HybridStore Redis + MySQL 混合存储
type HybridStore struct {
	redisStore       *RedisStore
	mysqlDatabase    *database.MySQLDB
	asyncSyncManager *asyncsync.Manager
}

// NewHybridStore 创建混合存储
func NewHybridStore(
	redisStore *RedisStore,
	mysqlDatabase *database.MySQLDB,
	asyncSyncManager *asyncsync.Manager,
) *HybridStore {
	return &HybridStore{
		redisStore:       redisStore,
		mysqlDatabase:    mysqlDatabase,
		asyncSyncManager: asyncSyncManager,
	}
}

// ==================== 记录保存 ====================

// SaveRecord 保存记录(先写 Redis，再异步同步到 MySQL)
func (store *HybridStore) SaveRecord(ctx context.Context, record push.Record) error {
	if err := store.saveToRedis(ctx, record); err != nil {
		return err
	}

	store.asyncSyncToMySQL(record)

	return nil
}

// saveToRedis 保存记录到 Redis
func (store *HybridStore) saveToRedis(ctx context.Context, record push.Record) error {
	err := store.redisStore.SaveRecord(ctx, record)
	if err != nil {
		return fmt.Errorf(errorMessageRedisStoreFailed, err)
	}
	return nil
}

// asyncSyncToMySQL 异步同步记录到 MySQL
func (store *HybridStore) asyncSyncToMySQL(record push.Record) {
	if store.asyncSyncManager == nil {
		return
	}

	syncData := store.buildSyncData(record)
	recordID := record.Key

	err := store.asyncSyncManager.AddRecord(tablePushRecords, operationInsert, recordID, syncData)
	if err != nil {
		log.Printf("%s异步同步添加失败: %v", logTagHybridRecorder, err)
		// 不影响主流程，只记录错误
	}
}

// buildSyncData 构建同步数据
func (store *HybridStore) buildSyncData(record push.Record) map[string]interface{} {
	return map[string]interface{}{
		"id":           record.Key,
		"namespace":    record.Namespace,
		"kind":         string(record.Kind),
		"subject":      record.Subject,
		"body":         record.Body,
		"message_data": record.MessageData,
		"channels":     record.Channels,
		"recipients":   record.Recipients,
		"status":       record.Status,
		"code":         record.Code,
		"content":      record.Content,
		"error_detail": record.ErrorDetail,
		"priority":     record.Priority,
		"created_at":   record.CreatedAt,
		"sent_at":      record.SentAt,
	}
}

// ==================== 记录裁剪 ====================

// Trim 裁剪记录(只在 Redis 中执行，MySQL 通过定期清理)
func (store *HybridStore) Trim(ctx context.Context) (int, error) {
	return store.redisStore.Trim(ctx)
}

// ==================== 记录查询 ====================

// QueryRecords 查询记录(优先从 Redis 查询，如果 Redis 没有则从 MySQL 查询)
func (store *HybridStore) QueryRecords(ctx context.Context, namespace string, limit int64) ([]push.Record, error) {
	// 先尝试从 Redis 查询
	records, err := store.queryFromRedis(ctx, namespace, limit)
	if err != nil {
		log.Printf("%sRedis查询失败，尝试MySQL: %v", logTagHybridRecorder, err)
		return store.queryFromMySQL(ctx, namespace, limit)
	}

	// 如果 Redis 有数据或 MySQL 不可用，返回 Redis 结果
	if len(records) > 0 || !store.isMySQLAvailable() {
		return records, nil
	}

	// Redis 没有数据，尝试从 MySQL 查询
	return store.queryFromMySQL(ctx, namespace, limit)
}

// queryFromRedis 从 Redis 查询记录
func (store *HybridStore) queryFromRedis(ctx context.Context, namespace string, limit int64) ([]push.Record, error) {
	return store.redisStore.QueryRecords(ctx, namespace, limit)
}

// queryFromMySQL 从 MySQL 查询记录
func (store *HybridStore) queryFromMySQL(ctx context.Context, namespace string, limit int64) ([]push.Record, error) {
	if !store.isMySQLAvailable() {
		return []push.Record{}, fmt.Errorf(errorMessageMySQLNotAvailable)
	}

	rows, err := store.executeQuery(namespace, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return store.parseQueryResults(rows)
}

// executeQuery 执行查询
func (store *HybridStore) executeQuery(namespace string, limit int64) (*sql.Rows, error) {
	rows, err := store.mysqlDatabase.Query(sqlQueryRecords, namespace, limit)
	if err != nil {
		return nil, fmt.Errorf(errorMessageQueryFailed, err)
	}
	return rows, nil
}

// parseQueryResults 解析查询结果
func (store *HybridStore) parseQueryResults(rows *sql.Rows) ([]push.Record, error) {
	var records []push.Record

	for rows.Next() {
		record, err := store.scanRecordRow(rows)
		if err != nil {
			log.Printf("%s扫描行失败: %v", logTagHybridRecorder, err)
			continue
		}

		records = append(records, record)
	}

	return records, nil
}

// scanRecordRow 扫描单行记录
func (store *HybridStore) scanRecordRow(rows *sql.Rows) (push.Record, error) {
	var record push.Record
	var recordID string
	var nullableFields recordNullableFields

	err := rows.Scan(
		&recordID,
		&record.Namespace,
		&record.Kind,
		&nullableFields.subject,
		&nullableFields.body,
		&nullableFields.messageData,
		&nullableFields.channels,
		&nullableFields.recipients,
		&record.Status,
		&record.Code,
		&record.Content,
		&nullableFields.errorDetail,
		&record.Priority,
		&record.CreatedAt,
		&nullableFields.sentAt,
	)

	if err != nil {
		return record, err
	}

	// 使用数据库 ID 作为 Key
	record.Key = recordID

	// 填充可为空的字段
	store.populateNullableFields(&record, nullableFields)

	// 解析 JSON 字段
	store.parseJSONFields(&record, nullableFields)

	return record, nil
}

// recordNullableFields 可为空的字段集合
type recordNullableFields struct {
	subject     sql.NullString
	body        sql.NullString
	messageData sql.NullString
	channels    sql.NullString
	recipients  sql.NullString
	errorDetail sql.NullString
	sentAt      sql.NullInt64
}

// populateNullableFields 填充可为空的字段
func (store *HybridStore) populateNullableFields(record *push.Record, fields recordNullableFields) {
	if fields.subject.Valid {
		record.Subject = fields.subject.String
	}

	if fields.body.Valid {
		record.Body = fields.body.String
	}

	if fields.errorDetail.Valid {
		record.ErrorDetail = fields.errorDetail.String
	}

	if fields.sentAt.Valid {
		record.SentAt = fields.sentAt.Int64
	}
}

// parseJSONFields 解析 JSON 字段
func (store *HybridStore) parseJSONFields(record *push.Record, fields recordNullableFields) {
	store.parseMessageData(record, fields.messageData)
	store.parseChannels(record, fields.channels)
	store.parseRecipients(record, fields.recipients)
}

// parseMessageData 解析消息数据 JSON
func (store *HybridStore) parseMessageData(record *push.Record, messageData sql.NullString) {
	if !messageData.Valid || messageData.String == "" {
		return
	}

	if err := json.Unmarshal([]byte(messageData.String), &record.MessageData); err != nil {
		log.Printf("%s解析message_data JSON失败: %v", logTagHybridRecorder, err)
	}
}

// parseChannels 解析通道 JSON
func (store *HybridStore) parseChannels(record *push.Record, channels sql.NullString) {
	if !channels.Valid || channels.String == "" {
		return
	}

	if err := json.Unmarshal([]byte(channels.String), &record.Channels); err != nil {
		log.Printf("%s解析channels JSON失败: %v", logTagHybridRecorder, err)
	}
}

// parseRecipients 解析收件人 JSON
func (store *HybridStore) parseRecipients(record *push.Record, recipients sql.NullString) {
	if !recipients.Valid || recipients.String == "" {
		return
	}

	if err := json.Unmarshal([]byte(recipients.String), &record.Recipients); err != nil {
		log.Printf("%s解析recipients JSON失败: %v", logTagHybridRecorder, err)
	}
}

// ==================== 记录清理 ====================

// CleanupOldRecords 清理过期记录(定期任务)
func (store *HybridStore) CleanupOldRecords(ctx context.Context, olderThan time.Duration) error {
	if !store.isMySQLAvailable() {
		return nil
	}

	cutoffTimestamp := calculateCutoffTimestamp(olderThan)

	affectedRows, err := store.deleteOldRecords(cutoffTimestamp)
	if err != nil {
		return err
	}

	store.logCleanupResult(affectedRows)
	return nil
}

// calculateCutoffTimestamp 计算截止时间戳
func calculateCutoffTimestamp(olderThan time.Duration) int64 {
	return time.Now().Add(-olderThan).Unix()
}

// deleteOldRecords 删除旧记录
func (store *HybridStore) deleteOldRecords(cutoffTimestamp int64) (int64, error) {
	result, err := store.mysqlDatabase.Exec(sqlDeleteOldRecords, cutoffTimestamp)
	if err != nil {
		return 0, fmt.Errorf(errorMessageCleanupFailed, err)
	}

	affectedRows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf(errorMessageGetAffectedFailed, err)
	}

	return affectedRows, nil
}

// logCleanupResult 记录清理结果
func (store *HybridStore) logCleanupResult(affectedRows int64) {
	log.Printf("%s清理了 %d 条过期记录", logTagHybridRecorder, affectedRows)
}

// ==================== 辅助方法 ====================

// isMySQLAvailable 检查 MySQL 是否可用
func (store *HybridStore) isMySQLAvailable() bool {
	return store.mysqlDatabase != nil
}
