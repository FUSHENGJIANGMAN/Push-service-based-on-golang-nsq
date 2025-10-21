package inbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"push-gateway/internal/asyncsync"
	"push-gateway/internal/database"
)

// ==================== 常量定义 ====================

const (
	tableInboxMessages = "inbox_messages"
	operationInsert    = "insert"
	operationUpdate    = "update"
	operationDelete    = "delete"
	statusUnread       = "unread"
	statusRead         = "read"
	sqlPlaceholder     = "?"
	sqlComma           = ","
	timestampZero      = 0
)

// ==================== 错误定义 ====================

var (
	// ErrMySQLUnavailable MySQL 不可用错误
	ErrMySQLUnavailable = errors.New("mysql database is not available")

	// ErrEmptyIDs ID 列表为空错误
	ErrEmptyIDs = errors.New("message ids cannot be empty")

	// ErrQueryFailed 查询失败错误
	ErrQueryFailed = errors.New("database query failed")

	// ErrScanFailed 扫描行失败错误
	ErrScanFailed = errors.New("failed to scan database row")
)

// ==================== 响应结构 ====================

// StoreResponse 存储操作统一响应
type StoreResponse struct {
	Code int         `json:"code"` // 响应码
	Data interface{} `json:"data"` // 响应数据
	Msg  string      `json:"msg"`  // 响应消息
}

// ListResult 列表查询结果
type ListResult struct {
	Messages []Message `json:"messages"` // 消息列表
	Total    int64     `json:"total"`    // 总数
}

// OperationResult 操作结果
type OperationResult struct {
	AffectedCount int    `json:"affectedCount"` // 影响的记录数
	MessageID     string `json:"messageId"`     // 消息ID（仅用于添加操作）
}

// ==================== 核心服务 ====================

// HybridStore Redis + MySQL 混合存储
// 采用 Redis 作为主存储提供高性能读写,MySQL 作为持久化层保证数据可靠性
type HybridStore struct {
	redisStore  *RedisStore
	mysqlDB     *database.MySQLDB
	syncManager *asyncsync.Manager
}

// NewHybridStore 创建混合存储实例
func NewHybridStore(
	redisStore *RedisStore,
	mysqlDB *database.MySQLDB,
	syncManager *asyncsync.Manager,
) *HybridStore {
	return &HybridStore{
		redisStore:  redisStore,
		mysqlDB:     mysqlDB,
		syncManager: syncManager,
	}
}

// Add 添加消息
// 先写入 Redis 保证快速响应,再异步同步到 MySQL 保证持久化
func (store *HybridStore) Add(ctx context.Context, message Message) (string, error) {
	id, err := store.redisStore.Add(ctx, message)
	if err != nil {
		return "", fmt.Errorf("redis add failed: %w", err)
	}

	message.ID = id
	store.asyncSyncInsert(id, message)

	return id, nil
}

// List 查询消息列表
// 优先从 Redis 查询以获得更好的性能,Redis 不可用时降级到 MySQL
func (store *HybridStore) List(
	ctx context.Context,
	userID string,
	status string,
	offset, limit int64,
) ([]Message, int64, error) {
	messages, total, err := store.redisStore.List(ctx, userID, status, offset, limit)
	if err != nil {
		log.Printf("[HYBRID_STORE] Redis query failed, degrading to MySQL: %v", err)
		return store.listFromMySQL(ctx, userID, status, offset, limit)
	}

	// 如果 Redis 有数据或 MySQL 不可用,直接返回 Redis 结果
	if len(messages) > 0 || store.mysqlDB == nil {
		return messages, total, nil
	}

	// Redis 无数据时,尝试从 MySQL 加载（可能是冷启动场景）
	return store.listFromMySQL(ctx, userID, status, offset, limit)
}

// MarkRead 标记消息为已读
// 先更新 Redis 保证用户立即看到变化,再异步同步到 MySQL
func (store *HybridStore) MarkRead(
	ctx context.Context,
	userID string,
	ids []string,
) (int, error) {
	count, err := store.redisStore.MarkRead(ctx, userID, ids)
	if err != nil {
		log.Printf("[HYBRID_STORE] Redis mark read failed, trying MySQL: %v", err)
		return store.markReadInMySQL(ctx, userID, ids)
	}

	if count > 0 {
		store.asyncSyncMarkRead(userID, ids)
	}

	return count, nil
}

// Delete 删除消息
// 先从 Redis 删除保证用户立即看到变化,再异步从 MySQL 删除
func (store *HybridStore) Delete(
	ctx context.Context,
	userID string,
	ids []string,
) (int, error) {
	count, err := store.redisStore.Delete(ctx, userID, ids)
	if err != nil {
		log.Printf("[HYBRID_STORE] Redis delete failed, trying MySQL: %v", err)
		return store.deleteFromMySQL(ctx, userID, ids)
	}

	if count > 0 {
		store.asyncSyncDelete(userID, ids)
	}

	return count, nil
}

// TrimUser 裁剪用户消息
// 仅在 Redis 中执行,MySQL 依靠 TTL 机制自动清理过期数据
func (store *HybridStore) TrimUser(ctx context.Context, userID string) (int, error) {
	return store.redisStore.TrimUser(ctx, userID)
}

// Put 兼容旧接口
// 为了向后兼容而保留,新代码应使用 Add 方法
func (store *HybridStore) Put(ctx context.Context, userID string, message Message) error {
	message.UserID = userID
	_, err := store.Add(ctx, message)
	return err
}

// ==================== 私有方法：异步同步 ====================

// asyncSyncInsert 异步同步插入操作到 MySQL
func (store *HybridStore) asyncSyncInsert(id string, message Message) {
	if store.syncManager == nil {
		return
	}

	err := store.syncManager.AddRecord(tableInboxMessages, operationInsert, id, message)
	if err != nil {
		log.Printf("[HYBRID_STORE] Async sync insert failed: %v", err)
	}
}

// asyncSyncMarkRead 异步同步标记已读操作到 MySQL
func (store *HybridStore) asyncSyncMarkRead(userID string, ids []string) {
	if store.syncManager == nil {
		return
	}

	readTime := time.Now().Unix()
	for _, id := range ids {
		updateData := store.buildMarkReadData(id, userID, readTime)
		err := store.syncManager.AddRecord(tableInboxMessages, operationUpdate, id, updateData)
		if err != nil {
			log.Printf("[HYBRID_STORE] Async sync mark read failed for id %s: %v", id, err)
		}
	}
}

// asyncSyncDelete 异步同步删除操作到 MySQL
func (store *HybridStore) asyncSyncDelete(userID string, ids []string) {
	if store.syncManager == nil {
		return
	}

	for _, id := range ids {
		deleteData := store.buildDeleteData(id, userID)
		err := store.syncManager.AddRecord(tableInboxMessages, operationDelete, id, deleteData)
		if err != nil {
			log.Printf("[HYBRID_STORE] Async sync delete failed for id %s: %v", id, err)
		}
	}
}

// ==================== 私有方法：数据构建 ====================

// buildMarkReadData 构建标记已读的数据
func (store *HybridStore) buildMarkReadData(
	id string,
	userID string,
	readTime int64,
) map[string]interface{} {
	return map[string]interface{}{
		"id":      id,
		"user_id": userID,
		"read_at": readTime,
	}
}

// buildDeleteData 构建删除数据
func (store *HybridStore) buildDeleteData(id string, userID string) map[string]interface{} {
	return map[string]interface{}{
		"id":      id,
		"user_id": userID,
	}
}

// ==================== 私有方法：MySQL 查询 ====================

// listFromMySQL 从 MySQL 查询消息列表
func (store *HybridStore) listFromMySQL(
	ctx context.Context,
	userID string,
	status string,
	offset, limit int64,
) ([]Message, int64, error) {
	if store.mysqlDB == nil {
		return []Message{}, 0, ErrMySQLUnavailable
	}

	whereClause, args := store.buildWhereClause(userID, status)

	total, err := store.queryTotalCount(whereClause, args)
	if err != nil {
		return nil, 0, err
	}

	messages, err := store.queryMessageList(whereClause, args, offset, limit)
	if err != nil {
		return nil, 0, err
	}

	return messages, total, nil
}

// buildWhereClause 构建 WHERE 查询条件
func (store *HybridStore) buildWhereClause(
	userID string,
	status string,
) (string, []interface{}) {
	whereClause := "WHERE user_id = ?"
	args := []interface{}{userID}

	// 根据状态添加额外条件,使用卫语句减少嵌套
	if status == statusUnread {
		whereClause += " AND read_at = 0"
	}

	if status == statusRead {
		whereClause += " AND read_at > 0"
	}

	return whereClause, args
}

// queryTotalCount 查询总记录数
func (store *HybridStore) queryTotalCount(
	whereClause string,
	args []interface{},
) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", tableInboxMessages, whereClause)

	var total int64
	err := store.mysqlDB.QueryRow(query, args...).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("%w: count query: %v", ErrQueryFailed, err)
	}

	return total, nil
}

// queryMessageList 查询消息列表
func (store *HybridStore) queryMessageList(
	whereClause string,
	args []interface{},
	offset, limit int64,
) ([]Message, error) {
	query := fmt.Sprintf(`
		SELECT id, user_id, kind, subject, body, data, created_at, read_at 
		FROM %s %s 
		ORDER BY created_at DESC 
		LIMIT ? OFFSET ?
	`, tableInboxMessages, whereClause)

	args = append(args, limit, offset)

	rows, err := store.mysqlDB.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("%w: list query: %v", ErrQueryFailed, err)
	}
	defer rows.Close()

	return store.scanMessages(rows)
}

// scanMessages 扫描查询结果为消息列表
func (store *HybridStore) scanMessages(rows interface {
	Next() bool
	Scan(...interface{}) error
	Close() error
}) ([]Message, error) {
	var messages []Message

	for rows.Next() {
		message, err := store.scanSingleMessage(rows)
		if err != nil {
			log.Printf("[HYBRID_STORE] %v", err)
			continue
		}
		messages = append(messages, message)
	}

	return messages, nil
}

// scanSingleMessage 扫描单条消息记录
func (store *HybridStore) scanSingleMessage(
	scanner interface{ Scan(...interface{}) error },
) (Message, error) {
	var message Message
	var dataStr string

	err := scanner.Scan(
		&message.ID,
		&message.UserID,
		&message.Kind,
		&message.Subject,
		&message.Body,
		&dataStr,
		&message.CreatedAt,
		&message.ReadAt,
	)

	if err != nil {
		return Message{}, fmt.Errorf("%w: %v", ErrScanFailed, err)
	}

	// 解析 JSON 数据字段,忽略解析失败以保证部分数据可用
	if dataStr != "" {
		if err := json.Unmarshal([]byte(dataStr), &message.Data); err != nil {
			log.Printf("[HYBRID_STORE] JSON unmarshal failed: %v", err)
		}
	}

	return message, nil
}

// ==================== 私有方法：MySQL 更新 ====================

// markReadInMySQL 在 MySQL 中标记消息为已读
func (store *HybridStore) markReadInMySQL(
	ctx context.Context,
	userID string,
	ids []string,
) (int, error) {
	if store.mysqlDB == nil {
		return 0, ErrMySQLUnavailable
	}

	if len(ids) == 0 {
		return 0, ErrEmptyIDs
	}

	query, args := store.buildMarkReadQuery(userID, ids)

	result, err := store.mysqlDB.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("mark read in mysql failed: %w", err)
	}

	return store.getAffectedRows(result)
}

// buildMarkReadQuery 构建标记已读的 SQL 查询
func (store *HybridStore) buildMarkReadQuery(
	userID string,
	ids []string,
) (string, []interface{}) {
	query := fmt.Sprintf(
		"UPDATE %s SET read_at = ? WHERE user_id = ? AND id IN (%s)",
		tableInboxMessages,
		store.buildInClausePlaceholders(len(ids)),
	)

	args := []interface{}{time.Now().Unix(), userID}
	args = append(args, store.convertIDsToInterfaces(ids)...)

	return query, args
}

// ==================== 私有方法：MySQL 删除 ====================

// deleteFromMySQL 从 MySQL 删除消息
func (store *HybridStore) deleteFromMySQL(
	ctx context.Context,
	userID string,
	ids []string,
) (int, error) {
	if store.mysqlDB == nil {
		return 0, ErrMySQLUnavailable
	}

	if len(ids) == 0 {
		return 0, ErrEmptyIDs
	}

	query, args := store.buildDeleteQuery(userID, ids)

	result, err := store.mysqlDB.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("delete from mysql failed: %w", err)
	}

	return store.getAffectedRows(result)
}

// buildDeleteQuery 构建删除的 SQL 查询
func (store *HybridStore) buildDeleteQuery(
	userID string,
	ids []string,
) (string, []interface{}) {
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE user_id = ? AND id IN (%s)",
		tableInboxMessages,
		store.buildInClausePlaceholders(len(ids)),
	)

	args := []interface{}{userID}
	args = append(args, store.convertIDsToInterfaces(ids)...)

	return query, args
}

// ==================== 私有方法：SQL 工具函数 ====================

// buildInClausePlaceholders 构建 IN 子句的占位符
// 例如: buildInClausePlaceholders(3) 返回 "?,?,?"
func (store *HybridStore) buildInClausePlaceholders(count int) string {
	if count == 0 {
		return ""
	}

	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = sqlPlaceholder
	}

	return strings.Join(placeholders, sqlComma)
}

// convertIDsToInterfaces 将字符串 ID 切片转换为 interface{} 切片
// 用于满足 database/sql 包的参数要求
func (store *HybridStore) convertIDsToInterfaces(ids []string) []interface{} {
	interfaces := make([]interface{}, len(ids))
	for i, id := range ids {
		interfaces[i] = id
	}
	return interfaces
}

// getAffectedRows 获取受影响的行数
func (store *HybridStore) getAffectedRows(
	result interface{ RowsAffected() (int64, error) },
) (int, error) {
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get affected rows failed: %w", err)
	}
	return int(affected), nil
}

// ==================== 响应构建函数 ====================

// NewAddResponse 创建添加消息响应
func NewAddResponse(messageID string, err error) StoreResponse {
	if err != nil {
		return StoreResponse{
			Code: 500,
			Data: nil,
			Msg:  err.Error(),
		}
	}

	return StoreResponse{
		Code: 200,
		Data: OperationResult{MessageID: messageID},
		Msg:  "message added successfully",
	}
}

// NewListResponse 创建列表查询响应
func NewListResponse(messages []Message, total int64, err error) StoreResponse {
	if err != nil {
		return StoreResponse{
			Code: 500,
			Data: nil,
			Msg:  err.Error(),
		}
	}

	return StoreResponse{
		Code: 200,
		Data: ListResult{Messages: messages, Total: total},
		Msg:  "success",
	}
}

// NewOperationResponse 创建操作响应（标记已读/删除）
func NewOperationResponse(affectedCount int, err error) StoreResponse {
	if err != nil {
		return StoreResponse{
			Code: 500,
			Data: nil,
			Msg:  err.Error(),
		}
	}

	return StoreResponse{
		Code: 200,
		Data: OperationResult{AffectedCount: affectedCount},
		Msg:  "operation completed successfully",
	}
}
