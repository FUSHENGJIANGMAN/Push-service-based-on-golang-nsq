// Package idempotency 提供基于 Redis + MySQL 的混合幂等性检查功能
// 采用 Redis 作为主要存储以保证高性能,MySQL 作为持久化备份以保证可靠性
package idempotency

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"push-gateway/internal/asyncsync"
	"push-gateway/internal/database"
	"push-gateway/internal/push"
)

// ==================== 常量定义 ====================

const (
	tableIdempotencyRecords = "idempotency_records"
	syncOperationInsert     = "insert"
	keyDelimiter            = "|"
)

// ==================== 错误定义 ====================

var (
	// ErrDatabaseUnavailable 数据库不可用错误
	ErrDatabaseUnavailable = errors.New("database is not available")

	// ErrMarshalFailed 序列化失败错误
	ErrMarshalFailed = errors.New("failed to marshal delivery data")

	// ErrInsertFailed 插入记录失败错误
	ErrInsertFailed = errors.New("failed to insert idempotency record")
)

// ==================== 模型定义 ====================

// CheckResult 幂等性检查结果
type CheckResult struct {
	IsNewRequest bool   `json:"isNewRequest"` // 是否为新请求
	KeyHash      string `json:"keyHash"`      // 请求的哈希键
}

// Response 统一响应结构
type Response struct {
	Code int         `json:"code"` // 响应码
	Data interface{} `json:"data"` // 响应数据
	Msg  string      `json:"msg"`  // 响应消息
}

// IdempotencyRecord 幂等性记录模型
type IdempotencyRecord struct {
	KeyHash      string    `json:"keyHash"`
	Namespace    string    `json:"namespace"`
	DeliveryData string    `json:"deliveryData"`
	CreatedAt    time.Time `json:"createdAt"`
	ExpiresAt    time.Time `json:"expiresAt"`
}

// ==================== 核心服务 ====================

// HybridChecker 混合幂等性检查器
// 结合 Redis 的高性能和 MySQL 的持久化优势,提供可靠的幂等性保证
type HybridChecker struct {
	redisChecker *RedisChecker
	mysqlDB      *database.MySQLDB
	syncManager  *asyncsync.Manager
}

// NewHybridChecker 创建混合幂等性检查器实例
func NewHybridChecker(
	redisChecker *RedisChecker,
	mysqlDB *database.MySQLDB,
	syncManager *asyncsync.Manager,
) *HybridChecker {
	return &HybridChecker{
		redisChecker: redisChecker,
		mysqlDB:      mysqlDB,
		syncManager:  syncManager,
	}
}

// CheckAndSet 检查并设置幂等性
// 采用 Redis 优先策略,失败时自动降级到 MySQL 以保证服务可用性
func (checker *HybridChecker) CheckAndSet(
	ctx context.Context,
	delivery push.Delivery,
	ttl time.Duration,
) (bool, string, error) {
	isNewRequest, keyHash, err := checker.redisChecker.CheckAndSet(ctx, delivery, ttl)
	if err != nil {
		log.Printf("[HYBRID_IDEMPOTENCY] Redis unavailable, degrading to MySQL: %v", err)
		return checker.checkAndSetInMySQL(ctx, delivery, ttl)
	}

	// 仅在新请求时才异步同步到 MySQL,避免不必要的写操作
	if isNewRequest {
		checker.asyncSyncToMySQL(keyHash, delivery, ttl)
	}

	return isNewRequest, keyHash, nil
}

// CleanupExpiredRecords 清理过期的幂等性记录
// 定期清理可以防止数据库膨胀,建议每小时执行一次
func (checker *HybridChecker) CleanupExpiredRecords(ctx context.Context) error {
	if checker.mysqlDB == nil {
		return nil
	}

	deletedCount, err := checker.deleteExpiredRecords()
	if err != nil {
		return err
	}

	log.Printf("[HYBRID_IDEMPOTENCY] Successfully cleaned up %d expired records", deletedCount)
	return nil
}

// ==================== 私有方法：异步同步 ====================

// asyncSyncToMySQL 异步同步幂等性记录到 MySQL
// 使用异步方式避免阻塞主请求链路,即使同步失败也不影响业务
func (checker *HybridChecker) asyncSyncToMySQL(
	keyHash string,
	delivery push.Delivery,
	ttl time.Duration,
) {
	if checker.syncManager == nil {
		return
	}

	now := time.Now()
	syncData := checker.buildSyncData(keyHash, delivery, now, ttl)

	err := checker.syncManager.AddRecord(
		tableIdempotencyRecords,
		syncOperationInsert,
		keyHash,
		syncData,
	)

	if err != nil {
		log.Printf("[HYBRID_IDEMPOTENCY] Async sync to MySQL failed: %v", err)
	}
}

// buildSyncData 构建异步同步数据
func (checker *HybridChecker) buildSyncData(
	keyHash string,
	delivery push.Delivery,
	now time.Time,
	ttl time.Duration,
) map[string]interface{} {
	return map[string]interface{}{
		"key_hash":      keyHash,
		"namespace":     checker.redisChecker.Namespace,
		"delivery_data": delivery,
		"created_at":    now.Unix(),
		"expires_at":    now.Add(ttl).Unix(),
	}
}

// ==================== 私有方法：MySQL 降级处理 ====================

// checkAndSetInMySQL 在 MySQL 中检查并设置幂等性
// 作为 Redis 的降级方案,确保在缓存故障时业务仍可继续
func (checker *HybridChecker) checkAndSetInMySQL(
	ctx context.Context,
	delivery push.Delivery,
	ttl time.Duration,
) (bool, string, error) {
	if checker.mysqlDB == nil {
		return false, "", ErrDatabaseUnavailable
	}

	keyHash := checker.generateKeyHash(delivery)

	exists, err := checker.checkRecordExists(keyHash)
	if err != nil {
		return false, "", fmt.Errorf("check existence failed: %w", err)
	}

	// 使用卫语句提前返回,减少嵌套
	if exists {
		return false, keyHash, nil
	}

	inserted, err := checker.insertIdempotencyRecord(keyHash, delivery, ttl)
	if err != nil {
		return false, "", err
	}

	return inserted, keyHash, nil
}

// checkRecordExists 检查幂等性记录是否已存在且未过期
// 通过查询数据库判断是否为重复请求
func (checker *HybridChecker) checkRecordExists(keyHash string) (bool, error) {
	query := `
		SELECT COUNT(*) 
		FROM idempotency_records 
		WHERE key_hash = ? AND expires_at > ?
	`

	var count int
	err := checker.mysqlDB.QueryRow(query, keyHash, time.Now().Unix()).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("query record existence failed: %w", err)
	}

	return count > 0, nil
}

// insertIdempotencyRecord 插入新的幂等性记录
// 使用数据库约束处理并发插入,返回是否为新记录
func (checker *HybridChecker) insertIdempotencyRecord(
	keyHash string,
	delivery push.Delivery,
	ttl time.Duration,
) (bool, error) {
	deliveryJSON, err := json.Marshal(delivery)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrMarshalFailed, err)
	}

	now := time.Now()
	query := `
		INSERT INTO idempotency_records 
		(key_hash, namespace, delivery_data, created_at, expires_at) 
		VALUES (?, ?, ?, ?, ?)
	`

	_, err = checker.mysqlDB.Exec(
		query,
		keyHash,
		checker.redisChecker.Namespace,
		string(deliveryJSON),
		now.Unix(),
		now.Add(ttl).Unix(),
	)

	// 插入失败时,可能是唯一键冲突(并发场景),需要二次确认
	if err != nil {
		return checker.handleInsertConflict(keyHash, err)
	}

	return true, nil
}

// handleInsertConflict 处理插入冲突
// 在并发场景下,插入失败可能是因为其他请求已插入,需要再次确认
func (checker *HybridChecker) handleInsertConflict(
	keyHash string,
	insertError error,
) (bool, error) {
	exists, checkErr := checker.checkRecordExists(keyHash)
	if checkErr != nil {
		return false, fmt.Errorf("%w: %v, recheck failed: %v", ErrInsertFailed, insertError, checkErr)
	}

	// 如果记录已存在,说明是并发插入导致的冲突,这是正常的
	if exists {
		return false, nil
	}

	// 记录不存在但插入失败,这是异常情况
	return false, fmt.Errorf("%w: %v", ErrInsertFailed, insertError)
}

// ==================== 私有方法：工具函数 ====================

// generateKeyHash 生成请求的唯一哈希标识
// 使用 SHA1 算法对消息的关键字段进行哈希,保证相同请求生成相同的标识
func (checker *HybridChecker) generateKeyHash(delivery push.Delivery) string {
	content := checker.buildHashContent(delivery)
	hash := sha1.Sum([]byte(content))
	return hex.EncodeToString(hash[:])
}

// buildHashContent 构建用于哈希的内容字符串
func (checker *HybridChecker) buildHashContent(delivery push.Delivery) string {
	return strings.Join([]string{
		string(delivery.Msg.Kind),
		delivery.Msg.Subject,
		delivery.Msg.Body,
	}, keyDelimiter)
}

// deleteExpiredRecords 执行过期记录删除操作
func (checker *HybridChecker) deleteExpiredRecords() (int64, error) {
	query := "DELETE FROM idempotency_records WHERE expires_at < ?"

	result, err := checker.mysqlDB.Exec(query, time.Now().Unix())
	if err != nil {
		return 0, fmt.Errorf("delete expired records failed: %w", err)
	}

	affectedRows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get affected rows failed: %w", err)
	}

	return affectedRows, nil
}

// ==================== 辅助函数：响应构建 ====================

// NewSuccessResponse 创建成功响应
func NewSuccessResponse(data interface{}) Response {
	return Response{
		Code: 200,
		Data: data,
		Msg:  "success",
	}
}

// NewErrorResponse 创建错误响应
func NewErrorResponse(code int, msg string) Response {
	return Response{
		Code: code,
		Data: nil,
		Msg:  msg,
	}
}

// NewCheckResultResponse 创建幂等性检查结果响应
func NewCheckResultResponse(isNewRequest bool, keyHash string) Response {
	result := CheckResult{
		IsNewRequest: isNewRequest,
		KeyHash:      keyHash,
	}
	return NewSuccessResponse(result)
}
