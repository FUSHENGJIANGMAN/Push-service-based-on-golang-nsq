// Package idempotency 提供多种幂等性检查实现
// 支持基于 Redis 的高性能幂等性检查,防止消息重复推送
package idempotency

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"push-gateway/internal/push"

	redis "github.com/redis/go-redis/v9"
)

// ==================== 常量定义 ====================

const (
	keySeparator          = ":"
	idempotencyPrefix     = "idemp"
	redisPlaceholderValue = "1"
	contentDelimiter      = "|"
)

// ==================== 错误定义 ====================

var (
	// ErrRedisSetFailed Redis 设置失败错误
	ErrRedisSetFailed = errors.New("failed to set idempotency key in redis")

	// ErrEmptyRecipient 收件人为空错误
	ErrEmptyRecipient = errors.New("no recipients found in delivery")
)

// ==================== 接口定义 ====================

// Checker 幂等性检查器接口
// 定义统一的幂等性检查行为,支持多种实现方式(Redis/MySQL/Hybrid)
type Checker interface {
	// CheckAndSet 检查并设置幂等性标记
	// 返回值: isNewRequest(true=新请求, false=重复请求), keyHash(唯一标识), error
	CheckAndSet(
		ctx context.Context,
		delivery push.Delivery,
		ttl time.Duration,
	) (bool, string, error)
}

// ==================== Redis 实现 ====================

// RedisChecker 基于 Redis 的幂等性检查器
// 利用 Redis 的 SETNX 命令实现原子性的幂等性检查和设置
type RedisChecker struct {
	client    *redis.Client
	Namespace string // 命名空间,用于隔离不同服务的幂等性键
}

// NewRedisChecker 创建 Redis 幂等性检查器实例
func NewRedisChecker(client *redis.Client, namespace string) *RedisChecker {
	return &RedisChecker{
		client:    client,
		Namespace: namespace,
	}
}

// CheckAndSet 检查并设置幂等性
// 使用 Redis SETNX 命令确保原子性操作,在高并发场景下防止重复请求
func (checker *RedisChecker) CheckAndSet(
	ctx context.Context,
	delivery push.Delivery,
	ttl time.Duration,
) (bool, string, error) {
	key := checker.buildIdempotencyKey(delivery)

	isNewRequest, err := checker.setIdempotencyFlag(ctx, key, ttl)
	if err != nil {
		return false, key, err
	}

	return isNewRequest, key, nil
}

// ==================== 私有方法：键构建 ====================

// buildIdempotencyKey 构建幂等性键
// 格式: {namespace}:idemp:{kind}:{recipientID}_{contentHash}
// 通过组合收件人和内容哈希,确保同一消息不会重复发送给同一用户
func (checker *RedisChecker) buildIdempotencyKey(delivery push.Delivery) string {
	recipientID := checker.extractRecipientID(delivery)
	contentHash := checker.generateContentHash(delivery)

	return checker.formatIdempotencyKey(delivery.Msg.Kind, recipientID, contentHash)
}

// formatIdempotencyKey 格式化幂等性键
// 使用 Join 替代多次字符串拼接,提升性能并增强可读性
func (checker *RedisChecker) formatIdempotencyKey(
	kind push.MessageKind,
	recipientID string,
	contentHash string,
) string {
	parts := []string{
		checker.Namespace,
		idempotencyPrefix,
		string(kind),
		fmt.Sprintf("%s_%s", recipientID, contentHash),
	}

	return strings.Join(parts, keySeparator)
}

// ==================== 私有方法：收件人处理 ====================

// extractRecipientID 提取首个收件人的唯一标识
// 按优先级提取: Email > Phone > UserID > WebInbox
// 优先使用更稳定的标识符来保证幂等性的准确性
func (checker *RedisChecker) extractRecipientID(delivery push.Delivery) string {
	if len(delivery.Msg.To) == 0 {
		return ""
	}

	recipient := delivery.Msg.To[0]
	return checker.getFirstAvailableIdentifier(recipient)
}

// getFirstAvailableIdentifier 获取收件人的第一个可用标识符
// 使用卫语句按优先级依次返回,避免深层嵌套
func (checker *RedisChecker) getFirstAvailableIdentifier(recipient push.Recipient) string {
	if recipient.Email != "" {
		return recipient.Email
	}

	if recipient.Phone != "" {
		return recipient.Phone
	}

	if recipient.UserID != "" {
		return recipient.UserID
	}

	if recipient.WebInbox != "" {
		return recipient.WebInbox
	}

	return ""
}

// ==================== 私有方法：内容哈希 ====================

// generateContentHash 生成消息内容的哈希值
// 使用 SHA1 算法对消息的关键字段进行哈希,保证相同内容产生相同标识
func (checker *RedisChecker) generateContentHash(delivery push.Delivery) string {
	content := checker.buildHashContent(delivery)
	hash := sha1.Sum([]byte(content))
	return hex.EncodeToString(hash[:])
}

// buildHashContent 构建用于哈希的内容字符串
// 将消息的关键字段拼接为统一格式,确保哈希的一致性
func (checker *RedisChecker) buildHashContent(delivery push.Delivery) string {
	return strings.Join([]string{
		string(delivery.Msg.Kind),
		delivery.Msg.Subject,
		delivery.Msg.Body,
	}, contentDelimiter)
}

// ==================== 私有方法：Redis 操作 ====================

// setIdempotencyFlag 在 Redis 中设置幂等性标记
// 使用 SETNX 命令保证只有第一次设置会成功,从而实现分布式锁的效果
func (checker *RedisChecker) setIdempotencyFlag(
	ctx context.Context,
	key string,
	ttl time.Duration,
) (bool, error) {
	isNewRequest, err := checker.client.SetNX(ctx, key, redisPlaceholderValue, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrRedisSetFailed, err)
	}

	return isNewRequest, nil
}

// IdempotencyResponse 幂等性检查响应
type IdempotencyResponse struct {
	Code int                `json:"code"` // 响应码
	Data *IdempotencyResult `json:"data"` // 响应数据
	Msg  string             `json:"msg"`  // 响应消息
}

// IdempotencyResult 幂等性检查结果
type IdempotencyResult struct {
	IsNewRequest bool   `json:"isNewRequest"` // 是否为新请求
	KeyHash      string `json:"keyHash"`      // 请求的哈希键
}

// NewIdempotencyResponse 创建幂等性检查响应
func NewIdempotencyResponse(isNewRequest bool, keyHash string, err error) IdempotencyResponse {
	if err != nil {
		return IdempotencyResponse{
			Code: 500,
			Data: nil,
			Msg:  err.Error(),
		}
	}

	result := &IdempotencyResult{
		IsNewRequest: isNewRequest,
		KeyHash:      keyHash,
	}

	return IdempotencyResponse{
		Code: 200,
		Data: result,
		Msg:  "success",
	}
}

// NewDuplicateRequestResponse 创建重复请求响应
func NewDuplicateRequestResponse(keyHash string) IdempotencyResponse {
	result := &IdempotencyResult{
		IsNewRequest: false,
		KeyHash:      keyHash,
	}

	return IdempotencyResponse{
		Code: 200,
		Data: result,
		Msg:  "duplicate request detected",
	}
}

// NewNewRequestResponse 创建新请求响应
func NewNewRequestResponse(keyHash string) IdempotencyResponse {
	result := &IdempotencyResult{
		IsNewRequest: true,
		KeyHash:      keyHash,
	}

	return IdempotencyResponse{
		Code: 200,
		Data: result,
		Msg:  "new request accepted",
	}
}
