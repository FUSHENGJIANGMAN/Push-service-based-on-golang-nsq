package status

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// ==================== 常量定义 ====================

const (
	channelSMS     = "sms"
	channelVoice   = "voice"
	channelEmail   = "email"
	channelWeb     = "web"
	channelUnknown = "unknown"

	defaultTTL = 24 * time.Hour

	redisKeyStatusFormat        = "msg_status:%s"
	redisKeyPendingFormat       = "pending_msgs:%s"
	redisKeyStatusHistoryFormat = "msg_status_history:%s"

	messageIDPrefixEmail = "Email_"
	messageIDPrefixSMS   = "sms_"
	messageIDPrefixVoice = "voice_"
	messageIDPrefixWeb   = "web_"
)

// ==================== 数据结构 ====================

// MessageStatus 消息状态
type MessageStatus struct {
	MessageID string `json:"message_id"`
	Channel   string `json:"channel"`
	Phone     string `json:"phone,omitempty"`
	Content   string `json:"content,omitempty"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

// StatusStore 状态存储接口
type StatusStore interface {
	SaveStatus(ctx context.Context, status *MessageStatus) error
	GetStatus(ctx context.Context, messageID string) (*MessageStatus, error)
	UpdateStatus(ctx context.Context, messageID string, newStatus string, error string) error
	GetPendingStatuses(ctx context.Context, channels []string) ([]*MessageStatus, error)
	CleanupOldStatuses(ctx context.Context, olderThan time.Duration) error
}

// RedisStatusStore Redis 状态存储实现
type RedisStatusStore struct {
	client *redis.Client
	ttl    time.Duration
}

// ==================== 构造函数 ====================

// NewRedisStatusStore 创建 Redis 状态存储实例
func NewRedisStatusStore(client *redis.Client, ttl time.Duration) *RedisStatusStore {
	if ttl == 0 {
		ttl = defaultTTL
	}

	return &RedisStatusStore{
		client: client,
		ttl:    ttl,
	}
}

// ==================== 核心方法 ====================

// SaveStatus 保存消息状态
func (store *RedisStatusStore) SaveStatus(ctx context.Context, status *MessageStatus) error {
	if err := store.validateMessageID(status.MessageID); err != nil {
		return err
	}

	statusKey := store.buildStatusKey(status.MessageID)
	if err := store.saveStatusToRedis(ctx, statusKey, status); err != nil {
		return err
	}

	store.addToPendingSetIfNeeded(ctx, status)
	store.logStatusSaved(status)

	return nil
}

// GetStatus 获取消息状态
func (store *RedisStatusStore) GetStatus(ctx context.Context, messageID string) (*MessageStatus, error) {
	statusKey := store.buildStatusKey(messageID)
	return store.fetchStatusFromRedis(ctx, statusKey)
}

// GetStatusHistory 获取消息状态历史
func (store *RedisStatusStore) GetStatusHistory(ctx context.Context, messageID string) ([]*MessageStatus, error) {
	historyKey := store.buildHistoryKey(messageID)
	return store.fetchStatusHistory(ctx, historyKey)
}

// UpdateStatus 更新消息状态
func (store *RedisStatusStore) UpdateStatus(
	ctx context.Context,
	messageID string,
	newStatus string,
	errorMessage string,
) error {
	existingStatus, err := store.getOrCreateStatus(ctx, messageID, newStatus, errorMessage)
	if err != nil {
		return err
	}

	store.updateStatusFields(existingStatus, newStatus, errorMessage)

	if err := store.SaveStatus(ctx, existingStatus); err != nil {
		return fmt.Errorf("failed to save updated status: %w", err)
	}

	store.appendStatusHistory(ctx, messageID, existingStatus)
	store.removeFromPendingSetIfNeeded(ctx, messageID, existingStatus.Channel, newStatus)

	return nil
}

// GetPendingStatuses 获取指定通道的待处理状态
func (store *RedisStatusStore) GetPendingStatuses(ctx context.Context, channels []string) ([]*MessageStatus, error) {
	var allStatuses []*MessageStatus

	for _, channel := range channels {
		statuses := store.fetchPendingStatusesForChannel(ctx, channel)
		allStatuses = append(allStatuses, statuses...)
	}

	return allStatuses, nil
}

// CleanupOldStatuses 清理过期状态
func (store *RedisStatusStore) CleanupOldStatuses(ctx context.Context, olderThan time.Duration) error {
	channels := []string{channelSMS, channelVoice}
	cutoffTimestamp := time.Now().Add(-olderThan).Unix()

	for _, channel := range channels {
		store.cleanupChannelPendingSet(ctx, channel, cutoffTimestamp)
	}

	return nil
}

// ==================== 私有方法 - Key 构建 ====================

func (store *RedisStatusStore) buildStatusKey(messageID string) string {
	return fmt.Sprintf(redisKeyStatusFormat, messageID)
}

func (store *RedisStatusStore) buildPendingKey(channel string) string {
	return fmt.Sprintf(redisKeyPendingFormat, channel)
}

func (store *RedisStatusStore) buildHistoryKey(messageID string) string {
	return fmt.Sprintf(redisKeyStatusHistoryFormat, messageID)
}

// ==================== 私有方法 - 验证 ====================

func (store *RedisStatusStore) validateMessageID(messageID string) error {
	if messageID == "" {
		return fmt.Errorf("message_id is required")
	}
	return nil
}

// ==================== 私有方法 - Redis 操作 ====================

func (store *RedisStatusStore) saveStatusToRedis(ctx context.Context, key string, status *MessageStatus) error {
	statusJSON, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("failed to marshal status: %w", err)
	}

	if err := store.client.Set(ctx, key, statusJSON, store.ttl).Err(); err != nil {
		return fmt.Errorf("failed to save status to redis: %w", err)
	}

	return nil
}

func (store *RedisStatusStore) fetchStatusFromRedis(ctx context.Context, key string) (*MessageStatus, error) {
	data, err := store.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get status from redis: %w", err)
	}

	return store.parseStatusJSON(data)
}

func (store *RedisStatusStore) fetchStatusHistory(ctx context.Context, key string) ([]*MessageStatus, error) {
	dataList, err := store.client.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get status history from redis: %w", err)
	}

	return store.parseStatusHistoryList(dataList)
}

// ==================== 私有方法 - 待处理集合管理 ====================

func (store *RedisStatusStore) addToPendingSetIfNeeded(ctx context.Context, status *MessageStatus) {
	if status.Status != statusPending {
		return
	}

	pendingKey := store.buildPendingKey(status.Channel)
	store.client.SAdd(ctx, pendingKey, status.MessageID)
	store.client.Expire(ctx, pendingKey, store.ttl)
}

func (store *RedisStatusStore) removeFromPendingSetIfNeeded(ctx context.Context, messageID, channel, newStatus string) {
	if newStatus == statusPending {
		return
	}

	pendingKey := store.buildPendingKey(channel)
	store.client.SRem(ctx, pendingKey, messageID)
}

func (store *RedisStatusStore) fetchPendingStatusesForChannel(ctx context.Context, channel string) []*MessageStatus {
	pendingKey := store.buildPendingKey(channel)

	messageIDs, err := store.client.SMembers(ctx, pendingKey).Result()
	if err != nil {
		log.Printf("[StatusStore] Failed to get pending messages (%s): %v", channel, err)
		return []*MessageStatus{}
	}

	return store.fetchStatusesByIDs(ctx, messageIDs)
}

func (store *RedisStatusStore) fetchStatusesByIDs(ctx context.Context, messageIDs []string) []*MessageStatus {
	var statuses []*MessageStatus

	for _, messageID := range messageIDs {
		status, err := store.GetStatus(ctx, messageID)
		if err != nil {
			log.Printf("[StatusStore] Failed to get message status (%s): %v", messageID, err)
			continue
		}

		if status != nil && status.Status == statusPending {
			statuses = append(statuses, status)
		}
	}

	return statuses
}

func (store *RedisStatusStore) cleanupChannelPendingSet(ctx context.Context, channel string, cutoffTimestamp int64) {
	pendingKey := store.buildPendingKey(channel)

	messageIDs, err := store.client.SMembers(ctx, pendingKey).Result()
	if err != nil {
		return
	}

	for _, messageID := range messageIDs {
		if store.shouldRemoveFromPendingSet(ctx, messageID, cutoffTimestamp) {
			store.client.SRem(ctx, pendingKey, messageID)
		}
	}
}

func (store *RedisStatusStore) shouldRemoveFromPendingSet(ctx context.Context, messageID string, cutoffTimestamp int64) bool {
	status, err := store.GetStatus(ctx, messageID)
	return err != nil || status == nil || status.CreatedAt < cutoffTimestamp
}

// ==================== 私有方法 - 状态处理 ====================

func (store *RedisStatusStore) getOrCreateStatus(
	ctx context.Context,
	messageID string,
	newStatus string,
	errorMessage string,
) (*MessageStatus, error) {
	existingStatus, err := store.GetStatus(ctx, messageID)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing status: %w", err)
	}

	if existingStatus != nil {
		return existingStatus, nil
	}

	return store.createNewStatus(messageID, newStatus, errorMessage), nil
}

func (store *RedisStatusStore) createNewStatus(messageID, newStatus, errorMessage string) *MessageStatus {
	now := time.Now().Unix()
	status := &MessageStatus{
		MessageID: messageID,
		Status:    newStatus,
		Error:     errorMessage,
		CreatedAt: now,
		UpdatedAt: now,
	}

	status.Channel = store.detectChannelFromMessageID(messageID)
	return status
}

func (store *RedisStatusStore) updateStatusFields(status *MessageStatus, newStatus, errorMessage string) {
	status.Status = newStatus
	status.Error = errorMessage
	status.UpdatedAt = time.Now().Unix()
}

func (store *RedisStatusStore) appendStatusHistory(ctx context.Context, messageID string, status *MessageStatus) {
	historyKey := store.buildHistoryKey(messageID)
	statusJSON, _ := json.Marshal(status)

	store.client.RPush(ctx, historyKey, statusJSON)
	store.client.Expire(ctx, historyKey, store.ttl)
}

// ==================== 私有方法 - 通道检测 ====================

func (store *RedisStatusStore) detectChannelFromMessageID(messageID string) string {
	if len(messageID) >= 6 && messageID[:6] == messageIDPrefixEmail {
		return channelEmail
	}
	if len(messageID) >= 4 && messageID[:4] == messageIDPrefixSMS {
		return channelSMS
	}
	if len(messageID) >= 6 && messageID[:6] == messageIDPrefixVoice {
		return channelVoice
	}
	if len(messageID) >= 4 && messageID[:4] == messageIDPrefixWeb {
		return channelWeb
	}
	return channelUnknown
}

// ==================== 私有方法 - JSON 解析 ====================

func (store *RedisStatusStore) parseStatusJSON(data string) (*MessageStatus, error) {
	var status MessageStatus
	if err := json.Unmarshal([]byte(data), &status); err != nil {
		return nil, fmt.Errorf("failed to unmarshal status: %w", err)
	}
	return &status, nil
}

func (store *RedisStatusStore) parseStatusHistoryList(dataList []string) ([]*MessageStatus, error) {
	var history []*MessageStatus

	for _, data := range dataList {
		var status MessageStatus
		if err := json.Unmarshal([]byte(data), &status); err == nil {
			history = append(history, &status)
		}
	}

	return history, nil
}

// ==================== 私有方法 - 日志 ====================

func (store *RedisStatusStore) logStatusSaved(status *MessageStatus) {
	log.Printf("[StatusStore] Status saved: %s -> %s (%s)",
		status.MessageID, status.Status, status.Channel)
}

// ==================== 工具函数 ====================

// GenerateMessageID 生成唯一的消息 ID
func GenerateMessageID() string {
	return fmt.Sprintf("msg_%d_%d", time.Now().UnixNano(), generateRandomInt())
}

func generateRandomInt() int64 {
	var buffer [8]byte
	_, _ = rand.Read(buffer[:])
	return int64(binary.LittleEndian.Uint64(buffer[:]))
}
