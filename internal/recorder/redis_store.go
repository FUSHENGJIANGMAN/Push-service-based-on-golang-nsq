package recorder

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"push-gateway/internal/push"

	redis "github.com/redis/go-redis/v9"
)

// ==================== 常量定义 ====================

const (
	defaultQueryLimit = 50
	redisKeyFormat    = "%s:record:%s"
	redisTimesKey     = "%s:record:times"
	redisSeqKey       = "%s:record:seq"
)

// ==================== 数据结构 ====================

// RedisStore Redis 存储实现
type RedisStore struct {
	client         *redis.Client
	namespace      string
	maxKeepRecords int64
	ttl            time.Duration
	timeProvider   func() time.Time
}

// NewRedisStore 创建 Redis 存储实例
func NewRedisStore(client *redis.Client, namespace string, maxKeep int64, ttl time.Duration) *RedisStore {
	return &RedisStore{
		client:         client,
		namespace:      namespace,
		maxKeepRecords: maxKeep,
		ttl:            ttl,
		timeProvider:   nil,
	}
}

// SetTimeProvider 设置时间提供函数（主要用于测试）
func (store *RedisStore) SetTimeProvider(provider func() time.Time) {
	store.timeProvider = provider
}

// Response 统一响应格式
type Response struct {
	Code int         `json:"code"`
	Data interface{} `json:"data"`
	Msg  string      `json:"msg"`
}

// recordHashData Hash 存储的记录数据
type recordHashData map[string]string

// ==================== Lua 脚本 ====================

var trimRecordsScript = redis.NewScript(`
local sortedSetKey = KEYS[1]
local maxKeepCount = tonumber(ARGV[1])
if maxKeepCount <= 0 then return 0 end

local totalCount = redis.call("ZCARD", sortedSetKey)
if totalCount <= maxKeepCount then return 0 end

local excessCount = totalCount - maxKeepCount
local oldRecordKeys = redis.call("ZRANGE", sortedSetKey, 0, excessCount - 1)

for i, recordKey in ipairs(oldRecordKeys) do
  redis.call("DEL", recordKey)
end

redis.call("ZREMRANGEBYRANK", sortedSetKey, 0, excessCount - 1)
return excessCount
`)

// ==================== 核心方法 ====================

// SaveRecord 保存推送记录到 Redis
func (store *RedisStore) SaveRecord(ctx context.Context, record push.Record) error {
	storageKey, err := store.generateStorageKey(ctx, record.Key)
	if err != nil {
		return fmt.Errorf("generate storage key failed: %w", err)
	}

	createdTimestamp := store.getCreatedTimestamp(record.CreatedAt)
	hashKey := store.buildRecordHashKey(storageKey)
	hashData := store.buildHashData(record, storageKey, createdTimestamp)

	return store.saveRecordWithPipeline(ctx, hashKey, hashData, createdTimestamp)
}

// Trim 清理超出限制的旧记录
func (store *RedisStore) Trim(ctx context.Context) (int, error) {
	if store.maxKeepRecords <= 0 {
		return 0, nil
	}

	deletedCount, err := trimRecordsScript.Run(
		ctx,
		store.client,
		[]string{store.buildTimesKey()},
		store.maxKeepRecords,
	).Int()

	if err != nil {
		return 0, fmt.Errorf("trim records failed: %w", err)
	}

	return deletedCount, nil
}

// QueryRecords 查询推送记录
func (store *RedisStore) QueryRecords(ctx context.Context, namespace string, limit int64) ([]push.Record, error) {
	limit = store.normalizeQueryLimit(limit)
	sortedSetKey := store.buildQueryKey(namespace)

	recordKeys, err := store.fetchRecordKeys(ctx, sortedSetKey, limit)
	if err != nil {
		return nil, err
	}

	return store.fetchRecords(ctx, recordKeys, namespace)
}

// GetTotalRecords 获取总记录数
func (store *RedisStore) GetTotalRecords(ctx context.Context, namespace string) (int64, error) {
	sortedSetKey := store.buildQueryKey(namespace)

	count, err := store.client.ZCard(ctx, sortedSetKey).Result()
	if err != nil {
		return 0, fmt.Errorf("get total records count failed: %w", err)
	}

	return count, nil
}

// ==================== 私有辅助方法 - Key 生成 ====================

func (store *RedisStore) buildRecordHashKey(id string) string {
	return fmt.Sprintf(redisKeyFormat, store.namespace, id)
}

func (store *RedisStore) buildTimesKey() string {
	return fmt.Sprintf(redisTimesKey, store.namespace)
}

func (store *RedisStore) buildSequenceKey() string {
	return fmt.Sprintf(redisSeqKey, store.namespace)
}

func (store *RedisStore) buildQueryKey(namespace string) string {
	if namespace != "" {
		return fmt.Sprintf(redisTimesKey, namespace)
	}
	return store.buildTimesKey()
}

// ==================== 私有辅助方法 - 存储逻辑 ====================

func (store *RedisStore) generateStorageKey(ctx context.Context, providedKey string) (string, error) {
	if providedKey != "" {
		return providedKey, nil
	}

	sequenceID, err := store.client.Incr(ctx, store.buildSequenceKey()).Result()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%d", sequenceID), nil
}

func (store *RedisStore) getCreatedTimestamp(recordCreatedAt int64) int64 {
	if recordCreatedAt != 0 {
		return recordCreatedAt
	}

	timeFunc := store.timeProvider
	if timeFunc == nil {
		timeFunc = time.Now
	}

	return timeFunc().Unix()
}

func (store *RedisStore) buildHashData(record push.Record, storageKey string, createdAt int64) recordHashData {
	messageDataJSON, _ := json.Marshal(record.MessageData)
	channelsJSON, _ := json.Marshal(record.Channels)
	recipientsJSON, _ := json.Marshal(record.Recipients)

	return recordHashData{
		"key":          storageKey,
		"namespace":    record.Namespace,
		"kind":         string(record.Kind),
		"subject":      record.Subject,
		"body":         record.Body,
		"message_data": string(messageDataJSON),
		"channels":     string(channelsJSON),
		"recipients":   string(recipientsJSON),
		"status":       record.Status,
		"code":         record.Code,
		"content":      record.Content,
		"error_detail": record.ErrorDetail,
		"priority":     fmt.Sprintf("%d", record.Priority),
		"created_at":   fmt.Sprintf("%d", createdAt),
		"sent_at":      fmt.Sprintf("%d", record.SentAt),
	}
}

func (store *RedisStore) saveRecordWithPipeline(ctx context.Context, hashKey string, data recordHashData, timestamp int64) error {
	pipeline := store.client.TxPipeline()

	pipeline.HSet(ctx, hashKey, data)

	if store.ttl > 0 {
		pipeline.Expire(ctx, hashKey, store.ttl)
	}

	pipeline.ZAdd(ctx, store.buildTimesKey(), redis.Z{
		Score:  float64(timestamp),
		Member: hashKey,
	})

	_, err := pipeline.Exec(ctx)
	if err != nil {
		return fmt.Errorf("save record pipeline failed: %w", err)
	}

	return nil
}

// ==================== 私有辅助方法 - 查询逻辑 ====================

func (store *RedisStore) normalizeQueryLimit(limit int64) int64 {
	if limit <= 0 {
		return defaultQueryLimit
	}
	return limit
}

func (store *RedisStore) fetchRecordKeys(ctx context.Context, sortedSetKey string, limit int64) ([]string, error) {
	recordKeys, err := store.client.ZRevRange(ctx, sortedSetKey, 0, limit-1).Result()
	if err != nil {
		return nil, fmt.Errorf("fetch record keys failed: %w", err)
	}
	return recordKeys, nil
}

func (store *RedisStore) fetchRecords(ctx context.Context, recordKeys []string, filterNamespace string) ([]push.Record, error) {
	records := make([]push.Record, 0, len(recordKeys))

	for _, recordKey := range recordKeys {
		record, err := store.fetchSingleRecord(ctx, recordKey, filterNamespace)
		if err != nil {
			continue
		}

		if record != nil {
			records = append(records, *record)
		}
	}

	return records, nil
}

func (store *RedisStore) fetchSingleRecord(ctx context.Context, recordKey string, filterNamespace string) (*push.Record, error) {
	hashData, err := store.client.HGetAll(ctx, recordKey).Result()
	if err != nil || len(hashData) == 0 {
		return nil, err
	}

	if !store.shouldIncludeRecord(hashData, filterNamespace) {
		return nil, nil
	}

	record := store.parseRecordFromHash(hashData)
	return &record, nil
}

func (store *RedisStore) shouldIncludeRecord(hashData map[string]string, filterNamespace string) bool {
	if filterNamespace == "" {
		return true
	}

	recordNamespace, exists := hashData["namespace"]
	return exists && recordNamespace == filterNamespace
}

// ==================== 私有辅助方法 - 数据解析 ====================

func (store *RedisStore) parseRecordFromHash(data map[string]string) push.Record {
	record := push.Record{
		Key:         data["key"],
		Namespace:   data["namespace"],
		Kind:        push.MessageKind(data["kind"]),
		Subject:     data["subject"],
		Body:        data["body"],
		Status:      data["status"],
		Code:        data["code"],
		Content:     data["content"],
		ErrorDetail: data["error_detail"],
	}

	store.parseJSONFields(&record, data)
	store.parseNumericFields(&record, data)

	return record
}

func (store *RedisStore) parseJSONFields(record *push.Record, data map[string]string) {
	if messageDataJSON := data["message_data"]; messageDataJSON != "" {
		var messageData map[string]interface{}
		if json.Unmarshal([]byte(messageDataJSON), &messageData) == nil {
			record.MessageData = messageData
		}
	}

	if channelsJSON := data["channels"]; channelsJSON != "" {
		var channels []string
		if json.Unmarshal([]byte(channelsJSON), &channels) == nil {
			record.Channels = make([]push.Channel, len(channels))
			for i, ch := range channels {
				record.Channels[i] = push.Channel(ch)
			}
		}
	}

	if recipientsJSON := data["recipients"]; recipientsJSON != "" {
		var recipients []string
		if json.Unmarshal([]byte(recipientsJSON), &recipients) == nil {
			record.Recipients = make([]push.Recipient, len(recipients))
			for i, recipient := range recipients {
				record.Recipients[i] = push.Recipient{UserID: recipient}
			}
		}
	}
}

func (store *RedisStore) parseNumericFields(record *push.Record, data map[string]string) {
	if createdAtStr := data["created_at"]; createdAtStr != "" {
		if timestamp, err := strconv.ParseInt(createdAtStr, 10, 64); err == nil {
			record.CreatedAt = timestamp
		}
	}

	if sentAtStr := data["sent_at"]; sentAtStr != "" {
		if timestamp, err := strconv.ParseInt(sentAtStr, 10, 64); err == nil {
			record.SentAt = timestamp
		}
	}

	if priorityStr := data["priority"]; priorityStr != "" {
		if priority, err := strconv.Atoi(priorityStr); err == nil {
			record.Priority = priority
		}
	}
}
