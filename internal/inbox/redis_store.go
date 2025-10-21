package inbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"
)

// ==================== 常量定义 ====================

const (
	keyPrefixInbox    = "inbox"
	keyPrefixSequence = "seq"
	keyPrefixMessage  = "msg"
	keyPrefixUser     = "user"
	keyPrefixZSet     = "z"
	keySeparator      = ":"

	defaultLimit = 20
	minOffset    = 0

	fieldID        = "id"
	fieldUserID    = "user_id"
	fieldKind      = "kind"
	fieldSubject   = "subject"
	fieldBody      = "body"
	fieldData      = "data"
	fieldCreatedAt = "created_at"
	fieldReadAt    = "read_at"
)

// ==================== 错误定义 ====================

var (
	// ErrEmptyUserID 用户ID为空错误
	ErrEmptyUserID = errors.New("user id cannot be empty")

	// ErrRedisOperationFailed Redis操作失败错误
	ErrRedisOperationFailed = errors.New("redis operation failed")

	// ErrMessageNotFound 消息不存在错误
	ErrMessageNotFound = errors.New("message not found")

	// ErrInvalidLimit 无效的查询限制错误
	ErrInvalidLimit = errors.New("invalid query limit")
)

// ==================== Lua 脚本定义 ====================

// trimUserScript 用户消息裁剪脚本
// 原子性地删除超过上限的旧消息,保证数据一致性
var trimUserScript = redis.NewScript(`
local zkey = KEYS[1]
local max  = tonumber(ARGV[1])
if max <= 0 then return 0 end
local total = redis.call("ZCARD", zkey)
if total <= max then return 0 end
local over = total - max
local olds = redis.call("ZRANGE", zkey, 0, over-1)
for i,k in ipairs(olds) do
  redis.call("DEL", k)
end
redis.call("ZREMRANGEBYRANK", zkey, 0, over-1)
return over
`)

// markReadScript 标记已读脚本
// 批量标记消息为已读,确保操作的原子性
var markReadScript = redis.NewScript(`
local ns = ARGV[1]
local uid = ARGV[2]
local now = ARGV[3]
local updated = 0
for i=4,#ARGV do
  local hkey = ns .. ":inbox:msg:" .. ARGV[i]
  local real = redis.call("HGET", hkey, "user_id")
  if real and real == uid then
    redis.call("HSET", hkey, "read_at", now)
    updated = updated + 1
  end
end
return updated
`)

// deleteScript 删除消息脚本
// 批量删除消息并从用户的时间线中移除,保证数据一致性
var deleteScript = redis.NewScript(`
local ns = ARGV[1]
local uid = ARGV[2]
local deleted = 0
for i=3,#ARGV do
  local msgId = ARGV[i]
  local hkey = ns .. ":inbox:msg:" .. msgId
  local real = redis.call("HGET", hkey, "user_id")
  if real and real == uid then
    redis.call("DEL", hkey)
    local zkey = ns .. ":inbox:user:" .. uid .. ":z"
    redis.call("ZREM", zkey, hkey)
    deleted = deleted + 1
  end
end
return deleted
`)

// ==================== 核心服务 ====================

// RedisStore 基于 Redis 的消息存储
// 使用 Hash 存储消息详情,ZSet 维护用户时间线,实现高性能的消息管理
type RedisStore struct {
	client  *redis.Client
	options Options
	// timeProvider 时间源提供者,便于测试时注入 mock 时间
	timeProvider func() time.Time
}

// NewRedisStore 创建 Redis 存储实例
func NewRedisStore(client *redis.Client, options Options) *RedisStore {
	return &RedisStore{
		client:       client,
		options:      options,
		timeProvider: time.Now,
	}
}

// Add 添加消息
// 生成全局唯一ID,存储消息内容,并加入用户时间线
func (store *RedisStore) Add(ctx context.Context, message Message) (string, error) {
	if err := store.validateUserID(message.UserID); err != nil {
		return "", err
	}

	message.CreatedAt = store.ensureCreatedTime(message.CreatedAt)

	messageID, err := store.generateMessageID(ctx)
	if err != nil {
		return "", err
	}

	if err := store.saveMessage(ctx, messageID, message); err != nil {
		return "", err
	}

	// 裁剪操作不影响主流程,失败仅记录日志
	store.trimUserMessages(ctx, message.UserID)

	return messageID, nil
}

// List 查询消息列表
// 从用户的有序集合中分页查询消息,支持按状态过滤
func (store *RedisStore) List(
	ctx context.Context,
	userID string,
	status string,
	offset, limit int64,
) ([]Message, int64, error) {
	if err := store.validateUserID(userID); err != nil {
		return nil, 0, err
	}

	limit = store.normalizeLimit(limit)
	offset = store.normalizeOffset(offset)

	total, messageKeys, err := store.fetchUserMessageKeys(ctx, userID, offset, limit)
	if err != nil {
		return nil, 0, err
	}

	if len(messageKeys) == 0 {
		return []Message{}, total, nil
	}

	messages, err := store.fetchMessages(ctx, messageKeys, status)
	if err != nil {
		return nil, 0, err
	}

	return messages, total, nil
}

// MarkRead 批量标记消息为已读
// 使用 Lua 脚本保证原子性,防止并发问题
func (store *RedisStore) MarkRead(
	ctx context.Context,
	userID string,
	messageIDs []string,
) (int, error) {
	if err := store.validateUserID(userID); err != nil {
		return 0, err
	}

	if len(messageIDs) == 0 {
		return 0, nil
	}

	args := store.buildMarkReadArguments(userID, messageIDs)
	updatedCount, err := markReadScript.Run(ctx, store.client, nil, args...).Int()
	if err != nil {
		return 0, fmt.Errorf("%w: mark read: %v", ErrRedisOperationFailed, err)
	}

	return updatedCount, nil
}

// Delete 批量删除消息
// 使用 Lua 脚本保证原子性删除消息和时间线记录
func (store *RedisStore) Delete(
	ctx context.Context,
	userID string,
	messageIDs []string,
) (int, error) {
	if err := store.validateUserID(userID); err != nil {
		return 0, err
	}

	if len(messageIDs) == 0 {
		return 0, nil
	}

	args := store.buildDeleteArguments(userID, messageIDs)
	deletedCount, err := deleteScript.Run(ctx, store.client, nil, args...).Int()
	if err != nil {
		return 0, fmt.Errorf("%w: delete: %v", ErrRedisOperationFailed, err)
	}

	return deletedCount, nil
}

// TrimUser 裁剪用户消息
// 删除超过数量上限的旧消息,避免单个用户占用过多存储空间
func (store *RedisStore) TrimUser(ctx context.Context, userID string) (int, error) {
	if store.options.MaxPerUser <= 0 {
		return 0, nil
	}

	userZSetKey := store.buildUserZSetKey(userID)
	trimmedCount, err := trimUserScript.Run(
		ctx,
		store.client,
		[]string{userZSetKey},
		store.options.MaxPerUser,
	).Int()

	if err != nil {
		return 0, fmt.Errorf("%w: trim user: %v", ErrRedisOperationFailed, err)
	}

	return trimmedCount, nil
}

// Put 兼容旧接口
// 为了向后兼容而保留,新代码应使用 Add 方法
func (store *RedisStore) Put(ctx context.Context, userID string, message Message) error {
	message.UserID = userID
	_, err := store.Add(ctx, message)
	return err
}

// ==================== 私有方法：验证 ====================

// validateUserID 验证用户ID是否有效
func (store *RedisStore) validateUserID(userID string) error {
	if strings.TrimSpace(userID) == "" {
		return ErrEmptyUserID
	}
	return nil
}

// normalizeLimit 规范化查询限制
func (store *RedisStore) normalizeLimit(limit int64) int64 {
	if limit <= 0 {
		return defaultLimit
	}
	return limit
}

// normalizeOffset 规范化查询偏移
func (store *RedisStore) normalizeOffset(offset int64) int64 {
	if offset < minOffset {
		return minOffset
	}
	return offset
}

// ensureCreatedTime 确保消息有创建时间
func (store *RedisStore) ensureCreatedTime(createdAt int64) int64 {
	if createdAt == 0 {
		return store.getCurrentTimestamp()
	}
	return createdAt
}

// ==================== 私有方法：消息保存 ====================

// generateMessageID 生成全局唯一的消息ID
// 使用 Redis INCR 命令保证ID的唯一性和递增性
func (store *RedisStore) generateMessageID(ctx context.Context) (string, error) {
	sequenceKey := store.buildSequenceKey()
	id, err := store.client.Incr(ctx, sequenceKey).Result()
	if err != nil {
		return "", fmt.Errorf("%w: generate id: %v", ErrRedisOperationFailed, err)
	}
	return strconv.FormatInt(id, 10), nil
}

// saveMessage 保存消息到 Redis
// 使用 Pipeline 批量执行命令,提升性能
func (store *RedisStore) saveMessage(
	ctx context.Context,
	messageID string,
	message Message,
) error {
	messageKey := store.buildMessageKey(messageID)
	userZSetKey := store.buildUserZSetKey(message.UserID)

	pipeline := store.client.Pipeline()

	// 保存消息哈希
	hashFields := store.buildMessageHashFields(messageID, message)
	hashCmd := pipeline.HSet(ctx, messageKey, hashFields...)

	// 设置过期时间
	var expireCmd *redis.BoolCmd
	if store.options.TTL > 0 {
		expireCmd = pipeline.Expire(ctx, messageKey, store.options.TTL)
	}

	// 添加到用户时间线
	zaddCmd := pipeline.ZAdd(ctx, userZSetKey, redis.Z{
		Score:  float64(message.CreatedAt),
		Member: messageKey,
	})

	// 执行 Pipeline
	if _, err := pipeline.Exec(ctx); err != nil {
		store.logPipelineErrors(messageKey, userZSetKey, hashCmd, expireCmd, zaddCmd)
		return fmt.Errorf("%w: save message: %v", ErrRedisOperationFailed, err)
	}

	return nil
}

// buildMessageHashFields 构建消息的哈希字段
func (store *RedisStore) buildMessageHashFields(
	messageID string,
	message Message,
) []interface{} {
	fields := []interface{}{
		fieldID, messageID,
		fieldUserID, message.UserID,
		fieldKind, message.Kind,
		fieldSubject, message.Subject,
		fieldBody, message.Body,
		fieldCreatedAt, strconv.FormatInt(message.CreatedAt, 10),
		fieldReadAt, strconv.FormatInt(message.ReadAt, 10),
	}

	// 序列化 Data 字段（如果存在）
	if message.Data != nil {
		if dataBytes, err := json.Marshal(message.Data); err == nil {
			fields = append(fields, fieldData, string(dataBytes))
		}
	}

	return fields
}

// logPipelineErrors 记录 Pipeline 执行错误
func (store *RedisStore) logPipelineErrors(
	messageKey string,
	userZSetKey string,
	hashCmd *redis.IntCmd,
	expireCmd *redis.BoolCmd,
	zaddCmd *redis.IntCmd,
) {
	log.Printf("[REDIS_STORE] Pipeline execution failed (messageKey=%s, zsetKey=%s)",
		messageKey, userZSetKey)

	if err := hashCmd.Err(); err != nil {
		log.Printf("[REDIS_STORE] HSET failed: %v", err)
	}

	if expireCmd != nil {
		if err := expireCmd.Err(); err != nil {
			log.Printf("[REDIS_STORE] EXPIRE failed: %v", err)
		}
	}

	if err := zaddCmd.Err(); err != nil {
		log.Printf("[REDIS_STORE] ZADD failed: %v", err)
	}
}

// trimUserMessages 裁剪用户消息（不影响主流程）
func (store *RedisStore) trimUserMessages(ctx context.Context, userID string) {
	if _, err := store.TrimUser(ctx, userID); err != nil {
		log.Printf("[REDIS_STORE] Trim user messages failed: %v", err)
	}
}

// ==================== 私有方法：消息查询 ====================

// fetchUserMessageKeys 获取用户的消息键列表
func (store *RedisStore) fetchUserMessageKeys(
	ctx context.Context,
	userID string,
	offset, limit int64,
) (int64, []string, error) {
	userZSetKey := store.buildUserZSetKey(userID)

	total, err := store.client.ZCard(ctx, userZSetKey).Result()
	if err != nil {
		return 0, nil, fmt.Errorf("%w: get total count: %v", ErrRedisOperationFailed, err)
	}

	// 按创建时间倒序查询（最新的在前）
	messageKeys, err := store.client.ZRevRange(
		ctx,
		userZSetKey,
		offset,
		offset+limit-1,
	).Result()

	if err != nil {
		return 0, nil, fmt.Errorf("%w: fetch message keys: %v", ErrRedisOperationFailed, err)
	}

	return total, messageKeys, nil
}

// fetchMessages 批量获取消息详情
func (store *RedisStore) fetchMessages(
	ctx context.Context,
	messageKeys []string,
	status string,
) ([]Message, error) {
	messageHashes, err := store.batchFetchMessageHashes(ctx, messageKeys)
	if err != nil {
		return nil, err
	}

	messages := store.parseAndFilterMessages(messageHashes, status)
	return messages, nil
}

// batchFetchMessageHashes 批量获取消息哈希
func (store *RedisStore) batchFetchMessageHashes(
	ctx context.Context,
	messageKeys []string,
) ([]*redis.MapStringStringCmd, error) {
	pipeline := store.client.Pipeline()
	commands := make([]*redis.MapStringStringCmd, 0, len(messageKeys))

	for _, key := range messageKeys {
		commands = append(commands, pipeline.HGetAll(ctx, key))
	}

	if _, err := pipeline.Exec(ctx); err != nil {
		// Pipeline 部分失败不影响结果,继续处理可用的消息
		log.Printf("[REDIS_STORE] Batch fetch partially failed: %v", err)
	}

	return commands, nil
}

// parseAndFilterMessages 解析并过滤消息
func (store *RedisStore) parseAndFilterMessages(
	commands []*redis.MapStringStringCmd,
	status string,
) []Message {
	messages := make([]Message, 0, len(commands))

	for _, cmd := range commands {
		messageHash, err := cmd.Result()
		if err != nil || len(messageHash) == 0 {
			// 消息可能已过期或被删除,跳过
			continue
		}

		message := store.parseMessage(messageHash)

		if store.shouldIncludeMessage(message, status) {
			messages = append(messages, message)
		}
	}

	return messages
}

// parseMessage 解析消息哈希为消息对象
func (store *RedisStore) parseMessage(messageHash map[string]string) Message {
	message := Message{
		ID:        messageHash[fieldID],
		UserID:    messageHash[fieldUserID],
		Kind:      messageHash[fieldKind],
		Subject:   messageHash[fieldSubject],
		Body:      messageHash[fieldBody],
		CreatedAt: parseTimestamp(messageHash[fieldCreatedAt]),
		ReadAt:    parseTimestamp(messageHash[fieldReadAt]),
	}

	// 反序列化 Data 字段
	if dataStr, exists := messageHash[fieldData]; exists && dataStr != "" {
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(dataStr), &data); err == nil {
			message.Data = data
		}
	}

	return message
}

// shouldIncludeMessage 判断是否应包含该消息（状态过滤）
func (store *RedisStore) shouldIncludeMessage(message Message, status string) bool {
	normalizedStatus := strings.ToLower(status)

	switch normalizedStatus {
	case statusUnread:
		return message.ReadAt == 0
	case statusRead:
		return message.ReadAt != 0
	default:
		return true
	}
}

// ==================== 私有方法：参数构建 ====================

// buildMarkReadArguments 构建标记已读的参数
func (store *RedisStore) buildMarkReadArguments(
	userID string,
	messageIDs []string,
) []interface{} {
	currentTime := strconv.FormatInt(store.getCurrentTimestamp(), 10)
	args := make([]interface{}, 0, 3+len(messageIDs))
	args = append(args, store.options.Namespace, userID, currentTime)

	for _, id := range messageIDs {
		args = append(args, id)
	}

	return args
}

// buildDeleteArguments 构建删除的参数
func (store *RedisStore) buildDeleteArguments(
	userID string,
	messageIDs []string,
) []interface{} {
	args := make([]interface{}, 0, 2+len(messageIDs))
	args = append(args, store.options.Namespace, userID)

	for _, id := range messageIDs {
		args = append(args, id)
	}

	return args
}

// ==================== 私有方法：键构建 ====================

// buildSequenceKey 构建序列号键
func (store *RedisStore) buildSequenceKey() string {
	return store.buildKey(keyPrefixInbox, keyPrefixSequence)
}

// buildMessageKey 构建消息键
func (store *RedisStore) buildMessageKey(messageID string) string {
	return store.buildKey(keyPrefixInbox, keyPrefixMessage, messageID)
}

// buildUserZSetKey 构建用户有序集合键
func (store *RedisStore) buildUserZSetKey(userID string) string {
	return store.buildKey(keyPrefixInbox, keyPrefixUser, userID, keyPrefixZSet)
}

// buildKey 通用键构建函数
func (store *RedisStore) buildKey(parts ...string) string {
	allParts := append([]string{store.options.Namespace}, parts...)
	return strings.Join(allParts, keySeparator)
}

// ==================== 私有方法：工具函数 ====================

// getCurrentTimestamp 获取当前时间戳
func (store *RedisStore) getCurrentTimestamp() int64 {
	return store.timeProvider().Unix()
}

// parseTimestamp 解析时间戳字符串
func parseTimestamp(timestampStr string) int64 {
	if timestampStr == "" {
		return 0
	}
	timestamp, _ := strconv.ParseInt(timestampStr, 10, 64)
	return timestamp
}

// ==================== 响应构建函数 ====================

// MessageResponse 消息操作统一响应
type MessageResponse struct {
	Code int         `json:"code"` // 响应码
	Data interface{} `json:"data"` // 响应数据
	Msg  string      `json:"msg"`  // 响应消息
}

// MessageListData 消息列表数据
type MessageListData struct {
	Messages []Message `json:"messages"` // 消息列表
	Total    int64     `json:"total"`    // 总数
}

// NewAddMessageResponse 创建添加消息响应
func NewAddMessageResponse(messageID string, err error) MessageResponse {
	if err != nil {
		return MessageResponse{
			Code: 500,
			Data: nil,
			Msg:  err.Error(),
		}
	}

	return MessageResponse{
		Code: 200,
		Data: map[string]string{"messageId": messageID},
		Msg:  "message added successfully",
	}
}

// NewListMessageResponse 创建消息列表响应
func NewListMessageResponse(messages []Message, total int64, err error) MessageResponse {
	if err != nil {
		return MessageResponse{
			Code: 500,
			Data: nil,
			Msg:  err.Error(),
		}
	}

	return MessageResponse{
		Code: 200,
		Data: MessageListData{Messages: messages, Total: total},
		Msg:  "success",
	}
}

// NewBatchOperationResponse 创建批量操作响应
func NewBatchOperationResponse(affectedCount int, err error) MessageResponse {
	if err != nil {
		return MessageResponse{
			Code: 500,
			Data: nil,
			Msg:  err.Error(),
		}
	}

	return MessageResponse{
		Code: 200,
		Data: map[string]int{"affectedCount": affectedCount},
		Msg:  "operation completed successfully",
	}
}
