package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"push-gateway/internal/config"
	"push-gateway/internal/database"
	"push-gateway/internal/inbox"

	"github.com/redis/go-redis/v9"
)

var (
	configFile = flag.String("config", "etc/app.yaml", "配置文件路径")
	mode       = flag.String("mode", "migrate", "操作模式: migrate|verify|cleanup")
	dryRun     = flag.Bool("dry-run", false, "仅预览，不执行实际操作")
)

func main() {
	flag.Parse()

	cfg := config.MustLoad(*configFile)

	// 连接 Redis
	rc := redis.NewClient(&redis.Options{Addr: cfg.Storage.RedisAddr})
	defer rc.Close()

	// 连接 MySQL
	mysqlDB, err := database.NewMySQLDB(cfg.Storage.MySQL)
	if err != nil {
		log.Fatalf("MySQL连接失败: %v", err)
	}
	defer mysqlDB.Close()

	// 初始化表
	if err := mysqlDB.InitTables(); err != nil {
		log.Fatalf("表初始化失败: %v", err)
	}

	migrator := &DataMigrator{
		redis:  rc,
		mysql:  mysqlDB,
		cfg:    &cfg,
		dryRun: *dryRun,
	}

	switch *mode {
	case "migrate":
		migrator.MigrateInboxMessages()
	case "verify":
		migrator.VerifyData()
	case "cleanup":
		migrator.CleanupRedisData()
	default:
		log.Fatalf("未知模式: %s", *mode)
	}
}

type DataMigrator struct {
	redis  *redis.Client
	mysql  *database.MySQLDB
	cfg    *config.Config
	dryRun bool
}

// MigrateInboxMessages 迁移站内信数据
func (m *DataMigrator) MigrateInboxMessages() {
	ctx := context.Background()

	log.Printf("开始迁移站内信数据...")

	// 扫描所有用户的收件箱
	pattern := fmt.Sprintf("%s:inbox:user:*:z", m.cfg.Storage.Namespace)
	keys, err := m.redis.Keys(ctx, pattern).Result()
	if err != nil {
		log.Fatalf("扫描用户收件箱失败: %v", err)
	}

	log.Printf("找到 %d 个用户收件箱", len(keys))

	totalMessages := 0
	migratedMessages := 0

	for _, key := range keys {
		// 提取用户ID
		userID := m.extractUserIDFromKey(key)
		if userID == "" {
			log.Printf("无法提取用户ID: %s", key)
			continue
		}

		log.Printf("迁移用户 %s 的消息...", userID)

		// 获取用户的所有消息
		messageKeys, err := m.redis.ZRevRange(ctx, key, 0, -1).Result()
		if err != nil {
			log.Printf("获取用户消息失败: %v", err)
			continue
		}

		totalMessages += len(messageKeys)

		for _, msgKey := range messageKeys {
			// 获取消息详情
			msgData, err := m.redis.HGetAll(ctx, msgKey).Result()
			if err != nil {
				log.Printf("获取消息详情失败: %v", err)
				continue
			}

			// 转换为消息结构
			msg, err := m.convertToMessage(msgData)
			if err != nil {
				log.Printf("转换消息失败: %v", err)
				continue
			}

			// 检查MySQL中是否已存在
			exists, err := m.messageExistsInMySQL(ctx, msg.ID)
			if err != nil {
				log.Printf("检查消息存在性失败: %v", err)
				continue
			}

			if exists {
				log.Printf("消息 %s 已存在，跳过", msg.ID)
				continue
			}

			// 插入到MySQL
			if !m.dryRun {
				err = m.insertMessageToMySQL(ctx, msg)
				if err != nil {
					log.Printf("插入消息到MySQL失败: %v", err)
					continue
				}
			}

			migratedMessages++
			if migratedMessages%100 == 0 {
				log.Printf("已迁移 %d/%d 消息", migratedMessages, totalMessages)
			}
		}
	}

	log.Printf("迁移完成: 总消息数 %d, 迁移成功 %d", totalMessages, migratedMessages)
}

// VerifyData 验证数据一致性
func (m *DataMigrator) VerifyData() {
	ctx := context.Background()

	log.Printf("开始验证数据一致性...")

	// 统计Redis中的消息数
	redisCount, err := m.countRedisMessages(ctx)
	if err != nil {
		log.Fatalf("统计Redis消息数失败: %v", err)
	}

	// 统计MySQL中的消息数
	mysqlCount, err := m.countMySQLMessages(ctx)
	if err != nil {
		log.Fatalf("统计MySQL消息数失败: %v", err)
	}

	log.Printf("Redis消息数: %d", redisCount)
	log.Printf("MySQL消息数: %d", mysqlCount)

	if redisCount == mysqlCount {
		log.Printf("✅ 数据一致性验证通过")
	} else {
		log.Printf("❌ 数据不一致，需要重新同步")
	}
}

// CleanupRedisData 清理Redis冗余数据
func (m *DataMigrator) CleanupRedisData() {
	ctx := context.Background()

	log.Printf("开始清理Redis冗余数据...")

	// 这里可以添加清理逻辑，比如删除过期消息等
	// 示例：删除超过30天的消息
	cutoff := time.Now().AddDate(0, 0, -30).Unix()

	pattern := fmt.Sprintf("%s:inbox:msg:*", m.cfg.Storage.Namespace)
	keys, err := m.redis.Keys(ctx, pattern).Result()
	if err != nil {
		log.Fatalf("扫描消息失败: %v", err)
	}

	deletedCount := 0
	for _, key := range keys {
		createdAtStr, err := m.redis.HGet(ctx, key, "created_at").Result()
		if err != nil {
			continue
		}

		createdAt := parseInt64(createdAtStr)
		if createdAt < cutoff {
			if !m.dryRun {
				err = m.redis.Del(ctx, key).Err()
				if err != nil {
					log.Printf("删除消息失败: %v", err)
					continue
				}
			}
			deletedCount++
		}
	}

	log.Printf("清理完成: 删除了 %d 条过期消息", deletedCount)
}

// 辅助函数

func (m *DataMigrator) extractUserIDFromKey(key string) string {
	// 从 "namespace:inbox:user:userID:z" 中提取 userID
	parts := strings.Split(key, ":")
	if len(parts) >= 5 {
		return parts[3]
	}
	return ""
}

func (m *DataMigrator) convertToMessage(data map[string]string) (*inbox.Message, error) {
	msg := &inbox.Message{
		ID:        data["id"],
		UserID:    data["user_id"],
		Kind:      data["kind"],
		Subject:   data["subject"],
		Body:      data["body"],
		CreatedAt: parseInt64(data["created_at"]),
		ReadAt:    parseInt64(data["read_at"]),
	}

	// 解析JSON数据
	if dataStr := data["data"]; dataStr != "" {
		var msgData map[string]interface{}
		if err := json.Unmarshal([]byte(dataStr), &msgData); err == nil {
			msg.Data = msgData
		}
	}

	return msg, nil
}

func (m *DataMigrator) messageExistsInMySQL(ctx context.Context, messageID string) (bool, error) {
	var count int
	err := m.mysql.QueryRow("SELECT COUNT(*) FROM inbox_messages WHERE id = ?", messageID).Scan(&count)
	return count > 0, err
}

func (m *DataMigrator) insertMessageToMySQL(ctx context.Context, msg *inbox.Message) error {
	dataBytes, _ := json.Marshal(msg.Data)
	query := `INSERT INTO inbox_messages 
		(id, user_id, kind, subject, body, data, created_at, read_at, redis_synced) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := m.mysql.Exec(query, msg.ID, msg.UserID, msg.Kind, msg.Subject, msg.Body,
		string(dataBytes), msg.CreatedAt, msg.ReadAt, true)

	return err
}

func (m *DataMigrator) countRedisMessages(ctx context.Context) (int, error) {
	pattern := fmt.Sprintf("%s:inbox:msg:*", m.cfg.Storage.Namespace)
	keys, err := m.redis.Keys(ctx, pattern).Result()
	return len(keys), err
}

func (m *DataMigrator) countMySQLMessages(ctx context.Context) (int, error) {
	var count int
	err := m.mysql.QueryRow("SELECT COUNT(*) FROM inbox_messages").Scan(&count)
	return count, err
}

func parseInt64(s string) int64 {
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}
