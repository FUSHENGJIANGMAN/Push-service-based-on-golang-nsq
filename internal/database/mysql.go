package database

import (
	"database/sql"
	"fmt"
	"log"

	"push-gateway/internal/config"

	_ "github.com/go-sql-driver/mysql"
)

// 表名常量
const (
	TableInboxMessages      = "inbox_messages"
	TablePushRecords        = "push_records"
	TableMessageStatus      = "message_status"
	TableIdempotencyRecords = "idempotency_records"
	TableAsyncSyncQueue     = "async_sync_queue"
)

// SQL 建表语句常量
// 使用 InnoDB 引擎支持事务,utf8mb4 支持完整 Unicode 字符集
const (
	// createInboxMessagesTableSQL 站内信表
	// 存储用户收件箱消息,支持按用户和时间快速查询
	createInboxMessagesTableSQL = `
		CREATE TABLE IF NOT EXISTS inbox_messages (
			id VARCHAR(128) PRIMARY KEY COMMENT '消息唯一标识',
			user_id VARCHAR(128) NOT NULL COMMENT '用户ID',
			kind VARCHAR(64) NOT NULL COMMENT '消息类型',
			subject TEXT COMMENT '消息主题',
			body TEXT COMMENT '消息正文',
			data JSON COMMENT '扩展数据',
			created_at BIGINT NOT NULL COMMENT '创建时间戳',
			read_at BIGINT DEFAULT 0 COMMENT '阅读时间戳',
			redis_synced BOOLEAN DEFAULT TRUE COMMENT 'Redis同步状态',
			INDEX idx_user_created (user_id, created_at DESC),
			INDEX idx_user_read (user_id, read_at),
			INDEX idx_created_at (created_at)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
		COMMENT='用户站内信收件箱'
	`

	// createPushRecordsTableSQL 推送记录表
	// 记录所有推送请求的完整信息,用于审计和统计分析
	createPushRecordsTableSQL = `
		CREATE TABLE IF NOT EXISTS push_records (
			id VARCHAR(255) PRIMARY KEY COMMENT '推送记录ID',
			namespace VARCHAR(128) NOT NULL COMMENT '命名空间',
			kind VARCHAR(64) NOT NULL COMMENT '消息类型',
			subject TEXT COMMENT '消息主题',
			body TEXT COMMENT '消息正文',
			message_data JSON COMMENT '消息数据',
			channels JSON COMMENT '推送通道列表',
			recipients JSON COMMENT '收件人列表',
			status VARCHAR(32) NOT NULL COMMENT '推送状态',
			code VARCHAR(16) COMMENT '状态码',
			content TEXT COMMENT '响应内容',
			error_detail TEXT COMMENT '错误详情',
			priority INT DEFAULT 0 COMMENT '优先级',
			created_at BIGINT NOT NULL COMMENT '创建时间戳',
			sent_at BIGINT DEFAULT 0 COMMENT '发送时间戳',
			redis_synced BOOLEAN DEFAULT TRUE COMMENT 'Redis同步状态',
			INDEX idx_namespace_created (namespace, created_at DESC),
			INDEX idx_kind_status (kind, status),
			INDEX idx_priority (priority),
			INDEX idx_created_at (created_at),
			INDEX idx_sent_at (sent_at)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
		COMMENT='推送记录表'
	`

	// createMessageStatusTableSQL 消息状态表
	// 跟踪短信/语音等异步消息的实时状态
	createMessageStatusTableSQL = `
		CREATE TABLE IF NOT EXISTS message_status (
			message_id VARCHAR(128) PRIMARY KEY COMMENT '消息ID',
			channel VARCHAR(64) NOT NULL COMMENT '推送通道',
			phone VARCHAR(32) COMMENT '手机号码',
			content TEXT COMMENT '消息内容',
			status VARCHAR(32) NOT NULL COMMENT '状态',
			error TEXT COMMENT '错误信息',
			created_at BIGINT NOT NULL COMMENT '创建时间戳',
			updated_at BIGINT NOT NULL COMMENT '更新时间戳',
			redis_synced BOOLEAN DEFAULT TRUE COMMENT 'Redis同步状态',
			INDEX idx_channel_status (channel, status),
			INDEX idx_phone (phone),
			INDEX idx_created_at (created_at)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
		COMMENT='消息状态追踪表'
	`

	// createIdempotencyRecordsTableSQL 幂等性记录表
	// 防止重复推送,通过唯一键保证接口幂等性
	createIdempotencyRecordsTableSQL = `
		CREATE TABLE IF NOT EXISTS idempotency_records (
			key_hash VARCHAR(128) PRIMARY KEY COMMENT '幂等键哈希值',
			namespace VARCHAR(128) NOT NULL COMMENT '命名空间',
			delivery_data JSON COMMENT '投递数据',
			created_at BIGINT NOT NULL COMMENT '创建时间戳',
			expires_at BIGINT NOT NULL COMMENT '过期时间戳',
			INDEX idx_namespace (namespace),
			INDEX idx_expires_at (expires_at)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
		COMMENT='幂等性记录表'
	`

	// createAsyncSyncQueueTableSQL 异步同步队列表
	// 实现 Redis 到 MySQL 的异步批量同步,提高写入性能
	createAsyncSyncQueueTableSQL = `
		CREATE TABLE IF NOT EXISTS async_sync_queue (
			id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
			table_name VARCHAR(64) NOT NULL COMMENT '目标表名',
			operation VARCHAR(16) NOT NULL COMMENT '操作类型', 
			record_id VARCHAR(128) NOT NULL COMMENT '记录ID',
			data JSON COMMENT '数据内容',
			attempts INT DEFAULT 0 COMMENT '重试次数',
			max_attempts INT DEFAULT 3 COMMENT '最大重试次数',
			created_at BIGINT NOT NULL COMMENT '创建时间戳',
			next_retry_at BIGINT NOT NULL COMMENT '下次重试时间',
			status VARCHAR(16) DEFAULT 'pending' COMMENT '任务状态',
			error TEXT COMMENT '错误信息',
			INDEX idx_status_retry (status, next_retry_at),
			INDEX idx_table_record (table_name, record_id),
			INDEX idx_created_at (created_at)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
		COMMENT='异步同步任务队列'
	`
)

// MySQLDB MySQL 数据库连接管理器
// 封装连接池和表初始化逻辑
type MySQLDB struct {
	*sql.DB
}

// NewMySQLDB 创建 MySQL 数据库连接
// 自动配置连接池参数并测试连接可用性
func NewMySQLDB(mysqlConfig config.MySQLConfig) (*MySQLDB, error) {
	database, err := sql.Open("mysql", mysqlConfig.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql connection: %w", err)
	}

	if err := configureConnectionPool(database, mysqlConfig); err != nil {
		database.Close()
		return nil, err
	}

	if err := testConnection(database); err != nil {
		database.Close()
		return nil, err
	}

	log.Printf("[MYSQL] 数据库连接成功")
	return &MySQLDB{DB: database}, nil
}

// configureConnectionPool 配置数据库连接池参数
// 合理的连接池配置可以提高并发性能和资源利用率
func configureConnectionPool(database *sql.DB, mysqlConfig config.MySQLConfig) error {
	database.SetMaxOpenConns(mysqlConfig.MaxOpenConns)
	database.SetMaxIdleConns(mysqlConfig.MaxIdleConns)
	database.SetConnMaxLifetime(mysqlConfig.ConnMaxLifetime)
	return nil
}

// testConnection 测试数据库连接是否可用
func testConnection(database *sql.DB) error {
	if err := database.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}
	return nil
}

// InitTables 初始化数据库表结构
// 幂等操作,多次执行不会产生副作用
func (database *MySQLDB) InitTables() error {
	if err := database.createAllTables(); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	log.Printf("[MYSQL] 数据库表初始化完成")
	return nil
}

// createAllTables 创建所有业务表
// 使用 IF NOT EXISTS 确保表已存在时不会报错
func (database *MySQLDB) createAllTables() error {
	tables := []tableDefinition{
		{name: TableInboxMessages, sql: createInboxMessagesTableSQL},
		{name: TablePushRecords, sql: createPushRecordsTableSQL},
		{name: TableMessageStatus, sql: createMessageStatusTableSQL},
		{name: TableIdempotencyRecords, sql: createIdempotencyRecordsTableSQL},
		{name: TableAsyncSyncQueue, sql: createAsyncSyncQueueTableSQL},
	}

	for _, table := range tables {
		if err := database.createTable(table); err != nil {
			return err
		}
	}

	return nil
}

// tableDefinition 表定义结构
type tableDefinition struct {
	name string
	sql  string
}

// createTable 创建单个数据表
func (database *MySQLDB) createTable(table tableDefinition) error {
	if _, err := database.Exec(table.sql); err != nil {
		log.Printf("[MYSQL] 创建表 %s 失败: %v", table.name, err)
		return fmt.Errorf("failed to create table %s: %w", table.name, err)
	}
	return nil
}

// Close 关闭数据库连接
// 释放所有连接池资源
func (database *MySQLDB) Close() error {
	if err := database.DB.Close(); err != nil {
		return fmt.Errorf("failed to close database connection: %w", err)
	}
	return nil
}
