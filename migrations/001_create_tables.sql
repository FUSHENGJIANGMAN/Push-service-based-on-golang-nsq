-- 创建推送服务数据库
CREATE DATABASE IF NOT EXISTS push_service DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE push_service;

-- 站内信表
CREATE TABLE IF NOT EXISTS inbox_messages (
    id VARCHAR(128) PRIMARY KEY,
    user_id VARCHAR(128) NOT NULL,
    kind VARCHAR(64) NOT NULL,
    subject TEXT,
    body TEXT,
    data JSON,
    created_at BIGINT NOT NULL,
    read_at BIGINT DEFAULT 0,
    redis_synced BOOLEAN DEFAULT TRUE,
    INDEX idx_user_created (user_id, created_at DESC),
    INDEX idx_user_read (user_id, read_at),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 推送记录表
CREATE TABLE IF NOT EXISTS push_records (
    id VARCHAR(255) PRIMARY KEY,
    namespace VARCHAR(128) NOT NULL,
    kind VARCHAR(64) NOT NULL,
    channels JSON,
    recipients JSON,
    status VARCHAR(32) NOT NULL,
    code VARCHAR(16),
    content TEXT,
    created_at BIGINT NOT NULL,
    redis_synced BOOLEAN DEFAULT TRUE,
    INDEX idx_namespace_created (namespace, created_at DESC),
    INDEX idx_kind_status (kind, status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 消息状态表
CREATE TABLE IF NOT EXISTS message_status (
    message_id VARCHAR(128) PRIMARY KEY,
    channel VARCHAR(64) NOT NULL,
    phone VARCHAR(32),
    content TEXT,
    status VARCHAR(32) NOT NULL,
    error TEXT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    redis_synced BOOLEAN DEFAULT TRUE,
    INDEX idx_channel_status (channel, status),
    INDEX idx_phone (phone),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 幂等性记录表
CREATE TABLE IF NOT EXISTS idempotency_records (
    key_hash VARCHAR(128) PRIMARY KEY,
    namespace VARCHAR(128) NOT NULL,
    delivery_data JSON,
    created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    INDEX idx_namespace (namespace),
    INDEX idx_expires_at (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 异步同步队列表
CREATE TABLE IF NOT EXISTS async_sync_queue (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(64) NOT NULL,
    operation VARCHAR(16) NOT NULL, 
    record_id VARCHAR(128) NOT NULL,
    data JSON,
    attempts INT DEFAULT 0,
    max_attempts INT DEFAULT 3,
    created_at BIGINT NOT NULL,
    next_retry_at BIGINT NOT NULL,
    status VARCHAR(16) DEFAULT 'pending',
    error TEXT,
    INDEX idx_status_retry (status, next_retry_at),
    INDEX idx_table_record (table_name, record_id),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
