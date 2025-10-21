-- 修复数据库表结构的脚本
USE push_service;

-- 修复 inbox_messages 表的 id 字段类型
ALTER TABLE inbox_messages MODIFY COLUMN id VARCHAR(128);

-- 修复 push_records 表的 id 字段类型  
ALTER TABLE push_records MODIFY COLUMN id VARCHAR(255);

-- 清理可能的损坏数据
DELETE FROM async_sync_queue WHERE table_name IN ('inbox_messages', 'push_records');

-- 显示修复后的表结构
DESCRIBE inbox_messages;
DESCRIBE push_records;
DESCRIBE message_status;
DESCRIBE async_sync_queue;
