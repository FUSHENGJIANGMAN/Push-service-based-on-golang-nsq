-- 查询数据库中的消息 - 复制这些SQL语句到phpMyAdmin或MySQL命令行中执行

-- 1. 查看所有表的记录数量
SELECT 'inbox_messages' as table_name, COUNT(*) as count FROM inbox_messages
UNION ALL
SELECT 'push_records' as table_name, COUNT(*) as count FROM push_records
UNION ALL
SELECT 'message_status' as table_name, COUNT(*) as count FROM message_status
UNION ALL
SELECT 'async_sync_queue' as table_name, COUNT(*) as count FROM async_sync_queue;

-- 2. 查看最新的10条站内信消息
SELECT id, user_id, kind, subject, body, created_at, read_at 
FROM inbox_messages 
ORDER BY created_at DESC 
LIMIT 10;

-- 3. 查看最新的10条推送记录
SELECT id, namespace, kind, status, code, content, created_at 
FROM push_records 
ORDER BY created_at DESC 
LIMIT 10;

-- 4. 查看最新的10条消息状态
SELECT message_id, channel, status, error, created_at, updated_at 
FROM message_status 
ORDER BY created_at DESC 
LIMIT 10;

-- 5. 查看异步同步队列状态
SELECT table_name, operation, status, COUNT(*) as count 
FROM async_sync_queue 
GROUP BY table_name, operation, status;

-- 6. 查看失败的同步记录
SELECT id, table_name, operation, record_id, attempts, status, error 
FROM async_sync_queue 
WHERE status = 'failed' OR attempts >= max_attempts 
ORDER BY created_at DESC;

-- 7. 按用户查看站内信（替换 'USER_ID' 为实际用户ID）
-- SELECT id, kind, subject, body, created_at, read_at 
-- FROM inbox_messages 
-- WHERE user_id = 'USER_ID' 
-- ORDER BY created_at DESC;

-- 8. 查看未读消息统计
SELECT user_id, COUNT(*) as unread_count 
FROM inbox_messages 
WHERE read_at = 0 
GROUP BY user_id;

-- 9. 查看最近1小时的活动
SELECT 'inbox_messages' as table_name, COUNT(*) as count 
FROM inbox_messages 
WHERE created_at > UNIX_TIMESTAMP(NOW() - INTERVAL 1 HOUR)
UNION ALL
SELECT 'push_records' as table_name, COUNT(*) as count 
FROM push_records 
WHERE created_at > UNIX_TIMESTAMP(NOW() - INTERVAL 1 HOUR);
