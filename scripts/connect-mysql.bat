@echo off
echo 正在连接到MySQL数据库...
echo 用户名: root
echo 密码: password
echo 数据库: push_service
echo.
echo 可以使用以下命令查看数据：
echo.
echo 1. 查看所有表的记录数量：
echo    SELECT 'inbox_messages' as table_name, COUNT(*) as count FROM inbox_messages;
echo.
echo 2. 查看最新消息：
echo    SELECT * FROM inbox_messages ORDER BY created_at DESC LIMIT 5;
echo.
echo 3. 查看推送记录：
echo    SELECT * FROM push_records ORDER BY created_at DESC LIMIT 5;
echo.
echo 正在启动MySQL客户端...
docker exec -it pushservice-mysql-1 mysql -u root -ppassword push_service
