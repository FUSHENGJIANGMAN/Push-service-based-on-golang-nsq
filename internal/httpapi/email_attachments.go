package httpapi

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"push-gateway/internal/channels/email"
	"push-gateway/internal/push"
)

// ==================== 常量配置 ====================

const (
	maxUploadFileSize      = 10 * 1024 * 1024 // 10MB
	maxAttachmentFileSize  = 10 * 1024 * 1024 // 10MB
	maxAttachmentTotalSize = 40 * 1024 * 1024 // 40MB
	uploadDirectory        = "uploads"
	maxMultipartMemory     = 32 << 20 // 32MB

	// 允许的文件类型前缀
	allowedMimeTypes = "image/,video/,audio/,application/pdf,application/msword," +
		"application/vnd.openxmlformats-officedocument,text/,application/zip," +
		"application/x-zip-compressed"
)

// ==================== 数据模型 ====================

// Response 统一响应结构
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"msg"`
	Data    interface{} `json:"data"`
}

// FileUploadData 文件上传响应数据
type FileUploadData struct {
	FilePath string `json:"filePath"`
	UploadID string `json:"uploadId"`
	FileName string `json:"fileName"`
	FileSize int64  `json:"fileSize"`
	FileType string `json:"fileType"`
}

// EmailRequest 邮件请求结构
type EmailRequest struct {
	To       []push.Recipient `json:"to"`
	Subject  string           `json:"subject"`
	Body     string           `json:"body"`
	Priority int              `json:"priority"`
}

// EmailWithAttachmentsRequest 带附件的邮件请求
type EmailWithAttachmentsRequest struct {
	EmailRequest
	Attachments []push.EmailAttachment `json:"attachments"`
}

// FileDeleteRequest 文件删除请求
type FileDeleteRequest struct {
	FilePath string `json:"filePath"`
}

// EmailSendData 邮件发送响应数据
type EmailSendData struct {
	Success         bool   `json:"success"`
	Message         string `json:"message"`
	RecipientCount  int    `json:"recipients"`
	AttachmentCount int    `json:"attachments"`
	Timestamp       string `json:"timestamp"`
}

// ==================== HTTP 响应辅助函数 ====================

// setCORS 设置跨域响应头
func setCORS(writer http.ResponseWriter, allowedMethods string) {
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	writer.Header().Set("Access-Control-Allow-Methods", allowedMethods)
	writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept")
}

// writeJSON 写入 JSON 响应
func writeJSON(writer http.ResponseWriter, statusCode int, response Response) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)
	_ = json.NewEncoder(writer).Encode(response)
}

// writeSuccess 写入成功响应
func writeSuccess(writer http.ResponseWriter, data interface{}) {
	writeJSON(writer, http.StatusOK, Response{
		Code:    200,
		Message: "success",
		Data:    data,
	})
}

// writeError 写入错误响应
func writeError(writer http.ResponseWriter, message string, statusCode int) {
	writeJSON(writer, statusCode, Response{
		Code:    statusCode,
		Message: message,
		Data:    nil,
	})
}

// ==================== 文件上传处理器 ====================

// FileUploadHandler 处理文件上传请求
func FileUploadHandler(writer http.ResponseWriter, request *http.Request) {
	setCORS(writer, "POST, OPTIONS")

	if request.Method == http.MethodOptions {
		return
	}

	if request.Method != http.MethodPost {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := handleFileUpload(writer, request); err != nil {
		log.Printf("[FILE_UPLOAD] 处理失败: %v", err)
	}
}

// handleFileUpload 处理文件上传的核心逻辑
func handleFileUpload(writer http.ResponseWriter, request *http.Request) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxUploadFileSize)

	if err := request.ParseMultipartForm(maxUploadFileSize); err != nil {
		writeError(writer, "文件太大或表单解析失败", http.StatusBadRequest)
		return err
	}

	file, header, err := request.FormFile("file")
	if err != nil {
		writeError(writer, "获取上传文件失败", http.StatusBadRequest)
		return err
	}
	defer file.Close()

	if err := validateFileType(header); err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return err
	}

	savedPath, fileSize, err := saveUploadedFile(file, header)
	if err != nil {
		writeError(writer, err.Error(), http.StatusInternalServerError)
		return err
	}

	uploadID := generateUploadID(header.Filename)
	log.Printf("[FILE_UPLOAD] 文件上传成功: %s (%d bytes)", savedPath, fileSize)

	writeSuccess(writer, FileUploadData{
		FilePath: savedPath,
		UploadID: uploadID,
		FileName: header.Filename,
		FileSize: fileSize,
		FileType: header.Header.Get("Content-Type"),
	})

	return nil
}

// validateFileType 验证文件类型是否允许
func validateFileType(header *multipart.FileHeader) error {
	contentType := header.Header.Get("Content-Type")
	if !isAllowedMimeType(contentType) {
		return fmt.Errorf("不允许的文件类型: %s", contentType)
	}
	return nil
}

// saveUploadedFile 保存上传的文件到磁盘
func saveUploadedFile(file multipart.File, header *multipart.FileHeader) (string, int64, error) {
	if err := ensureUploadDirectory(); err != nil {
		return "", 0, fmt.Errorf("创建上传目录失败: %w", err)
	}

	savedPath := buildSavedFilePath(header.Filename)

	destinationFile, err := os.Create(savedPath)
	if err != nil {
		return "", 0, fmt.Errorf("创建文件失败: %w", err)
	}
	defer destinationFile.Close()

	fileSize, err := io.Copy(destinationFile, file)
	if err != nil {
		_ = os.Remove(savedPath)
		return "", 0, fmt.Errorf("保存文件失败: %w", err)
	}

	return savedPath, fileSize, nil
}

// buildSavedFilePath 构建保存文件的路径
func buildSavedFilePath(originalFilename string) string {
	uploadID := generateUploadID(originalFilename)
	fileExtension := filepath.Ext(originalFilename)
	baseFilename := strings.TrimSuffix(originalFilename, fileExtension)
	savedFilename := fmt.Sprintf("%s_%s%s", baseFilename, uploadID, fileExtension)
	sanitizedFilename := sanitizeFilename(savedFilename)

	return filepath.Join(uploadDirectory, sanitizedFilename)
}

// ensureUploadDirectory 确保上传目录存在
func ensureUploadDirectory() error {
	return os.MkdirAll(uploadDirectory, 0755)
}

// ==================== 文件下载处理器 ====================

// FileDownloadHandler 处理文件下载请求
func FileDownloadHandler(writer http.ResponseWriter, request *http.Request) {
	setCORS(writer, "GET, OPTIONS")

	if request.Method == http.MethodOptions {
		return
	}

	if request.Method != http.MethodGet {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := handleFileDownload(writer, request); err != nil {
		log.Printf("[FILE_DOWNLOAD] 处理失败: %v", err)
	}
}

// handleFileDownload 处理文件下载的核心逻辑
func handleFileDownload(writer http.ResponseWriter, request *http.Request) error {
	filePath := request.URL.Query().Get("path")

	if filePath == "" {
		writeError(writer, "缺少文件路径参数", http.StatusBadRequest)
		return fmt.Errorf("缺少文件路径参数")
	}

	if err := validateFilePath(filePath); err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return err
	}

	if !fileExists(filePath) {
		writeError(writer, "文件不存在", http.StatusNotFound)
		return fmt.Errorf("文件不存在: %s", filePath)
	}

	if err := serveFile(writer, filePath); err != nil {
		writeError(writer, "文件传输失败", http.StatusInternalServerError)
		return err
	}

	log.Printf("[FILE_DOWNLOAD] 文件下载成功: %s", filePath)
	return nil
}

// validateFilePath 验证文件路径是否合法
// 防止路径遍历攻击
func validateFilePath(filePath string) error {
	if !strings.HasPrefix(filePath, uploadDirectory) {
		return fmt.Errorf("非法文件路径")
	}
	return nil
}

// fileExists 检查文件是否存在
func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

// serveFile 提供文件下载服务
func serveFile(writer http.ResponseWriter, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("获取文件信息失败: %w", err)
	}

	writer.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filepath.Base(filePath)))
	writer.Header().Set("Content-Type", "application/octet-stream")
	writer.Header().Set("Content-Length", fmt.Sprintf("%d", fileInfo.Size()))

	_, err = io.Copy(writer, file)
	return err
}

// ==================== 文件删除处理器 ====================

// FileDeleteHandler 处理文件删除请求
func FileDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	setCORS(writer, "POST, OPTIONS")

	if request.Method == http.MethodOptions {
		return
	}

	if request.Method != http.MethodPost {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := handleFileDelete(writer, request); err != nil {
		log.Printf("[FILE_DELETE] 处理失败: %v", err)
	}
}

// handleFileDelete 处理文件删除的核心逻辑
func handleFileDelete(writer http.ResponseWriter, request *http.Request) error {
	var deleteRequest FileDeleteRequest
	if err := json.NewDecoder(request.Body).Decode(&deleteRequest); err != nil {
		writeError(writer, "解析请求失败", http.StatusBadRequest)
		return err
	}

	if deleteRequest.FilePath == "" {
		writeError(writer, "缺少文件路径", http.StatusBadRequest)
		return fmt.Errorf("缺少文件路径")
	}

	if err := validateFilePath(deleteRequest.FilePath); err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return err
	}

	if err := deleteFile(deleteRequest.FilePath); err != nil {
		if os.IsNotExist(err) {
			writeError(writer, "文件不存在", http.StatusNotFound)
		} else {
			writeError(writer, "删除文件失败", http.StatusInternalServerError)
		}
		return err
	}

	log.Printf("[FILE_DELETE] 文件删除成功: %s", deleteRequest.FilePath)
	writeSuccess(writer, map[string]string{"message": "文件删除成功"})
	return nil
}

// deleteFile 删除指定文件
func deleteFile(filePath string) error {
	return os.Remove(filePath)
}

// ==================== 邮件发送处理器（Multipart）====================

// EmailWithAttachmentsHandler 处理带附件的邮件发送（Multipart表单）
type EmailWithAttachmentsHandler struct {
	service          Service
	attachmentHelper *email.AttachmentHelper
}

// NewEmailWithAttachmentsHandler 创建邮件处理器实例
func NewEmailWithAttachmentsHandler(service Service) *EmailWithAttachmentsHandler {
	return &EmailWithAttachmentsHandler{
		service:          service,
		attachmentHelper: &email.AttachmentHelper{},
	}
}

// ServeHTTP 实现 http.Handler 接口
func (handler *EmailWithAttachmentsHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	setCORS(writer, "POST, OPTIONS")

	if request.Method == http.MethodOptions {
		return
	}

	if request.Method != http.MethodPost {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := handler.handleEmailWithAttachments(writer, request); err != nil {
		log.Printf("[EMAIL_ATTACHMENTS] 处理失败: %v", err)
	}
}

// handleEmailWithAttachments 处理邮件发送的核心逻辑
func (handler *EmailWithAttachmentsHandler) handleEmailWithAttachments(writer http.ResponseWriter, request *http.Request) error {
	if err := request.ParseMultipartForm(maxMultipartMemory); err != nil {
		writeError(writer, "解析multipart表单失败", http.StatusBadRequest)
		return err
	}

	emailRequest, err := handler.parseEmailData(request)
	if err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return err
	}

	attachments, totalSize, err := handler.parseMultipartAttachments(request)
	if err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return err
	}

	if err := handler.validateAttachments(attachments, totalSize); err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return err
	}

	message := buildEmailMessage(emailRequest, attachments)

	if err := handler.sendEmail(request, message, "EmailWithAttachments"); err != nil {
		writeError(writer, fmt.Sprintf("发送失败: %v", err), http.StatusInternalServerError)
		return err
	}

	handler.writeEmailSuccessResponse(writer, message)
	log.Printf("[EMAIL_ATTACHMENTS] 邮件发送成功: 收件人=%d, 附件=%d", len(message.To), len(message.Attachments))
	return nil
}

// parseEmailData 从表单中解析邮件数据
func (handler *EmailWithAttachmentsHandler) parseEmailData(request *http.Request) (*EmailRequest, error) {
	emailDataString := request.FormValue("email_data")
	if emailDataString == "" {
		return nil, fmt.Errorf("缺少 email_data 字段")
	}

	var emailRequest EmailRequest
	if err := json.Unmarshal([]byte(emailDataString), &emailRequest); err != nil {
		return nil, fmt.Errorf("email_data JSON无效: %w", err)
	}

	return normalizeEmailRequest(&emailRequest), nil
}

// parseMultipartAttachments 从 multipart 表单中解析附件
func (handler *EmailWithAttachmentsHandler) parseMultipartAttachments(request *http.Request) ([]push.EmailAttachment, int64, error) {
	var attachments []push.EmailAttachment
	var totalSize int64

	if request.MultipartForm == nil {
		return attachments, 0, nil
	}

	for _, fileHeaders := range request.MultipartForm.File {
		for _, fileHeader := range fileHeaders {
			attachment, err := handler.processMultipartFile(fileHeader)
			if err != nil {
				return nil, 0, fmt.Errorf("处理文件 %s 失败: %w", fileHeader.Filename, err)
			}
			attachments = append(attachments, attachment)
			totalSize += int64(len(attachment.Content))
		}
	}

	return attachments, totalSize, nil
}

// processMultipartFile 处理单个 multipart 文件
func (handler *EmailWithAttachmentsHandler) processMultipartFile(fileHeader *multipart.FileHeader) (push.EmailAttachment, error) {
	if fileHeader.Size > maxAttachmentFileSize {
		return push.EmailAttachment{}, fmt.Errorf("文件过大: %s", fileHeader.Filename)
	}

	file, err := fileHeader.Open()
	if err != nil {
		return push.EmailAttachment{}, fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	content, err := io.ReadAll(io.LimitReader(file, maxAttachmentFileSize+1))
	if err != nil {
		return push.EmailAttachment{}, fmt.Errorf("读取文件失败: %w", err)
	}

	if int64(len(content)) > maxAttachmentFileSize {
		return push.EmailAttachment{}, fmt.Errorf("文件超出限制: %s", fileHeader.Filename)
	}

	mimeType := email.DetectMimeTypeAdvanced(fileHeader.Filename)
	sanitizedFilename := sanitizeFilename(fileHeader.Filename)

	return push.EmailAttachment{
		FileName: sanitizedFilename,
		FileType: mimeType,
		Content:  content,
	}, nil
}

// validateAttachments 验证附件大小和内容
func (handler *EmailWithAttachmentsHandler) validateAttachments(attachments []push.EmailAttachment, totalSize int64) error {
	if totalSize > maxAttachmentTotalSize {
		return fmt.Errorf("所有附件总大小超出限制(%dMB)", maxAttachmentTotalSize/1024/1024)
	}

	return handler.attachmentHelper.ValidateAttachments(attachments)
}

// writeEmailSuccessResponse 写入邮件发送成功响应
func (handler *EmailWithAttachmentsHandler) writeEmailSuccessResponse(writer http.ResponseWriter, message push.Message) {
	writeSuccess(writer, EmailSendData{
		Success:         true,
		Message:         "邮件发送成功",
		RecipientCount:  len(message.To),
		AttachmentCount: len(message.Attachments),
		Timestamp:       time.Now().Format(time.RFC3339),
	})
}

// sendEmail 发送邮件到推送服务
func (handler *EmailWithAttachmentsHandler) sendEmail(request *http.Request, message push.Message, topic string) error {
	routePlan := push.RoutePlan{
		Primary: []push.Channel{push.ChEmail},
		Timeout: 60 * time.Second,
	}

	delivery := push.Delivery{
		Msg:   message,
		Plan:  routePlan,
		Mode:  "real",
		Topic: topic,
	}

	return handler.service.SendSync(request.Context(), delivery)
}

// ==================== 邮件发送处理器（JSON）====================

// SimpleEmailWithAttachmentsHandler 处理带附件的邮件发送（纯JSON）
type SimpleEmailWithAttachmentsHandler struct {
	service          Service
	attachmentHelper *email.AttachmentHelper
}

// NewSimpleEmailWithAttachmentsHandler 创建简单邮件处理器实例
func NewSimpleEmailWithAttachmentsHandler(service Service) *SimpleEmailWithAttachmentsHandler {
	return &SimpleEmailWithAttachmentsHandler{
		service:          service,
		attachmentHelper: &email.AttachmentHelper{},
	}
}

// ServeHTTP 实现 http.Handler 接口
func (handler *SimpleEmailWithAttachmentsHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	setCORS(writer, "POST, OPTIONS")

	if request.Method == http.MethodOptions {
		return
	}

	if request.Method != http.MethodPost {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := handler.handleSimpleEmail(writer, request); err != nil {
		log.Printf("[SIMPLE_EMAIL] 处理失败: %v", err)
	}
}

// handleSimpleEmail 处理JSON格式邮件发送的核心逻辑
func (handler *SimpleEmailWithAttachmentsHandler) handleSimpleEmail(writer http.ResponseWriter, request *http.Request) error {
	var emailRequest EmailWithAttachmentsRequest
	if err := json.NewDecoder(request.Body).Decode(&emailRequest); err != nil {
		writeError(writer, fmt.Sprintf("JSON解析失败: %v", err), http.StatusBadRequest)
		return err
	}

	normalizedRequest := normalizeEmailRequest(&emailRequest.EmailRequest)
	attachments := deduplicateAttachments(emailRequest.Attachments)

	totalSize := calculateAttachmentsTotalSize(attachments)
	if err := handler.validateAttachments(attachments, totalSize); err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return err
	}

	message := buildEmailMessage(normalizedRequest, attachments)

	if err := handler.sendEmail(request, message, "SimpleEmailWithAttachments"); err != nil {
		writeError(writer, fmt.Sprintf("发送失败: %v", err), http.StatusInternalServerError)
		return err
	}

	handler.writeEmailSuccessResponse(writer, message)
	log.Printf("[SIMPLE_EMAIL] 邮件发送成功: 收件人=%d, 附件=%d", len(message.To), len(message.Attachments))
	return nil
}

// validateAttachments 验证附件
func (handler *SimpleEmailWithAttachmentsHandler) validateAttachments(attachments []push.EmailAttachment, totalSize int64) error {
	if totalSize > maxAttachmentTotalSize {
		return fmt.Errorf("所有附件总大小超出限制(%dMB)", maxAttachmentTotalSize/1024/1024)
	}

	return handler.attachmentHelper.ValidateAttachments(attachments)
}

// writeEmailSuccessResponse 写入成功响应
func (handler *SimpleEmailWithAttachmentsHandler) writeEmailSuccessResponse(writer http.ResponseWriter, message push.Message) {
	writeSuccess(writer, EmailSendData{
		Success:         true,
		Message:         "邮件发送成功",
		RecipientCount:  len(message.To),
		AttachmentCount: len(message.Attachments),
		Timestamp:       time.Now().Format(time.RFC3339),
	})
}

// sendEmail 发送邮件
func (handler *SimpleEmailWithAttachmentsHandler) sendEmail(request *http.Request, message push.Message, topic string) error {
	routePlan := push.RoutePlan{
		Primary: []push.Channel{push.ChEmail},
		Timeout: 60 * time.Second,
	}

	delivery := push.Delivery{
		Msg:   message,
		Plan:  routePlan,
		Mode:  "real",
		Topic: topic,
	}

	return handler.service.SendSync(request.Context(), delivery)
}

// ==================== 邮件相关辅助函数 ====================

// buildEmailMessage 构建邮件消息
func buildEmailMessage(request *EmailRequest, attachments []push.EmailAttachment) push.Message {
	return push.Message{
		Kind:        "email",
		Subject:     request.Subject,
		Body:        request.Body,
		To:          request.To,
		Priority:    request.Priority,
		CreatedAt:   time.Now(),
		Attachments: attachments,
		Data: map[string]any{
			"subject": request.Subject,
			"body":    request.Body,
		},
	}
}

// normalizeEmailRequest 规范化邮件请求
// 设置默认值并去重收件人
func normalizeEmailRequest(request *EmailRequest) *EmailRequest {
	if request.Subject == "" {
		request.Subject = "系统通知"
	}

	if request.Body == "" {
		request.Body = "您收到了一条新的推送消息。"
	}

	if request.Priority <= 0 {
		request.Priority = 3
	}

	request.To = deduplicateAndValidateRecipients(request.To)
	return request
}

// deduplicateAndValidateRecipients 去重并验证收件人列表
// 使用邮箱地址作为唯一标识，不区分大小写
func deduplicateAndValidateRecipients(recipients []push.Recipient) []push.Recipient {
	uniqueRecipients := make(map[string]push.Recipient)

	for _, recipient := range recipients {
		if recipient.Email == "" {
			continue
		}

		normalizedEmail := strings.ToLower(strings.TrimSpace(recipient.Email))
		if normalizedEmail == "" {
			continue
		}

		if _, exists := uniqueRecipients[normalizedEmail]; !exists {
			recipient.Email = normalizedEmail
			uniqueRecipients[normalizedEmail] = recipient
		}
	}

	result := make([]push.Recipient, 0, len(uniqueRecipients))
	for _, recipient := range uniqueRecipients {
		result = append(result, recipient)
	}

	// 稳定排序，确保输出可预测
	sort.Slice(result, func(i, j int) bool {
		return result[i].Email < result[j].Email
	})

	return result
}

// deduplicateAttachments 去重附件列表
// 基于文件名、大小和内容哈希去重
func deduplicateAttachments(attachments []push.EmailAttachment) []push.EmailAttachment {
	type attachmentKey struct {
		filename string
		size     int
		hash     string
	}

	uniqueAttachments := make(map[attachmentKey]push.EmailAttachment)

	for _, attachment := range attachments {
		sanitizedName := sanitizeFilename(attachment.FileName)
		if sanitizedName == "" || len(attachment.Content) == 0 {
			continue
		}

		contentHash := md5.Sum(attachment.Content)
		key := attachmentKey{
			filename: sanitizedName,
			size:     len(attachment.Content),
			hash:     hex.EncodeToString(contentHash[:8]),
		}

		if _, exists := uniqueAttachments[key]; !exists {
			attachment.FileName = sanitizedName
			uniqueAttachments[key] = attachment
		}
	}

	result := make([]push.EmailAttachment, 0, len(uniqueAttachments))
	for _, attachment := range uniqueAttachments {
		result = append(result, attachment)
	}

	// 稳定排序
	sort.Slice(result, func(i, j int) bool {
		return result[i].FileName < result[j].FileName
	})

	return result
}

// calculateAttachmentsTotalSize 计算附件总大小
func calculateAttachmentsTotalSize(attachments []push.EmailAttachment) int64 {
	var totalSize int64
	for _, attachment := range attachments {
		totalSize += int64(len(attachment.Content))
	}
	return totalSize
}

// ==================== 通用工具函数 ====================

// generateUploadID 生成唯一的上传ID
// 使用文件名和时间戳的MD5哈希前8位
func generateUploadID(filename string) string {
	data := fmt.Sprintf("%s_%d", filename, time.Now().UnixNano())
	hash := md5.Sum([]byte(data))
	return hex.EncodeToString(hash[:])[:8]
}

// isAllowedMimeType 检查MIME类型是否允许
func isAllowedMimeType(mimeType string) bool {
	if mimeType == "" {
		return false
	}

	allowedPrefixes := strings.Split(allowedMimeTypes, ",")
	for _, prefix := range allowedPrefixes {
		trimmedPrefix := strings.TrimSpace(prefix)
		if strings.HasPrefix(mimeType, trimmedPrefix) {
			return true
		}
	}

	return false
}

// sanitizeFilename 清理文件名，移除危险字符
// 防止路径遍历攻击
func sanitizeFilename(filename string) string {
	filename = strings.ReplaceAll(filename, "..", "")
	filename = strings.ReplaceAll(filename, "/", "_")
	filename = strings.ReplaceAll(filename, "\\", "_")
	return filename
}
