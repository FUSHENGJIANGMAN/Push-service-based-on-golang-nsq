package email

import (
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"push-gateway/internal/push"
)

//
// 常量定义
//

const (
	// 附件大小限制
	maxSingleAttachmentSize = 10 * 1024 * 1024 // 10MB
	maxTotalAttachmentSize  = 25 * 1024 * 1024 // 25MB
	maxAttachmentCount      = 10

	// 默认 MIME 类型
	defaultMimeType = "application/octet-stream"

	// 测试附件默认类型
	testAttachmentMimeType = "text/plain"
)

//
// 附件辅助工具
//

// AttachmentHelper 附件处理辅助工具
// 提供附件创建、验证和信息获取功能
type AttachmentHelper struct{}

// NewAttachmentHelper 创建附件辅助工具实例
func NewAttachmentHelper() *AttachmentHelper {
	return &AttachmentHelper{}
}

//
// 附件创建方法
//

// CreateAttachment 创建基于文件路径的附件
func (helper *AttachmentHelper) CreateAttachment(
	fileName string,
	filePath string,
) push.EmailAttachment {
	return push.EmailAttachment{
		FileName: fileName,
		FileType: DetectMimeType(fileName),
		FilePath: filePath,
	}
}

// CreateAttachmentWithContent 创建包含内容的附件
func (helper *AttachmentHelper) CreateAttachmentWithContent(
	fileName string,
	content []byte,
) push.EmailAttachment {
	return push.EmailAttachment{
		FileName: fileName,
		FileType: DetectMimeType(fileName),
		Content:  content,
	}
}

// CreateAttachmentWithCustomType 创建指定 MIME 类型的附件
func (helper *AttachmentHelper) CreateAttachmentWithCustomType(
	fileName string,
	mimeType string,
	filePath string,
) push.EmailAttachment {
	return push.EmailAttachment{
		FileName: fileName,
		FileType: mimeType,
		FilePath: filePath,
	}
}

// CreateTestAttachment 创建测试用的文本附件
// 用于单元测试和开发调试
func (helper *AttachmentHelper) CreateTestAttachment(
	fileName string,
	content string,
) push.EmailAttachment {
	return push.EmailAttachment{
		FileName: fileName,
		FileType: testAttachmentMimeType,
		Content:  []byte(content),
	}
}

//
// 附件验证方法
//

// ValidateAttachment 验证单个附件
// 检查文件名安全性、文件存在性和大小限制
func (helper *AttachmentHelper) ValidateAttachment(
	attachment push.EmailAttachment,
) error {
	if err := helper.validateFileName(attachment.FileName); err != nil {
		return err
	}

	if err := helper.validateAttachmentSource(attachment); err != nil {
		return err
	}

	return helper.validateAttachmentSize(attachment)
}

// ValidateAttachments 验证附件列表
// 检查附件数量和总大小限制
func (helper *AttachmentHelper) ValidateAttachments(
	attachments []push.EmailAttachment,
) error {
	if len(attachments) == 0 {
		return nil
	}

	if err := helper.validateAttachmentCount(attachments); err != nil {
		return err
	}

	if err := helper.validateIndividualAttachments(attachments); err != nil {
		return err
	}

	return helper.validateTotalSize(attachments)
}

//
// 验证辅助方法
//

// validateFileName 验证文件名安全性
// 防止路径遍历攻击
func (helper *AttachmentHelper) validateFileName(fileName string) error {
	if fileName == "" {
		return fmt.Errorf("附件文件名不能为空")
	}

	unsafePatterns := []string{"..", "/", "\\"}
	for _, pattern := range unsafePatterns {
		if strings.Contains(fileName, pattern) {
			return fmt.Errorf("不安全的文件名: %s", fileName)
		}
	}

	return nil
}

// validateAttachmentSource 验证附件来源
// 确保附件有文件路径或内容
func (helper *AttachmentHelper) validateAttachmentSource(
	attachment push.EmailAttachment,
) error {
	hasFilePath := attachment.FilePath != ""
	hasContent := len(attachment.Content) > 0

	if !hasFilePath && !hasContent {
		return fmt.Errorf("附件必须指定文件路径或内容")
	}

	if hasFilePath {
		return helper.validateFilePath(attachment.FilePath)
	}

	return nil
}

// validateFilePath 验证文件路径
// 检查文件是否存在
func (helper *AttachmentHelper) validateFilePath(filePath string) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return fmt.Errorf("附件文件不存在: %s", filePath)
	}

	return nil
}

// validateAttachmentSize 验证单个附件大小
func (helper *AttachmentHelper) validateAttachmentSize(
	attachment push.EmailAttachment,
) error {
	if len(attachment.Content) == 0 {
		return nil
	}

	contentSize := len(attachment.Content)
	if contentSize > maxSingleAttachmentSize {
		return fmt.Errorf(
			"附件过大: %d 字节 (最大 %d 字节)",
			contentSize,
			maxSingleAttachmentSize,
		)
	}

	return nil
}

// validateAttachmentCount 验证附件数量
func (helper *AttachmentHelper) validateAttachmentCount(
	attachments []push.EmailAttachment,
) error {
	if len(attachments) > maxAttachmentCount {
		return fmt.Errorf(
			"附件数量过多: %d (最大 %d)",
			len(attachments),
			maxAttachmentCount,
		)
	}

	return nil
}

// validateIndividualAttachments 逐个验证附件
func (helper *AttachmentHelper) validateIndividualAttachments(
	attachments []push.EmailAttachment,
) error {
	for index, attachment := range attachments {
		if err := helper.ValidateAttachment(attachment); err != nil {
			return fmt.Errorf("附件 %d 验证失败: %w", index+1, err)
		}
	}

	return nil
}

// validateTotalSize 验证附件总大小
func (helper *AttachmentHelper) validateTotalSize(
	attachments []push.EmailAttachment,
) error {
	totalSize := helper.calculateTotalSize(attachments)

	if totalSize > maxTotalAttachmentSize {
		return fmt.Errorf(
			"附件总大小过大: %d 字节 (最大 %d 字节)",
			totalSize,
			maxTotalAttachmentSize,
		)
	}

	return nil
}

// calculateTotalSize 计算所有附件的总大小
func (helper *AttachmentHelper) calculateTotalSize(
	attachments []push.EmailAttachment,
) int {
	totalSize := 0

	for _, attachment := range attachments {
		totalSize += helper.getAttachmentSize(attachment)
	}

	return totalSize
}

// getAttachmentSize 获取单个附件大小
func (helper *AttachmentHelper) getAttachmentSize(attachment push.EmailAttachment) int {
	// 优先使用内容大小
	if len(attachment.Content) > 0 {
		return len(attachment.Content)
	}

	// 否则尝试获取文件大小
	if attachment.FilePath != "" {
		return helper.getFileSizeFromPath(attachment.FilePath)
	}

	return 0
}

// getFileSizeFromPath 从文件路径获取文件大小
func (helper *AttachmentHelper) getFileSizeFromPath(filePath string) int {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return 0
	}

	return int(fileInfo.Size())
}

//
// 附件信息获取方法
//

// GetAttachmentInfo 获取附件详细信息
// 返回包含文件名、类型、大小等信息的映射
func (helper *AttachmentHelper) GetAttachmentInfo(
	attachment push.EmailAttachment,
) map[string]interface{} {
	info := helper.createBasicInfo(attachment)
	helper.addFilePathInfo(info, attachment)
	helper.addContentInfo(info, attachment)

	return info
}

// createBasicInfo 创建基础信息
func (helper *AttachmentHelper) createBasicInfo(
	attachment push.EmailAttachment,
) map[string]interface{} {
	return map[string]interface{}{
		"fileName": attachment.FileName,
		"fileType": attachment.FileType,
	}
}

// addFilePathInfo 添加文件路径相关信息
func (helper *AttachmentHelper) addFilePathInfo(
	info map[string]interface{},
	attachment push.EmailAttachment,
) {
	if attachment.FilePath == "" {
		return
	}

	info["filePath"] = attachment.FilePath

	fileInfo, err := os.Stat(attachment.FilePath)
	if err != nil {
		return
	}

	info["fileSize"] = fileInfo.Size()
	info["modTime"] = fileInfo.ModTime()
}

// addContentInfo 添加内容相关信息
func (helper *AttachmentHelper) addContentInfo(
	info map[string]interface{},
	attachment push.EmailAttachment,
) {
	if len(attachment.Content) > 0 {
		info["contentSize"] = len(attachment.Content)
	}
}

//
// MIME 类型检测
//

// MimeTypeRegistry MIME 类型注册表
// 存储常见文件扩展名与 MIME 类型的映射
var MimeTypeRegistry = map[string]string{
	// 文本文档
	".txt":  "text/plain",
	".html": "text/html",
	".css":  "text/css",
	".js":   "application/javascript",
	".json": "application/json",
	".xml":  "application/xml",

	// PDF 文档
	".pdf": "application/pdf",

	// Microsoft Office 文档
	".doc":  "application/msword",
	".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	".xls":  "application/vnd.ms-excel",
	".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	".ppt":  "application/vnd.ms-powerpoint",
	".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",

	// 压缩文件
	".zip": "application/zip",
	".rar": "application/x-rar-compressed",
	".7z":  "application/x-7z-compressed",
	".tar": "application/x-tar",
	".gz":  "application/gzip",

	// 图片文件
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".png":  "image/png",
	".gif":  "image/gif",
	".bmp":  "image/bmp",
	".ico":  "image/x-icon",
	".svg":  "image/svg+xml",

	// 音频文件
	".mp3": "audio/mpeg",
	".wav": "audio/wav",

	// 视频文件
	".mp4": "video/mp4",
	".avi": "video/x-msvideo",
	".mov": "video/quicktime",
}

// DetectMimeType 检测文件的 MIME 类型
// 根据文件扩展名判断 MIME 类型
func DetectMimeType(fileName string) string {
	extension := extractFileExtension(fileName)

	// 尝试使用标准库检测
	if mimeType := mime.TypeByExtension(extension); mimeType != "" {
		return mimeType
	}

	// 尝试使用自定义注册表
	if mimeType, exists := MimeTypeRegistry[extension]; exists {
		return mimeType
	}

	// 返回默认类型
	return defaultMimeType
}

// extractFileExtension 提取文件扩展名
// 转换为小写以实现不区分大小写的匹配
func extractFileExtension(fileName string) string {
	extension := filepath.Ext(fileName)
	return strings.ToLower(extension)
}

//
// 向后兼容的别名
//

// NewAttachment 创建新的附件 (向后兼容)
func (helper *AttachmentHelper) NewAttachment(fileName, filePath string) push.EmailAttachment {
	return helper.CreateAttachment(fileName, filePath)
}

// NewAttachmentWithContent 创建包含内容的附件 (向后兼容)
func (helper *AttachmentHelper) NewAttachmentWithContent(
	fileName string,
	content []byte,
) push.EmailAttachment {
	return helper.CreateAttachmentWithContent(fileName, content)
}

// NewAttachmentWithType 创建指定类型的附件 (向后兼容)
func (helper *AttachmentHelper) NewAttachmentWithType(
	fileName string,
	fileType string,
	filePath string,
) push.EmailAttachment {
	return helper.CreateAttachmentWithCustomType(fileName, fileType, filePath)
}

// DetectMimeTypeAdvanced 高级 MIME 类型检测 (向后兼容)
func DetectMimeTypeAdvanced(fileName string) string {
	return DetectMimeType(fileName)
}

// CommonMimeTypes 常见 MIME 类型映射 (向后兼容)
var CommonMimeTypes = MimeTypeRegistry
