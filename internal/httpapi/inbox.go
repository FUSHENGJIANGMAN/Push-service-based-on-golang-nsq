package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"push-gateway/internal/inbox"
	"push-gateway/internal/push"
)

// ==================== Inbox Handler ====================

// InboxHandler 收件箱处理器
type InboxHandler struct {
	store    inbox.Store
	registry *push.MessageRegistry
}

// NewInboxHandler 创建收件箱处理器
func NewInboxHandler(store inbox.Store) *InboxHandler {
	return &InboxHandler{
		store:    store,
		registry: nil,
	}
}

// NewInboxHandlerWithRegistry 创建支持动态消息格式的收件箱处理器
func NewInboxHandlerWithRegistry(store inbox.Store, registry *push.MessageRegistry) *InboxHandler {
	return &InboxHandler{
		store:    store,
		registry: registry,
	}
}

// ==================== HTTP 处理方法 ====================

// MessageFormats 获取消息格式配置
// GET /v1/inbox/message-formats
func (handler *InboxHandler) MessageFormats(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	formats := handler.getMessageFormats()
	writeSuccess(writer, formats)
}

// Query 查询收件箱消息列表
// GET /v1/inbox?user_id=u1&status=unread|read|all&offset=0&limit=20
func (handler *InboxHandler) Query(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	queryParams, err := handler.parseQueryParameters(request)
	if err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return
	}

	items, total, err := handler.store.List(
		request.Context(),
		queryParams.userID,
		queryParams.status,
		queryParams.offset,
		queryParams.limit,
	)
	if err != nil {
		writeError(writer, "查询失败: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeInboxQuerySuccess(writer, items, total)
}

// MarkRead 标记消息为已读
// POST /v1/inbox/mark_read
// body: { "user_id": "u1", "ids": ["1","2","3"] }
func (handler *InboxHandler) MarkRead(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	markRequest, err := handler.parseMarkReadRequest(request)
	if err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return
	}

	updatedCount, err := handler.store.MarkRead(
		request.Context(),
		markRequest.UserID,
		markRequest.IDs,
	)
	if err != nil {
		writeError(writer, "标记已读失败: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeSuccess(writer, map[string]any{
		"updated": updatedCount,
		"message": "标记成功",
	})
}

// Delete 删除收件箱消息
// POST /v1/inbox/delete
// body: { "user_id": "u1", "ids": ["1","2","3"] }
func (handler *InboxHandler) Delete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	deleteRequest, err := handler.parseDeleteRequest(request)
	if err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return
	}

	deletedCount, err := handler.store.Delete(
		request.Context(),
		deleteRequest.UserID,
		deleteRequest.IDs,
	)
	if err != nil {
		writeError(writer, "删除失败: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeSuccess(writer, map[string]any{
		"deleted": deletedCount,
		"message": "删除成功",
	})
}

// ==================== 数据模型 ====================

// inboxQueryParams 收件箱查询参数
type inboxQueryParams struct {
	userID string
	status string
	offset int64
	limit  int64
}

// markReadRequest 标记已读请求
type markReadRequest struct {
	UserID string   `json:"user_id"`
	IDs    []string `json:"ids"`
}

// deleteRequest 删除请求
type deleteRequest struct {
	UserID string   `json:"user_id"`
	IDs    []string `json:"ids"`
}

// messageFieldConfig 消息字段配置
type messageFieldConfig struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Label string `json:"label"`
}

// ==================== 解析和验证方法 ====================

// parseQueryParameters 解析查询参数
func (handler *InboxHandler) parseQueryParameters(request *http.Request) (*inboxQueryParams, error) {
	query := request.URL.Query()

	userID := strings.TrimSpace(query.Get("user_id"))
	if userID == "" {
		return nil, fmt.Errorf("缺少 user_id 参数")
	}

	status := query.Get("status")
	if status == "" {
		status = "all"
	}

	offset := parseIntWithDefault(query.Get("offset"), 0)
	limit := parseIntWithDefault(query.Get("limit"), 20)

	return &inboxQueryParams{
		userID: userID,
		status: status,
		offset: offset,
		limit:  limit,
	}, nil
}

// parseMarkReadRequest 解析标记已读请求
func (handler *InboxHandler) parseMarkReadRequest(request *http.Request) (*markReadRequest, error) {
	var req markReadRequest
	if err := json.NewDecoder(request.Body).Decode(&req); err != nil {
		return nil, fmt.Errorf("JSON解析失败: %w", err)
	}

	req.UserID = strings.TrimSpace(req.UserID)
	if req.UserID == "" {
		return nil, fmt.Errorf("缺少 user_id")
	}

	if len(req.IDs) == 0 {
		return nil, fmt.Errorf("缺少 ids")
	}

	return &req, nil
}

// parseDeleteRequest 解析删除请求
func (handler *InboxHandler) parseDeleteRequest(request *http.Request) (*deleteRequest, error) {
	var req deleteRequest
	if err := json.NewDecoder(request.Body).Decode(&req); err != nil {
		return nil, fmt.Errorf("JSON解析失败: %w", err)
	}

	req.UserID = strings.TrimSpace(req.UserID)
	if req.UserID == "" {
		return nil, fmt.Errorf("缺少 user_id")
	}

	if len(req.IDs) == 0 {
		return nil, fmt.Errorf("缺少 ids")
	}

	return &req, nil
}

// ==================== 业务逻辑方法 ====================

// getMessageFormats 获取消息格式配置
// 优先从动态注册表获取，否则返回静态配置
func (handler *InboxHandler) getMessageFormats() map[string]any {
	if handler.registry != nil {
		return handler.getDynamicFormats()
	}
	return handler.getStaticFormats()
}

// getDynamicFormats 从注册表获取动态格式配置
func (handler *InboxHandler) getDynamicFormats() map[string]any {
	formats := make(map[string]any)
	allFormats := handler.registry.GetAllFormats()

	for topic, format := range allFormats {
		fieldConfigs := make([]messageFieldConfig, len(format.Fields))
		for i, field := range format.Fields {
			fieldConfigs[i] = messageFieldConfig{
				Name:  field.Name,
				Type:  field.Type,
				Label: getFieldLabel(field.Name, field.Description),
			}
		}
		formats[topic] = fieldConfigs
	}

	return formats
}

// getStaticFormats 获取静态格式配置
// 用于注册表不可用时的备用方案
func (handler *InboxHandler) getStaticFormats() map[string]any {
	return map[string]any{
		"Alert": []messageFieldConfig{
			{Name: "severity", Type: "string", Label: "严重级别"},
			{Name: "message", Type: "string", Label: "警报信息"},
			{Name: "timestamp", Type: "string", Label: "时间"},
			{Name: "userIds", Type: "[]uint64", Label: "用户ID"},
			{Name: "sender", Type: "uint64", Label: "发送人"},
			{Name: "alertId", Type: "string", Label: "警报ID"},
			{Name: "category", Type: "string", Label: "分类"},
		},
	}
}

// ==================== 响应写入方法 ====================

// writeInboxQuerySuccess 写入收件箱查询成功响应
func writeInboxQuerySuccess(writer http.ResponseWriter, items interface{}, total int64) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)

	response := map[string]any{
		"code":  200,
		"msg":   "success",
		"total": total,
		"data":  items,
	}

	_ = json.NewEncoder(writer).Encode(response)
}

// ==================== 工具函数 ====================

// parseIntWithDefault 解析整数，失败则返回默认值
func parseIntWithDefault(value string, defaultValue int64) int64 {
	if value == "" {
		return defaultValue
	}

	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return defaultValue
	}

	return parsed
}

// getFieldLabel 生成字段标签
// 优先使用描述，否则查找预定义映射，最后返回字段名
func getFieldLabel(name, description string) string {
	if description != "" && description != "无描述" {
		return description
	}

	if label, exists := fieldLabelMap[name]; exists {
		return label
	}

	return name
}

// fieldLabelMap 字段名到中文标签的映射
var fieldLabelMap = map[string]string{
	"severity":         "严重级别",
	"message":          "消息内容",
	"timestamp":        "时间戳",
	"userIds":          "用户ID列表",
	"sender":           "发送人",
	"alertId":          "警报ID",
	"category":         "分类",
	"maintenanceId":    "维护ID",
	"title":            "标题",
	"description":      "描述",
	"startTime":        "开始时间",
	"endTime":          "结束时间",
	"affectedServices": "受影响服务",
	"priority":         "优先级",
	"appId":            "应用ID",
	"content":          "内容",
	"receivers":        "接收人",
	"type":             "类型",
	"body":             "内容",
}
