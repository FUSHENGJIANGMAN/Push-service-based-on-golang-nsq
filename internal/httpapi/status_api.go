package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"push-gateway/internal/status"
)

// ==================== Status Handler ====================

// StatusHandler 状态查询处理器
type StatusHandler struct {
	store status.StatusStore
}

// NewStatusHandler 创建状态处理器
func NewStatusHandler(store status.StatusStore) *StatusHandler {
	return &StatusHandler{
		store: store,
	}
}

// ==================== HTTP 处理方法 ====================

// HandleStatusQuery 处理状态查询请求
// GET /v1/status?message_id=xxx 或 GET /v1/status?channels=sms,email
func (handler *StatusHandler) HandleStatusQuery(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	messageID := request.URL.Query().Get("message_id")
	channelsParam := request.URL.Query().Get("channels")

	if messageID != "" {
		handler.handleSingleMessageQuery(writer, request, messageID)
		return
	}

	if channelsParam != "" {
		handler.handleChannelPendingQuery(writer, request, channelsParam)
		return
	}

	writeError(writer, "缺少 message_id 或 channels 参数", http.StatusBadRequest)
}

// HandleStatusUpdate 处理状态更新请求
// POST /v1/status
// body: { "message_id": "xxx", "status": "success", "error": "..." }
func (handler *StatusHandler) HandleStatusUpdate(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	updateRequest, err := handler.parseUpdateRequest(request)
	if err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return
	}

	if err := handler.validateUpdateRequest(updateRequest); err != nil {
		writeError(writer, err.Error(), http.StatusBadRequest)
		return
	}

	if err := handler.updateMessageStatus(request, updateRequest); err != nil {
		writeError(writer, "更新状态失败: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeSuccess(writer, map[string]string{
		"message": "状态更新成功",
	})
}

// ==================== 数据模型 ====================

// statusUpdateRequest 状态更新请求
type statusUpdateRequest struct {
	MessageID string `json:"message_id"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

// ==================== 查询处理 ====================

// handleSingleMessageQuery 处理单个消息状态查询
func (handler *StatusHandler) handleSingleMessageQuery(
	writer http.ResponseWriter,
	request *http.Request,
	messageID string,
) {
	messageStatus, err := handler.store.GetStatus(request.Context(), messageID)
	if err != nil {
		writeError(writer, "获取状态失败: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if messageStatus == nil {
		writeError(writer, "状态未找到", http.StatusNotFound)
		return
	}

	writeSuccess(writer, messageStatus)
}

// handleChannelPendingQuery 处理通道待处理状态查询
func (handler *StatusHandler) handleChannelPendingQuery(
	writer http.ResponseWriter,
	request *http.Request,
	channelsParam string,
) {
	channels := handler.parseChannels(channelsParam)

	pendingStatuses, err := handler.store.GetPendingStatuses(request.Context(), channels)
	if err != nil {
		writeError(writer, "获取待处理状态失败: "+err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"pending_count": len(pendingStatuses),
		"statuses":      pendingStatuses,
	}

	writeSuccess(writer, response)
}

// ==================== 更新处理 ====================

// parseUpdateRequest 解析更新请求
func (handler *StatusHandler) parseUpdateRequest(request *http.Request) (*statusUpdateRequest, error) {
	var updateRequest statusUpdateRequest
	if err := json.NewDecoder(request.Body).Decode(&updateRequest); err != nil {
		return nil, fmt.Errorf("JSON解析失败: %w", err)
	}
	return &updateRequest, nil
}

// validateUpdateRequest 验证更新请求
func (handler *StatusHandler) validateUpdateRequest(request *statusUpdateRequest) error {
	if request.MessageID == "" {
		return fmt.Errorf("缺少 message_id")
	}

	if request.Status == "" {
		return fmt.Errorf("缺少 status")
	}

	return nil
}

// updateMessageStatus 更新消息状态
func (handler *StatusHandler) updateMessageStatus(
	request *http.Request,
	updateRequest *statusUpdateRequest,
) error {
	return handler.store.UpdateStatus(
		request.Context(),
		updateRequest.MessageID,
		updateRequest.Status,
		updateRequest.Error,
	)
}

// ==================== 工具函数 ====================

// parseChannels 解析通道参数
func (handler *StatusHandler) parseChannels(channelsParam string) []string {
	channels := strings.Split(channelsParam, ",")

	for i := range channels {
		channels[i] = strings.TrimSpace(channels[i])
	}

	return channels
}
