package httpapi

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"push-gateway/internal/push"
)

// ==================== 常量定义 ====================

const (
	defaultPage        = 1
	defaultPageSize    = 20
	maxPageSize        = 100
	defaultQueryLimit  = 2000
	maxQueryLimit      = 2000
	hoursInDay         = 24
	defaultSuccessRate = 0.0
	timestampLayout    = "2006-01-02 15:04:05"
)

// ==================== 接口定义 ====================

// PushRecordStore 推送记录存储接口
type PushRecordStore interface {
	QueryRecords(ctx context.Context, namespace string, limit int64) ([]push.Record, error)
	SaveRecord(ctx context.Context, record push.Record) error
}

// TotalRecordsGetter 获取总记录数的接口
// 用于可选的扩展功能
type TotalRecordsGetter interface {
	GetTotalRecords(ctx context.Context, namespace string) (int64, error)
}

// ==================== Handler 处理器 ====================

// PushRecordsHandler 推送记录查询处理器
type PushRecordsHandler struct {
	store     PushRecordStore
	namespace string
}

// NewPushRecordsHandler 创建推送记录处理器
func NewPushRecordsHandler(store PushRecordStore, namespace string) *PushRecordsHandler {
	return &PushRecordsHandler{
		store:     store,
		namespace: namespace,
	}
}

// ==================== HTTP 处理方法 ====================

// HandleQuery 处理推送记录查询请求
// GET /v1/push-records?page=1&page_size=20&status=success&kind=email&namespace=default&priority=1&start_time=1234567890&end_time=1234567899
func (handler *PushRecordsHandler) HandleQuery(writer http.ResponseWriter, request *http.Request) {
	setCORS(writer, "GET, OPTIONS")

	if request.Method == http.MethodOptions {
		return
	}

	if request.Method != http.MethodGet {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := handler.handleRecordQuery(writer, request); err != nil {
		log.Printf("[PUSH_RECORDS] 查询失败: %v", err)
	}
}

// HandleStats 处理推送记录统计请求
// GET /v1/push-records/stats?namespace=default
func (handler *PushRecordsHandler) HandleStats(writer http.ResponseWriter, request *http.Request) {
	setCORS(writer, "GET, OPTIONS")

	if request.Method == http.MethodOptions {
		return
	}

	if request.Method != http.MethodGet {
		writeError(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := handler.handleRecordStats(writer, request); err != nil {
		log.Printf("[PUSH_RECORDS_STATS] 统计失败: %v", err)
	}
}

// ==================== 核心处理逻辑 ====================

// handleRecordQuery 处理记录查询的核心逻辑
func (handler *PushRecordsHandler) handleRecordQuery(writer http.ResponseWriter, request *http.Request) error {
	queryParams := handler.parseQueryParams(request)
	filters := handler.parseFilters(request)

	log.Printf("[PUSH_RECORDS] 查询参数: namespace=%s, page=%d, pageSize=%d",
		queryParams.namespace, queryParams.page, queryParams.pageSize)

	records, err := handler.fetchRecords(request.Context(), queryParams)
	if err != nil {
		writeError(writer, "查询记录失败: "+err.Error(), http.StatusInternalServerError)
		return err
	}

	log.Printf("[PUSH_RECORDS] 查询到 %d 条原始记录", len(records))

	filteredRecords := handler.applyFilters(records, filters)
	log.Printf("[PUSH_RECORDS] 过滤后 %d 条记录", len(filteredRecords))

	paginatedRecords, pagination := handler.paginateRecords(
		filteredRecords,
		queryParams.page,
		queryParams.pageSize,
	)

	handler.writeQueryResponse(writer, paginatedRecords, pagination, filters)
	return nil
}

// handleRecordStats 处理记录统计的核心逻辑
func (handler *PushRecordsHandler) handleRecordStats(writer http.ResponseWriter, request *http.Request) error {
	namespace := handler.getNamespaceFromQuery(request)
	log.Printf("[PUSH_RECORDS_STATS] 查询统计: namespace=%s", namespace)

	records, err := handler.store.QueryRecords(request.Context(), namespace, maxQueryLimit)
	if err != nil {
		writeError(writer, "查询记录失败: "+err.Error(), http.StatusInternalServerError)
		return err
	}

	log.Printf("[PUSH_RECORDS_STATS] 查询到 %d 条记录用于统计", len(records))

	stats := handler.calculateStats(records)
	handler.updateStatsWithTotalRecords(request.Context(), namespace, stats, len(records))

	handler.writeStatsResponse(writer, stats)
	return nil
}

// ==================== 数据模型 ====================

// queryParams 查询参数
type queryParams struct {
	page      int64
	pageSize  int64
	namespace string
	limit     int64
}

// recordFilters 记录过滤器
type recordFilters struct {
	namespace string
	status    string
	kind      string
	priority  string
	startTime string
	endTime   string
}

// paginationInfo 分页信息
type paginationInfo struct {
	CurrentPage int64 `json:"current_page"`
	PageSize    int64 `json:"page_size"`
	TotalCount  int   `json:"total_count"`
	TotalPages  int64 `json:"total_pages"`
	HasPrev     bool  `json:"has_prev"`
	HasNext     bool  `json:"has_next"`
}

// ==================== 参数解析 ====================

// parseQueryParams 解析查询参数
func (handler *PushRecordsHandler) parseQueryParams(request *http.Request) queryParams {
	query := request.URL.Query()

	page := parsePageParam(query.Get("page"))
	pageSize := parsePageSizeParam(query.Get("page_size"))
	namespace := handler.getNamespaceFromQuery(request)
	limit := calculateQueryLimit(page, pageSize)

	return queryParams{
		page:      page,
		pageSize:  pageSize,
		namespace: namespace,
		limit:     limit,
	}
}

// parseFilters 解析过滤参数
func (handler *PushRecordsHandler) parseFilters(request *http.Request) recordFilters {
	query := request.URL.Query()

	return recordFilters{
		namespace: query.Get("namespace"),
		status:    query.Get("status"),
		kind:      query.Get("kind"),
		priority:  query.Get("priority"),
		startTime: query.Get("start_time"),
		endTime:   query.Get("end_time"),
	}
}

// getNamespaceFromQuery 从查询参数获取 namespace
func (handler *PushRecordsHandler) getNamespaceFromQuery(request *http.Request) string {
	namespace := request.URL.Query().Get("namespace")
	if namespace == "" {
		namespace = handler.namespace
	}
	return namespace
}

// ==================== 数据查询 ====================

// fetchRecords 获取记录
func (handler *PushRecordsHandler) fetchRecords(ctx context.Context, params queryParams) ([]push.Record, error) {
	return handler.store.QueryRecords(ctx, params.namespace, params.limit)
}

// ==================== 过滤逻辑 ====================

// applyFilters 应用过滤条件
func (handler *PushRecordsHandler) applyFilters(records []push.Record, filters recordFilters) []push.Record {
	var filtered []push.Record

	for _, record := range records {
		if !handler.matchesFilters(record, filters) {
			continue
		}
		filtered = append(filtered, record)
	}

	return filtered
}

// matchesFilters 检查记录是否匹配过滤条件
func (handler *PushRecordsHandler) matchesFilters(record push.Record, filters recordFilters) bool {
	if !handler.matchesNamespace(record, filters.namespace) {
		return false
	}

	if !handler.matchesStatus(record, filters.status) {
		return false
	}

	if !handler.matchesKind(record, filters.kind) {
		return false
	}

	if !handler.matchesPriority(record, filters.priority) {
		return false
	}

	if !handler.matchesTimeRange(record, filters.startTime, filters.endTime) {
		return false
	}

	return true
}

// matchesNamespace 检查命名空间匹配
func (handler *PushRecordsHandler) matchesNamespace(record push.Record, namespace string) bool {
	if namespace == "" {
		return true
	}
	return strings.EqualFold(record.Namespace, namespace)
}

// matchesStatus 检查状态匹配
func (handler *PushRecordsHandler) matchesStatus(record push.Record, status string) bool {
	if status == "" {
		return true
	}
	return strings.EqualFold(record.Status, status)
}

// matchesKind 检查类型匹配
func (handler *PushRecordsHandler) matchesKind(record push.Record, kind string) bool {
	if kind == "" {
		return true
	}
	return strings.EqualFold(string(record.Kind), kind)
}

// matchesPriority 检查优先级匹配
func (handler *PushRecordsHandler) matchesPriority(record push.Record, priorityString string) bool {
	if priorityString == "" {
		return true
	}

	priority, err := strconv.Atoi(priorityString)
	if err != nil {
		return true
	}

	return record.Priority == priority
}

// matchesTimeRange 检查时间范围匹配
func (handler *PushRecordsHandler) matchesTimeRange(record push.Record, startTimeString, endTimeString string) bool {
	if startTimeString != "" {
		startTime, err := strconv.ParseInt(startTimeString, 10, 64)
		if err == nil && record.CreatedAt < startTime {
			return false
		}
	}

	if endTimeString != "" {
		endTime, err := strconv.ParseInt(endTimeString, 10, 64)
		if err == nil && record.CreatedAt > endTime {
			return false
		}
	}

	return true
}

// ==================== 分页逻辑 ====================

// paginateRecords 对记录进行分页
func (handler *PushRecordsHandler) paginateRecords(
	records []push.Record,
	page int64,
	pageSize int64,
) ([]push.Record, paginationInfo) {
	totalCount := len(records)
	offset := (page - 1) * pageSize
	endIndex := offset + pageSize

	paginatedRecords := handler.sliceRecords(records, offset, endIndex)
	pagination := handler.buildPaginationInfo(page, pageSize, totalCount)

	return paginatedRecords, pagination
}

// sliceRecords 切片记录
func (handler *PushRecordsHandler) sliceRecords(records []push.Record, offset, endIndex int64) []push.Record {
	if offset >= int64(len(records)) {
		return []push.Record{}
	}

	if endIndex > int64(len(records)) {
		return records[offset:]
	}

	return records[offset:endIndex]
}

// buildPaginationInfo 构建分页信息
func (handler *PushRecordsHandler) buildPaginationInfo(page, pageSize int64, totalCount int) paginationInfo {
	totalPages := (int64(totalCount) + pageSize - 1) / pageSize

	return paginationInfo{
		CurrentPage: page,
		PageSize:    pageSize,
		TotalCount:  totalCount,
		TotalPages:  totalPages,
		HasPrev:     page > 1,
		HasNext:     page < totalPages,
	}
}

// ==================== 统计逻辑 ====================

// calculateStats 计算统计信息
func (handler *PushRecordsHandler) calculateStats(records []push.Record) map[string]interface{} {
	statusCount := make(map[string]int)
	kindCount := make(map[string]int)
	channelCount := make(map[string]int)
	recentCount := handler.countRecentRecords(records)

	for _, record := range records {
		statusCount[record.Status]++
		kindCount[string(record.Kind)]++

		for _, channel := range record.Channels {
			channelCount[string(channel)]++
		}
	}

	log.Printf("[PUSH_RECORDS_STATS] 统计: 状态=%+v, 类型=%+v, 通道=%+v",
		statusCount, kindCount, channelCount)

	return map[string]interface{}{
		"total":             len(records),
		"success":           handler.countSuccessRecords(statusCount),
		"pending":           handler.countPendingRecords(statusCount),
		"failed":            handler.countFailedRecords(statusCount),
		"total_records":     len(records),
		"recent_24h_count":  recentCount,
		"status_breakdown":  statusCount,
		"kind_breakdown":    kindCount,
		"channel_breakdown": channelCount,
		"summary": map[string]interface{}{
			"success_rate":      handler.calculateSuccessRate(statusCount),
			"most_used_channel": handler.getMostUsedItem(channelCount),
			"most_used_kind":    handler.getMostUsedItem(kindCount),
		},
	}
}

// countRecentRecords 统计最近24小时的记录数
func (handler *PushRecordsHandler) countRecentRecords(records []push.Record) int {
	now := time.Now()
	count := 0

	for _, record := range records {
		if record.CreatedAt > 0 {
			recordTime := time.Unix(record.CreatedAt, 0)
			if now.Sub(recordTime).Hours() <= hoursInDay {
				count++
			}
		}
	}

	return count
}

// countSuccessRecords 统计成功记录数
func (handler *PushRecordsHandler) countSuccessRecords(statusCount map[string]int) int {
	return statusCount["success"] + statusCount["consumed_success"] + statusCount["fallback_success"]
}

// countPendingRecords 统计待处理记录数
func (handler *PushRecordsHandler) countPendingRecords(statusCount map[string]int) int {
	return statusCount["pending"] + statusCount["enqueued"] + statusCount["consumed_pending"]
}

// countFailedRecords 统计失败记录数
func (handler *PushRecordsHandler) countFailedRecords(statusCount map[string]int) int {
	return statusCount["failed"] + statusCount["consumed_failed"] + statusCount["fallback_failed"]
}

// calculateSuccessRate 计算成功率
func (handler *PushRecordsHandler) calculateSuccessRate(statusCount map[string]int) float64 {
	total := 0
	success := 0

	for status, count := range statusCount {
		total += count
		if handler.isSuccessStatus(status) {
			success += count
		}
	}

	if total == 0 {
		return defaultSuccessRate
	}

	return float64(success) / float64(total) * 100
}

// isSuccessStatus 判断是否为成功状态
func (handler *PushRecordsHandler) isSuccessStatus(status string) bool {
	successStatuses := []string{"success", "consumed_success", "fallback_success"}
	for _, successStatus := range successStatuses {
		if status == successStatus {
			return true
		}
	}
	return false
}

// getMostUsedItem 获取使用最多的项目
func (handler *PushRecordsHandler) getMostUsedItem(countMap map[string]int) string {
	maxCount := 0
	mostUsed := ""

	for item, count := range countMap {
		if count > maxCount {
			maxCount = count
			mostUsed = item
		}
	}

	return mostUsed
}

// updateStatsWithTotalRecords 更新统计信息中的总记录数
// 如果存储实现了 TotalRecordsGetter 接口，则获取真实总数
func (handler *PushRecordsHandler) updateStatsWithTotalRecords(
	ctx context.Context,
	namespace string,
	stats map[string]interface{},
	queryCount int,
) {
	getter, ok := handler.store.(TotalRecordsGetter)
	if !ok {
		return
	}

	totalRecords, err := getter.GetTotalRecords(ctx, namespace)
	if err != nil {
		log.Printf("[PUSH_RECORDS_STATS] 获取真实总记录数失败: %v", err)
		return
	}

	if totalRecords > int64(queryCount) {
		log.Printf("[PUSH_RECORDS_STATS] 更新总记录数: %d", totalRecords)
		stats["total"] = totalRecords
		stats["total_records"] = totalRecords
	}
}

// ==================== 数据格式化 ====================

// formatRecords 格式化记录用于 API 响应
func (handler *PushRecordsHandler) formatRecords(records []push.Record) []map[string]interface{} {
	formatted := make([]map[string]interface{}, len(records))

	for i, record := range records {
		formatted[i] = handler.formatSingleRecord(record)
	}

	return formatted
}

// formatSingleRecord 格式化单条记录
func (handler *PushRecordsHandler) formatSingleRecord(record push.Record) map[string]interface{} {
	return map[string]interface{}{
		"namespace":    record.Namespace,
		"kind":         string(record.Kind),
		"subject":      record.Subject,
		"body":         record.Body,
		"message_data": record.MessageData,
		"channels":     handler.formatChannels(record.Channels),
		"recipients":   handler.formatRecipients(record.Recipients),
		"status":       record.Status,
		"code":         record.Code,
		"content":      record.Content,
		"error_detail": record.ErrorDetail,
		"priority":     record.Priority,
		"created_at":   record.CreatedAt,
		"sent_at":      record.SentAt,
		"created_time": handler.formatTimestamp(record.CreatedAt),
		"sent_time":    handler.formatTimestamp(record.SentAt),
	}
}

// formatChannels 格式化通道列表
func (handler *PushRecordsHandler) formatChannels(channels []push.Channel) []string {
	formatted := make([]string, len(channels))
	for i, channel := range channels {
		formatted[i] = string(channel)
	}
	return formatted
}

// formatRecipients 格式化接收者列表
func (handler *PushRecordsHandler) formatRecipients(recipients []push.Recipient) []map[string]interface{} {
	formatted := make([]map[string]interface{}, len(recipients))

	for i, recipient := range recipients {
		formatted[i] = handler.formatSingleRecipient(recipient)
	}

	return formatted
}

// formatSingleRecipient 格式化单个接收者
func (handler *PushRecordsHandler) formatSingleRecipient(recipient push.Recipient) map[string]interface{} {
	formatted := map[string]interface{}{
		"user_id": recipient.UserID,
	}

	if recipient.Email != "" {
		formatted["email"] = recipient.Email
	}

	if recipient.Phone != "" {
		formatted["phone"] = recipient.Phone
	}

	if recipient.WebInbox != "" {
		formatted["web_inbox"] = recipient.WebInbox
	}

	return formatted
}

// formatTimestamp 格式化时间戳
func (handler *PushRecordsHandler) formatTimestamp(timestamp int64) string {
	if timestamp <= 0 {
		return ""
	}
	return time.Unix(timestamp, 0).Format(timestampLayout)
}

// ==================== 响应写入 ====================

// writeQueryResponse 写入查询响应
func (handler *PushRecordsHandler) writeQueryResponse(
	writer http.ResponseWriter,
	records []push.Record,
	pagination paginationInfo,
	filters recordFilters,
) {
	response := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"records":    handler.formatRecords(records),
			"pagination": pagination,
		},
		"filters": map[string]interface{}{
			"namespace":  filters.namespace,
			"status":     filters.status,
			"kind":       filters.kind,
			"priority":   filters.priority,
			"start_time": filters.startTime,
			"end_time":   filters.endTime,
		},
		"timestamp": time.Now().Unix(),
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(response)
}

// writeStatsResponse 写入统计响应
func (handler *PushRecordsHandler) writeStatsResponse(writer http.ResponseWriter, stats map[string]interface{}) {
	response := map[string]interface{}{
		"success":   true,
		"code":      200,
		"msg":       "success",
		"data":      stats,
		"timestamp": time.Now().Unix(),
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(response)
}

// ==================== 工具函数 ====================

// parsePageParam 解析页码参数
func parsePageParam(pageString string) int64 {
	if pageString == "" {
		return defaultPage
	}

	page, err := strconv.ParseInt(pageString, 10, 64)
	if err != nil || page < 1 {
		return defaultPage
	}

	return page
}

// parsePageSizeParam 解析页面大小参数
func parsePageSizeParam(pageSizeString string) int64 {
	if pageSizeString == "" {
		return defaultPageSize
	}

	pageSize, err := strconv.ParseInt(pageSizeString, 10, 64)
	if err != nil || pageSize < 1 {
		return defaultPageSize
	}

	if pageSize > maxPageSize {
		return maxPageSize
	}

	return pageSize
}

// calculateQueryLimit 计算查询限制
func calculateQueryLimit(page, pageSize int64) int64 {
	limit := pageSize * page * 2
	if limit > maxQueryLimit {
		return maxQueryLimit
	}
	return limit
}
