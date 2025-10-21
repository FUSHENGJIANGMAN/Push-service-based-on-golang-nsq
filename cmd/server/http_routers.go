package main

import (
	"context"
	"io"
	"io/fs"
	"log"
	"net/http"
	"strings"

	"push-gateway/internal/assets"
	"push-gateway/internal/httpapi"
	"push-gateway/internal/inbox"
	"push-gateway/internal/push"

	"github.com/gin-gonic/gin"
)

//
// 数据模型定义
//

// UnifiedResponse 统一的 API 响应格式
type UnifiedResponse struct {
	Code int         `json:"code"`
	Data interface{} `json:"data,omitempty"`
	Msg  string      `json:"msg"`
}

// InboxQueryRequest 收件箱查询请求参数
type InboxQueryRequest struct {
	UserID string `form:"user_id" binding:"required"`
	Status string `form:"status"`
	Offset int64  `form:"offset"`
	Limit  int64  `form:"limit"`
}

// InboxActionRequest 收件箱操作请求(标记已读/删除)
type InboxActionRequest struct {
	UserID string   `json:"user_id" binding:"required"`
	IDs    []string `json:"ids" binding:"required,min=1"`
}

// ModemStatusResponse 调制解调器状态响应
type ModemStatusResponse struct {
	Available     bool   `json:"available"`
	VoiceQueueLen int    `json:"voice_queue_len"`
	SmsQueueLen   int    `json:"sms_queue_len"`
	VoiceBusy     bool   `json:"voice_busy"`
	SmsBusy       bool   `json:"sms_busy"`
	Error         string `json:"error,omitempty"`
}

//
// 接口定义
//

// TemplateExecutor 模板执行器接口
// 用于渲染 HTML 模板
type TemplateExecutor interface {
	Execute(writer io.Writer, data interface{}) error
}

// ModemManager 调制解调器管理器接口
type ModemManager interface {
	GetQueueStatus() (int, int, bool, bool, bool)
	GetLastError() error
}

//
// 辅助函数 - 响应处理
//

// sendSuccessResponse 发送成功响应
func sendSuccessResponse(context *gin.Context, data interface{}) {
	context.JSON(http.StatusOK, UnifiedResponse{
		Code: http.StatusOK,
		Data: data,
		Msg:  "success",
	})
}

// sendErrorResponse 发送错误响应
func sendErrorResponse(context *gin.Context, httpStatus int, message string) {
	context.JSON(httpStatus, UnifiedResponse{
		Code: httpStatus,
		Data: nil,
		Msg:  message,
	})
}

//
// 辅助函数 - 参数处理
//

// parseIntWithDefault 解析整数参数,失败时返回默认值
// 用于处理可选的分页参数,确保参数合法性

// applyDefaultPagination 为查询请求应用默认分页参数
// 避免客户端不传参数时查询过大数据集
func applyDefaultPagination(request *InboxQueryRequest) {
	if request.Limit == 0 {
		request.Limit = 20
	}
	if request.Limit > 100 {
		request.Limit = 100
	}
}

//
// 中间件
//

// corsMiddleware 跨域资源共享中间件
// 允许所有来源访问,便于前端开发和集成
// 生产环境建议根据需求配置白名单
func corsMiddleware() gin.HandlerFunc {
	return func(context *gin.Context) {
		context.Header("Access-Control-Allow-Origin", "*")
		context.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		context.Header("Access-Control-Allow-Headers", "Content-Type, Accept, Authorization")

		if context.Request.Method == "OPTIONS" {
			context.AbortWithStatus(http.StatusNoContent)
			return
		}

		context.Next()
	}
}

//
// 处理器 - 收件箱相关
//

// InboxHandler 收件箱业务处理器
type InboxHandler struct {
	store inbox.Store
}

// NewInboxHandler 创建收件箱处理器实例
func NewInboxHandler(store inbox.Store) *InboxHandler {
	return &InboxHandler{store: store}
}

// handleQueryInbox 处理收件箱查询请求
func (handler *InboxHandler) handleQueryInbox(context *gin.Context) {
	var request InboxQueryRequest

	if err := context.ShouldBindQuery(&request); err != nil {
		sendErrorResponse(context, http.StatusBadRequest, "参数验证失败: "+err.Error())
		return
	}

	applyDefaultPagination(&request)

	messages, total, err := handler.store.List(
		context.Request.Context(),
		request.UserID,
		request.Status,
		request.Offset,
		request.Limit,
	)

	if err != nil {
		log.Printf("[InboxHandler] 查询失败: %v", err)
		sendErrorResponse(context, http.StatusInternalServerError, "查询收件箱失败")
		return
	}

	// 直接返回原有格式
	context.JSON(http.StatusOK, map[string]interface{}{
		"total": total,
		"data":  messages,
	})
}

// handleMarkAsRead 处理标记已读请求
func (handler *InboxHandler) handleMarkAsRead(context *gin.Context) {
	handler.performInboxAction(context, handler.store.MarkRead)
}

// handleDeleteMessages 处理删除消息请求
func (handler *InboxHandler) handleDeleteMessages(context *gin.Context) {
	handler.performInboxAction(context, handler.store.Delete)
}

// performInboxAction 执行收件箱操作的通用方法
// 提取标记已读和删除的公共逻辑,遵循 DRY 原则
func (handler *InboxHandler) performInboxAction(
	context *gin.Context,
	action func(ctx context.Context, userID string, ids []string) (int, error),
) {
	var request InboxActionRequest

	if err := context.ShouldBindJSON(&request); err != nil {
		sendErrorResponse(context, http.StatusBadRequest, "请求格式错误: "+err.Error())
		return
	}

	affectedCount, err := action(context.Request.Context(), request.UserID, request.IDs)
	if err != nil {
		log.Printf("[InboxHandler] 操作失败: %v", err)
		sendErrorResponse(context, http.StatusInternalServerError, "操作执行失败")
		return
	}

	sendSuccessResponse(context, map[string]interface{}{
		"updated": affectedCount,
		"deleted": affectedCount,
	})
}

//
// 处理器 - 消息格式相关
//

// FormatHandler 消息格式处理器
type FormatHandler struct {
	formats map[string]push.MessageFormat
}

// NewFormatHandler 创建格式处理器实例
func NewFormatHandler(formats map[string]push.MessageFormat) *FormatHandler {
	return &FormatHandler{formats: formats}
}

// handleGetFormats 获取所有支持的消息格式
// 用于前端动态渲染表单字段
func (handler *FormatHandler) handleGetFormats(context *gin.Context) {
	// 直接返回原有格式，不使用统一响应包装
	context.JSON(http.StatusOK, map[string]interface{}{
		"data": handler.formats,
	})
}

//
// 处理器 - 调制解调器相关
//

// ModemHandler 调制解调器状态处理器
type ModemHandler struct {
	manager ModemManager
}

// NewModemHandler 创建调制解调器处理器实例
func NewModemHandler(manager ModemManager) *ModemHandler {
	return &ModemHandler{manager: manager}
}

// handleGetStatus 获取调制解调器运行状态
func (handler *ModemHandler) handleGetStatus(context *gin.Context) {
	voiceQueueLen, smsQueueLen, voiceBusy, smsBusy, available := handler.manager.GetQueueStatus()

	response := ModemStatusResponse{
		Available:     available,
		VoiceQueueLen: voiceQueueLen,
		SmsQueueLen:   smsQueueLen,
		VoiceBusy:     voiceBusy,
		SmsBusy:       smsBusy,
	}

	// 仅在不可用时返回错误信息,避免冗余数据
	if !available {
		if err := handler.manager.GetLastError(); err != nil {
			response.Error = err.Error()
		}
	}

	sendSuccessResponse(context, response)
}

//
// 处理器 - 页面渲染相关
//

// PageHandler 页面渲染处理器
type PageHandler struct{}

// NewPageHandler 创建页面处理器实例
func NewPageHandler() *PageHandler {
	return &PageHandler{}
}

// handleHomePage 渲染首页
func (handler *PageHandler) handleHomePage(context *gin.Context) {
	handler.renderTemplate(context, assets.IndexTpl, nil)
}

// handleDynamicInboxPage 渲染动态收件箱页面
func (handler *PageHandler) handleDynamicInboxPage(context *gin.Context) {
	handler.renderTemplate(context, assets.DynamicInboxTpl, nil)
}

// handleMessageSenderPage 渲染消息发送器演示页面
func (handler *PageHandler) handleMessageSenderPage(context *gin.Context) {
	handler.renderTemplate(context, assets.MessageSenderTpl, nil)
}

// handlePushRecordsPage 渲染推送记录查询页面
func (handler *PageHandler) handlePushRecordsPage(context *gin.Context) {
	handler.renderTemplate(context, assets.PushRecordsTpl, nil)
}

// renderTemplate 渲染 HTML 模板的通用方法
// 集中处理模板错误,避免在每个页面处理器中重复代码
func (handler *PageHandler) renderTemplate(
	context *gin.Context,
	template TemplateExecutor,
	data interface{},
) {
	context.Header("Content-Type", "text/html; charset=utf-8")

	if err := template.Execute(context.Writer, data); err != nil {
		log.Printf("[PageHandler] 模板渲染失败: %v", err)
		context.String(http.StatusInternalServerError, "页面渲染失败")
	}
}

//
// 辅助函数 - 格式构建
//

// buildMessageFormats 构建消息格式映射
// 从配置中提取格式定义,转换为前端可用的结构
func buildMessageFormats(config *AppContext) map[string]push.MessageFormat {
	formats := make(map[string]push.MessageFormat)

	for formatType, formatConfig := range config.Config.MessageFormats.GetAllFormats() {
		fields := make([]push.MessageField, len(formatConfig.Fields))

		for index, field := range formatConfig.Fields {
			fields[index] = push.MessageField{
				Name:        field.Name,
				Type:        field.Type,
				Required:    field.Required,
				Description: field.Description,
			}
		}

		formats[formatType] = push.MessageFormat{
			Type:   formatConfig.Type,
			Fields: fields,
		}
	}

	return formats
}

// extractAllowedTopics 提取允许的消息主题列表
func extractAllowedTopics(formats map[string]push.MessageFormat) []string {
	topics := make([]string, 0, len(formats))

	for topic := range formats {
		topics = append(topics, topic)
	}

	return topics
}

//
// 辅助函数 - 动态路由注册
//

// registerDynamicRoutes 注册动态消息类型路由
// 为每个非内置的消息类型创建独立的 API 端点
func registerDynamicRoutes(
	router *gin.Engine,
	service httpapi.Service,
	registry *push.MessageRegistry,
	formats map[string]push.MessageFormat,
) {
	// 内置消息类型不需要动态路由
	builtinTypes := map[string]bool{
		"Msg":          true,
		"Notification": true,
		"Alert":        true,
		"VoiceMessage": true,
		"SMSMessage":   true,
	}

	registeredRoutes := make(map[string]bool)

	// 从注册表注册路由
	for messageType := range registry.GetAllFormats() {
		if !builtinTypes[messageType] && !registeredRoutes[messageType] {
			handler := httpapi.NewDynamicMessageHandler(service, registry, messageType)
			router.POST("/v1/push/"+messageType, gin.WrapH(handler))
			registeredRoutes[messageType] = true
		}
	}

	// 从格式配置注册路由(兜底)
	for messageType := range formats {
		if !builtinTypes[messageType] && !registeredRoutes[messageType] {
			handler := httpapi.NewDynamicMessageHandler(service, registry, messageType)
			router.POST("/v1/push/"+messageType, gin.WrapH(handler))
			registeredRoutes[messageType] = true
		}
	}
}

//
// 路由构建主函数
//

// BuildGinRouter 构建 Gin 路由器
// 集中管理所有 HTTP 路由,包括 API 接口、动态消息类型、页面和静态资源
func BuildGinRouter(app *AppContext) *gin.Engine {
	router := gin.Default()

	// 应用全局中间件
	router.Use(corsMiddleware())

	// 构建消息格式和主题
	formats := buildMessageFormats(app)
	allowedTopics := extractAllowedTopics(formats)

	// 初始化处理器
	inboxHandler := NewInboxHandler(app.InboxStore)
	formatHandler := NewFormatHandler(formats)
	pageHandler := NewPageHandler()

	// API v1 路由组
	apiV1 := router.Group("/v1")
	{
		// 推送相关接口
		registerPushRoutes(apiV1, app, allowedTopics, formats)

		// 状态查询接口
		registerStatusRoutes(apiV1, app)

		// 推送记录接口
		registerRecordRoutes(apiV1, app)

		// 文件操作接口
		registerFileRoutes(apiV1)

		// 调制解调器接口(仅在启用时注册)
		registerModemRoutes(apiV1, app)
	}

	// 收件箱相关路由
	registerInboxRoutes(router, inboxHandler, formatHandler)

	// 页面路由
	registerPageRoutes(router, pageHandler, inboxHandler)

	// 静态资源路由
	registerStaticRoutes(router)

	// 动态消息类型路由
	registerDynamicRoutes(router, app.Service, app.MessageRegistry, formats)

	return router
}

// registerPushRoutes 注册推送相关路由
func registerPushRoutes(
	group *gin.RouterGroup,
	app *AppContext,
	allowedTopics []string,
	formats map[string]push.MessageFormat,
) {
	mainHandler := httpapi.NewHandlerWithRegistry(
		app.Service,
		allowedTopics,
		formats,
		app.MessageRegistry,
		app.StatusStore,
		app.ModemManager,
	)

	group.POST("/push", gin.WrapH(mainHandler))
	group.POST("/voice", gin.WrapH(httpapi.NewVoiceMessageHandler(app.Service)))
	group.POST("/sms", gin.WrapH(httpapi.NewSMSMessageHandler(app.Service)))
}

// registerStatusRoutes 注册状态查询路由
func registerStatusRoutes(group *gin.RouterGroup, app *AppContext) {
	statusHandler := httpapi.NewStatusHandler(app.StatusStore)
	group.GET("/status", gin.WrapF(statusHandler.HandleStatusQuery))
	group.POST("/status/update", gin.WrapF(statusHandler.HandleStatusUpdate))
}

// registerRecordRoutes 注册推送记录路由
func registerRecordRoutes(group *gin.RouterGroup, app *AppContext) {
	// 尝试类型断言为 httpapi.PushRecordStore
	recordStore, ok := app.RecordStoreImpl.(httpapi.PushRecordStore)
	if !ok {
		log.Println("[Router] RecordStore 不支持查询接口,跳过推送记录路由注册")
		return
	}

	recordHandler := httpapi.NewPushRecordsHandler(
		recordStore,
		app.Config.Storage.Namespace,
	)

	group.GET("/push-records", gin.WrapF(recordHandler.HandleQuery))
	group.GET("/push-records/stats", gin.WrapF(recordHandler.HandleStats))
}

// registerFileRoutes 注册文件操作路由
func registerFileRoutes(group *gin.RouterGroup) {
	group.POST("/upload", gin.WrapF(httpapi.FileUploadHandler))
	group.GET("/download", gin.WrapF(httpapi.FileDownloadHandler))
	group.DELETE("/delete", gin.WrapF(httpapi.FileDeleteHandler))
}

// registerModemRoutes 注册调制解调器路由
// 仅在 ModemManager 启用时注册,避免空指针
func registerModemRoutes(group *gin.RouterGroup, app *AppContext) {
	if app.ModemManager == nil {
		return
	}

	modemHandler := NewModemHandler(app.ModemManager)
	group.GET("/modem/status", modemHandler.handleGetStatus)
}

// registerInboxRoutes 注册收件箱 API 路由
func registerInboxRoutes(
	router *gin.Engine,
	inboxHandler *InboxHandler,
	formatHandler *FormatHandler,
) {
	inboxGroup := router.Group("/dynamic-inbox")
	{
		inboxGroup.GET("/message-formats", formatHandler.handleGetFormats)
		inboxGroup.GET("/query", inboxHandler.handleQueryInbox)
		inboxGroup.POST("/mark_read", inboxHandler.handleMarkAsRead)
		inboxGroup.POST("/delete", inboxHandler.handleDeleteMessages)
	}
}

// registerPageRoutes 注册页面路由
// 支持浏览器直接访问和 JSON API 两种模式
func registerPageRoutes(
	router *gin.Engine,
	pageHandler *PageHandler,
	inboxHandler *InboxHandler,
) {
	router.GET("/", pageHandler.handleHomePage)
	router.GET("/message-sender", pageHandler.handleMessageSenderPage)
	router.GET("/push-records", pageHandler.handlePushRecordsPage)

	// 动态收件箱页面支持双模式:
	// 1. HTML 模式: 浏览器访问返回页面
	// 2. JSON 模式: Accept 头包含 application/json 时返回数据
	router.GET("/dynamic-inbox", func(context *gin.Context) {
		acceptHeader := context.GetHeader("Accept")
		userID := context.Query("user_id")

		if strings.Contains(acceptHeader, "application/json") && userID != "" {
			inboxHandler.handleQueryInbox(context)
			return
		}

		pageHandler.handleDynamicInboxPage(context)
	})
}

// registerStaticRoutes 注册静态资源路由
// 使用嵌入式文件系统提供前端资源
func registerStaticRoutes(router *gin.Engine) {
	webFS, err := fs.Sub(assets.WebFS, "webui")
	if err != nil {
		log.Printf("[Router] 静态资源子系统创建失败: %v", err)
		return
	}

	router.StaticFS("/static", http.FS(webFS))
}
