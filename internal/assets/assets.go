package assets

import (
	"embed"
	"html/template"
)

//go:embed webui/*
var WebFS embed.FS

// 预解析模板，按需扩展
var (
	IndexTpl         = template.Must(template.ParseFS(WebFS, "webui/index.html"))
	InboxTpl         = template.Must(template.ParseFS(WebFS, "webui/inbox.html"))
	DynamicInboxTpl  = template.Must(template.ParseFS(WebFS, "webui/dynamic_inbox.html"))
	MessageSenderTpl = template.Must(template.ParseFS(WebFS, "webui/message_sender.html"))
	PushRecordsTpl   = template.Must(template.ParseFS(WebFS, "webui/push_records.html"))
)
