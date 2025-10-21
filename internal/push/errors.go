// push/errors.go
package push

import "errors"

// 定义公共错误变量
var (
	ErrNoRecipient         = errors.New("no valid recipient")
	ErrNoProvider          = errors.New("no provider for channel")
	ErrSendFailed          = errors.New("message send failed")
	ErrIdempotentDuplicate = errors.New("idempotent request duplicate")
	ErrInvalidConfig       = errors.New("invalid configuration")
)

// 带上下文的错误包装
func WrapError(err error, msg string) error {
	return errors.Join(errors.New(msg), err)
}
