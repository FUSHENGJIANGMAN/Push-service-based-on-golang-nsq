package email

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/smtp"
	"time"

	"push-gateway/internal/config"
)

// SMTP 协议默认端口常量
const (
	DefaultSMTPPort         = 25  // 普通 SMTP 端口
	DefaultSMTPSSLPort      = 465 // SSL/TLS 加密端口
	DefaultSMTPSTARTTLSPort = 587 // STARTTLS 升级端口
	DefaultDialTimeout      = 30 * time.Second
)

// SMTPTransport 负责底层 SMTP 连接、认证和邮件发送
// 统一管理 SSL、STARTTLS 等不同安全协议的连接方式
type SMTPTransport struct {
	emailConfig config.EmailProvider
}

// NewSMTPTransport 创建 SMTP 传输实例
func NewSMTPTransport(emailConfig config.EmailProvider) *SMTPTransport {
	return &SMTPTransport{
		emailConfig: emailConfig,
	}
}

// resolvePort 根据安全协议推断默认端口
// 优先使用配置的端口,否则根据 SSL/TLS 协议自动选择标准端口
func (transport *SMTPTransport) resolvePort() int {
	if transport.emailConfig.SMTPPort > 0 {
		return transport.emailConfig.SMTPPort
	}

	if transport.emailConfig.UseSSL {
		return DefaultSMTPSSLPort
	}

	if transport.emailConfig.UseTLS {
		return DefaultSMTPSTARTTLSPort
	}

	return DefaultSMTPPort
}

// dial 建立 SMTP 客户端连接
// 根据配置自动选择 SSL 或 STARTTLS 协议,返回客户端和清理函数
func (transport *SMTPTransport) dial(ctx context.Context) (*smtp.Client, func(), error) {
	if transport.emailConfig.SMTPHost == "" {
		return nil, nil, errors.New("smtp host cannot be empty")
	}

	connection, err := transport.dialConnection(ctx)
	if err != nil {
		return nil, nil, err
	}

	// SSL 协议需要在 TCP 连接上直接建立 TLS 层
	if transport.emailConfig.UseSSL {
		return transport.createSSLClient(connection)
	}

	// 普通连接,可选择使用 STARTTLS 升级
	return transport.createPlainClient(connection)
}

// dialConnection 建立底层 TCP 连接
// 支持 context 超时控制,确保不会无限等待
func (transport *SMTPTransport) dialConnection(ctx context.Context) (net.Conn, error) {
	port := transport.resolvePort()
	address := net.JoinHostPort(transport.emailConfig.SMTPHost, fmt.Sprintf("%d", port))

	var dialer net.Dialer

	// 如果 context 设置了截止时间,使用 context 控制超时
	if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
		connection, err := dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, fmt.Errorf("failed to dial smtp server %s: %w", address, err)
		}

		// 为连接设置整体超时时间
		_ = connection.SetDeadline(deadline)
		return connection, nil
	}

	// 否则使用默认超时时间
	connection, err := net.DialTimeout("tcp", address, DefaultDialTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to dial smtp server %s: %w", address, err)
	}

	return connection, nil
}

// createSSLClient 创建 SSL 加密的 SMTP 客户端
// 在已建立的 TCP 连接上进行 TLS 握手
func (transport *SMTPTransport) createSSLClient(connection net.Conn) (*smtp.Client, func(), error) {
	tlsConfig := &tls.Config{
		ServerName: transport.emailConfig.SMTPHost,
	}

	tlsConnection := tls.Client(connection, tlsConfig)

	if err := tlsConnection.Handshake(); err != nil {
		_ = connection.Close()
		return nil, nil, fmt.Errorf("ssl handshake failed: %w", err)
	}

	client, err := smtp.NewClient(tlsConnection, transport.emailConfig.SMTPHost)
	if err != nil {
		_ = connection.Close()
		return nil, nil, fmt.Errorf("failed to create smtp client with ssl: %w", err)
	}

	closeFunction := func() {
		_ = client.Quit()
		_ = connection.Close()
	}

	return client, closeFunction, nil
}

// createPlainClient 创建普通 SMTP 客户端
// 可选择使用 STARTTLS 将明文连接升级为加密连接
func (transport *SMTPTransport) createPlainClient(connection net.Conn) (*smtp.Client, func(), error) {
	client, err := smtp.NewClient(connection, transport.emailConfig.SMTPHost)
	if err != nil {
		_ = connection.Close()
		return nil, nil, fmt.Errorf("failed to create smtp client: %w", err)
	}

	// STARTTLS 用于在普通连接建立后升级为加密连接
	if transport.emailConfig.UseTLS {
		tlsConfig := &tls.Config{
			ServerName: transport.emailConfig.SMTPHost,
		}

		if err = client.StartTLS(tlsConfig); err != nil {
			_ = client.Quit()
			_ = connection.Close()
			return nil, nil, fmt.Errorf("starttls upgrade failed: %w", err)
		}
	}

	closeFunction := func() {
		_ = client.Quit()
		_ = connection.Close()
	}

	return client, closeFunction, nil
}

// createAuthentication 创建 SMTP 认证实例
// 目前仅支持 PLAIN 认证方式(最广泛使用的认证机制)
func (transport *SMTPTransport) createAuthentication() smtp.Auth {
	if transport.emailConfig.Username == "" || transport.emailConfig.Password == "" {
		return nil
	}

	return smtp.PlainAuth(
		"",
		transport.emailConfig.Username,
		transport.emailConfig.Password,
		transport.emailConfig.SMTPHost,
	)
}

// SendRaw 发送原始邮件数据
// rawMessage: 完整的 MIME 格式邮件内容(包含头部和正文)
// recipients: 信封收件人列表(包含 To、CC、BCC 所有接收者)
func (transport *SMTPTransport) SendRaw(ctx context.Context, rawMessage []byte, recipients []string) error {
	if len(recipients) == 0 {
		return errors.New("recipients list cannot be empty")
	}

	client, closeFunction, err := transport.dial(ctx)
	if err != nil {
		return err
	}
	defer closeFunction()

	if err := transport.authenticate(client); err != nil {
		return err
	}

	if err := transport.setMailEnvelope(client, recipients); err != nil {
		return err
	}

	if err := transport.writeMessageData(client, rawMessage); err != nil {
		return err
	}

	return nil
}

// authenticate 执行 SMTP 身份认证
// 如果配置了用户名和密码,则进行认证;否则尝试匿名发送
func (transport *SMTPTransport) authenticate(client *smtp.Client) error {
	authentication := transport.createAuthentication()

	// 部分 SMTP 服务器允许匿名发送,因此认证是可选的
	if authentication == nil {
		return nil
	}

	if err := client.Auth(authentication); err != nil {
		return fmt.Errorf("smtp authentication failed: %w", err)
	}

	return nil
}

// setMailEnvelope 设置邮件信封信息
// 包括发件人(MAIL FROM)和所有收件人(RCPT TO)
func (transport *SMTPTransport) setMailEnvelope(client *smtp.Client, recipients []string) error {
	if err := client.Mail(transport.emailConfig.From); err != nil {
		return fmt.Errorf("MAIL FROM command failed: %w", err)
	}

	for _, recipient := range recipients {
		// 跳过空收件人地址
		if recipient == "" {
			continue
		}

		if err := client.Rcpt(recipient); err != nil {
			return fmt.Errorf("RCPT TO command failed for %s: %w", recipient, err)
		}
	}

	return nil
}

// writeMessageData 写入邮件正文数据
// 发送 DATA 命令并传输完整的邮件内容
func (transport *SMTPTransport) writeMessageData(client *smtp.Client, rawMessage []byte) error {
	writer, err := client.Data()
	if err != nil {
		return fmt.Errorf("DATA command failed: %w", err)
	}

	if _, err = writer.Write(rawMessage); err != nil {
		return fmt.Errorf("failed to write message body: %w", err)
	}

	if err = writer.Close(); err != nil {
		return fmt.Errorf("failed to close message body: %w", err)
	}

	return nil
}
