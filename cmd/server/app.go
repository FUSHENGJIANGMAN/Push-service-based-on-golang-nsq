// 常量定义
package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"push-gateway/internal/config"
)

const (
	configFilePath         = "etc/app.yaml"
	gracefulShutdownPeriod = 5 * time.Second
	serialDevicePattern    = "ls /dev/ttyUSB* /dev/ttyS* 2>/dev/null"
)

//
// 串口设备检测与选择
//

// SerialPortDetector 串口设备检测器
// 仅在 Linux 环境下工作,其他操作系统会直接跳过
type SerialPortDetector struct {
	operatingSystem string
	scanner         *bufio.Scanner
}

// NewSerialPortDetector 创建串口检测器实例
func NewSerialPortDetector() *SerialPortDetector {
	return &SerialPortDetector{
		operatingSystem: runtime.GOOS,
		scanner:         bufio.NewScanner(os.Stdin),
	}
}

// DetectAndSelect 检测串口设备并提供交互式选择
// 仅在 Linux 环境执行,避免在 Windows/macOS 上报错
func (detector *SerialPortDetector) DetectAndSelect(configuration *config.Config) {
	if !detector.isLinuxEnvironment() {
		return
	}

	fmt.Println("检测到 Linux 环境,正在查找可用串口设备...")

	devices, err := detector.listSerialDevices()
	if err != nil {
		detector.handleDeviceNotFound()
		return
	}

	detector.displayDeviceList(devices)
	selectedPort := detector.getUserSelection(devices)

	if selectedPort != "" {
		configuration.Providers.Modem.PortName = selectedPort
		fmt.Printf("已选择串口设备: %s\n", selectedPort)
	} else {
		fmt.Println("输入无效,使用默认配置")
	}
}

// isLinuxEnvironment 检查是否为 Linux 环境
func (detector *SerialPortDetector) isLinuxEnvironment() bool {
	return detector.operatingSystem == "linux"
}

// listSerialDevices 列出系统中可用的串口设备
func (detector *SerialPortDetector) listSerialDevices() ([]string, error) {
	output, err := exec.Command("bash", "-c", serialDevicePattern).Output()
	if err != nil || len(output) == 0 {
		return nil, fmt.Errorf("no serial devices found")
	}

	return strings.Fields(string(output)), nil
}

// handleDeviceNotFound 处理未找到设备的情况
func (detector *SerialPortDetector) handleDeviceNotFound() {
	fmt.Println("未检测到串口设备,请确认设备已正确插入")
}

// displayDeviceList 显示可用设备列表
func (detector *SerialPortDetector) displayDeviceList(devices []string) {
	fmt.Println("可用串口设备列表:")
	for index, device := range devices {
		fmt.Printf("[%d] %s\n", index, device)
	}
}

// getUserSelection 获取用户选择的设备
func (detector *SerialPortDetector) getUserSelection(devices []string) string {
	fmt.Print("请输入要使用的设备编号: ")

	if !detector.scanner.Scan() {
		return ""
	}

	input := detector.scanner.Text()
	selectedIndex, err := strconv.Atoi(input)

	if err != nil || !detector.isValidIndex(selectedIndex, len(devices)) {
		return ""
	}

	return devices[selectedIndex]
}

// isValidIndex 验证索引是否有效
func (detector *SerialPortDetector) isValidIndex(index int, maxLength int) bool {
	return index >= 0 && index < maxLength
}

//
// HTTP 服务器管理
//

// ServerManager HTTP 服务器管理器
type ServerManager struct {
	server *http.Server
}

// NewServerManager 创建服务器管理器实例
func NewServerManager(address string, handler http.Handler) *ServerManager {
	return &ServerManager{
		server: &http.Server{
			Addr:    address,
			Handler: handler,
		},
	}
}

// Start 启动 HTTP 服务器
// 在独立的 goroutine 中运行,避免阻塞主流程
func (manager *ServerManager) Start() {
	go func() {
		log.Printf("[Server] HTTP 服务启动于 %s", manager.server.Addr)

		if err := manager.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[Server] 启动失败: %v", err)
		}
	}()
}

// GracefulShutdown 优雅关闭服务器
// 等待现有请求完成或超时后强制关闭
func (manager *ServerManager) GracefulShutdown() error {
	log.Println("[Server] 开始优雅关闭...")

	shutdownContext, cancel := context.WithTimeout(
		context.Background(),
		gracefulShutdownPeriod,
	)
	defer cancel()

	if err := manager.server.Shutdown(shutdownContext); err != nil {
		log.Printf("[Server] 关闭过程出现错误: %v", err)
		return err
	}

	log.Println("[Server] 优雅关闭完成")
	return nil
}

//
// 信号处理器
//

// SignalHandler 系统信号处理器
type SignalHandler struct {
	notifyContext context.Context
	stopFunc      context.CancelFunc
}

// NewSignalHandler 创建信号处理器实例
// 监听 SIGINT 和 SIGTERM 信号用于优雅关闭
func NewSignalHandler() *SignalHandler {
	notifyContext, stopFunc := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	return &SignalHandler{
		notifyContext: notifyContext,
		stopFunc:      stopFunc,
	}
}

// WaitForShutdownSignal 等待关闭信号
// 阻塞直到收到中断信号
func (handler *SignalHandler) WaitForShutdownSignal() {
	<-handler.notifyContext.Done()
	handler.stopFunc()
	log.Println("[SignalHandler] 收到关闭信号")
}

//
// 应用程序启动器
//

// ApplicationRunner 应用程序运行器
// 负责整个应用的生命周期管理
type ApplicationRunner struct {
	configuration config.Config
	serverManager *ServerManager
	signalHandler *SignalHandler
	appContext    *AppContext
}

// NewApplicationRunner 创建应用运行器实例
func NewApplicationRunner() *ApplicationRunner {
	configuration := config.MustLoad(configFilePath)

	return &ApplicationRunner{
		configuration: configuration,
		signalHandler: NewSignalHandler(),
	}
}

// Run 运行应用程序
// 执行完整的启动、运行和关闭流程
func (runner *ApplicationRunner) Run() {
	runner.detectSerialPort()
	runner.initializeApplication()
	runner.startConsumers()
	runner.startHTTPServer()
	runner.waitForShutdown()
}

// detectSerialPort 检测并选择串口设备
// 仅在 Linux 环境下执行
func (runner *ApplicationRunner) detectSerialPort() {
	detector := NewSerialPortDetector()
	detector.DetectAndSelect(&runner.configuration)
}

// initializeApplication 初始化应用程序
func (runner *ApplicationRunner) initializeApplication() {
	runner.appContext = InitAppContext(runner.configuration)
	log.Println("[Runner] 应用程序初始化完成")
}

// startConsumers 启动消息队列消费者
func (runner *ApplicationRunner) startConsumers() {
	startMainConsumer(runner.appContext)
	startVoiceSMSConsumers(runner.appContext)
	log.Println("[Runner] 消息队列消费者启动完成")
}

// startHTTPServer 启动 HTTP 服务器
func (runner *ApplicationRunner) startHTTPServer() {
	router := BuildGinRouter(runner.appContext)

	runner.serverManager = NewServerManager(runner.configuration.App.Addr, router)
	runner.serverManager.Start()
}

// waitForShutdown 等待并执行优雅关闭
// 确保所有资源正确释放
func (runner *ApplicationRunner) waitForShutdown() {
	runner.signalHandler.WaitForShutdownSignal()
	runner.performShutdown()
}

// performShutdown 执行关闭流程
func (runner *ApplicationRunner) performShutdown() {
	// 先关闭 HTTP 服务器,停止接收新请求
	if err := runner.serverManager.GracefulShutdown(); err != nil {
		log.Printf("[Runner] 服务器关闭出现错误: %v", err)
	}

	// 再关闭应用上下文,释放所有资源
	if runner.appContext != nil {
		runner.appContext.Close()
		log.Println("[Runner] 应用上下文资源释放完成")
	}

	log.Println("[Runner] 应用程序已完全关闭")
}
