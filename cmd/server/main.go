package main

import (
	"log"
)

func main() {
	log.Println("[Main] 推送网关服务启动中...")

	runner := NewApplicationRunner()
	runner.Run()

	log.Println("[Main] 推送网关服务已停止")
}
