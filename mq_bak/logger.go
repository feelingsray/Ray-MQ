package mq_bak

import (
	"fmt"
	"time"
)

const (
	LoggerInfo  = "info"
	LoggerError = "error"
	LoggerWarn  = "warn"
	LoggerDebug = "debug"
)

type Logger struct {
	Model     string // 模块
	Type      string // 类型
	Err       error  // 错误信息
	Msg       string // 日志信息
	Timestamp int64  // 日志时间
}

type MsgLoggerCallback func(model string, t string, err error, msg string)

func loggerDebug(model string, msg string) {
	logger := &Logger{
		Model:     model,
		Type:      LoggerDebug,
		Err:       nil,
		Msg:       msg,
		Timestamp: time.Now().Unix(),
	}
	fmt.Printf("%+v\n", logger)
}
func loggerInfo(model string, msg string) {
	logger := &Logger{
		Model:     model,
		Type:      LoggerInfo,
		Err:       nil,
		Msg:       msg,
		Timestamp: time.Now().Unix(),
	}
	fmt.Printf("%+v\n", logger)
}
func loggerWarn(model string, msg string) {
	logger := &Logger{
		Model:     model,
		Type:      LoggerWarn,
		Err:       nil,
		Msg:       msg,
		Timestamp: time.Now().Unix(),
	}
	fmt.Printf("%+v\n", logger)
}
func loggerError(model string, err error, msg string) {
	logger := &Logger{
		Model:     model,
		Type:      LoggerError,
		Err:       err,
		Msg:       msg,
		Timestamp: time.Now().Unix(),
	}
	fmt.Printf("%+v\n", logger)
}
