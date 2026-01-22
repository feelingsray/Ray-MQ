package mq_bak

import (
	"fmt"
)

// 日志回调
func (rmq *RMQ) loggerCallback(model string, level string, err error, msg string) {
	switch level {
	case LoggerDebug:
		loggerDebug(model, msg)
	case LoggerInfo:
		loggerInfo(model, msg)
	case LoggerWarn:
		loggerWarn(model, msg)
	case LoggerError:
		loggerError(model, err, msg)
	}
}

// 数据描述回调
func (rmq *RMQ) msgMetaCallback(meta *MsgMeta) error {
	// 完成数据写入以后,记录元数据信息
	metaDB, ok := rmq.msgMetaDB[meta.Topic]
	if !ok {
		return fmt.Errorf("meta of %s not exist", meta.Topic)
	}
	// 将元数据写入DB
	err := metaDB.PushMeta(meta)
	if err != nil {
		return err
	}
	// 写入成功后,需要将数据在写到consumer的消费队列中

	// 这里考虑一个事情是不是应该不应该consumer带topic，应该topic带consumer？

	fmt.Printf("%+v\n", meta)
	return nil
}

// 分段文件被滚动时执行
func (rmq *RMQ) msgSegmentRollCallback(topic string, segmentIdx uint16, msgId uint64) error {
	// 分段文件已经被滚动,需要删除meta数据及consumer_marker
	// 删除meta数据
	if _, ok := rmq.msgMetaDB[topic]; !ok {
		return nil
	}
	err := rmq.msgMetaDB[topic].DelMetaBeforeLatestID(msgId)
	if err != nil {
		return err
	}
	// 删除consumer_maker数据

	return nil
}
