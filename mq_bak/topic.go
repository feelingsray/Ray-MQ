package mq_bak


// Topic结构体
type TopicConf struct {
	Name                string
	MaxSegmentBytes     uint32 // 分段文件大小
	MaxSegmentIndex     uint16 // 最大分片数
	IndexSyncDiskDuring uint16 // 索引刷盘时间,单位毫秒
	MsgSyncDiskDuring   uint16 // 消息刷盘时间,单位毫秒
	MustSync            bool   // 强一致性要求
}

type Topic struct {
	conf *TopicConf

	writer *

}

