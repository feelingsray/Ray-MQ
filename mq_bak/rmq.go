package mq_bak

import (
	"context"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/feelingsray/Ray-MQ/utils"
	"github.com/feelingsray/ray-utils-go/v2/tools"
	"github.com/sony/sonyflake"
	"path"
	"strings"
	"sync"
	"time"
)

func NewRMQ(root string) (*RMQ, error) {
	if root == "" {
		root = tools.GetAppPath()
	}

	rmq := &RMQ{}
	rmq.root = root
	rmq.uidFactory = sonyflake.NewSonyflake(sonyflake.Settings{
		StartTime: time.Date(2025, 12, 24, 15, 57, 0, 0, time.UTC),
		MachineID: utils.MachineIDFromIPHash,
		CheckMachineID: func(id uint16) bool {
			return id < 256
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	rmq.ctx = ctx
	rmq.cancel = cancel
	rmq.wg = &sync.WaitGroup{}

	rmq.TopicList = make(map[string]*MsgTopic)
	rmq.ConsumerList = make(map[string]*MsgConsumer)
	rmq.markerDBList = make(map[string]*pebble.DB)

	rmq.msgMetaDB = make(map[string]*MsgMetaDB)
	writer, err := NewMsgWriter(rmq.ctx, rmq.wg, rmq.root, rmq.uidFactory, rmq.msgMetaCallback, rmq.msgSegmentRollCallback, rmq.loggerCallback)
	if err != nil {
		return nil, err
	}
	rmq.msgWriter = writer

	return rmq, nil
}

type RMQ struct {
	root string

	// 全局UID,保证单机流水号唯一
	uidFactory *sonyflake.Sonyflake
	ctx        context.Context
	cancel     context.CancelFunc
	wg         *sync.WaitGroup

	TopicList map[string]*MsgTopic

	ConsumerList map[string]*MsgConsumer

	//消息元数据库
	msgMetaDB map[string]*MsgMetaDB
	msgWriter *MsgWriter

	msgReader *MsgReader
}

// 添加一个Topic
func (rmq *RMQ) AppendSimpleTopic(name string) error {
	err := rmq.AppendTopic(name, 0, 0, 0, 0, false)
	if err != nil {
		return err
	}
	return nil
}

// 添加一个Topic
func (rmq *RMQ) AppendSimpleSyncTopic(name string) error {
	err := rmq.AppendTopic(name, 0, 0, 0, 0, true)
	if err != nil {
		return err
	}
	return nil
}

// 添加一个Topic
func (rmq *RMQ) AppendTopic(name string, maxSegmentBytes uint32, maxSegmentIdx uint16, idxSyncDiskDuring uint16, msgSyncDiskDuring uint16, mustSync bool) error {
	if strings.Contains(name, ":") {
		return fmt.Errorf("topic cannot contain colon:%s", name)
	}
	if rmq.TopicList == nil {
		rmq.TopicList = make(map[string]*MsgTopic)
	}
	if _, ok := rmq.TopicList[name]; ok {
		return fmt.Errorf("topic already exists")
	}
	if name == "" {
		name = "RMQTopic"
	}
	if maxSegmentIdx == 0 || maxSegmentIdx > 65000 {
		maxSegmentIdx = 65000
	}
	if maxSegmentBytes == 0 {
		maxSegmentBytes = 1024 * 1024 * 128 // 128M
	}
	// 构建一个topic
	topic := &MsgTopic{
		Name:                name,
		MaxSegmentBytes:     maxSegmentBytes,
		MaxSegmentIndex:     maxSegmentIdx,
		IndexSyncDiskDuring: idxSyncDiskDuring,
		MsgSyncDiskDuring:   msgSyncDiskDuring,
		MustSync:            mustSync,
	}
	if rmq.msgWriter == nil {
		return fmt.Errorf("no msg writer be new")
	}
	// 创建一个Topic写入器
	err := rmq.msgWriter.AppendNewWriter(topic)
	if err != nil {
		return err
	}
	// 创建一个meta元数据记录
	if _, ok := rmq.msgMetaDB[topic.Name]; !ok {
		metaDB, err := NewMsgMetaDB(rmq.ctx, rmq.wg, rmq.root, topic, rmq.loggerCallback)
		if err != nil {
			return err
		}
		rmq.msgMetaDB[topic.Name] = metaDB
	}
	rmq.TopicList[topic.Name] = topic
	return nil
}

// 添加消费者
func (rmq *RMQ) AppendConsumer(group string, id string, topic string, subType uint16) (*MsgConsumer, error) {
	if strings.Contains(group, ":") {
		return nil, fmt.Errorf("group cannot contain colon:%s", group)
	}
	if strings.Contains(id, ":") {
		return nil, fmt.Errorf("consumer id cannot contain colon: %s", id)
	}
	if _, ok := rmq.markerDBList[id]; !ok {
		marker, err := pebble.Open(path.Join(rmq.root, "RMQ", "marker", id), &pebble.Options{})
		if err != nil {
			return nil, err
		}
		consumer := NewMsgConsumer(rmq.root, rmq.TopicList, id, marker)
		rmq.markerDBList[id] = marker
		rmq.ConsumerList[id] = consumer
	}
	return rmq.ConsumerList[id], nil
}

// 为Consumer添加订阅Topic
func (rmq *RMQ) RegTopicForConsumer(consumer *MsgConsumer, topic string, subType uint16) error {
	if _, ok := rmq.ConsumerList[consumer.ID]; !ok {
		return fmt.Errorf("consumer not exists: %s", consumer.ID)
	}
	if _, ok := rmq.markerDBList[consumer.ID]; !ok {
		return fmt.Errorf("marker not exists: %s", consumer.ID)
	}
	if _, ok := rmq.TopicList[topic]; !ok {
		return fmt.Errorf("topic not exists: %s", topic)
	}
	err := consumer.AppendConsumerMarker(topic, subType)
	if err != nil {
		return err
	}
	return nil
}

func (rmq *RMQ) Start() {
	rmq.wg.Wait()
}

func (rmq *RMQ) Destroy(syncDisk bool) error {
	rmq.cancel()
	time.Sleep(1 * time.Second)
	err := rmq.msgWriter.Destroy(syncDisk)
	if err != nil {
		return err
	}
	for _, meta := range rmq.msgMetaDB {
		err = meta.Destroy()
		if err != nil {
			return err
		}
	}
	for _, marker := range rmq.markerDBList {
		if marker != nil {
			_ = marker.Flush()
			err := marker.Close()
			if err != nil {
				return err
			}
		}
	}
	//rmq.cancel()
	return nil
}
