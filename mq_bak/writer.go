package mq_bak

import (
	"context"
	"fmt"
	"github.com/sony/sonyflake"
	"sync"
	"time"
)

// Topic列表发生变化必须重新启动创建
func NewMsgWriter(ctx context.Context, wg *sync.WaitGroup, root string, uf *sonyflake.Sonyflake, metaCB MsgMetaCallback, rollCB MsgSegmentRollCallback, logCB MsgLoggerCallback) (*MsgWriter, error) {
	msgWriter := &MsgWriter{}
	msgWriter.msgSegList = make(map[string]*MsgSegment)
	msgWriter.idxSyncTicker = make(map[string]*time.Ticker)
	msgWriter.msgSyncTicker = make(map[string]*time.Ticker)

	msgWriter.metaCallback = metaCB
	msgWriter.segmentRollCallback = rollCB
	msgWriter.logCallback = logCB

	msgWriter.ctx = ctx
	msgWriter.wg = wg
	msgWriter.uidFactory = uf

	msgWriter.root = root
	return msgWriter, nil
}

type MsgWriterChannel struct {
	ID        uint64
	Topic     string
	Key       string
	Val       string
	timestamp int64
}

type MsgWriter struct {
	msgSegList    map[string]*MsgSegment
	idxSyncTicker map[string]*time.Ticker
	msgSyncTicker map[string]*time.Ticker

	metaCallback        MsgMetaCallback
	segmentRollCallback MsgSegmentRollCallback
	logCallback         MsgLoggerCallback

	ctx        context.Context
	wg         *sync.WaitGroup
	uidFactory *sonyflake.Sonyflake
	root       string
}

func (mw *MsgWriter) Destroy(syncDisk bool) error {
	for _, idxSync := range mw.idxSyncTicker {
		idxSync.Stop()
	}
	for _, msgSync := range mw.msgSyncTicker {
		msgSync.Stop()
	}
	for _, msg := range mw.msgSegList {
		err := msg.Destroy(syncDisk)
		if err != nil {
			return err
		}
	}

	return nil
}

// 创建并追加一个写入器
func (mw *MsgWriter) AppendNewWriter(topic *MsgTopic) error {
	// 创建分段写入对象
	if _, ok := mw.msgSegList[topic.Name]; !ok {
		ms, err := NewMsgSegment(mw.root, topic.Name, topic.MaxSegmentIndex, topic.MaxSegmentBytes, topic.MustSync, mw.segmentRollCallback)
		if err != nil {
			return err
		}
		mw.msgSegList[topic.Name] = ms
	}
	// 创建同步索引刷盘服务
	if topic.IndexSyncDiskDuring > 0 {
		if _, ok := mw.idxSyncTicker[topic.Name]; !ok {
			ticker := time.NewTicker(time.Duration(topic.IndexSyncDiskDuring) * time.Millisecond)
			go func() {
				mw.wg.Add(1)
				defer mw.wg.Done()
				for range ticker.C {

					err := mw.msgSegList[topic.Name].SyncSegmentIdx()
					if err != nil {
						mw.logCallback("writer", LoggerError, err, fmt.Sprintf("fail to sync segment idx:%s", topic.Name))
					}

					select {
					case <-mw.ctx.Done():
						{
							mw.logCallback("writer", LoggerInfo, nil, fmt.Sprintf("close to sync segment idx:%s", topic.Name))
							return
						}
					default:
						time.Sleep(1 * time.Millisecond)
					}
				}
			}()
			mw.idxSyncTicker[topic.Name] = ticker
		}
	}

	// 创建同步数据刷盘服务
	if topic.MsgSyncDiskDuring > 0 {
		if _, ok := mw.msgSyncTicker[topic.Name]; ok {
			ticker := time.NewTicker(time.Duration(topic.MsgSyncDiskDuring) * time.Millisecond)
			go func() {
				mw.wg.Add(1)
				defer mw.wg.Done()
				for range ticker.C {
					err := mw.msgSegList[topic.Name].SyncSegmentMsg()
					if err != nil {
						mw.logCallback("writer", LoggerError, err, fmt.Sprintf("fail to sync segment msg:%s", topic.Name))
					}

					select {
					case <-mw.ctx.Done():
						{
							mw.logCallback("writer", LoggerInfo, nil, fmt.Sprintf("close to sync segment msg:%s", topic.Name))
							return
						}
					default:
						time.Sleep(1 * time.Millisecond)
					}
				}
			}()
			mw.msgSyncTicker[topic.Name] = ticker
		}
	}

	return nil
}

// 写入消息,返回数据id
func (mw *MsgWriter) Write(topic string, key string, msg string) (*MsgMeta, error) {
	if _, ok := mw.msgSegList[topic]; !ok {
		mw.logCallback("writer", LoggerError, fmt.Errorf("topic not exist:%s", topic), "")
		return nil, fmt.Errorf("topic not exist:%s", topic)
	}
	// 构建MsgMeta
	meta := &MsgMeta{}
	meta.Topic = topic
	meta.Key = key
	msgId, err := mw.uidFactory.NextID()
	if err != nil {
		return nil, err
	}
	meta.MsgID = msgId
	meta.timestamp = time.Now().UnixNano()
	// 实际写入数据到分段文件
	segId, startOffset, msgSize, isNewSegment, err := mw.msgSegList[topic].AppendMsg(meta.MsgID, []byte(msg))
	if err != nil {
		return nil, err
	}
	meta.SegmentID = segId
	meta.StartOffset = startOffset
	meta.Size = msgSize

	_ = isNewSegment

	// 将元数据写入pb

	//  ??? 这里是不是还得搞一下？
	err = mw.metaCallback(meta)
	if err != nil {
		return nil, err
	}

	return meta, nil
}
