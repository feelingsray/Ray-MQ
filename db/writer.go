package db

import (
	"context"
	"fmt"
	"github.com/sony/sonyflake"
	"sync"
	"time"
)

type MsgTopic struct {
	Name                string
	Root                string
	SegmentPrefix       string // 分段文件前缀
	SegmentSuffix       string // 分段文件后缀
	MaxSegmentBytes     uint32 // 分段文件大小
	MaxSegmentIndex     uint16 // 最大分片数
	IndexSyncDiskDuring uint16 // 索引刷盘时间,单位毫秒
	MsgSyncDiskDuring   uint16 // 消息刷盘时间,单位毫秒
	MustSync            bool   // 强一致性要求
	MarkerCallback      MsgSegmentClearMarkerCallback
	iSyncTicker         *time.Ticker
	mSyncTicker         *time.Ticker
}

// Topic列表发生变化必须重新启动创建
func NewMsgWriter(topicList []*MsgTopic, metaCallback MetaCallback, logCallback LogCallback) (*MsgWriter, error) {
	msgWriter := &MsgWriter{}

	ctx, cancel := context.WithCancel(context.Background())
	msgWriter.ctx = ctx
	msgWriter.cancel = cancel
	msgWriter.sf = sonyflake.NewSonyflake(sonyflake.Settings{})

	msgWriter.metaCallback = metaCallback
	msgWriter.logCallback = logCallback

	msgWriter.msgChanList = make(map[string]chan *Message)
	msgWriter.msgSegList = make(map[string]*MsgSegment)
	msgWriter.topicList = topicList
	for _, topic := range topicList {
		msgWriter.createMsgChan(topic)
		err := msgWriter.createMsgSegment(topic)
		if err != nil {
			return nil, err
		}
	}
	return msgWriter, nil
}

type Message struct {
	ID    uint64
	Topic string
	Key   string
	Val   string
}

type MsgMeta struct {
	ID          uint64
	SegmentID   uint16
	StartOffset uint32
	Size        uint32
}

type MetaCallback func(*MsgMeta) error

const (
	LoggerInfo  = "info"
	LoggerError = "error"
	LoggerWarn  = "warn"
)

type LogMsg struct {
	Type string
	Err  error
	Msg  string
}

type LogCallback func(*LogMsg)

type MsgWriter struct {
	topicList []*MsgTopic

	msgSegList  map[string]*MsgSegment
	msgChanList map[string]chan *Message

	logCallback LogCallback

	sf           *sonyflake.Sonyflake
	metaCallback MetaCallback

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (w *MsgWriter) Destroy() {
	for _, topic := range w.topicList {
		if topic.iSyncTicker != nil {
			topic.iSyncTicker.Stop()
		}
		if topic.mSyncTicker != nil {
			topic.mSyncTicker.Stop()
		}

	}
	w.cancel()
	for _, msgChan := range w.msgChanList {
		close(msgChan)
	}

}

func (w *MsgWriter) Run() {
	w.wg.Wait()
}

// 创建Topic消息通道
func (w *MsgWriter) createMsgChan(topic *MsgTopic) {
	if _, ok := w.msgChanList[topic.Name]; !ok {
		msgChan := make(chan *Message, 1)
		w.msgChanList[topic.Name] = msgChan
		// 启动一个消息顺序刷入器
		go w.orderFlushMsg(topic.Name)
	}
}

// 创建Topic消息分段
func (w *MsgWriter) createMsgSegment(topic *MsgTopic) error {
	if _, ok := w.msgSegList[topic.Name]; !ok {
		conf := &MsgSegmentConf{
			Root:            topic.Root,
			Topic:           topic.Name,
			SegmentPrefix:   topic.SegmentPrefix,
			SegmentSuffix:   topic.SegmentSuffix,
			MaxSegmentBytes: topic.MaxSegmentBytes,
			MaxSegmentIndex: topic.MaxSegmentIndex,
			MustSync:        topic.MustSync,
		}
		msgSegment, err := NewMsgSegment(conf, topic.MarkerCallback)
		if err != nil {
			return err
		}
		if topic.IndexSyncDiskDuring > 0 {
			topic.iSyncTicker = time.NewTicker(time.Duration(topic.IndexSyncDiskDuring) * time.Millisecond)
			go func() {
				for range topic.iSyncTicker.C {
					err := msgSegment.SyncIndex()
					if err != nil {
						w.logCallback(&LogMsg{
							Type: LoggerError,
							Err:  err,
							Msg:  "index write sync disk error",
						})
					}
				}
			}()
		}
		if topic.MsgSyncDiskDuring > 0 {
			topic.mSyncTicker = time.NewTicker(time.Duration(topic.MsgSyncDiskDuring) * time.Millisecond)
			go func() {
				for range topic.mSyncTicker.C {
					err := msgSegment.SyncMsg()
					if err != nil {
						w.logCallback(&LogMsg{
							Type: LoggerError,
							Err:  err,
							Msg:  "msg write sync disk error",
						})
					}
				}
			}()
		}
		w.msgSegList[topic.Name] = msgSegment
	}
	return nil
}

// 写入消息,返回数据id
func (w *MsgWriter) Write(topic string, key string, val string) (uint64, error) {
	if key == "" {
		key = "X"
	}
	if _, ok := w.msgChanList[topic]; !ok {
		w.logCallback(&LogMsg{
			Type: LoggerError,
			Err:  fmt.Errorf("topic not exist:%s", topic),
		})
		return 0, fmt.Errorf("topic not exist:%s", topic)
	}
	// 构建消息体
	msg := &Message{}
	uid, _ := w.sf.NextID()
	msg.Topic = topic
	msg.ID = uid
	msg.Key = key
	msg.Val = val
	// 写入消息通道
	w.msgChanList[topic] <- msg
	return uid, nil
}

// 顺序刷入消息
func (w *MsgWriter) orderFlushMsg(topic string) {
	w.wg.Add(1)
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			{
				w.logCallback(&LogMsg{
					Type: LoggerInfo,
					Msg:  fmt.Sprintf("Flush msg channel closed: %s", topic),
				})
				return
			}
		case msg, ok := <-w.msgChanList[topic]:
			{
				if !ok {
					continue
				}
				if msg != nil {
					segId, startOffset, msgSize, err := w.msgSegList[topic].AppendMsg([]byte(msg.Key), []byte(msg.Val))
					if err != nil {
						w.logCallback(&LogMsg{
							Type: LoggerError,
							Err:  err,
							Msg:  fmt.Sprintf("Flush msg write err: %v", msg),
						})
						continue
					}
					// 创建Meta元数据
					meta := &MsgMeta{}
					meta.ID = msg.ID
					meta.SegmentID = segId
					meta.StartOffset = startOffset
					meta.Size = msgSize
					err = w.metaCallback(meta)
					if err != nil {
						w.logCallback(&LogMsg{
							Type: LoggerError,
							Err:  err,
							Msg:  fmt.Sprintf("MetaCallback Error"),
						})
					}
				}
			}
		}
	}
}
