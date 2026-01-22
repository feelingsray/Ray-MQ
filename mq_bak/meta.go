package mq_bak

import (
	"context"
	"fmt"
	"github.com/cockroachdb/pebble"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MsgMeta struct {
	Topic       string
	Key         string
	MsgID       uint64
	SegmentID   uint16
	StartOffset uint32
	Size        uint32
	timestamp   int64
}

type MsgMetaCallback func(*MsgMeta) error

func NewMsgMetaDB(ctx context.Context, wg *sync.WaitGroup, root string, topic *MsgTopic, logCB MsgLoggerCallback) (*MsgMetaDB, error) {
	metaDb := &MsgMetaDB{}
	metaDb.root = root
	metaDb.topic = topic
	metaDb.ctx = ctx
	metaDb.wg = wg
	metaDb.loggerCallback = logCB
	err := metaDb.open()
	if err != nil {
		return nil, err
	}
	return metaDb, nil
}

type MsgMetaDB struct {
	root  string
	topic *MsgTopic
	db    *pebble.DB

	ctx context.Context
	wg  *sync.WaitGroup

	ticker         *time.Ticker
	loggerCallback MsgLoggerCallback
}

func (m *MsgMetaDB) open() error {
	if m.db == nil {
		db, err := pebble.Open(path.Join(m.root, "RMQ", "meta", m.topic.Name), &pebble.Options{})
		if err != nil {
			return err
		}
		m.db = db
		if m.ticker == nil {
			m.asyncFlush()
		}
	}
	return nil
}
func (m *MsgMetaDB) asyncFlush() {
	ticker := time.NewTicker(time.Duration(m.topic.IndexSyncDiskDuring) * time.Millisecond)
	m.ticker = ticker
	go func() {
		m.wg.Add(1)
		defer m.wg.Done()
		for range ticker.C {
			err := m.db.Flush()
			if err != nil {
				m.loggerCallback("meta", LoggerError, err, fmt.Sprintf("failed to flush meta db: %s", m.topic.Name))
			}

			select {
			case <-m.ctx.Done():
				{
					m.loggerCallback("meta", LoggerInfo, nil, fmt.Sprintf("close to flush meta db: %s", m.topic.Name))
					return
				}
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()
}

func (m *MsgMetaDB) metaDecode(msgId uint64, data []byte) (*MsgMeta, error) {
	dataList := strings.Split(string(data), ":")
	if len(dataList) != 5 {
		return nil, fmt.Errorf("meta not found or error")
	}
	meta := &MsgMeta{}
	meta.Topic = m.topic.Name
	meta.MsgID = msgId
	meta.Key = dataList[4]
	segId, err := strconv.ParseUint(dataList[0], 10, 16)
	if err != nil {
		return nil, err
	}
	meta.SegmentID = uint16(segId)
	offset, err := strconv.ParseUint(dataList[1], 10, 32)
	if err != nil {
		return nil, err
	}
	meta.StartOffset = uint32(offset)
	size, err := strconv.ParseUint(dataList[2], 10, 32)
	if err != nil {
		return nil, err
	}
	meta.Size = uint32(size)
	meta.timestamp, err = strconv.ParseInt(dataList[3], 10, 64)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (m *MsgMetaDB) metaEncode(meta *MsgMeta) []byte {
	return []byte(fmt.Sprintf("%d:%d:%d:%d:%s", meta.SegmentID, meta.StartOffset, meta.Size, meta.timestamp, meta.Key))
}

func (m *MsgMetaDB) PushMeta(meta *MsgMeta) error {
	if strings.Contains(meta.Key, ":") {
		return fmt.Errorf("msg meta info contains ':': %s", meta.Key)
	}
	keyBytes := []byte(meta.Key)
	if len(keyBytes) > 128 {
		return fmt.Errorf("msg meta info is too large")
	}
	if m.db == nil {
		if err := m.open(); err != nil {
			return err
		}
	}
	if m.topic.MustSync {
		err := m.db.Set([]byte(fmt.Sprintf("%d", meta.MsgID)), m.metaEncode(meta), pebble.Sync)
		if err != nil {
			return err
		}
	} else {
		err := m.db.Set([]byte(fmt.Sprintf("%d", meta.MsgID)), m.metaEncode(meta), pebble.NoSync)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MsgMetaDB) GetMeta(msgId uint64) (*MsgMeta, error) {
	if m.db == nil {
		if err := m.open(); err != nil {
			return nil, err
		}
	}
	data, _, err := m.db.Get([]byte(fmt.Sprintf("%d", msgId)))
	if err != nil {
		return nil, err
	}
	meta, err := m.metaDecode(msgId, data)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (m *MsgMetaDB) GetNewestID() (*MsgMeta, error) {
	if m.db == nil {
		if err := m.open(); err != nil {
			return nil, err
		}
	}
	iter, err := m.db.NewIter(nil)
	defer iter.Close()

	if err != nil {
		return nil, err
	}
	if iter.Last() {
		mid := iter.Key()
		data := iter.Value()

		msgId, err := strconv.ParseUint(string(mid), 10, 64)
		if err != nil {
			return nil, err
		}
		meta, err := m.metaDecode(msgId, data)
		return meta, nil
	} else {
		return nil, fmt.Errorf("meta not found or error")
	}
}

// 顺序获取数据
func (m *MsgMetaDB) GetNextMsg(startMsgId uint64, msgChan chan *MsgMeta) error {
	if m.db == nil {
		if err := m.open(); err != nil {
			return err
		}
	}
	iter, err := m.db.NewIter(nil)
	defer iter.Close()
	if err != nil {
		return err
	}

	if startMsgId < 0 {
		// Oldest
		iter.First()
	} else if startMsgId == 0 {
		// Oldest
		iter.Last()
	} else {
		seek := iter.SeekLT([]byte(fmt.Sprintf("%d", startMsgId)))
		if !seek {
			iter.First()
		}
	}

	go func() {
		m.wg.Add(1)
		defer m.wg.Done()

		for {
			if iter.Next() {
				mid := iter.Key()
				data := iter.Value()

				msgId, err := strconv.ParseUint(string(mid), 10, 64)
				if err != nil {
					m.loggerCallback("meta", LoggerError, err, fmt.Sprintf("failed to parse msg id: %s", mid))
					continue
				}
				meta, err := m.metaDecode(msgId, data)
				if err != nil {
					m.loggerCallback("meta", LoggerError, err, fmt.Sprintf("failed to parse msg id: %s", mid))
					continue
				}
				msgChan <- meta
			}

			select {
			case <-m.ctx.Done():
				return
			default:

			}
		}
	}()

	return nil
}

func (m *MsgMetaDB) GetLatestID() (*MsgMeta, error) {
	if m.db == nil {
		if err := m.open(); err != nil {
			return nil, err
		}
	}
	iter, err := m.db.NewIter(nil)
	defer iter.Close()

	if err != nil {
		return nil, err
	}
	if iter.First() {
		mid := iter.Key()
		data := iter.Value()

		metaId, err := strconv.ParseUint(string(mid), 10, 64)
		if err != nil {
			return nil, err
		}
		dataList := strings.Split(string(data), ":")
		if len(dataList) != 5 {
			return nil, fmt.Errorf("meta not found or error")
		}
		meta := &MsgMeta{}
		meta.Topic = m.topic.Name
		meta.MsgID = metaId
		meta.Key = dataList[4]
		segId, err := strconv.ParseUint(dataList[0], 10, 16)
		if err != nil {
			return nil, err
		}
		meta.SegmentID = uint16(segId)
		offset, err := strconv.ParseUint(dataList[1], 10, 32)
		if err != nil {
			return nil, err
		}
		meta.StartOffset = uint32(offset)
		size, err := strconv.ParseUint(dataList[2], 10, 32)
		if err != nil {
			return nil, err
		}
		meta.Size = uint32(size)
		meta.timestamp, err = strconv.ParseInt(dataList[3], 10, 64)
		if err != nil {
			return nil, err
		}
		return meta, nil
	} else {
		return nil, fmt.Errorf("meta not found or error")
	}
}

func (m *MsgMetaDB) DelMetaByOneID(metaId uint64) error {
	if m.db == nil {
		if err := m.open(); err != nil {
			return err
		}
	}
	err := m.db.Delete([]byte(fmt.Sprintf("%d", metaId)), pebble.NoSync)
	if err != nil {
		return err
	}
	return nil
}

func (m *MsgMetaDB) DelMetaBeforeLatestID(metaId uint64) error {
	if m.db == nil {
		if err := m.open(); err != nil {
			return err
		}
	}
	err := m.db.DeleteRange(nil, []byte(fmt.Sprintf("%d", metaId)), pebble.NoSync)
	if err != nil {
		return err
	}
	return nil
}

func (m *MsgMetaDB) Destroy() error {
	if m.ticker != nil {
		m.ticker.Stop()
	}
	if m.db != nil {
		_ = m.db.Flush()
		err := m.db.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
