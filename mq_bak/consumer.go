package mq_bak

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"path"
)

const (
	SubNewestMsg = 0
	SubOldestMsg = -1
)

func NewMsgConsumer(root string, id string) *MsgConsumer {
	consumer := &MsgConsumer{}
	consumer

	return consumer
}

type ConsumerMarker struct {
	ID         string
	Topic      string
	SubType    uint16
	LastMetaID uint64
}

type MsgConsumer struct {
	root     string
	ID       string
	Group    string
	Topic    string
	SubType  uint16
	markerDB *pebble.DB //dir:"id",key:topic,val:msgId
}

func (cs *MsgConsumer) AppendConsumerMarker(topic string, subType uint16) error {
	if _, ok := cs.markerDBList[fmt.Sprintf("%s:%s", cs.ID, topic)]; !ok {
		db, err := pebble.Open(path.Join(cs.root, "RMQ", "marker", fmt.Sprintf("%s:%s", cs.ID, topic)), &pebble.Options{})
		if err != nil {
			return err
		}
		cs.markerDBList[fmt.Sprintf("%s:%s", cs.ID, topic)] = db
	}

	if subType == SubNewestMsg {
		// 加载最新的一条数据

	} else {
		// 加载最久的一条数据
	}

}
