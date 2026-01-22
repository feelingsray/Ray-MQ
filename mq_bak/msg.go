package mq_bak

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	CRCLen     uint32 = 4
	IDLen      uint32 = 8
	MsgSizeLen uint32 = 4
	HeaderLen  uint32 = CRCLen + IDLen + MsgSizeLen
)

type MsgEntry struct {
	Crc     uint32
	ID      uint64
	MsgSize uint32
	Msg     []byte
}

// Encode 编码数据实体为二进制对象流
// 格式为:CRC[4]+ID[8]+MsgSize[4]+Msg[msg的长度]
func (e *MsgEntry) Encode() ([]byte, error) {
	if e.MsgSize == 0 {
		return nil, errors.New("数据实体为空")
	}
	// 创建一个buf,大小为: HeaderLen+MsgSize
	buf := make([]byte, HeaderLen+e.MsgSize)
	binary.BigEndian.PutUint32(buf[0:4], e.Crc)
	binary.BigEndian.PutUint64(buf[4:12], e.ID)
	binary.BigEndian.PutUint32(buf[12:16], e.MsgSize)
	copy(buf[HeaderLen:], e.Msg)
	return buf, nil
}

func (e *MsgEntry) Decode(buf []byte) (*MsgEntry, error) {
	crc := binary.BigEndian.Uint32(buf[0:4])
	id := binary.BigEndian.Uint64(buf[4:12])
	msgSize := binary.BigEndian.Uint32(buf[12:16])
	msg := buf[HeaderLen:]
	return &MsgEntry{
		Crc:     crc,
		ID:      id,
		MsgSize: msgSize,
		Msg:     msg,
	}, nil
}

func (e *MsgEntry) GetCRC() (uint32, error) {
	data := fmt.Sprintf("%d:%s", e.ID, string(e.Msg))
	crc := crc32.ChecksumIEEE([]byte(data))
	return crc, nil
}

func (e *MsgEntry) GetHeaderLen() uint32 {
	return HeaderLen
}

func (e *MsgEntry) GetMsgEntryLen() uint32 {
	return HeaderLen + e.MsgSize
}

// 写入数据
func PutMsgEntry(id uint64, msg []byte) (*MsgEntry, []byte, error) {
	if id == 0 {
		return nil, nil, fmt.Errorf("ID is empty")
	}
	msgSize := len(msg)
	if msgSize == 0 {
		return nil, nil, fmt.Errorf("msg is empty")
	}
	msgEntry := &MsgEntry{}
	msgEntry.ID = id
	msgEntry.MsgSize = uint32(msgSize)
	msgEntry.Msg = msg
	crc, err := msgEntry.GetCRC()
	if err != nil {
		return nil, nil, err
	}
	msgEntry.Crc = crc
	buf, err := msgEntry.Encode()
	if err != nil {
		return nil, nil, err
	}
	return msgEntry, buf, nil
}

func GetMsgEntry(buf []byte) (*MsgEntry, error) {
	msgEntry := &MsgEntry{}
	msgEntry, err := msgEntry.Decode(buf)
	if err != nil {
		return nil, err
	}
	getCRC, err := msgEntry.GetCRC()
	if err != nil {
		return nil, err
	}
	if getCRC != msgEntry.Crc {
		return nil, fmt.Errorf("CRC mismatch:%d,%d", getCRC, msgEntry.Crc)
	}

	return msgEntry, nil
}
