package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	CRCLen     uint32 = 4
	KeySizeLen uint32 = 4
	ValSizeLen uint32 = 4
	HeaderLen  uint32 = CRCLen + KeySizeLen + ValSizeLen
)

type MsgEntry struct {
	Crc     uint32
	KeySize uint32
	ValSize uint32
	Key     []byte
	Val     []byte
}

// Encode 编码数据实体为二进制对象流
// 格式为:CRC[4]+Mark[2]+KS[4]+VS[4]+Key[key的长度]+Val[val的长度]
func (e *MsgEntry) Encode() ([]byte, error) {
	if e.KeySize == 0 || e.ValSize == 0 {
		return nil, errors.New("数据实体为空")
	}
	// 创建一个buf,大小为: HeaderLen+KeySize+ValSize
	buf := make([]byte, HeaderLen+e.KeySize+e.ValSize)
	binary.BigEndian.PutUint32(buf[0:4], e.Crc)
	binary.BigEndian.PutUint32(buf[4:8], e.KeySize)
	binary.BigEndian.PutUint32(buf[8:12], e.ValSize)
	copy(buf[HeaderLen:HeaderLen+e.KeySize], e.Key)
	copy(buf[HeaderLen+e.KeySize:], e.Val)
	return buf, nil
}

func (e *MsgEntry) Decode(buf []byte) (*MsgEntry, error) {
	crc := binary.BigEndian.Uint32(buf[0:4])
	keySize := binary.BigEndian.Uint32(buf[4:8])
	valSize := binary.BigEndian.Uint32(buf[8:12])
	key := buf[HeaderLen : HeaderLen+keySize]
	val := buf[HeaderLen+keySize:]
	return &MsgEntry{
		Crc:     crc,
		KeySize: keySize,
		ValSize: valSize,
		Key:     key,
		Val:     val,
	}, nil
}

func (e *MsgEntry) GetCRC() (uint32, error) {
	data := fmt.Sprintf("%s:%s", string(e.Key), string(e.Val))
	crc := crc32.ChecksumIEEE([]byte(data))
	return crc, nil
}

func (e *MsgEntry) GetHeaderLen() uint32 {
	return HeaderLen
}

func (e *MsgEntry) GetMsgEntryLen() uint32 {
	return HeaderLen + e.KeySize + e.ValSize
}

// 写入数据
func PutMsgEntry(key []byte, val []byte) (*MsgEntry, []byte, error) {
	keySize := len(key)
	valSize := len(val)
	if keySize == 0 || valSize == 0 {
		return nil, nil, fmt.Errorf("key or val is empty")
	}
	msgEntry := &MsgEntry{}
	msgEntry.KeySize = uint32(keySize)
	msgEntry.ValSize = uint32(valSize)
	msgEntry.Key = key
	msgEntry.Val = val
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

func GetMsgEntry(buf []byte) (*MsgEntry, []byte, []byte, error) {
	msgEntry := &MsgEntry{}
	msgEntry, err := msgEntry.Decode(buf)
	if err != nil {
		return nil, nil, nil, err
	}
	getCRC, err := msgEntry.GetCRC()
	if err != nil {
		return nil, nil, nil, err
	}
	if getCRC != msgEntry.Crc {
		if string(msgEntry.Key) != "msg" {
			fmt.Println(string(msgEntry.Val))
		}
		return nil, nil, nil, fmt.Errorf("CRC mismatch:%d,%d", getCRC, msgEntry.Crc)
	}

	return msgEntry, msgEntry.Key, msgEntry.Val, nil
}
