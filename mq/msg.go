package mq

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Msg 数据体结构
// ID 消息ID,长度为8,唯一标识不可重复,snowflake.Sonyflake.NextID生成,由写入器生成
// Size 数据体大小,长度为4
// Content 数据内容,长度为Size的值
type Msg struct {
	ID      uint64 // 消息ID,唯一标识
	Size    uint32
	Content []byte
}

// Encode 将消息体编码为二进制Buffer
// 允许传入空数据
func (m *Msg) Encode() ([]byte, error) {
	if m.Size != uint32(len(m.Content)) {
		return nil, errors.New("msg content size error")
	}
	buf := make([]byte, 12+m.Size)
	binary.BigEndian.PutUint64(buf[0:8], m.ID)
	binary.BigEndian.PutUint32(buf[8:12], m.Size)
	copy(buf[12:], m.Content)
	return buf, nil
}

// Decode 将二进制Buffer解码为消息体
func (m *Msg) Decode(buf []byte) (*Msg, error) {
	id := binary.BigEndian.Uint64(buf[0:8])
	size := binary.BigEndian.Uint32(buf[8:12])
	content := buf[12:]
	return &Msg{
		ID:      id,
		Size:    size,
		Content: content,
	}, nil
}

// 获取消息内容
func (m *Msg) String() string {
	return string(m.Content)
}

// MsgToBuf 消息转二进制
func MsgToBuf(id uint64, content []byte) (*Msg, []byte, error) {
	if id == 0 {
		return nil, nil, fmt.Errorf("ID is empty")
	}
	msg := &Msg{}
	msg.ID = id
	msg.Size = uint32(len(content))
	msg.Content = content
	buf, err := msg.Encode()
	if err != nil {
		return nil, nil, err
	}
	return msg, buf, nil
}

// BufToMsg 二进制转消息
func BufToMsg(buf []byte) (*Msg, error) {
	msg := &Msg{}
	msg, err := msg.Decode(buf)
	if err != nil {
		return nil, err
	}
	return msg, nil
}
