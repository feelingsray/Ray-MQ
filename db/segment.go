package db

import (
	"encoding/binary"
	"fmt"
	"github.com/feelingsray/ray-utils-go/v2/tools"
	"golang.org/x/exp/mmap"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

func NewMsgSegment(conf *MsgSegmentConf, clear MsgSegmentClearMarkerCallback) (*MsgSegment, error) {
	if conf == nil {
		conf = &MsgSegmentConf{}
	}
	if conf.Root == "" {
		conf.Root = path.Join(tools.GetAppPath(), "RMQ", "DB")
	} else {
		conf.Root = path.Join(conf.Root, "RMQ", "DB")
	}
	if conf.Topic == "" {
		conf.Topic = "RMQTopic"
	}
	if conf.SegmentPrefix == "" {
		conf.SegmentPrefix = "seg"
	}
	if conf.SegmentSuffix == "" {
		conf.SegmentSuffix = "dat"
	}
	if conf.MaxSegmentIndex == 0 || conf.MaxSegmentIndex > 65000 {
		conf.MaxSegmentIndex = 65000
	}
	if conf.MaxSegmentBytes == 0 {
		conf.MaxSegmentBytes = 1024 * 1024 * 128 // 128M
	}

	msgSegment := &MsgSegment{}
	msgSegment.cfg = conf
	msgSegment.clearCallback = clear
	err := msgSegment.initSegment()
	if err != nil {
		return nil, err
	}
	return msgSegment, nil
}

type MsgSegmentConf struct {
	Root            string // 分段根目录
	Topic           string // 分段Topic
	SegmentPrefix   string // 分段文件前缀
	SegmentSuffix   string // 分段文件后缀
	MaxSegmentBytes uint32 // 分段文件大小
	MaxSegmentIndex uint16 // 最大分片数
	MustSync        bool   // 强制同步
}

type MsgSegmentClearMarkerCallback func(segmentIndex uint16) error

type MsgSegment struct {
	cfg           *MsgSegmentConf
	mu            sync.Mutex
	dirPath       string
	indexW        *os.File
	indexR        *mmap.ReaderAt
	curIndex      uint16
	cur           *CurSegment
	clearCallback MsgSegmentClearMarkerCallback
}

type CurSegment struct {
	file   *os.File
	offset uint32
}

// 获取并创建目录,root/topic,返回包括:目录地址,所有文件句柄,错误信息
func (s *MsgSegment) getDirPath() error {
	dirPath := path.Join(s.cfg.Root, s.cfg.Topic)
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err = os.MkdirAll(dirPath, os.ModePerm)
		if err != nil {
			return err
		}
	}
	s.dirPath = dirPath
	// 加载索引文件写
	ifp := path.Join(s.cfg.Root, s.cfg.Topic, fmt.Sprintf("%s.index", s.cfg.Topic))
	fw, err := os.OpenFile(ifp, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	s.indexW = fw
	// 加载索引文件读
	fr, err := mmap.Open(ifp)
	if err != nil {
		return err
	}
	s.indexR = fr
	return nil
}

// 设置索引,offset为下一次的开始索引,即endoffset
func (s *MsgSegment) setIndex() error {
	msg, bufIndex, err := PutMsgEntry([]byte(fmt.Sprintf("index:%s", s.cfg.Topic)), []byte(fmt.Sprintf("%d:%d", s.curIndex, s.cur.offset)))
	if err != nil {
		return err
	}
	buf := make([]byte, 4+msg.GetMsgEntryLen())
	binary.BigEndian.PutUint32(buf[0:4], msg.GetMsgEntryLen())
	copy(buf[4:], bufIndex)
	_, err = s.indexW.WriteAt(buf, 0)
	if err != nil {
		return err
	}

	// 强一致性要求
	if s.cfg.MustSync {
		err = s.SyncIndex()
		if err != nil {
			return err
		}
	}
	//fmt.Println("index_size:", msg.GetMsgEntryLen())
	//fmt.Println("set_index:", string(msg.Val))
	return nil
}

func (s *MsgSegment) SyncIndex() error {
	if s.indexW != nil {
		err := s.indexW.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

// 获取索引文件信息:segmentIndex,offset,err
func (s *MsgSegment) getIndex() (uint16, uint32, error) {
	if s.indexR.Len() == 0 {
		return 0, 0, nil
	}

	bufSize := make([]byte, 4)
	_, err := s.indexR.ReadAt(bufSize, 0)
	if err != nil {
		return 0, 0, err
	}
	size := binary.BigEndian.Uint32(bufSize)
	buf := make([]byte, size)
	_, err = s.indexR.ReadAt(buf, 4)
	msg, _, val, err := GetMsgEntry(buf)
	if err != nil {
		return 0, 0, err
	}
	_ = msg
	index, err := strconv.ParseUint(strings.Split(string(val), ":")[0], 0, 16)
	if err != nil {
		return 0, 0, err
	}
	offset, err := strconv.ParseUint(strings.Split(string(val), ":")[1], 0, 32)
	if err != nil {
		return 0, 0, err
	}
	//fmt.Println("get_index:", string(msg.Val))
	return uint16(index), uint32(offset), nil
}

// 获取分段文件路径
func (s *MsgSegment) getSegmentPath() (string, error) {
	fileName := fmt.Sprintf("%s_%012d.%s", s.cfg.SegmentPrefix, s.curIndex, s.cfg.SegmentSuffix)
	return path.Join(s.cfg.Root, s.cfg.Topic, fileName), nil
}

// 加载分段文件,程序启动时调用
func (s *MsgSegment) initSegment() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 加载文件目录和索引文件
	err := s.getDirPath()
	if err != nil {
		return err
	}
	index, endOffset, err := s.getIndex()
	if err != nil {
		return err
	}
	s.curIndex = index
	// 加载分段文件
	if s.cur != nil {
		err = s.cur.file.Close()
		if err != nil {
			return err
		}
	}
	fp, err := s.getSegmentPath()
	if err != nil {
		return err
	}
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if fi.Size() == 0 {
		// 新建的一个分段文件
		// 预分配空间
		_ = f.Truncate(int64(s.cfg.MaxSegmentBytes))
		// 设置文件偏移为0,用零填充但文件大小视为 0
		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
		err = f.Sync()
		if err != nil {
			return err
		}
	}
	s.cur = &CurSegment{
		file:   f,
		offset: endOffset,
	}
	return nil
}

func (s *MsgSegment) Destroy() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur != nil {
		err := s.cur.file.Sync()
		if err != nil {
			return err
		}
		err = s.cur.file.Close()
		if err != nil {
			return err
		}
	}
	if s.indexW != nil {
		// 中间不去刷盘,销毁时刷新
		err := s.indexW.Sync()
		if err != nil {
			return err
		}
		err = s.indexW.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// 创建新的分段文件,超出分段最大索引后自动回滚,必须在初始化加载后调用
func (s *MsgSegment) createNewSegment() error {
	if s.cur.file != nil {
		err := s.cur.file.Sync()
		if err != nil {
			return err
		}
		err = s.cur.file.Close()
		if err != nil {
			return err
		}
	}
	s.curIndex++
	if s.curIndex > s.cfg.MaxSegmentIndex {
		// 超过最大分段,强行滚动
		s.curIndex = 0
	}
	fp, err := s.getSegmentPath()
	if err != nil {
		return err
	}
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if fi.Size() == 0 {
		// 文件内无空间,先预分配空间
		_ = f.Truncate(int64(s.cfg.MaxSegmentBytes))
		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
		err = f.Sync()
		if err != nil {
			return err
		}
	} else {
		// 如果打开的文件空间大小不为0证明这个文件是滚动写入的文件
		// 需要回调清除外部索引,offsetMarker
		err = s.clearCallback(s.curIndex)
		if err != nil {
			return err
		}
	}
	s.cur = &CurSegment{
		file:   f,
		offset: 0,
	}
	return nil
}

// 追加数据,返回 SegmentIndex,startOffset,msgSize
func (s *MsgSegment) AppendMsg(key []byte, val []byte) (uint16, uint32, uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg, buf, err := PutMsgEntry(key, val)
	if err != nil {
		return 0, 0, 0, err
	}
	if msg.GetMsgEntryLen() >= s.cfg.MaxSegmentBytes {
		// 写入的消息大小不能大于等于最大分段文件大小,要保证同一条消息必须落到同一个分段内
		return 0, 0, 0, fmt.Errorf("msg entry too large")
	}
	if s.cur == nil {
		// 如果激活的文件未打开要重新初始化分片
		err := s.initSegment()
		if err != nil {
			return 0, 0, 0, err
		}
	}

	if s.cur.offset+msg.GetMsgEntryLen() > s.cfg.MaxSegmentBytes {
		// 预算空间写完以后大小会超过文件大小,新开一个文件
		err = s.createNewSegment()
		if err != nil {
			return 0, 0, 0, err
		}
	}

	startOffset := s.cur.offset
	endOffset := s.cur.offset + msg.GetMsgEntryLen()

	_, err = s.cur.file.WriteAt(buf, int64(startOffset))
	if err != nil {
		return 0, 0, 0, err
	}
	// 强一致性要求
	if s.cfg.MustSync {
		err = s.SyncMsg()
		if err != nil {
			return 0, 0, 0, err
		}
	}
	s.cur.offset = endOffset

	// 记录写入索引,一定要在endoffset赋值curoffset之后
	err = s.setIndex()
	if err != nil {
		return 0, 0, 0, err
	}
	return s.curIndex, startOffset, endOffset - startOffset, nil
}

func (s *MsgSegment) SyncMsg() error {
	if s.cur.file != nil {
		err := s.cur.file.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}
