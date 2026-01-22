package mq_bak

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/feelingsray/ray-utils-go/v2/tools"
	"golang.org/x/exp/mmap"
)

// 分段文件滚动时回调
type MsgSegmentRollCallback func(topic string, segmentIdx uint16, latestMsgID uint64) error

// 创建消息分段
func NewMsgSegment(root string, topic string, maxSegmentIdx uint16, maxSegmentBytes uint32, mustSync bool, rollCB MsgSegmentRollCallback) (*MsgSegment, error) {
	ms := &MsgSegment{}
	ms.root = root
	if ms.root == "" {
		ms.root = tools.GetAppPath()
	}
	ms.topic = topic
	ms.maxSegmentIndex = maxSegmentIdx
	ms.maxSegmentBytes = maxSegmentBytes
	ms.mustSync = mustSync
	// 创建分段文件目录
	dirPath := path.Join(ms.root, "RMQ", "db", ms.topic)
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err = os.MkdirAll(dirPath, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	ms.dirPath = dirPath
	// 加载分段文件索引
	ifp := path.Join(dirPath, fmt.Sprintf("%s.index", topic))
	fw, err := os.OpenFile(ifp, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	ms.indexW = fw
	// 加载索引文件读
	fr, err := mmap.Open(ifp)
	if err != nil {
		return nil, err
	}
	ms.indexR = fr

	// 加载当前执行的分段文件
	err = ms.loadSegment()
	if err != nil {
		return nil, err
	}

	// 回调
	ms.rollCallback = rollCB

	return ms, nil
}

type MsgSegment struct {
	mu sync.Mutex

	root    string
	topic   string
	dirPath string

	maxSegmentBytes uint32 // 分段文件大小
	maxSegmentIndex uint16 // 最大分片数
	mustSync        bool   // 强制同步

	indexW *os.File
	indexR *mmap.ReaderAt

	curIndex  uint16
	curFile   *os.File
	curOffset uint32

	rollCallback MsgSegmentRollCallback
}

// 加载分段文件,程序启动时调用
func (s *MsgSegment) loadSegment() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 获取索引
	index, endOffset, err := s.getSegmentIdx()
	if err != nil {
		return err
	}
	s.curIndex = index
	// 加载分段文件
	if s.curFile != nil {
		err = s.curFile.Close()
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
		_ = f.Truncate(int64(s.maxSegmentBytes))
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
	s.curFile = f
	s.curOffset = endOffset
	return nil
}

// 获取分段文件路径
func (s *MsgSegment) getSegmentPath() (string, error) {
	fileName := fmt.Sprintf("seg_%012d.dat", s.curIndex)
	return path.Join(s.dirPath, fileName), nil
}

// 获取索引文件信息:segmentIndex,offset,err
func (s *MsgSegment) getSegmentIdx() (uint16, uint32, error) {
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
	msg, err := GetMsgEntry(buf)
	if err != nil {
		return 0, 0, err
	}
	_ = msg
	index, err := strconv.ParseUint(strings.Split(string(msg.Msg), ":")[0], 0, 16)
	if err != nil {
		return 0, 0, err
	}
	offset, err := strconv.ParseUint(strings.Split(string(msg.Msg), ":")[1], 0, 32)
	if err != nil {
		return 0, 0, err
	}
	return uint16(index), uint32(offset), nil
}

// 设置索引,offset为下一次的开始索引,即endoffset
func (s *MsgSegment) setSegmentIdx() error {
	msg, bufIndex, err := PutMsgEntry(0, []byte(fmt.Sprintf("%d:%d", s.curIndex, s.curOffset)))
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
	if s.mustSync {
		err = s.SyncSegmentIdx()
		if err != nil {
			return err
		}
	}
	return nil
}

// 同步索引刷盘
func (s *MsgSegment) SyncSegmentIdx() error {
	if s.indexW != nil {
		err := s.indexW.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

// 执行销毁
func (s *MsgSegment) Destroy(syncDisk bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.curFile != nil {
		//t := time.Now()
		if syncDisk {
			err := s.curFile.Sync()
			if err != nil {
				return err
			}
		}

		//fmt.Println("ssss:", time.Since(t))
		err := s.curFile.Close()
		if err != nil {
			return err
		}
	}
	if s.indexW != nil {
		if syncDisk {
			// 中间不去刷盘,销毁时刷新
			err := s.indexW.Sync()
			if err != nil {
				return err
			}
		}

		err := s.indexW.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// 创建新的分段文件,超出分段最大索引后自动回滚,必须在初始化加载后调用
func (s *MsgSegment) createNewSegment() error {
	if s.curFile != nil {
		err := s.curFile.Sync()
		if err != nil {
			return err
		}
		err = s.curFile.Close()
		if err != nil {
			return err
		}
	}
	s.curIndex++
	if s.curIndex > s.maxSegmentIndex {
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
		_ = f.Truncate(int64(s.maxSegmentBytes))
		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
		err = f.Sync()
		if err != nil {
			return err
		}
	} else {
		// 一旦滚动则整个分段文件的数据都需要被清除,那么就需要知道下一个分段文件的其实msgID是多少以便于清除meta数据和consumerMarker
		latestMsgID, err := s.getLatestMsgID()
		if err != nil {
			return err
		}
		// 分段滚动回调
		err = s.rollCallback(s.topic, s.curIndex, latestMsgID)
		if err != nil {
			return err
		}
	}
	s.curFile = f
	s.curOffset = 0
	return nil
}

// 获取此分段组下最早的一条数据的MsgID,如果MsgID==0则表示为初始消息
func (s *MsgSegment) getLatestMsgID() (uint64, error) {
	latestFN := fmt.Sprintf("seg_%012d.dat", s.curIndex+1)
	latestfp := path.Join(s.dirPath, latestFN)
	latestF, err := os.OpenFile(latestfp, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	latestFi, err := latestF.Stat()
	if err != nil {
		return 0, err
	}
	if latestFi.Size() == 0 {
		return 0, nil
	}
	bufLatestMsgID := make([]byte, 8)
	_, err = latestF.ReadAt(bufLatestMsgID, 4)
	if err != nil {
		return 0, err
	}
	latestMsgID := binary.BigEndian.Uint64(bufLatestMsgID)
	return latestMsgID, nil
}

// 追加数据,返回 SegmentIndex,startOffset,msgSize,isNewSeg
func (s *MsgSegment) AppendMsg(id uint64, val []byte) (uint16, uint32, uint32, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	isNewSegment := false

	msg, buf, err := PutMsgEntry(id, val)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if msg.GetMsgEntryLen() >= s.maxSegmentBytes {
		// 写入的消息大小不能大于等于最大分段文件大小,要保证同一条消息必须落到同一个分段内
		return 0, 0, 0, false, fmt.Errorf("msg entry too large")
	}
	if s.curFile == nil {
		// 如果激活的文件未打开要重新初始化分片
		err := s.loadSegment()
		if err != nil {
			return 0, 0, 0, false, err
		}
	}

	if s.curOffset+msg.GetMsgEntryLen() > s.maxSegmentBytes {
		// 预算空间写完以后大小会超过文件大小,新开一个文件
		err = s.createNewSegment()
		if err != nil {
			return 0, 0, 0, false, err
		}

		// 这里是分段文件切换,需要记录当前当前分段开始第一个数据的msgID??????????????????????????
		isNewSegment = true
	}

	startOffset := s.curOffset
	endOffset := s.curOffset + msg.GetMsgEntryLen()

	_, err = s.curFile.WriteAt(buf, int64(startOffset))
	if err != nil {
		return 0, 0, 0, isNewSegment, err
	}
	// 强一致性要求
	if s.mustSync {
		err = s.SyncSegmentMsg()
		if err != nil {
			return 0, 0, 0, isNewSegment, err
		}
	}
	s.curOffset = endOffset

	// 记录写入索引,一定要在endoffset赋值curoffset之后
	err = s.setSegmentIdx()
	if err != nil {
		return 0, 0, 0, isNewSegment, err
	}
	return s.curIndex, startOffset, endOffset - startOffset, isNewSegment, nil
}

func (s *MsgSegment) SyncSegmentMsg() error {
	if s.curFile != nil {
		err := s.curFile.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}
