package main

import (
	"fmt"
	"github.com/feelingsray/Ray-MQ/db"
	"golang.org/x/exp/rand"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"time"
)

// https://github.com/ByteStorage/FlyDB/blob/master/db/data/log_record.go
// https://github.com/rosedblabs/mini-bitcask

const (
	Root string = "/Users/ray/jylink/Ray-MQ"
)

func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func main() {

	strData := randString(1024 * 1024 * 2)

	cpu, _ := os.Create("cpu.pprof")
	mem, _ := os.Create("mem.pprof")
	tr, _ := os.Create("trace.out")
	pprof.StartCPUProfile(cpu)
	trace.Start(tr)

	t := time.Now()
	topicList := make([]*db.MsgTopic, 0)
	staticTopic := &db.MsgTopic{
		Name:                "StaticData",
		Root:                Root,
		MaxSegmentIndex:     6500,
		MaxSegmentBytes:     1024 * 1024 * 128,
		IndexSyncDiskDuring: 50,
		MsgSyncDiskDuring:   10,
		MustSync:            false,
		MarkerCallback: func(segmentIndex uint16) error {
			fmt.Printf("MarkerCallback:StaticData,%d\n", segmentIndex)
			return nil
		},
	}
	topicList = append(topicList, staticTopic)
	realTopic := &db.MsgTopic{
		Name:            "RealData",
		Root:            Root,
		MaxSegmentIndex: 6500,
		MaxSegmentBytes: 1024 * 1024 * 128,
		MarkerCallback: func(segmentIndex uint16) error {
			fmt.Printf("MarkerCallback:RealData,%d\n", segmentIndex)
			return nil
		},
	}
	topicList = append(topicList, realTopic)

	writer, err := db.NewMsgWriter(topicList, func(meta *db.MsgMeta) error {
		//fmt.Printf("msg meta: %v\n", meta)
		return nil
	}, func(logMsg *db.LogMsg) {
		//fmt.Printf("msg logMsg: %v\n", logMsg)
	})

	if err != nil {
		fmt.Println("NewMsgWriter Error:", err)
	}

	go writer.Run()

	for i := 0; i < 1024; i++ {
		key := ""
		//val := fmt.Sprintf("val:%d,%d", i, time.Now().Unix())
		val := strData
		msgID1, err := writer.Write("StaticData", key, val)
		if err != nil {
			fmt.Println("Write StaticData Error:", err)
		}
		fmt.Println("Write StaticData:", msgID1)
		msgID2, err := writer.Write("RealData", key, val)
		if err != nil {
			fmt.Println("Write RealData Error:", err)
		}
		fmt.Println("Write RealData:", msgID2)
		//if i%2 == 0 {
		//	val := fmt.Sprintf("val:%d,%d", i, time.Now().Unix())
		//	writer.Write("RealData", key, val)
		//} else {
		//
		//}
	}
	writer.Destroy()

	fmt.Println("运行时间:", time.Since(t))

	trace.Stop()
	pprof.StopCPUProfile()
	pprof.WriteHeapProfile(mem)

	cpu.Close()
	mem.Close()
	tr.Close()

}
