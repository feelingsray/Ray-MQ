package main

import (
	"fmt"
	"github.com/feelingsray/Ray-MQ/mq_bak"
	"golang.org/x/exp/rand"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"sync"
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

	strData := randString(1024)
	_ = strData

	/***************************************/
	cpu, _ := os.Create("cpu.pprof")
	mem, _ := os.Create("mem.pprof")
	tr, _ := os.Create("trace.out")
	_ = pprof.StartCPUProfile(cpu)
	_ = trace.Start(tr)
	t := time.Now()
	/***************************************/

	rmq, err := mq_bak.NewRMQ(Root)
	if err != nil {
		fmt.Println(err)
	}

	topicCount := 1
	dataCount := 100000

	// 创建Topic
	for i := 0; i < topicCount; i++ {
		err = rmq.AppendSimpleTopic(fmt.Sprintf("T%d", i))
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	// 创建消费者
	consumer, err := rmq.AppendConsumer("abc")
	if err != nil {
		fmt.Println(err)
		return
	}
	consumer.AppendConsumerTopic("T1", mq_bak.SubNewestMsg)
	consumer.AppendConsumerTopic("T1", mq_bak.SubNewestMsg)

	go rmq.Start()

	wg := &sync.WaitGroup{}

	for i := 0; i < topicCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < dataCount; j++ {
				msgId, err := rmq.MsgWriter.Write(fmt.Sprintf("T%d", i), "", strData)
				if err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Println(fmt.Sprintf("T%d", i), msgId)
			}
		}()
	}
	wg.Wait()

	fmt.Println("Data运行时间:", time.Since(t))

	err = rmq.Destroy(false)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("销毁运行时间:", time.Since(t))
	/***************************************/
	trace.Stop()
	pprof.StopCPUProfile()
	_ = pprof.WriteHeapProfile(mem)
	_ = cpu.Close()
	_ = mem.Close()
	_ = tr.Close()
	/***************************************/

}
