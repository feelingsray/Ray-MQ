package mq_bak

import (
	"context"
	"sync"
)

type MsgReader struct {
	ctx  context.Context
	wg   *sync.WaitGroup
	root string
}

type MsgReaderChannel struct {
}
