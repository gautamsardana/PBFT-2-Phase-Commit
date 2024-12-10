package logic

import (
	"context"
	"fmt"
	"time"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/client/config"
)

func Callback(ctx context.Context, conf *config.Config, resp *common.ProcessTxnResponse) {
	fmt.Printf("received response for txn: %v\n", resp)

	conf.TxnQueueLock.Lock()
	conf.LatencyQueue = append(conf.LatencyQueue, time.Since(conf.TxnStartTime[resp.Txn.TxnID]))
	conf.TxnQueueLock.Unlock()

	return
}
