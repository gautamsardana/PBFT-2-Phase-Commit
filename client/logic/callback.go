package logic

import (
	"context"
	"fmt"
	"math"
	"time"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/client/config"
)

func Callback(ctx context.Context, conf *config.Config, resp *common.ProcessTxnResponse) {
	if resp.Txn.Type == TypeIntraShard {
		fmt.Printf("received response for intra-shard txn: %v\n", resp)

		conf.TxnQueueLock.Lock()
		conf.LatencyQueue = append(conf.LatencyQueue, time.Since(conf.TxnStartTime[resp.Txn.TxnID]))
		conf.TxnQueueLock.Unlock()

		return
	}

	fmt.Printf("received response for çross-shard txn: %v\n", resp)

	conf.Lock.Lock()
	conf.TxnResponses[resp.Txn.TxnID] = append(conf.TxnResponses[resp.Txn.TxnID], resp)
	conf.Lock.Unlock()

	if len(conf.TxnResponses[resp.Txn.TxnID]) == 2 {
		senderCluster := math.Ceil(float64(resp.Txn.Sender) / float64(conf.DataItemsPerShard))
		receiverCluster := math.Ceil(float64(resp.Txn.Receiver) / float64(conf.DataItemsPerShard))

		for _, txnResponse := range conf.TxnResponses[resp.Txn.TxnID] {
			if txnResponse.Status == StatusFailed {
				fmt.Printf("txn %v failed, sending abort\n", resp.Txn.TxnID)

				HandleAbort(conf, int32(senderCluster), int32(receiverCluster), resp.Txn)
				return
			}
		}
		fmt.Printf("txn %v succeeded with status %v, sending final commit\n", resp.Txn.TxnID, resp.Status)
		HandleCommit(conf, int32(senderCluster), int32(receiverCluster), resp.Txn)
		return
	}
}
