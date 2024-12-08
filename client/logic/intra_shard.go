package logic

import (
	"context"
	"fmt"
	"time"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/client/config"
)

func ProcessIntraShardTxn(conf *config.Config, txn *common.TxnRequest, cluster int32, contactServers []string) {
	conf.TxnQueueLock.Lock()
	conf.TxnStartTime[txn.TxnID] = time.Now()
	conf.TxnQueueLock.Unlock()

	server, err := conf.Pool.GetServer(GetContactServerForCluster(conf, cluster, contactServers))
	if err != nil {
		fmt.Println(err)
	}

	_, err = server.EnqueueTxn(context.Background(), txn)
	if err != nil {
		fmt.Println(err)
	}
}
