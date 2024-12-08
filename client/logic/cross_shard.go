package logic

import (
	"context"
	"fmt"
	"time"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/client/config"
)

func ProcessCrossShardTxn(conf *config.Config, txn *common.TxnRequest, senderCluster, receiverCluster int32, contactServers []string) {
	conf.TxnQueueLock.Lock()
	conf.TxnStartTime[txn.TxnID] = time.Now()
	conf.TxnQueueLock.Unlock()

	senderServer, err := conf.Pool.GetServer(GetContactServerForCluster(conf, senderCluster, contactServers))
	if err != nil {
		fmt.Println(err)
	}

	_, err = senderServer.EnqueueTxn(context.Background(), txn)
	if err != nil {
		fmt.Println(err)
	}

	receiverServer, err := conf.Pool.GetServer(GetContactServerForCluster(conf, receiverCluster, contactServers))
	if err != nil {
		fmt.Println(err)
	}

	_, err = receiverServer.EnqueueTxn(context.Background(), txn)
	if err != nil {
		fmt.Println(err)
	}
}

func HandleCommit(conf *config.Config, senderCluster, receiverCluster int32, txn *common.TxnRequest) {
	defer func() {
		conf.TxnQueueLock.Lock()
		conf.LatencyQueue = append(conf.LatencyQueue, time.Since(conf.TxnStartTime[txn.TxnID]))
		conf.TxnQueueLock.Unlock()
	}()

	for _, serverNo := range conf.MapClusterToServers[senderCluster] {
		senderServer, err := conf.Pool.GetServer(mapServerNoToServerAddr[serverNo])
		if err != nil {
			fmt.Printf("error getting sender server for cluster %d: %v", senderCluster, err)
		}

		senderCommitResp, senderErr := senderServer.TwoPCCommit(context.Background(), txn)
		if senderErr != nil {
			fmt.Printf("error committing transaction on sender server: %v\n", senderErr)
		} else {
			fmt.Println("Sender Commit Response:", senderCommitResp)
		}
	}

	for _, serverNo := range conf.MapClusterToServers[receiverCluster] {
		receiverServer, err := conf.Pool.GetServer(mapServerNoToServerAddr[serverNo])
		if err != nil {
			fmt.Printf("error getting receiver server for cluster %d: %v", senderCluster, err)
		}

		senderCommitResp, senderErr := receiverServer.TwoPCCommit(context.Background(), txn)
		if senderErr != nil {
			fmt.Printf("error committing transaction on sender server: %v\n", senderErr)
		} else {
			fmt.Println("Receiver Commit Response:", senderCommitResp)
		}
	}
}

func HandleAbort(conf *config.Config, senderCluster, receiverCluster int32, txn *common.TxnRequest) {
	defer func() {
		conf.TxnQueueLock.Lock()
		conf.LatencyQueue = append(conf.LatencyQueue, time.Since(conf.TxnStartTime[txn.TxnID]))
		conf.TxnQueueLock.Unlock()
	}()

	for _, serverNo := range conf.MapClusterToServers[senderCluster] {
		senderServer, err := conf.Pool.GetServer(mapServerNoToServerAddr[serverNo])
		if err != nil {
			fmt.Printf("error getting sender server for cluster %d: %v", senderCluster, err)
		}

		senderAbortResp, senderErr := senderServer.TwoPCAbort(context.Background(), txn)
		if senderErr != nil {
			fmt.Printf("error aborting transaction on sender server: %v\n", senderErr)
		} else {
			fmt.Println("Sender Abort Response:", senderAbortResp)
		}
	}

	for _, serverNo := range conf.MapClusterToServers[receiverCluster] {
		receiverServer, err := conf.Pool.GetServer(mapServerNoToServerAddr[serverNo])
		if err != nil {
			fmt.Printf("error getting receiver server for cluster %d: %v", senderCluster, err)
		}

		receiverAbortResp, receiverErr := receiverServer.TwoPCAbort(context.Background(), txn)
		if receiverErr != nil {
			fmt.Printf("error aborting transaction on receiver server: %v\n", receiverErr)
		} else {
			fmt.Println("Receiver Abort Response:", receiverAbortResp)
		}
	}
}
