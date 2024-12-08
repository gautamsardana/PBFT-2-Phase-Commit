package logic

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"log"
	"math"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/client/config"
)

func ProcessTxnSet(ctx context.Context, req *common.TxnSet, conf *config.Config) error {
	isServerAlive := map[int32]bool{
		1: false,
		2: false,
		3: false,
		4: false,
		5: false,
		6: false,
		7: false,
		8: false,
		9: false,
	}

	for _, aliveServer := range req.LiveServers {
		isServerAlive[mapServerToServerNo[aliveServer]] = true
	}

	updateServerStateReq := &common.UpdateServerStateRequest{
		Clusters: make(map[int32]*common.ClusterDistribution),
	}
	for key, values := range conf.MapClusterToServers {
		updateServerStateReq.Clusters[key] = &common.ClusterDistribution{
			Values: values,
		}
	}

	for cluster, servers := range conf.MapClusterToServers {
		for _, serverNo := range servers {
			server, err := conf.Pool.GetServer(mapServerNoToServerAddr[serverNo])
			if err != nil {
				fmt.Println(err)
			}
			serverReq := &common.UpdateServerStateRequest{
				Clusters:          updateServerStateReq.Clusters,
				ClusterNumber:     cluster,
				IsAlive:           isServerAlive[serverNo],
				DataItemsPerShard: conf.DataItemsPerShard,
			}
			server.UpdateServerState(ctx, serverReq)
		}
	}

	fmt.Println(req.Txns)

	for _, txn := range req.Txns {
		txnID, err := uuid.NewRandom()
		if err != nil {
			log.Fatalf("failed to generate UUID: %v", err)
		}
		txn.TxnID = txnID.String()

		fmt.Println("processing", txn, "\n")
		senderCluster := math.Ceil(float64(txn.Sender) / float64(conf.DataItemsPerShard))
		receiverCluster := math.Ceil(float64(txn.Receiver) / float64(conf.DataItemsPerShard))

		if senderCluster == receiverCluster {
			ProcessIntraShardTxn(conf, txn, int32(senderCluster), req.ContactServers)
		} else {
			ProcessCrossShardTxn(conf, txn, int32(senderCluster), int32(receiverCluster), req.ContactServers)
		}
	}
	return nil
}
