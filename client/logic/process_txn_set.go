package logic

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"log"
	"math"
	"time"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/client/config"
)

func ProcessTxnSet(ctx context.Context, req *common.TxnSet, conf *config.Config) error {
	isServerAlive := GetServerStateMap()
	isServerByzantine := GetServerStateMap()

	for _, aliveServer := range req.LiveServers {
		isServerAlive[mapServerToServerNo[aliveServer]] = true
	}

	for _, byzantineServer := range req.ByzantineServers {
		isServerByzantine[mapServerToServerNo[byzantineServer]] = true
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
				IsByzantine:       isServerByzantine[serverNo],
				DataItemsPerShard: conf.DataItemsPerShard,
			}
			server.UpdateServerState(ctx, serverReq)
		}
	}

	for _, txn := range req.Txns {
		txnID, err := uuid.NewRandom()
		if err != nil {
			log.Fatalf("failed to generate UUID: %v", err)
		}
		txn.TxnID = txnID.String()

		fmt.Println("processing", txn, "\n")
		senderCluster := math.Ceil(float64(txn.Sender) / float64(conf.DataItemsPerShard))

		conf.TxnCount++

		ProcessTxn(conf, txn, int32(senderCluster), req.ContactServers)
	}
	return nil
}

func ProcessTxn(conf *config.Config, txn *common.TxnRequest, cluster int32, contactServers []string) {
	conf.TxnQueueLock.Lock()
	conf.TxnStartTime[txn.TxnID] = time.Now()
	conf.TxnQueueLock.Unlock()

	server, err := conf.Pool.GetServer(GetContactServerForCluster(conf, cluster, contactServers))
	if err != nil {
		fmt.Println(err)
	}

	_, err = server.ProcessTxn(context.Background(), txn)
	if err != nil {
		fmt.Println(err)
	}
}

func GetContactServerForCluster(conf *config.Config, cluster int32, contactServers []string) string {
	for _, serverNo := range conf.MapClusterToServers[cluster] {
		for _, contactServer := range contactServers {
			if mapServerToServerNo[contactServer] == serverNo {
				return mapServerNoToServerAddr[serverNo]
			}
		}
	}
	return ""
}

func GetServerStateMap() map[int32]bool {
	return map[int32]bool{
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
}
