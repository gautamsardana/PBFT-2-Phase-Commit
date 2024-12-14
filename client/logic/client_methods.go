package logic

import (
	"context"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"
	"log"
	"math"
	"math/rand"
	"time"

	common "GolandProjects/2pcbyz-gautamsardana/api_common"
	"GolandProjects/2pcbyz-gautamsardana/client/config"
)

func PrintBalance(ctx context.Context, req *common.PrintBalanceRequest, conf *config.Config) (*common.PrintBalanceResponse, error) {
	userCluster := int32(math.Ceil(float64(req.User) / float64(conf.DataItemsPerShard)))
	result := map[int32]float32{}

	for _, serverNo := range conf.MapClusterToServers[userCluster] {
		server, err := conf.Pool.GetServer(mapServerNoToServerAddr[serverNo])
		if err != nil {
			return nil, err
		}

		resp, err := server.PrintBalance(context.Background(), req)
		if err != nil {
			return nil, err
		}

		for k, v := range resp.Balance {
			result[k] = v
		}
	}

	return &common.PrintBalanceResponse{Balance: result}, nil
}

func PrintDB(ctx context.Context, req *common.PrintDBRequest, conf *config.Config) (*common.PrintDBResponse, error) {
	serverAddr := mapServerNoToServerAddr[req.Server]
	server, err := conf.Pool.GetServer(serverAddr)
	if err != nil {
		return nil, err
	}
	resp, err := server.PrintDB(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func Performance(_ context.Context, conf *config.Config) (*common.PerformanceResponse, error) {
	var totalLatency time.Duration

	completedTxns := len(conf.LatencyQueue)
	for i := 0; i < completedTxns; i++ {
		totalLatency += conf.LatencyQueue[i]
	}

	var throughput float64
	if totalLatency > 0 {
		throughput = float64(completedTxns) / totalLatency.Seconds()
	}

	resp := &common.PerformanceResponse{
		TxnCount:   conf.TxnCount,
		Latency:    durationpb.New(totalLatency),
		Throughput: float32(throughput),
	}
	return resp, nil
}

func Benchmark(_ context.Context, conf *config.Config, req *common.BenchmarkRequest) (*common.PerformanceResponse, error) {
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < int(req.TxnNumber); i++ {
		sender := rand.Intn(1000) + 1
		receiver := rand.Intn(1000) + 1
		for receiver == sender {
			receiver = rand.Intn(1000) + 1
		}

		// amount 0 to 10
		amount := float32(rand.Intn(10) + 1)
		txnID, err := uuid.NewRandom()
		if err != nil {
			log.Fatalf("failed to generate UUID: %v", err)
		}

		txn := &common.TxnRequest{
			TxnID:    txnID.String(),
			Sender:   int32(sender),
			Receiver: int32(receiver),
			Amount:   amount,
		}

		senderCluster := math.Ceil(float64(txn.Sender) / float64(conf.DataItemsPerShard))
		ProcessTxn(conf, txn, int32(senderCluster), req.ContactServers)
		conf.TxnCount++
	}

	return Performance(nil, conf)
}
